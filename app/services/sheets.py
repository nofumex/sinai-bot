from __future__ import annotations

import asyncio
import logging
from functools import lru_cache
from urllib.parse import quote

import aiohttp
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import Settings, get_settings
from app.models import Lead, User

logger = logging.getLogger(__name__)

SCOPES = ("https://www.googleapis.com/auth/spreadsheets",)

HEADERS = [
    "№",
    "Дата создания",
    "Платформа",
    "Тип заявки",
    "Статус",
    "Имя клиента",
    "Username клиента",
    "Телефон клиента",
    "Рекомендатель",
    "Username рекомендателя",
    "Телефон рекомендателя",
    "Кем приходится рекомендателю",
]

HEADERS.extend(
    [
        "amo_lead_id",
        "amo_contact_id",
        "amo_pipeline_id",
        "amo_status_id",
        "amo_sync_status",
        "amo_sync_error",
        "amo_synced_at",
    ]
)

TYPE_LABELS = {
    "consultation": "Консультация",
    "question": "Вопрос специалисту",
    "agent_client": "Клиент от агента",
}

STATUS_LABELS = {
    "new": "Новая",
    "in_progress": "В работе",
    "closed": "Закрыта",
    "canceled": "Отменена",
}

SHEET_COLUMNS = "A:S"


def _is_configured(settings: Settings) -> bool:
    return bool(settings.google_sheets_id and settings.google_service_account_file)


def _sheet_range(settings: Settings, cells: str) -> str:
    sheet = settings.google_sheets_worksheet.replace("'", "''")
    return f"'{sheet}'!{cells}"


def _range_url(settings: Settings, cells: str, suffix: str = "") -> str:
    range_name = quote(_sheet_range(settings, cells), safe="")
    return f"https://sheets.googleapis.com/v4/spreadsheets/{settings.google_sheets_id}/values/{range_name}{suffix}"


@lru_cache(maxsize=1)
def _credentials(path: str):
    from google.oauth2 import service_account

    return service_account.Credentials.from_service_account_file(path, scopes=SCOPES)


async def _access_token(settings: Settings) -> str | None:
    try:
        from google.auth.transport.requests import Request
    except ImportError:
        logger.warning("google-auth is not installed; Google Sheets sync is disabled")
        return None

    credentials = _credentials(settings.google_service_account_file or "")
    if not credentials.valid:
        await asyncio.to_thread(credentials.refresh, Request())
    return credentials.token


def _date(value) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S") if value else ""


def _username(user: User | None) -> str:
    if not user:
        return ""
    if user.username:
        return f"@{user.username}"
    if user.platform == "max" and user.platform_user_id:
        return f"MAX ID {user.platform_user_id}"
    return ""


def _user_id(user: User | None) -> str:
    if not user:
        return ""
    return str(user.platform_user_id or user.telegram_id or "")


def _full_name(user: User | None) -> str:
    if not user:
        return ""
    return " ".join(part for part in [user.first_name, user.last_name] if part) or user.username or str(user.platform_user_id or user.telegram_id or "")


def _row(lead: Lead) -> list[str]:
    client = lead.user
    recommender = lead.agent or (client.referrer if client else None)
    return [
        str(lead.id),
        _date(lead.created_at),
        "MAX" if lead.platform == "max" else "Telegram",
        TYPE_LABELS.get(lead.type or "", lead.type or ""),
        STATUS_LABELS.get(lead.status or "", lead.status or ""),
        lead.client_name or _full_name(client),
        _username(client),
        lead.phone or (client.phone if client else "") or "",
        _full_name(recommender),
        _username(recommender),
        recommender.phone if recommender and recommender.phone else "",
        lead.relation_to_agent or "",
        str(lead.amo_lead_id or ""),
        str(lead.amo_contact_id or ""),
        str(lead.amo_pipeline_id or ""),
        str(lead.amo_status_id or ""),
        lead.amo_sync_status or "",
        lead.amo_sync_error or "",
        _date(lead.amo_synced_at),
    ]


def enqueue_lead_sync(lead_id: int) -> None:
    settings = get_settings()
    if not _is_configured(settings):
        return
    try:
        asyncio.get_running_loop().create_task(sync_lead_to_sheets_by_id(lead_id))
    except RuntimeError:
        logger.warning("No running event loop; skipped Google Sheets sync for lead %s", lead_id)


async def sync_lead_to_sheets_by_id(lead_id: int) -> None:
    from app.database import SessionLocal

    async with SessionLocal() as session:
        result = await session.execute(
            select(Lead)
            .where(Lead.id == lead_id)
            .options(
                selectinload(Lead.user).selectinload(User.referrer),
                selectinload(Lead.agent),
            )
        )
        lead = result.scalar_one_or_none()
        if lead:
            await sync_lead_to_sheets(session, lead)


async def sync_lead_to_sheets(session: AsyncSession, lead: Lead) -> None:
    settings = get_settings()
    if not _is_configured(settings):
        return

    try:
        await session.refresh(lead, ["user", "agent"])
        if lead.user:
            await session.refresh(lead.user, ["referrer"])
        token = await _access_token(settings)
        if not token:
            return
        headers = {"Authorization": f"Bearer {token}"}
        async with aiohttp.ClientSession(headers=headers) as client:
            await _ensure_sheet(client, settings)
            await _ensure_headers(client, settings)
            row_number = await _find_row_number(client, settings, lead.id)
            values = [_row(lead)]
            if row_number:
                url = _range_url(settings, f"A{row_number}:S{row_number}", "?valueInputOption=RAW")
                await _request(client, "PUT", url, json={"values": values})
            else:
                url = _range_url(settings, SHEET_COLUMNS, ":append?valueInputOption=RAW&insertDataOption=INSERT_ROWS")
                await _request(client, "POST", url, json={"values": values})
    except Exception:
        logger.exception("Failed to sync lead %s to Google Sheets", lead.id)


async def _ensure_headers(client: aiohttp.ClientSession, settings: Settings) -> None:
    url = _range_url(settings, "A1:S1")
    data = await _request(client, "GET", url)
    values = data.get("values") or []
    if values and values[0] == HEADERS:
        return
    await _request(client, "POST", _range_url(settings, "T:Z", ":clear"), json={})
    await _request(client, "PUT", url + "?valueInputOption=RAW", json={"values": [HEADERS]})


async def _ensure_sheet(client: aiohttp.ClientSession, settings: Settings) -> None:
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{settings.google_sheets_id}?fields=sheets.properties.title"
    data = await _request(client, "GET", url)
    titles = {item.get("properties", {}).get("title") for item in data.get("sheets", [])}
    if settings.google_sheets_worksheet in titles:
        return
    batch_url = f"https://sheets.googleapis.com/v4/spreadsheets/{settings.google_sheets_id}:batchUpdate"
    await _request(
        client,
        "POST",
        batch_url,
        json={"requests": [{"addSheet": {"properties": {"title": settings.google_sheets_worksheet}}}]},
    )


async def _find_row_number(client: aiohttp.ClientSession, settings: Settings, lead_id: int) -> int | None:
    data = await _request(client, "GET", _range_url(settings, "A:A"))
    for index, row in enumerate(data.get("values") or [], start=1):
        if row and row[0] == str(lead_id):
            return index
    return None


async def clear_sheet_rows() -> None:
    settings = get_settings()
    if not _is_configured(settings):
        return
    token = await _access_token(settings)
    if not token:
        return
    headers = {"Authorization": f"Bearer {token}"}
    async with aiohttp.ClientSession(headers=headers) as client:
        await _ensure_sheet(client, settings)
        await _request(client, "POST", _range_url(settings, "A:Z", ":clear"), json={})
        await _ensure_headers(client, settings)


async def _request(client: aiohttp.ClientSession, method: str, url: str, **kwargs) -> dict:
    async with client.request(method, url, **kwargs) as response:
        text = await response.text()
        if response.status >= 400:
            raise RuntimeError(f"Google Sheets API error {response.status}: {text[:500]}")
        if not text:
            return {}
        return await response.json()
