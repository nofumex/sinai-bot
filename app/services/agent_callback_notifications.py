from __future__ import annotations

import logging

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.adapters.max.client import MaxBotClient
from app.models import Lead
from app.services.amocrm import callback_phone_result_for_lead, mark_callback_phone_notified
from app.utils.agent_client_followup import call_phone_result_text

logger = logging.getLogger(__name__)


async def pending_callback_phone_leads(session: AsyncSession, platform: str, limit: int = 25) -> list[Lead]:
    result = await session.execute(
        select(Lead)
        .where(
            Lead.platform == platform,
            Lead.amo_callback_task_id.is_not(None),
            Lead.amo_callback_task_notified_at.is_(None),
        )
        .order_by(Lead.amo_callback_task_created_at.asc(), Lead.id.asc())
        .limit(limit)
        .options(selectinload(Lead.agent))
    )
    return list(result.scalars().all())


async def notify_telegram_callback_phone_results(bot: Bot, session: AsyncSession) -> None:
    for lead in await pending_callback_phone_leads(session, "telegram"):
        phone = await callback_phone_result_for_lead(session, lead)
        if not phone:
            continue
        agent = lead.agent
        if not agent or agent.telegram_id is None:
            continue
        try:
            await bot.send_message(agent.telegram_id, call_phone_result_text(phone))
        except TelegramAPIError:
            logger.exception("Failed to send callback phone result to Telegram agent %s", agent.telegram_id)
            continue
        await mark_callback_phone_notified(session, lead)


async def notify_max_callback_phone_results(client: MaxBotClient, session: AsyncSession) -> None:
    for lead in await pending_callback_phone_leads(session, "max"):
        phone = await callback_phone_result_for_lead(session, lead)
        if not phone:
            continue
        agent = lead.agent
        if not agent or not agent.platform_user_id:
            continue
        sent = await client.safe_send(user_id=agent.platform_user_id, text=call_phone_result_text(phone))
        if sent:
            await mark_callback_phone_notified(session, lead)
