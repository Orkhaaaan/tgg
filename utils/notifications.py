"""
Bildiriş sistemi - Telegram vasitəsilə bildirişlər göndərir
"""
import logging
from typing import Optional
from datetime import datetime
from aiogram import Bot

logger = logging.getLogger(__name__)


async def send_telegram_notification(bot: Bot, chat_id: int, message: str) -> bool:
    """Telegram vasitəsilə bildiriş göndərir"""
    try:
        await bot.send_message(chat_id=chat_id, text=message)
        return True
    except Exception as e:
        logger.error(f"Telegram bildirişi göndərilmədi {chat_id}: {e}")
        return False


async def notify_call_center(bot: Bot, admin_id: int, message: str, user_phone: Optional[str] = None) -> None:
    """Çağrı mərkəzinə bildiriş göndərir (admin-ə Telegram)"""
    # Admin-ə Telegram bildirişi
    await send_telegram_notification(bot, admin_id, message)


async def notify_registration_complete(bot: Bot, admin_id: int, user_name: str, user_phone: str, user_fin: str, code: str) -> None:
    """Qeydiyyat tamamlandıqda admin-ə bildiriş"""
    message = (
        f"✅ Yeni qeydiyyat:\n\n"
        f"👤 Ad: {user_name}\n"
        f"🆔 FIN: {user_fin}\n"
        f"📞 Telefon: {user_phone}\n"
        f"📋 Kod: {code}\n"
        f"📅 Tarix: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    if admin_id != 0:
        await send_telegram_notification(bot, admin_id, message)


async def notify_rule_violation(
    bot: Bot, 
    admin_id: int, 
    user_id: int,
    user_name: str, 
    user_phone: Optional[str],
    violation_type: str,
    details: str
) -> None:
    """Qayda pozuntusu zamanı bildiriş göndərir"""
    message = (
        f"⚠️ Qayda pozuntusu:\n\n"
        f"👤 İstifadəçi: {user_name} (ID: {user_id})\n"
        f"📞 Telefon: {user_phone or 'Yoxdur'}\n"
        f"🔴 Pozuntunun növü: {violation_type}\n"
        f"📝 Detallar: {details}"
    )
    
    # İstifadəçiyə bildiriş
    try:
        await send_telegram_notification(bot, user_id, 
            f"⚠️ Xəbərdarlıq\n\n{violation_type}\n\n{details}")
    except Exception:
        pass
    
    # Admin və çağrı mərkəzinə bildiriş
    if admin_id != 0:
        await notify_call_center(bot, admin_id, message, user_phone)

