__all__ = ["TelegramBot"]

import aiohttp

from app.config import logger


class TelegramBot:
    """Telegram Bot class"""

    def __init__(self, session: aiohttp.ClientSession | None = None):
        self._session = session or aiohttp.ClientSession()

    async def send_message(self, bot_token: str, chat_id: int, text: str) -> dict:
        try:
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            data = {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            async with self._session.post(url, json=data) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return {"error": str(e)}

    async def close(self) -> None:
        await self._session.close()
