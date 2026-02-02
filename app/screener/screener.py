__all__ = ["Screener"]

import asyncio

from app.config import logger
from app.database import Database
from app.models import SettingsDTO

from .consumer import Consumer
from .producer import Producer


class Screener:
    """Класс для управления процессами скринера."""

    _running: bool = False
    _tasks: list[asyncio.Task] = []
    _consumer: Consumer | None = None
    _producer: Producer | None = None

    @classmethod
    async def start(cls) -> None:
        """Запускает скринер."""
        if cls._running:
            return
        cls._running = True

        cls._tasks.append(cls._update_settings_cycle())
        cls._producer = Producer()
        settings = await cls._fetch_settings()
        cls._consumer = Consumer(cls._producer, settings)
        cls._tasks.append(asyncio.create_task(cls._consumer.start()))
        cls._tasks.append(asyncio.create_task(cls._producer.start()))
        await asyncio.gather(*cls._tasks)

    @classmethod
    async def stop(cls) -> None:
        """Останавливает скринер."""
        if cls._consumer:
            await cls._consumer.stop()
        if cls._producer:
            await cls._producer.stop()
        for task in cls._tasks:
            task.cancel()

    @classmethod
    def _update_settings_cycle(cls, update_interval: int = 10) -> asyncio.Task:
        """Запускает обновление настроек скринера."""

        async def _cycle():
            while True:
                try:
                    settings = await cls._fetch_settings()
                    if cls._consumer:
                        cls._consumer.update_settings(settings)
                except Exception as e:
                    logger.error(f"Error while fetching settings: {e}")
                await asyncio.sleep(update_interval)

        return asyncio.create_task(_cycle(), name="settings")

    @classmethod
    async def _fetch_settings(cls) -> SettingsDTO:
        """Возвращает настройки скринера."""
        async with Database.session_context() as db:
            settings = await db.settings_repo.get()
            if not settings:
                raise RuntimeError("Settings was not found")
            return SettingsDTO.model_validate(settings)
