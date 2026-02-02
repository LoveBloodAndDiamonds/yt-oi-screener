__all__ = ["SettingsRepository"]

from sqlalchemy.ext.asyncio import AsyncSession

from ..models import SettingsORM
from .abstract import Repository


class SettingsRepository(Repository[SettingsORM]):
    """Репозиторий для работы с настройками."""

    def __init__(self, session: AsyncSession):
        super().__init__(SettingsORM, session)

    async def get(self) -> SettingsORM | None:
        """Возвращает модель с id=1. Если не найдена, то создает `None`."""
        return await super().get(ident=1)

    async def create(self) -> SettingsORM:
        """Создает модель с id=1."""
        model = SettingsORM()
        self.session.add(model)
        return model
