__all__ = ["SettingsORM"]

from sqlalchemy import BigInteger
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class SettingsORM(Base):
    """Настройки скринера."""

    __tablename__ = "settings"

    id: Mapped[int] = mapped_column(primary_key=True, default=1)  # singlethon
    """Айди настроек. По умолчанию 1."""

    interval: Mapped[int] = mapped_column(default=60)
    """Интервал для измерения роста открытого интереса в секундах."""

    min_growth_prct: Mapped[float | None] = mapped_column(default=5)
    """Минимальный процент роста открытого интереса."""

    timeout: Mapped[int] = mapped_column(default=60)
    """Таймаут между сигналами по одинаковой монете в секундах."""

    chat_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    """Айди телеграм чата для отправки уведомлений."""

    bot_token: Mapped[str | None] = mapped_column(nullable=True)
    """Токен телеграм бота для отправки уведомлений."""
