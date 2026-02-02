"""Базовые модели SQLAlchemy."""

__all__ = ["Base"]


from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Базовая модель SQLAlchemy."""

    async def __admin_repr__(self, *_, **__) -> str:
        return f"[{self.__class__.__name__}]"

    async def __admin_select2_repr__(self, *_, **__) -> str:
        return f"<span>{self.__class__.__name__}</span>"
