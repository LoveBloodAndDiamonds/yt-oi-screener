__all__ = ["SettingsDTO"]

from pydantic import BaseModel


class SettingsDTO(BaseModel):
    """Модель настроек для передачи данных между слоями приложения."""

    model_config = {"from_attributes": True}

    id: int
    interval: int
    min_growth_prct: float
    timeout: int
    chat_id: int | None
    bot_token: str | None

    @property
    def is_ready(self) -> bool:
        return (
            self.interval > 0
            and self.min_growth_prct > 0
            and self.timeout > 0
            and self.chat_id is not None
            and self.bot_token is not None
        )
