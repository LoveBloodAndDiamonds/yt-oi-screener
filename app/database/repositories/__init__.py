"""Пакет репозиториев для работы с базой данных."""

__all__ = ["Repository", "SettingsRepository"]

from .abstract import Repository
from .settings import SettingsRepository
