"""Точка входа FastAPI-приложения."""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from unicex import start_exchanges_info

from .admin import register_admin_routes
from .config import config, logger
from .database import Database
from .schemas import EnvironmentType
from .screener import Screener


async def _create_settings_if_not_exists() -> None:
    """Создает настройки, если они не существуют."""
    async with Database.session_context() as db:
        settings = await db.settings_repo.get()
        if not settings:
            await db.settings_repo.create()
            await db.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения."""
    # Launch logs
    logger.info(f"Admin panel startup! Environment: {config.environment}")
    if config.environment == EnvironmentType.DEVELOPMENT:
        logger.debug("Admin panel url: http://127.0.0.1:8000/admin")
    else:
        logger.info("App started!")

    # Create settings if not exists
    await _create_settings_if_not_exists()

    # Register admin routes
    register_admin_routes(app)

    # Start exchanges info
    await start_exchanges_info()

    # Start screener
    screener = Screener()
    asyncio.create_task(screener.start())

    # Give control to FastAPI
    yield

    # Stop screener
    await screener.stop()

    # Shutdown logs
    logger.info("Admin panel shutdown!")


# Main FastAPI object
app = FastAPI(
    lifespan=lifespan,
    **{
        "docs_url": None,
        "redoc_url": None,
        "openapi_url": None,
    }
    if config.environment == EnvironmentType.PRODUCTION
    else {},  # type: ignore
)
