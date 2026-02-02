__all__ = ["Consumer"]

import asyncio
import time

from unicex import Exchange, MarketType, OpenInterestItem
from unicex.extra import TimeoutTracker
from unicex.types import TickerDailyItem

from app.config import config, logger
from app.models import SettingsDTO
from app.utils import TelegramBot, create_text

from .producer import Producer


class Consumer:
    """Обработчик данных для скринера."""

    _PARSE_INTERVAL: int = 1
    """Интервал проверки данных."""

    def __init__(
        self,
        producer: Producer,
        settings: SettingsDTO,
        exchange: Exchange = config.exchange,
        market_type: MarketType = config.market_type,
    ) -> None:
        self._producer = producer
        self._settings = settings
        self._exchange = exchange
        self._market_type = market_type
        self._telegram_bot = TelegramBot()
        self._timeout_tracker = TimeoutTracker[str]()
        self._running = True

    def update_settings(self, settings: SettingsDTO) -> None:
        """Обновляет настройки скринера."""
        self._settings = settings

    async def start(self) -> None:
        """Запускает обработку данных."""
        logger.info("Starting consumer...")
        while self._running:
            try:
                if not self._settings.is_ready:
                    continue
                await self._process()
            except Exception as e:
                logger.error(f"Error processing data: {e}")
            finally:
                await asyncio.sleep(self._PARSE_INTERVAL)

    async def stop(self) -> None:
        """Останавливает обработку данных."""
        logger.info("Stopping consumer...")
        self._running = False
        await self._telegram_bot.close()

    async def _process(self) -> None:
        """Обрабатывает данные."""
        all_klines = await self._producer.fetch_collected_data()
        all_ticker_daily = await self._producer.fetch_ticker_daily()

        tasks = []
        for symbol, klines in all_klines.items():
            if self._timeout_tracker.is_blocked(symbol):
                continue

            ticker_daily = all_ticker_daily.get(symbol)
            if not ticker_daily:
                logger.warning(f"Ticker daily data not found for symbol {symbol}")
                continue

            task = await self._process_symbol(symbol, klines, ticker_daily)
            if task:
                self._timeout_tracker.block(symbol, self._settings.timeout)
                tasks.append(task)

        if tasks:
            await asyncio.gather(*tasks)
            logger.info(f"Sended {len(tasks)} signals!")

    async def _process_symbol(
        self,
        symbol: str,
        open_interest: list[OpenInterestItem],
        ticker_daily: TickerDailyItem,
    ) -> asyncio.Task | None:
        """Обрабатывает данные по тикеру."""
        change_pct = self._calculate_open_interest(open_interest)

        if change_pct and change_pct > self._settings.min_growth_prct:
            logger.success(f"{symbol}: {change_pct}x, {ticker_daily}")
            return asyncio.create_task(
                self._telegram_bot.send_message(
                    bot_token=self._settings.bot_token,  # type: ignore
                    chat_id=self._settings.chat_id,  # type: ignore
                    text=create_text(
                        symbol,
                        change_pct,
                        self._exchange,
                        self._market_type,
                        ticker_daily["p"],
                        ticker_daily["q"],
                    ),
                )
            )

    def _calculate_open_interest(self, open_interest: list[OpenInterestItem]) -> float | None:
        """Считает рост открытого интереса в процентах."""
        # Find min and max open interest values
        _min = float("inf")
        _max = float("-inf")

        found = False
        time_threshold = (time.time() - self._settings.interval) * 1000
        for el in open_interest:
            # Check if data is stale
            if el["t"] < time_threshold:
                continue

            # Find lowest open interest
            if el["v"] < _min:
                found = True
                _min = el["v"]
                _max = el["v"]  # Restore max open interest

            # Find highest open interest
            if el["v"] > _max:
                _max = el["v"]

        # Check if data was found
        if not found:
            # self._logger.debug(f"{self} No data found for {symbol}")
            return None

        # Find percentage change
        try:
            return (_max - _min) / _min * 100
        except ZeroDivisionError:
            # logger.debug(f"Zero division error for {symbol}")
            return None
