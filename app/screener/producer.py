__all__ = ["Producer"]

import asyncio
import time
from collections import defaultdict
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from unicex import (
    Exchange,
    IUniClient,
    get_uni_client,
)
from unicex.types import OpenInterestDict, OpenInterestItem, TickerDailyDict

from app.config import config, logger


class Producer:
    """Продюсер данных для скринера."""

    _MAX_HISTORY_LEN = 60 * 15
    """Максимальная длина истории в секундах для всех парсеров."""

    _PARSE_INTERVAL = {Exchange.ASTER: 20}
    """Интервал парсинга данных в секундах."""

    _DEFAULT_PARSE_INTERVAL = 5
    """Интервал парсинга данных в секундах."""

    _CHUNK_SIZE = {Exchange.BINANCE: 20, Exchange.GATE: 7}
    """Кастомный размер чанка для одновременного запроса данных."""

    _DEFAULT_CHUNK_SIZE = 20
    """Размер чанка для одновременного запроса данных по умолчанию."""

    _CHUNK_INTERVAL = {}
    """Кастомный интервал между запросами данных в секундах."""

    _DEFAULT_CHUNK_INTERVAL = 0.33
    """Интервал между запросами данных в секундах по умолчанию."""

    _TICKER_DAILY_UPDATE_INTERVAL: int = 5
    """Интервал обновления данных о тикерах в секундах."""

    def __init__(self, exchange: Exchange = config.exchange) -> None:
        """Инициализирует парсера данных.

        Args:
            exchange (Exchange): На какой бирже парсить данные.
            market_type (MarketType): Тип рынка с которого парсить данные.
        """
        self._exchange = exchange
        self._is_running = True

        self._open_interest_lock = asyncio.Lock()
        self._ticker_daily_lock = asyncio.Lock()
        self._open_interest: dict[str, list[OpenInterestItem]] = defaultdict(list)
        self._ticker_daily: TickerDailyDict = {}

    async def start(self) -> None:
        """Запускает парсер данных."""
        logger.info(f"{self.repr} started")
        asyncio.create_task(self._update_ticker_daily())
        while self._is_running:
            try:
                start_time = time.perf_counter()
                async with self._client_context(logger=logger) as client:
                    snapshot = await self._fetch_open_interest_snapshot(client)
                    snapshot = await self._normalize_open_interest_snapshot(client, snapshot)
                async with self._open_interest_lock:
                    self._process_snapshot(snapshot)
                logger.debug(
                    f"{self.repr} data fetched, took {time.perf_counter() - start_time:.2f} s"
                )
            except TimeoutError:
                logger.error(f"{self.repr} timeout error occurred")
            except Exception as e:
                logger.exception(f"{self.repr} error: {e}")
            await self._safe_sleep(
                self._PARSE_INTERVAL.get(self._exchange, self._DEFAULT_PARSE_INTERVAL)
            )

    async def stop(self) -> None:
        """Останавливает парсер данных."""
        logger.info(f"{self.repr} stopped")
        self._is_running = False

    async def fetch_collected_data(self) -> dict[str, list[OpenInterestItem]]:
        """Возвращает накопленные данные. Возвращает ссылку на объект в котором хранятся данные."""
        async with self._open_interest_lock:
            return self._open_interest

    async def fetch_ticker_daily(self) -> TickerDailyDict:
        """Возвращает накопленные данные. Возвращает ссылку на объект в котором хранятся данные."""
        async with self._ticker_daily_lock:
            return self._ticker_daily

    async def _fetch_open_interest_snapshot(self, client: IUniClient) -> OpenInterestDict:
        """Получает текущие значения открытого интереса."""
        if self._exchange in {Exchange.BINANCE, Exchange.BINGX, Exchange.GATE}:
            return await self._fetch_open_interest_snapshot_batched(client)
        return await client.open_interest()

    async def _fetch_open_interest_snapshot_batched(self, client: IUniClient) -> OpenInterestDict:
        """Получает текущие значения открытого интереса итерируясь по каждому тикеру.
        Используется для бирж, на которых невозможно получить все данные одним запросом."""
        chunk_size = self._CHUNK_SIZE.get(self._exchange, self._DEFAULT_CHUNK_SIZE)
        chunk_interval = self._CHUNK_INTERVAL.get(self._exchange, self._DEFAULT_CHUNK_INTERVAL)
        chunked_tickers_list = await client.futures_tickers_batched(batch_size=chunk_size)

        results = {}
        for chunk in chunked_tickers_list:
            tasks = [client.open_interest(ticker) for ticker in chunk]
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            for ticker, response in zip(chunk, responses, strict=False):
                if isinstance(response, Exception):
                    logger.error(
                        f"{self.repr} failed to fetch open interest for {ticker}: {response}"
                    )
                    continue
                if not response:
                    logger.warning(f"{self.repr} empty open interest for {ticker}")
                    continue
                results[ticker] = response
            await asyncio.sleep(chunk_interval)

        return results

    async def _normalize_open_interest_snapshot(
        self, client: IUniClient, open_interest: OpenInterestDict
    ) -> OpenInterestDict:
        """Нормализует открытый интерес в случае, если его нужно дополнительно обработать."""
        if self._exchange in [Exchange.BINGX, Exchange.ASTER]:
            last_prices = await client.futures_last_price()

            for ticker, open_interest_item in open_interest.items():
                last_price = last_prices.get(ticker)
                if not last_price:
                    # logger.debug(f"{self.repr} missing last price for {ticker}, keep OI as-is")
                    continue

                try:
                    open_interest_value = open_interest_item["v"] / last_price
                except (KeyError, TypeError, ZeroDivisionError) as e:
                    logger.debug(
                        f"{self.repr} failed to normalize OI for {ticker} because {e}, keep OI as-is"
                    )
                    continue

                open_interest_item["v"] = open_interest_value

        return open_interest

    async def _update_ticker_daily(self) -> None:
        """В цикле обновляет суточную статистику тикеров."""
        while self._is_running:
            try:
                async with self._client_context() as client:
                    self._ticker_daily = await client.futures_ticker_24hr()
            except Exception as e:
                logger.error(f"{self.repr} error while updating ticker daily: {e}")
            await asyncio.sleep(self._TICKER_DAILY_UPDATE_INTERVAL)

    def _process_snapshot(
        self, open_interest: OpenInterestDict
    ) -> dict[str, list[OpenInterestItem]]:
        """Обрабатывает полученный снапшот открытого интереса и сохраняет его в локальное хранилище."""
        threshold: float = (time.time() - self._MAX_HISTORY_LEN) * 1000
        for ticker, open_interest_item in open_interest.items():
            self._open_interest[ticker] = [
                el for el in self._open_interest[ticker] if el["t"] >= threshold
            ]
            self._open_interest[ticker].append(open_interest_item)
        return self._open_interest

    async def _safe_sleep(self, seconds: int) -> None:
        """Безопасное ожидание, которое может прерваться в процессе."""
        for _ in range(seconds):
            if not self._is_running:
                return
            await asyncio.sleep(1)

    @asynccontextmanager
    async def _client_context(self, **kwargs: Any) -> AsyncIterator[IUniClient]:
        """Создаёт клиента unicex и гарантированно закрывает соединение по завершении контекста.

        Args:
            **kwargs (Any): Любые параметры, которые нужно передать в UniClient.create (например, logger).

        Yields:
            AsyncIterator[Any]: Инициализированный и готовый к работе клиент.
        """
        client = await get_uni_client(self._exchange).create(**kwargs)
        async with client:
            yield client

    @property
    def repr(self) -> str:
        """Возвращает строковое представление парсера."""
        return f"{self.__class__.__name__}"
