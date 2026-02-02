__all__ = ["create_text"]


from unicex import Exchange, MarketType
from unicex.extra import generate_ex_link, make_humanreadable


def create_text(
    symbol: str,
    change_pct: float,
    exchange: Exchange,
    market_type: MarketType,
    daily_price: float,
    daily_volume: float,
) -> str:
    """Формирует красивый текст сигнала о резком изменении открытого интереса. Готовый текст для отправки пользователю."""
    # Ссылка на биржу для быстрого перехода к инструменту
    ex_link = generate_ex_link(exchange, market_type, symbol)

    direction_emoji = "🚀" if change_pct >= 0 else "🔻"

    # Основной заголовок сигнала
    header = f"{direction_emoji} Резкий рост ОИ: {symbol}"

    # Читаемая часть с цифрами
    body = (
        f"Открытый интерес вырос на {change_pct:.2f}%\n"
        f"Изменение цены за день: {daily_price:.2f}%\n"
        f"Объем за день: {make_humanreadable(daily_volume, locale='ru')} $"
    )

    # Призыв к действию и ссылка
    footer = f"{ex_link}"

    return f"{header}\n\n{body}\n\n{footer}"
