SELECT
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    previous_close,
    price_change,
    daily_return_pct,
    moving_avg_7,
    moving_avg_20,
    rolling_volatility_20,
    volume_avg_20
FROM finance.daily_prices