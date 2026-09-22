SELECT
    symbol,
    date,
    close,
    daily_return_pct,
    moving_avg_7,
    moving_avg_20,
    rolling_volatility_20,
    volume,
    volume_avg_20,
    price_change
FROM {{ ref('fct_ibm_daily') }}