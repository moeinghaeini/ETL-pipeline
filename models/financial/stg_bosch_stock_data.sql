-- Staging model for Bosch stock data from yfinance
-- This model cleans and standardizes the raw stock data

with source_data as (
    select
        symbol,
        date as trading_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        adjusted_close,
        data_fetched_at,
        -- Calculate daily metrics
        close_price - open_price as daily_change,
        (close_price - open_price) / open_price as daily_change_percent,
        high_price - low_price as daily_range,
        (high_price - low_price) / open_price as daily_range_percent,
        -- Volume metrics
        volume / 1000000 as volume_millions,  -- Convert to millions
        -- Price levels
        case 
            when close_price > open_price then 'green'
            when close_price < open_price then 'red'
            else 'neutral'
        end as candle_color,
        -- Market session (assuming Indian market hours)
        case 
            when extract(hour from data_fetched_at) < 12 then 'morning'
            when extract(hour from data_fetched_at) < 16 then 'afternoon'
            else 'evening'
        end as market_session
    from {{ source('financial', 'bosch_stock_data') }}
    where symbol is not null
      and trading_date is not null
      and close_price > 0
),

-- Add technical indicators
with_technical_indicators as (
    select
        *,
        -- Moving averages
        avg(close_price) over (
            partition by symbol 
            order by trading_date 
            rows between 4 preceding and current row
        ) as ma_5,
        avg(close_price) over (
            partition by symbol 
            order by trading_date 
            rows between 19 preceding and current row
        ) as ma_20,
        avg(close_price) over (
            partition by symbol 
            order by trading_date 
            rows between 49 preceding and current row
        ) as ma_50,
        -- Volume moving average
        avg(volume) over (
            partition by symbol 
            order by trading_date 
            rows between 19 preceding and current row
        ) as volume_ma_20,
        -- Price volatility (standard deviation of daily returns)
        stddev(daily_change_percent) over (
            partition by symbol 
            order by trading_date 
            rows between 19 preceding and current row
        ) as volatility_20d,
        -- RSI calculation
        case 
            when avg(case when daily_change_percent > 0 then daily_change_percent else 0 end) over (
                partition by symbol 
                order by trading_date 
                rows between 13 preceding and current row
            ) = 0 then 0
            else 100 - (100 / (1 + (
                avg(case when daily_change_percent > 0 then daily_change_percent else 0 end) over (
                    partition by symbol 
                    order by trading_date 
                    rows between 13 preceding and current row
                ) / 
                avg(case when daily_change_percent < 0 then abs(daily_change_percent) else 0 end) over (
                    partition by symbol 
                    order by trading_date 
                    rows between 13 preceding and current row
                )
            )))
        end as rsi_14,
        -- Bollinger Bands
        avg(close_price) over (
            partition by symbol 
            order by trading_date 
            rows between 19 preceding and current row
        ) + (2 * stddev(close_price) over (
            partition by symbol 
            order by trading_date 
            rows between 19 preceding and current row
        )) as bb_upper,
        avg(close_price) over (
            partition by symbol 
            order by trading_date 
            rows between 19 preceding and current row
        ) - (2 * stddev(close_price) over (
            partition by symbol 
            order by trading_date 
            rows between 19 preceding and current row
        )) as bb_lower,
        avg(close_price) over (
            partition by symbol 
            order by trading_date 
            rows between 19 preceding and current row
        ) as bb_middle
    from source_data
),

-- Add derived metrics
final as (
    select
        *,
        -- Volume ratio
        case 
            when volume_ma_20 > 0 then volume / volume_ma_20
            else 1
        end as volume_ratio,
        -- Price position in Bollinger Bands
        case 
            when bb_upper > bb_lower then (close_price - bb_lower) / (bb_upper - bb_lower)
            else 0.5
        end as bb_position,
        -- Trend indicators
        case 
            when close_price > ma_20 and ma_20 > ma_50 then 'bullish'
            when close_price < ma_20 and ma_20 < ma_50 then 'bearish'
            else 'neutral'
        end as trend_direction,
        -- RSI signals
        case 
            when rsi_14 > 70 then 'overbought'
            when rsi_14 < 30 then 'oversold'
            else 'neutral'
        end as rsi_signal,
        -- Volatility classification
        case 
            when volatility_20d > 0.03 then 'high'
            when volatility_20d > 0.02 then 'medium'
            else 'low'
        end as volatility_level,
        -- Volume classification
        case 
            when volume_ratio > 2 then 'high'
            when volume_ratio > 1.5 then 'above_average'
            when volume_ratio < 0.5 then 'low'
            else 'normal'
        end as volume_classification
    from with_technical_indicators
)

select
    symbol,
    trading_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    adjusted_close,
    daily_change,
    daily_change_percent,
    daily_range,
    daily_range_percent,
    volume_millions,
    candle_color,
    market_session,
    -- Technical indicators
    ma_5,
    ma_20,
    ma_50,
    volume_ma_20,
    volatility_20d,
    rsi_14,
    bb_upper,
    bb_lower,
    bb_middle,
    -- Derived metrics
    volume_ratio,
    bb_position,
    trend_direction,
    rsi_signal,
    volatility_level,
    volume_classification,
    -- Metadata
    data_fetched_at,
    current_timestamp() as processed_at
from final
order by symbol, trading_date
