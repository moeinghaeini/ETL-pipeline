-- Fact table for Bosch stock performance analysis
-- Aggregates daily stock data into performance metrics

with daily_data as (
    select
        symbol,
        trading_date,
        close_price,
        volume,
        daily_change_percent,
        volatility_20d,
        rsi_14,
        trend_direction,
        rsi_signal,
        volume_classification,
        ma_20,
        ma_50,
        bb_position
    from {{ ref('stg_bosch_stock_data') }}
),

-- Calculate performance metrics
performance_metrics as (
    select
        symbol,
        trading_date,
        close_price,
        volume,
        daily_change_percent,
        volatility_20d,
        rsi_14,
        trend_direction,
        rsi_signal,
        volume_classification,
        ma_20,
        ma_50,
        bb_position,
        -- Performance calculations
        close_price - lag(close_price, 1) over (
            partition by symbol 
            order by trading_date
        ) as price_change_1d,
        close_price - lag(close_price, 5) over (
            partition by symbol 
            order by trading_date
        ) as price_change_5d,
        close_price - lag(close_price, 20) over (
            partition by symbol 
            order by trading_date
        ) as price_change_20d,
        -- Percentage changes
        (close_price - lag(close_price, 1) over (
            partition by symbol 
            order by trading_date
        )) / lag(close_price, 1) over (
            partition by symbol 
            order by trading_date
        ) * 100 as price_change_1d_percent,
        (close_price - lag(close_price, 5) over (
            partition by symbol 
            order by trading_date
        )) / lag(close_price, 5) over (
            partition by symbol 
            order by trading_date
        ) * 100 as price_change_5d_percent,
        (close_price - lag(close_price, 20) over (
            partition by symbol 
            order by trading_date
        )) / lag(close_price, 20) over (
            partition by symbol 
            order by trading_date
        ) * 100 as price_change_20d_percent,
        -- Volume metrics
        volume - lag(volume, 1) over (
            partition by symbol 
            order by trading_date
        ) as volume_change_1d,
        avg(volume) over (
            partition by symbol 
            order by trading_date 
            rows between 19 preceding and current row
        ) as avg_volume_20d,
        -- Moving average relationships
        case 
            when close_price > ma_20 then 1 else 0
        end as above_ma_20,
        case 
            when close_price > ma_50 then 1 else 0
        end as above_ma_50,
        case 
            when ma_20 > ma_50 then 1 else 0
        end as ma_20_above_ma_50
    from daily_data
),

-- Add market performance indicators
with_market_indicators as (
    select
        *,
        -- Performance classification
        case 
            when price_change_1d_percent > 5 then 'strong_gain'
            when price_change_1d_percent > 2 then 'moderate_gain'
            when price_change_1d_percent > 0 then 'small_gain'
            when price_change_1d_percent > -2 then 'small_loss'
            when price_change_1d_percent > -5 then 'moderate_loss'
            else 'strong_loss'
        end as daily_performance,
        -- Trend strength
        case 
            when trend_direction = 'bullish' and above_ma_20 = 1 and above_ma_50 = 1 then 'strong_bullish'
            when trend_direction = 'bullish' then 'weak_bullish'
            when trend_direction = 'bearish' and above_ma_20 = 0 and above_ma_50 = 0 then 'strong_bearish'
            when trend_direction = 'bearish' then 'weak_bearish'
            else 'neutral'
        end as trend_strength,
        -- Risk level
        case 
            when volatility_20d > 0.03 and abs(price_change_1d_percent) > 3 then 'high_risk'
            when volatility_20d > 0.02 or abs(price_change_1d_percent) > 2 then 'medium_risk'
            else 'low_risk'
        end as risk_level,
        -- Volume significance
        case 
            when volume > avg_volume_20d * 2 then 'exceptional_volume'
            when volume > avg_volume_20d * 1.5 then 'high_volume'
            when volume < avg_volume_20d * 0.5 then 'low_volume'
            else 'normal_volume'
        end as volume_significance
    from performance_metrics
),

-- Add trading signals
with_trading_signals as (
    select
        *,
        -- Buy/Sell signals based on multiple indicators
        case 
            when trend_direction = 'bullish' 
                 and rsi_signal = 'oversold' 
                 and bb_position < 0.2 
                 and volume_significance in ('high_volume', 'exceptional_volume') then 'strong_buy'
            when trend_direction = 'bullish' 
                 and rsi_14 < 50 
                 and bb_position < 0.5 then 'buy'
            when trend_direction = 'bearish' 
                 and rsi_signal = 'overbought' 
                 and bb_position > 0.8 
                 and volume_significance in ('high_volume', 'exceptional_volume') then 'strong_sell'
            when trend_direction = 'bearish' 
                 and rsi_14 > 50 
                 and bb_position > 0.5 then 'sell'
            else 'hold'
        end as trading_signal,
        -- Confidence level
        case 
            when (trend_direction = 'bullish' and rsi_signal = 'oversold' and volume_significance = 'exceptional_volume') 
                 or (trend_direction = 'bearish' and rsi_signal = 'overbought' and volume_significance = 'exceptional_volume') then 'high'
            when (trend_direction in ('bullish', 'bearish') and volume_significance in ('high_volume', 'exceptional_volume')) then 'medium'
            else 'low'
        end as signal_confidence
    from with_market_indicators
)

select
    symbol,
    trading_date,
    close_price,
    volume,
    -- Price changes
    price_change_1d,
    price_change_5d,
    price_change_20d,
    price_change_1d_percent,
    price_change_5d_percent,
    price_change_20d_percent,
    -- Technical indicators
    volatility_20d,
    rsi_14,
    trend_direction,
    rsi_signal,
    volume_classification,
    ma_20,
    ma_50,
    bb_position,
    -- Performance metrics
    daily_performance,
    trend_strength,
    risk_level,
    volume_significance,
    -- Trading signals
    trading_signal,
    signal_confidence,
    -- Moving average relationships
    above_ma_20,
    above_ma_50,
    ma_20_above_ma_50,
    -- Volume metrics
    volume_change_1d,
    avg_volume_20d,
    -- Metadata
    current_timestamp() as processed_at
from with_trading_signals
order by symbol, trading_date
