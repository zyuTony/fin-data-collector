create or replace temporary view rsi as 
with price_changes as (
    select 
        date, close,
        lag(close) over (order by date) as prev_close,
        close - lag(close) over (order by date) as price_change
    from stock_historical_price
    where symbol = 'DKS'
),
rsi_calculation as (
    select date, close, price_change,
        case when price_change > 0 then price_change else 0 end as gain,
        case when price_change < 0 then abs(price_change) else 0 end as loss
    from price_changes
),
avg_gains_losses as (
    select date, close, price_change,
        avg(gain) over (order by date rows between 13 preceding and current row) as avg_gain,
        avg(loss) over (order by date rows between 13 preceding and current row) as avg_loss
    from rsi_calculation
)
select date,
    100 - (100 / (1 + avg_gain / nullif(avg_loss, 0))) as rsi
from avg_gains_losses;

with spy_sma as (
    select date,
        avg(close) over (order by date rows between 49 preceding and current row) as sma_50,
        avg(close) over (order by date rows between 199 preceding and current row) as sma_200
    from stock_historical_price
    where symbol = 'XLY'
),
macd as (
    select date, close,
        exp(sum(log(close)) over (order by date rows between 11 preceding and current row) / 12) as ema_12,
        exp(sum(log(close)) over (order by date rows between 25 preceding and current row) / 26) as ema_26
    from stock_historical_price
    where symbol = 'DKS'
),
vwap as (
    select date, close, volume,
        sum(close * volume) over (order by date rows between unbounded preceding and current row) / 
        nullif(sum(volume) over (order by date rows between unbounded preceding and current row), 0) as vwap
    from stock_historical_price
    where symbol = 'DKS'
),
indicators as (
    select a.*, r.rsi, v.vwap, m.ema_12, m.ema_26,
        s.sma_50 as spy_sma_50, s.sma_200 as spy_sma_200,
        avg(a.close) over (order by a.date rows between 49 preceding and current row) as stock_sma_50,
        avg(a.close) over (order by a.date rows between 199 preceding and current row) as stock_sma_200,
        ((a.open - lag(a.close) over (order by a.date)) / nullif(lag(a.close) over (order by a.date), 0)) * 100 as overnight_change_percent,
        ((a.close - a.open) / nullif(a.open, 0)) * 100 as day_change_percent,
        ((lead(a.close, 3) over (order by a.date) - a.open) / nullif(a.open, 0)) * 100 as change_3d_percent,
        ((lead(a.close, 5) over (order by a.date) - a.open) / nullif(a.open, 0)) * 100 as change_5d_percent,
        ((lead(a.close, 7) over (order by a.date) - a.open) / nullif(a.open, 0)) * 100 as change_7d_percent
    from stock_historical_price a
    join spy_sma s on a.date = s.date
    join macd m on a.date = m.date
    join vwap v on a.date = v.date
    join rsi r on a.date = r.date
    where a.symbol = 'DKS'
)

select 
    case 
        when overnight_change_percent <= -10 then 'Group 1: <= -10%'
        when overnight_change_percent > -10 and overnight_change_percent <= -7 then 'Group 2: -10% < x <= -7%'
        when overnight_change_percent > -7 and overnight_change_percent <= -5 then 'Group 3: -7% < x <= -5%'
        when overnight_change_percent > -5 and overnight_change_percent <= -3 then 'Group 4: -5% < x <= -3%'
        when overnight_change_percent > -3 and overnight_change_percent <= -1 then 'Group 5: -3% < x <= -1%'
        when overnight_change_percent > -1 and overnight_change_percent <= 0 then 'Group 6: -1% < x <= 0%'
        when overnight_change_percent > 0 and overnight_change_percent <= 1 then 'Group 7: 0% < x <= 1%'
        when overnight_change_percent > 1 and overnight_change_percent <= 3 then 'Group 8: 1% < x <= 3%'
        when overnight_change_percent > 3 and overnight_change_percent <= 5 then 'Group 9: 3% < x <= 5%'
        when overnight_change_percent > 5 and overnight_change_percent <= 7 then 'Group 10: 5% < x <= 7%'
        when overnight_change_percent > 7 and overnight_change_percent <= 10 then 'Group 11: 7% < x <= 10%'
        else 'Group 12: > 10%'
    end as overnight_change_group,
    case 
        when rsi >= 70 then 1 
        when rsi >= 30 then -1 
        else 0 
    end as rsi_signal,
    case when ema_12 >= ema_26 then 1 else 0 end as macd_signal,
    case when close >= vwap then 1 else 0 end as above_vwap,
    count(*) as num_days,
    round(sum(day_change_percent) / count(distinct date), 1) as avg_day_change_percent,
    percentile_cont(0.25) within group (order by day_change_percent) as q1_day_change_percent,
    percentile_cont(0.5) within group (order by day_change_percent) as median_day_change_percent,
    percentile_cont(0.75) within group (order by day_change_percent) as q3_day_change_percent,
    round(sum(change_3d_percent) / count(distinct date), 1) as avg_change_3d_percent,
    percentile_cont(0.25) within group (order by change_3d_percent) as q1_change_3d_percent,
    percentile_cont(0.5) within group (order by change_3d_percent) as median_change_3d_percent,
    percentile_cont(0.75) within group (order by change_3d_percent) as q3_change_3d_percent,
    round(sum(change_5d_percent) / count(distinct date), 1) as avg_change_5d_percent,
    percentile_cont(0.25) within group (order by change_5d_percent) as q1_change_5d_percent,
    percentile_cont(0.5) within group (order by change_5d_percent) as median_change_5d_percent,
    percentile_cont(0.75) within group (order by change_5d_percent) as q3_change_5d_percent,
    round(sum(change_7d_percent) / count(distinct date), 1) as avg_change_7d_percent,
    percentile_cont(0.25) within group (order by change_7d_percent) as q1_change_7d_percent,
    percentile_cont(0.5) within group (order by change_7d_percent) as median_change_7d_percent,
    percentile_cont(0.75) within group (order by change_7d_percent) as q3_change_7d_percent
from indicators
where abs(overnight_change_percent) <= 3  
    and date >= '2021-08-20'
group by overnight_change_group, rsi_signal, macd_signal, above_vwap
order by rsi_signal, macd_signal, above_vwap, overnight_change_group
