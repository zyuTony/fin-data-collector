create or replace temporary view ta_indicators2 as  
with btc_prices as (
    select symbol, date, close 
    from coin_historical_price
    where symbol = 'BTCUSDT'
),
in_btc_prices as (
select a.symbol, a.date, a.close/b.close as price_in_btc
from coin_historical_price a 
join btc_prices b 
on a.date=b.date
), 
add_ta as (
select symbol, date, price_in_btc,
avg(price_in_btc) over (partition by symbol order by date rows between 139 preceding and current row) as sma_140d
from in_btc_prices
order by symbol, date
),
eval_ta as (
select *,
max(date) over(partition by symbol) - min(date) over(partition by symbol) as data_points,
case when price_in_btc >= sma_140d then 1 else 0 end as above_140d_sma 
from add_ta
where date between '2023-10-19' and '2024-08-01'	-- UPDATE DATE 
),
ta_result as (
select symbol, data_points,
(100*sum(above_140d_sma)::float / count(*)::float) as pct_above_140d_sma
from eval_ta
group by symbol,data_points)

select *
from ta_result 
order by pct_above_140d_sma desc;

with symbol_date_range as (
    select symbol, date, close, volume, 
		   row_number() over(partition by symbol ORDER by date) as date_index, 
           min(date) over(partition by symbol) as start_date, 
           max(date) over(partition by symbol) as end_date,
           min(close) over(partition by symbol) as cycle_low, 
           max(close) over(partition by symbol) as cycle_high
    from coin_historical_price
    where date between '2023-05-01' and '2024-08-01'	-- UPDATE DATE 
),
symbol_prices as (
    select a.*, 
           b.close as init_price, 
           c.close as end_price,
           (a.close) / b.close as rolling_roi,
           btc_start.close as btc_init_price,
           btc_end.close as btc_end_price
    from symbol_date_range a 
    join coin_historical_price b 
    on a.symbol = b.symbol and a.start_date = b.date
    join coin_historical_price c 
    on a.symbol = c.symbol and a.end_date = c.date
    join coin_historical_price btc_start
    on btc_start.symbol = 'BTCUSDT' and a.start_date = btc_start.date
    join coin_historical_price btc_end
    on btc_end.symbol = 'BTCUSDT' and a.end_date = btc_end.date
),
peak_final_roi as (-- coins performances
    select distinct 
           symbol as symbol, 
           round(init_price, 2) as initial_price, 
           round(end_price, 2) as ending_price,
           round(end_price / init_price, 2) as final_roi, 
		   round(btc_end_price / btc_init_price, 2) as btc_final_roi, 
           dense_rank() over (order by (end_price / init_price) desc) as roi_rank,
           round(cycle_high / cycle_low, 2) as peak_roi, 
           dense_rank() over (order by (cycle_high / cycle_low) desc) as peak_roi_rank
    from symbol_prices
    order by final_roi desc
),
btc_prices as (
    select symbol, date, close 
    from coin_historical_price
    where symbol = 'BTCUSDT'
),
rolling_roi as (
    select a.*, 
           b.close as btc_relative_start_price,
           c.close as btc_relative_curr_price
    from symbol_prices a 
    left join btc_prices b
    on a.start_date = b.date
    left join btc_prices c
    on a.date = c.date
),
rolling_roi_t1 as (
select symbol, date, date_index, start_date, end_date, 
init_price, close, rolling_roi as alts_rolling_roi, 
btc_relative_start_price, 
btc_relative_curr_price, 
btc_relative_curr_price/btc_relative_start_price as btc_relative_rolling_roi
from rolling_roi
),
pct_below_btc_gain as (
select symbol, start_date, end_date, count(*) as total_count,
sum(case when alts_rolling_roi/btc_relative_rolling_roi > 1 then 1 else 0 end) as above_one_count,
round((sum(case when alts_rolling_roi/btc_relative_rolling_roi > 1 then 1 else 0 end) * 100.0 / count(*)),0) as pct_above_one 
from rolling_roi_t1 
group by symbol, start_date, end_date)

select a.*, round(final_roi/btc_final_roi,1) as final_relative_roi,
b.pct_above_one, c.pct_above_140d_sma, c.data_points
from peak_final_roi a 
join pct_below_btc_gain b 
on a.symbol=b.symbol
join ta_indicators2 c
on a.symbol=c.symbol
order by pct_above_140d_sma desc 


-- select a.symbol, date_index, alts_rolling_roi/btc_relative_rolling_roi as rolling_relative_roi, round(final_roi/btc_final_roi,1) as final_relative_roi
-- from rolling_roi_t1 a 
-- join peak_final_roi b
-- on a.symbol=b.symbol
-- where date_index in (1,30,60,90,120,150,180,210,240,270,300,330,360,390,420,450,480,510,540,570,600,630,660,690,720)
-- -- WHERE date IN (
-- -- '2022-11-01', '2022-12-01',
-- -- '2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01', 
-- -- '2023-05-01', '2023-06-01', '2023-07-01', '2023-08-01', 
-- -- '2023-09-01', '2023-10-01', '2023-11-01', '2023-12-01',
-- -- '2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', 
-- -- '2024-05-01', '2024-06-01', '2024-07-01', '2024-08-01')
-- order by final_roi/btc_final_roi desc, symbol, date;












