create or replace temporary view ta_indicators as  
with btc_prices as (
    select symbol, date, close 
    from cg_coin_hist_price
    where symbol = 'BTC'
),
t1 as (
select a.symbol, a.date, a.close as price, a.close/b.close as price_in_btc
from cg_coin_hist_price a 
join btc_prices b 
on a.date=b.date
), 
add_ta as (
select symbol, date, price, price_in_btc,
avg(price_in_btc) over (partition by symbol order by date rows between 139 preceding and current row) as to_btc_sma_140d,
avg(price) over (partition by symbol order by date rows between 139 preceding and current row) as sma_140d
from t1
order by symbol, date
),
eval_ta as (
select *,
DATE_PART('day', MAX(date) OVER (PARTITION BY symbol) - MIN(date) OVER (PARTITION BY symbol)) as data_points,
case when price >= sma_140d then 1 else 0 end as above_140d_sma,
case when price_in_btc >= to_btc_sma_140d then 1 else 0 end as to_btc_above_140d_sma
from add_ta
where date between '2019-02-01' and '2019-07-15'
),
ta_result as (
select symbol, 
MIN(date::date) as start_date,
MAX(date::date) as end_date, 
data_points,
(100*sum(to_btc_above_140d_sma)::float / count(*)::float) as pct_above_140d_sma_to_btc,
(100*sum(above_140d_sma)::float / count(*)::float) as pct_above_140d_sma
from eval_ta
group by symbol,data_points)

select *
from ta_result 
order by pct_above_140d_sma_to_btc desc;

-- 2018 cycle 
-- start: '2019-02-01'
-- mid peak: '2019-07-15'
-- mid bottom: '2020-03-16'
-- first end: '2021-05-10'
-- end: '2021-11-08'
create or replace temporary view first_half_coin_performance as 
with symbol_date_range as (
    select symbol, date, close, 
           min(date) over(partition by symbol) as start_date, 
           max(date) over(partition by symbol) as end_date,
           min(close) over(partition by symbol) as cycle_low, 
           max(close) over(partition by symbol) as cycle_high
    from cg_coin_hist_price
    where date between '2019-02-01' and '2019-07-15'
),
symbol_prices as (
    select a.*, 
           b.close as init_price, 
           c.close as end_price,
           (a.close) / b.close as rolling_roi,
           btc_start.close as btc_init_price,
           btc_end.close as btc_end_price
    from symbol_date_range a 
    join cg_coin_hist_price b 
    on a.symbol = b.symbol and a.start_date = b.date
    join cg_coin_hist_price c 
    on a.symbol = c.symbol and a.end_date = c.date
    join cg_coin_hist_price btc_start
    on btc_start.symbol = 'BTC' and a.start_date = btc_start.date
    join cg_coin_hist_price btc_end
    on btc_end.symbol = 'BTC' and a.end_date = btc_end.date
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
    from cg_coin_hist_price
    where symbol = 'BTC'
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
select symbol, date,  start_date, end_date, 
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

select a.symbol, 
start_date, end_date, 
pct_above_one as pct_days_above_btc_roi, 
initial_price, ending_price, 
dense_rank() over(order by final_roi/btc_final_roi desc) as relative_final_roi_rank,dense_rank() over(order by pct_above_one desc) as pct_above_rank,
roi_rank, 
final_roi, btc_final_roi,
final_roi/btc_final_roi as relative_final_roi

from pct_below_btc_gain a 
join peak_final_roi b 
on a.symbol=b.symbol
order by relative_final_roi_rank, final_roi desc;


-- what else matter: PREVIOUS ALL TIME HIGH; 
create or replace temporary view second_half_coin_performance as 
with symbol_date_range as (
    select symbol, date, close,
           min(date) over(partition by symbol) as start_date, 
           max(date) over(partition by symbol) as end_date,
           min(close) over(partition by symbol) as cycle_low, 
           max(close) over(partition by symbol) as cycle_high
    from cg_coin_hist_price
    where date between '2020-03-16' and '2021-05-10'
),
symbol_prices as (
    select a.*, 
           b.close as init_price, 
           c.close as end_price,
           (a.close) / b.close as rolling_roi,
           btc_start.close as btc_init_price,
           btc_end.close as btc_end_price
    from symbol_date_range a 
    join cg_coin_hist_price b 
    on a.symbol = b.symbol and a.start_date = b.date
    join cg_coin_hist_price c 
    on a.symbol = c.symbol and a.end_date = c.date
    join cg_coin_hist_price btc_start
    on btc_start.symbol = 'BTC' and a.start_date = btc_start.date
    join cg_coin_hist_price btc_end
    on btc_end.symbol = 'BTC' and a.end_date = btc_end.date
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
    from cg_coin_hist_price
    where symbol = 'BTC'
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
select symbol, date,  start_date, end_date, 
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

select a.symbol, 
start_date, end_date, 
pct_above_one as pct_days_above_btc_roi, 
initial_price, ending_price, 
dense_rank() over(order by final_roi/btc_final_roi desc) as relative_final_roi_rank,dense_rank() over(order by pct_above_one desc) as pct_above_rank,
roi_rank, 
final_roi, btc_final_roi,
final_roi/btc_final_roi as relative_final_roi

from pct_below_btc_gain a 
join peak_final_roi b 
on a.symbol=b.symbol
order by relative_final_roi_rank, final_roi desc;
 

select a.symbol, 
-- a.pct_days_above_btc_roi, 
a.relative_final_roi_rank, a.pct_above_rank, 
round(a.relative_final_roi, 2) as relative_roi, 
-- round(a.final_roi, 2) as roi, round(a.btc_final_roi, 2) as btc_roi,
b.symbol,
-- b.pct_days_above_btc_roi, 
b.relative_final_roi_rank, b.pct_above_rank, 
round(b.relative_final_roi, 2) as relative_roi, 
-- round(b.final_roi, 2) as roi, round(b.btc_final_roi, 2) as btc_roi,

c.pct_above_140d_sma_to_btc as pct_above_140d_sma_to_btc_1st_half, 
c.pct_above_140d_sma as pct_above_140d_sma_1st_half, c.data_points
from first_half_coin_performance a 
full join second_half_coin_performance b 
on a.symbol=b.symbol
full join ta_indicators c
on b.symbol=c.symbol
-- order by a.relative_final_roi_rank
order by pct_above_140d_sma_to_btc desc