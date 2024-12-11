-- create or replace temporary view ta_indicators as  
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







