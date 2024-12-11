-- Calculate ROI for Bitcoin (BTC)
WITH btc_roi AS (
    SELECT 
        (MAX(close) - MIN(close)) / MIN(close) * 100 AS btc_roi
    FROM alt_analysis_historical_price
    WHERE symbol = 'BTCUSDT' 
      AND date BETWEEN '2019-06-28' AND '2021-11-01'
),

-- Calculate ROI for Altcoins
altcoin_roi AS (
    SELECT 
        symbol,
        (MAX(close) - MIN(close)) / MIN(close) * 100 AS altcoin_roi
    FROM alt_analysis_historical_price
    WHERE symbol != 'BTCUSDT' 
      AND date BETWEEN '2019-06-28' AND '2021-11-01'
    GROUP BY symbol
),


-- Calculate ROI for Bitcoin (BTC)
btc_roi_first_half AS (
    SELECT 
        (MAX(close) - MIN(close)) / MIN(close) * 100 AS btc_roi
    FROM alt_analysis_historical_price
    WHERE symbol = 'BTCUSDT' 
      AND date BETWEEN '2018-12-16' AND '2019-06-28'
),

-- Calculate ROI for Altcoins
altcoin_roi_first_half AS (
    SELECT 
        symbol,
        (MAX(close) - MIN(close)) / MIN(close) * 100 AS altcoin_roi
    FROM alt_analysis_historical_price
    WHERE symbol != 'BTCUSDT' 
      AND date BETWEEN '2018-12-16' AND '2019-06-28'
    GROUP BY symbol
),

second_half AS (
    -- Same query as the first one but with a CTE
    SELECT 
        a.symbol,
        a.altcoin_roi,
        b.btc_roi,
        a.altcoin_roi / b.btc_roi AS relative_roi,
        RANK() OVER (ORDER BY a.altcoin_roi / b.btc_roi DESC) AS rank
    FROM altcoin_roi a
    JOIN btc_roi b ON true
),

first_half AS (
    -- Same query as the second one but with a CTE
    SELECT 
        a.symbol,
        a.altcoin_roi,
        b.btc_roi,
        a.altcoin_roi / b.btc_roi AS relative_roi,
        RANK() OVER (ORDER BY a.altcoin_roi / b.btc_roi DESC) AS rank
    FROM altcoin_roi_first_half a
    JOIN btc_roi_first_half b ON true
)



-- Compare rankings
SELECT 
    sh.symbol,
	fh.rank AS first_half_rank, 
	fh.relative_roi as first_half_roi,
    sh.rank AS second_half_rank,
	sh.relative_roi as second_half_roi,
    sh.rank - fh.rank AS rank_change
FROM second_half sh
JOIN first_half fh ON sh.symbol = fh.symbol
ORDER BY first_half_rank;

