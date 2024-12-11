#!/bin/bash

echo "----------------------------------------------------------------"
echo "--- Start at $(date) ---"
cd /Users/zyu/Desktop/repo/financial-master-database

# Track the start time of the entire script
start_time=$(date +%s)

# Clean up checkpoints
echo "--- Checkpoints Clean Up ---"
FILE="/Users/zyu/Desktop/repo/financial-master-database/backend/data/checkpoints/coin_calc_pipeline.json"
[ -f "$FILE" ] && rm "$FILE" && echo "$FILE deleted" || echo "$FILE does not exist"

 
echo "--- Start running pipeline_coin_price_overview_updater---"
pipeline_start_time=$(date +%s)
/Users/zyu/Desktop/repo/financial-master-database/backend/venv/bin/python3 /Users/zyu/Desktop/repo/financial-master-database/backend/pipeline_coin_price_overview_updater.py
pipeline_end_time=$(date +%s)
echo "--- Finish running coin pipeline_coin_price_overview_updater---"
echo "--- pipeline_coin_price_overview_updater Duration: $((pipeline_end_time - pipeline_start_time)) seconds ---"

 
echo "--- Start running pipeline_coin_hourly_price_updater---"
pipeline_start_time=$(date +%s)
/Users/zyu/Desktop/repo/financial-master-database/backend/venv/bin/python3 /Users/zyu/Desktop/repo/financial-master-database/backend/pipeline_coin_hourly_price_updater.py
pipeline_end_time=$(date +%s)
echo "--- Finish running pipeline_coin_hourly_price_updater---"
echo "--- pipeline_coin_hourly_price_updater Duration: $((pipeline_end_time - pipeline_start_time)) seconds ---"

 
echo "--- Start running pipeline_coin_signal_updater_coint---"
pipeline_start_time=$(date +%s)
/Users/zyu/Desktop/repo/financial-master-database/backend/venv/bin/python3 /Users/zyu/Desktop/repo/financial-master-database/backend/pipeline_coin_signal_updater_coint.py
pipeline_end_time=$(date +%s)
echo "--- Finish running pipeline_coin_signal_updater_coint---"
echo "--- pipeline_coin_signal_updater_coint Duration: $((pipeline_end_time - pipeline_start_time)) seconds ---"

 
# Track the end time of the entire script
end_time=$(date +%s)
echo "--- Finished at $(date) ---"
echo "--- Total Duration: $((end_time - start_time)) seconds ---"
echo "----------------------------------------------------------------"