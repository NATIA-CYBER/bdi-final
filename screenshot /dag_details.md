# DAG Details

## readsb-hist DAG
- **Name**: readsb_hist_processing
- **Start Date**: 2023-11-01
- **End Date**: 2024-11-01
- **Schedule**: @monthly
- **Tasks**:
  1. start_processing
  2. download_files (downloads exactly 100 files)
  3. process_files (converts to parquet)
  4. end_processing

## Fuel Consumption DAG
- **Name**: aircraft_fuel_consumption
- **Start Date**: 2023-11-01
- **Schedule**: @daily
- **Tasks**:
  1. download_fuel_data
  2. process_fuel_data

## Aircraft Database DAG
- **Name**: aircraft_database
- **Start Date**: 2023-11-01
- **Schedule**: @daily
- **Tasks**:
  1. download_aircraft_db
  2. process_aircraft_db
  3. load_to_postgres (with idempotency)
