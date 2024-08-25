stock_market_pipeline

A real-time data pipeline pulling stock market data on the 100 most valuable IPOs once every hour. This pipeline is written in python and uses Apache Airflow for scheduling and execution, dbt for transformation, and Snowflake for storage.

project structure:

dags/

- Pipeline_Master.py: defines and runs an airflow dag that the three python scripts outlined in the src directory, scheduled to run at the top of every hour.


src/

- scrape_tickers.py: scrapes the 100 most valuable IPO tickers from tradingview.com and dumps them into a json file

- ingest_stock_market_data.py: Pulls stock market data from RealStonks api using the scraped tickers and dumps them to a csv and json file

- upload_to_snowflake.py: loads csv data to Snowflake data warehouse

- ddl.sql: sql script for creating Snowflake database, schema, and table
