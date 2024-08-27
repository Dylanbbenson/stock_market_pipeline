**stock_market_pipeline**

A ELT pipeline pulling stock market data on the 100 most valuable IPOs once every hour. This pipeline is written in python and uses Apache Airflow for scheduling and execution, dbt for transformation, and Snowflake for storage.

project structure:

**dags/**

- Pipeline_Master.py: defines and runs an airflow dag that the three python scripts outlined in the src directory, scheduled to run at the top of every hour.
- transform_snowflake_data.py: an airflow dag that runs the dbt models against the data in the warehouse

**src/**

- scrape_tickers.py: scrapes the 100 most valuable IPO tickers from tradingview.com and dumps them into a json file

- ingest_stock_market_data.py: Pulls stock market data from RealStonks api using the scraped tickers and dumps them to a csv and json file

- upload_to_snowflake.py: loads csv data to Snowflake data warehouse

- ddl.sql: sql script for creating Snowflake database, schema, and table

**stock_market_pipeline/models/**

- stocks.sql: stock prices including current and past values

- industries.sql: unique industries and mapping for each

- companies.sql: unique companies based on ticker symbol

- daily_avg.sql: aggregates daily averages per stock

- daily_top_price_changes.sql: aggregates stocks with biggest price change daily

- monthly_avg.sql: aggregates monthly averages per stock

- monthly_top_price_changes.sql: aggregates stocks with biggest price change monthly

- monthly_volume_trends.sql: aggregates trends of each stock's volume monthly

- rolling_avgs: aggregates rolling average each month for each stock

- avg_price_by_industry.sql: aggregates average stock price per industry

- historical_price_summary.sql: summarizes historical prices of each stock

**/**
- requirements.txt: specifies python packages to install via pip
  
- dockerfile

![Stock Market ELT Pipeline](https://github.com/user-attachments/assets/7335b531-60df-490d-9ab2-9205f0adbe38)
