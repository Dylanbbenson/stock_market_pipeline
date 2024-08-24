import os
import snowflake.connector
import json
import logging
import pandas as pd
from datetime import date, datetime
current_date = date.today().strftime('%Y-%m-%d')
current_time = datetime.now().strftime("%H")
csv_file = f"./data/stock_market_{current_date}_{current_time}.csv"
table_name = 'stocks'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def create_table_if_not_exists(cursor, table_name, df):
    schema = ', '.join([f"{col} STRING" for col in df.columns])
    create_table_sql = f"""
    CREATE OR REPLACE TABLE {table_name} (
        {schema}
    );
    """
    cursor.execute(create_table_sql)

def upload_csv_to_snowflake():
    try:
        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"CSV file not found: {csv_file}")

        #Load credentials
        with open('./config/credentials.json') as f:
            credentials = json.load(f)

        conn = snowflake.connector.connect(
            user=credentials['snow_user'],
            password=credentials['snow_password'],
            account=credentials['snow_account'],
            warehouse=credentials['snow_warehouse'],
            database=credentials['snow_db'],
            schema=credentials['snow_schema'],
        )
        logging.info("Successfully connected to Snowflake")

        #Create stocks table from file if not exists
        cursor = conn.cursor()
        df = pd.read_csv(csv_file)
        create_table_if_not_exists(cursor, table_name, df)

        # Create a Snowflake stage for file
        stage_name = 'STOCK_STAGE'
        conn.cursor().execute(f"CREATE OR REPLACE STAGE {stage_name};")

        #Upload CSV to stage
        with open(csv_file, 'rb') as file:
            conn.cursor().execute(
                f"PUT file://{csv_file} @{stage_name}"
            )
        logging.info(f"Successfully uploaded {csv_file} to Snowflake stage {stage_name}")
        create_table_sql = f"""
        CREATE TABLE 
        """
        #Copy data from stage to Snowflake table
        copy_sql = f"""
        COPY INTO {table_name}
        FROM @{stage_name}
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' FIELD_DELIMITER = ',' SKIP_HEADER = 1);
        """
        conn.cursor().execute(copy_sql)
        logging.info(f"Successfully copied data from stage to table {table_name}")

        #Clean up
        conn.cursor().execute(f"REMOVE @{stage_name}/*")
        logging.info(f"Cleaned up files from stage {stage_name}")
        conn.close()
        logging.info("Snowflake connection closed")
    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == '__main__':
    upload_csv_to_snowflake()
