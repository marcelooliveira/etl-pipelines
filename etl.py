import requests
import pandas as pd
import pandera as pa
import sqlite3
from sqlalchemy import create_engine
from prefect import flow, task, get_run_logger
import os

@task(retries=3, retry_delay_seconds=5)
def extract_data():
    logger = get_run_logger()
    logger.info("Fetching cryptocurrency data from CoinGecko API")
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
        "sparkline": False
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    logger.info("Data extraction successful")
    return response.json()

@task
def validate_data(df):
    logger = get_run_logger()
    logger.info("Validating data schema")
    schema = pa.DataFrameSchema({
        "Id": pa.Column(str),
        "Symbol": pa.Column(str),
        "Name": pa.Column(str),
        "Current Price": pa.Column(float),
        "Market Cap": pa.Column(int),
        "Total Volume": pa.Column(int),
    })
    return schema.validate(df)

@task
def transform_data(data):
    logger = get_run_logger()
    logger.info("Transforming data")
    df = pd.json_normalize(data)
    df = df[["id", "symbol", "name", "current_price", "market_cap", "total_volume"]]
    df.columns = [col.replace("_", " ").title() for col in df.columns]
    logger.info("Data transformation complete")
    return df

@task
def load_data(df):
    logger = get_run_logger()
    logger.info("Loading data into SQLite database")
    db_path = "data/crypto.db"  # Define the database path
    db_dir = os.path.dirname(db_path) #get the directory
    if db_dir: # Check if the directory exists
        os.makedirs(db_dir, exist_ok=True)  # Create the directory if it doesn't exist
        logger.info(f"Database directory created: {db_dir}") #log
    engine = create_engine(f"sqlite:///{db_path}") #use the full path
    df.to_sql("cryptocurrencies", engine, if_exists="replace", index=False)
    logger.info("Data successfully loaded")

@flow(name="crypto-etl-pipeline")
def etl_pipeline():
    raw_data = extract_data()
    cleaned_data = transform_data(raw_data)
    validated_data = validate_data(cleaned_data)
    load_data(validated_data)

