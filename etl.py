import os
import sys
import requests
import pandas as pd
import pandera as pa
import sqlite3
from sqlalchemy import create_engine
from prefect import flow, task, get_run_logger


@task(retries=3, retry_delay_seconds=5)
def extract_data():
    logger = get_run_logger()
    log_info(logger, "Fetching cryptocurrency data from CoinGecko API")
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
    log_info(logger, "Data extraction successful")
    return response.json()

@task
def validate_data(df):
    logger = get_run_logger()
    log_info(logger, "Validating data schema")
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
    log_info(logger, "Transforming data")
    df = pd.json_normalize(data)
    df = df[["id", "symbol", "name", "current_price", "market_cap", "total_volume"]]
    df.columns = [col.replace("_", " ").title() for col in df.columns]
    log_info(logger, "Data transformation complete")
    return df

@task
def load_data(df):
    logger = get_run_logger()
    log_info(logger, "Loading data into SQLite database")
    db_file = "sqlite:///data/crypto.db"

    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        # Change the db_file to the absolute path
        db_file = os.path.join(sys._MEIPASS, "crypto.db")

    print("db_file:")
    print(db_file)

    engine = create_engine(db_file)
    df.to_sql("cryptocurrencies", engine, if_exists="replace", index=False)
    log_info(logger, "Data successfully loaded")

@flow(name="crypto-etl-pipeline")
def etl_pipeline():
    raw_data = extract_data()
    cleaned_data = transform_data(raw_data)
    validated_data = validate_data(cleaned_data)
    load_data(validated_data)

def log_info(logger, msg):
    logger.info(msg)
    print(msg)

raw_data = extract_data()
cleaned_data = transform_data(raw_data)
validated_data = validate_data(cleaned_data)
load_data(validated_data)