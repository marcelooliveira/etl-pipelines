import requests
import pandas as pd
import pandera as pa
import sqlite3
from sqlalchemy import create_engine
import os

def extract_data():
    print("Fetching cryptocurrency data from CoinGecko API")
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
    print("Data extraction successful")
    return response.json()

def validate_data(df):
    print("Validating data schema")
    schema = pa.DataFrameSchema({
        "Id": pa.Column(str),
        "Symbol": pa.Column(str),
        "Name": pa.Column(str),
        "Current Price": pa.Column(float),
        "Market Cap": pa.Column(int),
        "Total Volume": pa.Column(int),
    })
    return schema.validate(df)

def transform_data(data):
    print("Transforming data")
    df = pd.json_normalize(data)
    df = df[["id", "symbol", "name", "current_price", "market_cap", "total_volume"]]
    df.columns = [col.replace("_", " ").title() for col in df.columns]
    print("Data transformation complete")
    return df

def load_data(df):
    print("Loading data into SQLite database")
    db_path = "data/crypto.db"  # Define the database path
    db_dir = os.path.dirname(db_path) #get the directory
    if db_dir: # Check if the directory exists
        os.makedirs(db_dir, exist_ok=True)  # Create the directory if it doesn't exist
        print(f"Database directory created: {db_dir}") #log
    engine = create_engine(f"sqlite:///{db_path}") #use the full path
    df.to_sql("cryptocurrencies", engine, if_exists="replace", index=False)
    print("Data successfully loaded")

def load_md(df):
    print("Saving data to Markdown file")
    md_path = "data/crypto.md"  # Define the Markdown file path
    md_dir = os.path.dirname(md_path) # Get the directory
    if md_dir: # Check if the directory exists
        os.makedirs(md_dir, exist_ok=True)  # Create the directory if it doesn't exist
        print(f"Markdown directory created: {md_dir}") # Log
    df_markdown = df.to_markdown(index=False)
    with open(md_path, "w") as f:
        f.write(df_markdown)
    print(f"Data successfully saved to: {md_path}")

def etl_pipeline():
    raw_data = extract_data()
    cleaned_data = transform_data(raw_data)
    validated_data = validate_data(cleaned_data)
    load_data(validated_data)
    load_md(validated_data)

if __name__ == "__main__":
    etl_pipeline()