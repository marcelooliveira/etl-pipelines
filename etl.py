import requests
import pandas as pd
import pandera as pa
import sqlite3
from sqlalchemy import create_engine
import os
from datetime import datetime

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

# def load_data(df):
#     print("Saving data to Markdown file")
#     md_path = "data/crypto.md"  # Define the Markdown file path
#     md_dir = os.path.dirname(md_path) # Get the directory
#     if md_dir: # Check if the directory exists
#         os.makedirs(md_dir, exist_ok=True)  # Create the directory if it doesn't exist
#         print(f"Markdown directory created: {md_dir}") # Log
#     df_markdown = df.to_markdown(index=False)
#     with open(md_path, "w") as f:
#         f.write(df_markdown)
#     print(f"Data successfully saved to: {md_path}")

def format_currency(value):
    """Formats a numeric value as currency with commas for thousands."""
    return f"${value:,.2f}"

def format_market_cap(value):
    """Formats a large integer as market capitalization with appropriate units."""
    if value >= 1_000_000_000_000:
        return f"${value / 1_000_000_000_000:.2f}T"
    elif value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    elif value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    else:
        return f"${value:,.2f}"

def get_crypto_icon(symbol):
    """Returns an emoji or a default icon for a given cryptocurrency symbol."""
    icons = {
        "btc": "₿",  # Bitcoin symbol
        "eth": "Ξ",  # Ethereum symbol
        "usdt": "₮", # Tether symbol
        "xrp": "Ripple", # No widely recognized emoji, using text
        "bnb": "BNB", # Binance Coin symbol
        "sol": "☀️", # Solana sun emoji
        "usdc": "Circle", # No widely recognized emoji, using text
        "doge": "🐕", # Dogecoin dog emoji
        "ada": "ADA", # Cardano symbol
        "trx": "TRX", # TRON symbol
    }
    return icons.get(symbol.lower(), "🪙") # Default coin emoji

def create_markdown_table(df):
    """Creates a formatted Markdown table from the DataFrame."""
    df['Icon'] = df['Symbol'].apply(get_crypto_icon)
    df['Current Price'] = df['Current Price'].apply(format_currency)
    df['Market Cap'] = df['Market Cap'].apply(format_market_cap)
    df['Total Volume'] = df['Total Volume'].apply(format_market_cap)
    df = df[['Icon', 'Id', 'Symbol', 'Name', 'Current Price', 'Market Cap', 'Total Volume']]
    markdown_table = df.to_markdown(index=False)
    return markdown_table

def load_data(df):
    print("Saving cryptocurrency data to Markdown file")
    md_path = "data/crypto.md"  # Define the Markdown file path
    md_dir = os.path.dirname(md_path) # Get the directory
    if md_dir: # Check if the directory exists
        os.makedirs(md_dir, exist_ok=True)  # Create the directory if it doesn't exist
        print(f"Markdown directory created: {md_dir}") # Log

    title = "# Top 10 Cryptocurrencies by Market Cap"
    description = "This table displays the top 10 cryptocurrencies by market capitalization, showing their current price, market cap, and total volume."
    markdown_table = create_markdown_table(df.copy()) # Use a copy to avoid modifying the original DataFrame
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z%z")
    footer = f"*Last updated: {timestamp} (Guarulhos, State of São Paulo, Brazil)*"

    with open(md_path, "w") as f:
        f.write(title + "\n\n")
        f.write(description + "\n\n")
        f.write(markdown_table + "\n\n")
        f.write(footer + "\n")

    print(f"Cryptocurrency data successfully saved to: {md_path}")

def etl_pipeline():
    raw_data = extract_data()
    cleaned_data = transform_data(raw_data)
    validated_data = validate_data(cleaned_data)
    load_data(validated_data)

if __name__ == "__main__":
    etl_pipeline()