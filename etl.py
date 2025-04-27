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
        "Total Volume": pa.Column(float),
    })
    return schema.validate(df)

def transform_data(data):
    print("Transforming data")
    df = pd.json_normalize(data)

    # Select and rename columns
    df = df[[
        "id", "symbol", "name", "image", "current_price", "market_cap",
        "market_cap_rank", "fully_diluted_valuation", "total_volume",
        "high_24h", "low_24h", "price_change_24h",
        "price_change_percentage_24h", "market_cap_change_24h",
        "market_cap_change_percentage_24h", "circulating_supply",
        "total_supply", "max_supply", "ath", "ath_change_percentage",
        "ath_date", "atl", "atl_change_percentage", "atl_date",
        "last_updated", "price_change_percentage_1h", "sparkline_in_7d.price"
    ]]
    df.columns = [
        "ID", "Symbol", "Name", "Image URL", "Current Price", "Market Cap",
        "Market Cap Rank", "Fully Diluted Valuation", "Total Volume",
        "High 24h", "Low 24h", "Price Change 24h",
        "Price Change % 24h", "Market Cap Change 24h",
        "Market Cap Change % 24h", "Circulating Supply",
        "Total Supply", "Max Supply", "ATH", "ATH Change %",
        "ATH Date", "ATL", "ATL Change %", "ATL Date",
        "Last Updated", "Price Change % 1h", "Sparkline 7d"
    ]

    # Convert timestamps to datetime objects
    df["ATH Date"] = pd.to_datetime(df["ATH Date"])
    df["ATL Date"] = pd.to_datetime(df["ATL Date"])
    df["Last Updated"] = pd.to_datetime(df["Last Updated"])

    # Calculate additional metrics
    df["Price Change from ATH"] = df["Current Price"] - df["ATH"]
    df["Price Change from ATL"] = df["Current Price"] - df["ATL"]

    # Format currency columns
    currency_cols = ["Current Price", "Market Cap", "Fully Diluted Valuation", "Total Volume", "High 24h", "Low 24h", "ATH", "ATL"]
    for col in currency_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: f"${x:,.2f}")

    # Format percentage columns
    percentage_cols = ["Price Change % 24h", "Market Cap Change % 24h", "ATH Change %", "ATL Change %", "Price Change % 1h"]
    for col in percentage_cols:
        if col in df.columns:
          df[col] = df[col].apply(lambda x: f"{x:.2f}%")
    print("Data transformation complete")
    return df

def format_currency(value):
    """Formats a numeric value as currency with commas for thousands and 2 decimal places."""
    if pd.isna(value):
        return "N/A"
    return f"${value:,.2f}"

def format_market_cap(value):
    """Formats a large integer as market capitalization with appropriate units."""
    if pd.isna(value):
        return "N/A"
    if value >= 1_000_000_000_000:
        return f"${value / 1_000_000_000_000:.2f}T"
    elif value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    elif value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    else:
        return f"${value:,.2f}"

def format_percentage(value):
    """Formats a percentage value with 2 decimal places and a '%' sign."""
    if pd.isna(value):
        return "N/A"
    return f"{value:.2f}%"

def get_crypto_icon_url(symbol):
    """Returns the URL of a tiny icon for a given cryptocurrency symbol."""
    base_url = "https://raw.githubusercontent.com/cjdowner/cryptocurrency-icons/master/32/color/"
    icon_map = {
        "btc": "btc.png",
        "eth": "eth.png",
        "usdt": "usdt.png",
        "xrp": "xrp.png",
        "bnb": "bnb.png",
        "sol": "sol.png",
        "usdc": "usdc.png",
        "doge": "doge.png",
        "ada": "ada.png",
        "trx": "trx.png",
    }
    filename = icon_map.get(symbol.lower())
    if filename:
        return f'<img src="{base_url}{filename}" width="16" height="16" align="absmiddle"> '
    else:
        return "🪙 " # Default coin emoji as fallback

def create_markdown_table(df):
    """Creates a formatted Markdown table with right-aligned numerical columns."""
    df['Icon'] = df['Symbol'].apply(get_crypto_icon_url)
    df['Current Price'] = df['Current Price'].apply(format_currency)
    df['Market Cap'] = df['Market Cap'].apply(format_market_cap)
    df['Total Volume'] = df['Total Volume'].apply(format_market_cap)
    df['Price Change % 24h'] = df['Price Change % 24h'].apply(format_percentage)
    df['ATH'] = df['ATH'].apply(format_currency)
    df['ATH Change %'] = df['ATH Change %'].apply(format_percentage)
    df['ATL'] = df['ATL'].apply(format_currency)
    df['ATL Change %'] = df['ATL Change %'].apply(format_percentage)
    df['% Diff from ATH'] = df['% Diff from ATH'].apply(format_percentage)
    df['% Diff from ATL'] = df['% Diff from ATL'].apply(format_percentage)

    columns_to_display = ['Icon', 'Name', 'Current Price', 'Market Cap', 'Market Cap Rank',
                          'Price Change % 24h', 'ATH', 'ATH Change %', 'ATL', 'ATL Change %',
                          '% Diff from ATH', '% Diff from ATL', 'Closer To']
    df_display = df[columns_to_display].copy()

    # Create the Markdown string with right alignment for numerical columns
    markdown_lines = ["| " + " | ".join(df_display.columns) + " |"]
    markdown_lines.append("|" + " ---|" * len(df_display.columns))
    for index, row in df_display.iterrows():
        row_values = [str(val) for val in row.tolist()]
        # Right-align numerical columns (adjust indices based on displayed columns)
        right_align_indices = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
        for i in right_align_indices:
            if i < len(row_values):
                row_values[i] = f" {row_values[i]}"
        markdown_lines.append("| " + " | ".join(row_values) + " |")

    return "\n".join(markdown_lines)

def load_data(df):
    print("Saving cryptocurrency data to Markdown file")
    md_path = "data/crypto.md"  # Define the Markdown file path
    md_dir = os.path.dirname(md_path) # Get the directory
    if md_dir: # Check if the directory exists
        os.makedirs(md_dir, exist_ok=True)  # Create the directory if it doesn't exist
        print(f"Markdown directory created: {md_dir}") # Log

    title = "# Top 10 Cryptocurrencies - Enhanced Analysis"
    description = "Detailed cryptocurrency data obtained from the [CoinGecko API](https://api.coingecko.com/api/v3/coins/markets), including price performance relative to their all-time high and low."
    markdown_table = create_markdown_table(df.copy()) # Use a copy
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z%z")
    footer = f"*Last updated: {timestamp.strip()}*"

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
    load_data(cleaned_data)

if __name__ == "__main__":
    etl_pipeline()