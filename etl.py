import requests
import pandas as pd
import pandera as pa
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
        "Price Change 24H": pa.Column(float),
        "Market Cap": pa.Column(int),
    })
    return schema.validate(df)

def transform_data(data):
    print("Transforming data")
    df = pd.json_normalize(data)
    df = df[["id", "symbol", "name", "current_price", "price_change_24h", "price_change_percentage_24h", "market_cap"]]
    df.columns = [col.replace("_", " ").title() for col in df.columns]

    # Combine "Price Change 24H" and "Price Change Percentage 24H"
    df['Price Change 24H'] = df.apply(
        lambda row: f"{row['Price Change 24H']:.2f} ({row['Price Change Percentage 24H']:.2f}%)",
        axis=1
    )
    df = df.drop(columns=['Price Change Percentage 24H'])

    print("Data transformation complete")
    return df

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
    df = df[['Icon', 'Name', 'Current Price', 'Price Change 24H', 'Market Cap']]

    # Create the Markdown string with right alignment for the last 2 columns
    markdown_lines = ["| " + " | ".join(df.columns) + " |"]
    markdown_lines.append("| ---| ---| ---:| ---:|")
    for index, row in df.iterrows():
        row_values = [str(val) for val in row.tolist()]
        # Right-align the last two columns
        row_values[-2:] = [f" {val}" for val in row_values[-2:]]
        markdown_lines.append("| " + " | ".join(row_values) + " |")

    return "\n".join(markdown_lines)

def load_data(df):
    print("Saving cryptocurrency data to Markdown file")
    md_path = "data/crypto.md"  # Define the Markdown file path
    md_dir = os.path.dirname(md_path) # Get the directory
    if md_dir: # Check if the directory exists
        os.makedirs(md_dir, exist_ok=True)  # Create the directory if it doesn't exist
        print(f"Markdown directory created: {md_dir}") # Log

    title = "# Top 10 Cryptocurrencies by Market Cap"
    description = "Data obtained from the [CoinGecko API](https://api.coingecko.com/api/v3/coins/markets)."
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

if __name__ == "__main__":
    etl_pipeline()