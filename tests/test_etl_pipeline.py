import pytest
from etl_pipeline import transform_data, validate_data

sample_data = [{
    "id": "bitcoin",
    "symbol": "btc",
    "name": "Bitcoin",
    "current_price": 30000.0,
    "market_cap": 500000000,
    "total_volume": 10000000
}]

def test_transform():
    df = transform_data.fn(sample_data)
    assert "Name" in df.columns

def test_validation():
    df = transform_data.fn(sample_data)
    validated = validate_data.fn(df)
    assert validated.shape[0] == 1
