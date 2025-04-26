import unittest
import pandas as pd
from etl import transform_data, validate_data

class TestETLPipeline(unittest.TestCase):

    def test_transform_data(self):
        raw_data = [{"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "current_price": 50000, "market_cap": 900000000000, "total_volume": 30000000000}]
        transformed_df = transform_data(raw_data)
        expected_columns = ["Name", "Current Price", "Market Cap", "Total Volume"]
        self.assertEqual(list(transformed_df.columns), expected_columns)
        self.assertEqual(transformed_df["Name"][0], "Bitcoin")

    def test_validate_data_valid(self):
        valid_data = pd.DataFrame({
            "Name": ["Bitcoin"],
            "Current Price": [50000.0],
            "Market Cap": [900000000000],
            "Total Volume": [30000000000],
        })
        try:
            validate_data(valid_data)
            self.assertTrue(True) # No exception raised
        except Exception as e:
            self.fail(f"Validation failed unexpectedly: {e}")

    def test_validate_data_invalid_type(self):
        invalid_data = pd.DataFrame({
            "Name": ["Bitcoin"],
            "Current Price": ["invalid"], # Incorrect data type
            "Market Cap": [900000000000],
            "Total Volume": [30000000000],
        })
        with self.assertRaises(Exception): # Expect a SchemaError or similar
            validate_data(invalid_data)

if __name__ == '__main__':
    unittest.main()