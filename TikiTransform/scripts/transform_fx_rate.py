"""
Input: Raw API response or fallback rate
Output: DataFrame ready for BigQuery dim_exchange_rate table

Schema:
    - date: DATE (partition key)
    - from_currency: STRING
    - to_currency: STRING  
    - rate: FLOAT64
    - source: STRING
    - inserted_at: TIMESTAMP (microsecond precision for BigQuery)
"""

import pandas as pd
import pyarrow as pa
import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
FALLBACK_RATE = 25400.0
TARGET_CURRENCY = 'VND'
BASE_CURRENCY = 'USD'


def transform_fx_rate(
    rate: float,
    logical_date: str,
    source: str = 'open.er-api.com',
    output_dir: str = '/tmp/fx_rate'
) -> str:
    """
    Transform exchange rate data into BigQuery-ready Parquet format.
    
    Args:
        rate: The exchange rate value (USD to VND)
        logical_date: Airflow logical date in 'YYYY-MM-DD' format
        source: Data source name ('open.er-api.com' or 'Fallback')
        output_dir: Directory to save output Parquet file
        
    Returns:
        Path to the saved Parquet file
    """
    logger.info("=" * 60)
    logger.info("FX RATE TRANSFORM")
    logger.info("=" * 60)
    
    # ============================================================
    # STEP 1: Create DataFrame with single row
    # ============================================================
    
    # Use Airflow's logical date (not datetime.now()) for backfill capability
    logical_date_parsed = datetime.strptime(logical_date, '%Y-%m-%d').date()
    
    # Create timestamp with microsecond precision for BigQuery compatibility
    inserted_at = pd.Timestamp.now(tz='UTC').floor('us')
    
    data = {
        'date': [logical_date_parsed],
        'from_currency': [BASE_CURRENCY],
        'to_currency': [TARGET_CURRENCY],
        'rate': [float(rate)],
        'source': [source],
        'inserted_at': [inserted_at]
    }
    
    df = pd.DataFrame(data)
    
    logger.info(f"Logical Date: {logical_date}")
    logger.info(f"Rate: 1 {BASE_CURRENCY} = {rate:,.2f} {TARGET_CURRENCY}")
    logger.info(f"Source: {source}")
    logger.info(f"Inserted At: {inserted_at}")
    
    # ============================================================
    # STEP 2: Validate data types
    # ============================================================
    
    df['rate'] = df['rate'].astype('float64')
    df['from_currency'] = df['from_currency'].astype('string')
    df['to_currency'] = df['to_currency'].astype('string')
    df['source'] = df['source'].astype('string')
    
    logger.info("\nDataFrame Schema:")
    for col in df.columns:
        logger.info(f"   - {col}: {df[col].dtype}")
    
    # ============================================================
    # STEP 3: Save as Parquet with PyArrow schema
    # ============================================================
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output filename
    output_path = os.path.join(output_dir, f"fx_rate_{logical_date}.parquet")
    
    # Define PyArrow schema for BigQuery compatibility
    # CRITICAL: Use microsecond precision for TIMESTAMP
    schema = pa.schema([
        ('date', pa.date32()),
        ('from_currency', pa.string()),
        ('to_currency', pa.string()),
        ('rate', pa.float64()),
        ('source', pa.string()),
        ('inserted_at', pa.timestamp('us', tz='UTC'))  # Microsecond precision
    ])
    
    df.to_parquet(output_path, index=False, engine='pyarrow', schema=schema)
    
    file_size = os.path.getsize(output_path)
    logger.info(f"\nSaved Parquet: {output_path}")
    logger.info(f"File size: {file_size} bytes")
    
    return output_path


def validate_parquet(parquet_path: str) -> bool:
    """
    Validate the transformed Parquet file.
    
    Args:
        parquet_path: Path to Parquet file
        
    Returns:
        True if valid, raises exception otherwise
    """
    logger.info(f"Validating Parquet: {parquet_path}")
    
    df = pd.read_parquet(parquet_path)
    
    # Check required columns
    required_columns = ['date', 'from_currency', 'to_currency', 'rate', 'source', 'inserted_at']
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing columns: {missing_columns}")
    
    # Check data types
    assert df['rate'].dtype == 'float64', f"rate should be float64, got {df['rate'].dtype}"
    
    # Check no nulls
    null_counts = df.isnull().sum()
    if null_counts.sum() > 0:
        raise ValueError(f"Null values found: {null_counts.to_dict()}")
    
    # Check rate is positive
    if df['rate'].iloc[0] <= 0:
        raise ValueError(f"Invalid rate: {df['rate'].iloc[0]}")
    
    logger.info("Parquet validation passed!")
    return True


# ============================================================
# LOCAL TESTING
# ============================================================

if __name__ == "__main__":
    import sys
    
    print("\n" + "=" * 60)
    print("TESTING FX RATE TRANSFORM")
    print("=" * 60 + "\n")
    
    # Test with sample data
    test_rate = 25432.5
    test_date = "2026-02-01"
    
    if len(sys.argv) > 1:
        test_rate = float(sys.argv[1])
    if len(sys.argv) > 2:
        test_date = sys.argv[2]
    
    output_path = transform_fx_rate(
        rate=test_rate,
        logical_date=test_date,
        source='test',
        output_dir='./test_output'
    )
    
    validate_parquet(output_path)
    
    print("\n" + "=" * 60)
    print("OUTPUT DATA")
    print("=" * 60)
    df = pd.read_parquet(output_path)
    print(df.to_string(index=False))
    
    print(f"\nTransform test completed successfully!")
    print(f"Output: {output_path}")
