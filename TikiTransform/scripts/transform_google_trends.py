
import pandas as pd
import os
from datetime import datetime, timezone
from typing import Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform_trends_data(raw_file_path: str, output_dir: str = "/tmp/google_trends") -> str:
    """
    Transform raw Google Trends CSV from wide format to long format.
        
    Output Schema (Long Format - BigQuery Ready):
        date (DATE), keyword (STRING), score (INT64), is_partial (BOOL), inserted_at (TIMESTAMP)
    """
    
    logger.info(f"Loading raw CSV: {raw_file_path}")
    
    # ============================================================
    # STEP 1: Read CSV with smart date detection
    # ============================================================
    
    # Try reading with date as index first
    try:
        df = pd.read_csv(raw_file_path, parse_dates=['date'])
        date_is_column = True
        logger.info("Date detected as column")
    except (KeyError, ValueError):
        # Date might be the index
        df = pd.read_csv(raw_file_path, index_col=0, parse_dates=True)
        df = df.reset_index()
        df = df.rename(columns={df.columns[0]: 'date'})
        date_is_column = True
        logger.info("Date detected as index, converted to column")
    
    logger.info(f"Raw data shape: {df.shape}")
    logger.info(f"Columns: {df.columns.tolist()}")
    
    # ============================================================
    # STEP 2: Preprocessing - Separate isPartial from keywords
    # ============================================================
    
    # Identify isPartial column (case-insensitive)
    is_partial_col = None
    for col in df.columns:
        if col.lower() == 'ispartial':
            is_partial_col = col
            break
    
    if is_partial_col is None:
        logger.warning("isPartial column not found, creating with False values")
        df['isPartial'] = False
        is_partial_col = 'isPartial'
    
    # Identify keyword columns (everything except date and isPartial)
    keyword_columns = [col for col in df.columns if col not in ['date', is_partial_col]]
    
    logger.info(f"Found {len(keyword_columns)} keyword columns: {keyword_columns}")
    
    # ============================================================
    # STEP 3: Melt - Wide to Long format (CRUCIAL STEP)
    # ============================================================
    
    df_long = df.melt(
        id_vars=['date', is_partial_col],
        value_vars=keyword_columns,
        var_name='keyword',
        value_name='score'
    )
    
    # Rename isPartial to is_partial for consistency
    df_long = df_long.rename(columns={is_partial_col: 'is_partial'})
    
    logger.info(f"After melt: {df_long.shape[0]} rows × {df_long.shape[1]} columns")
    
    # ============================================================
    # STEP 4: Data Cleaning - The "Hardening" part
    # ============================================================
    
    # 4a. Handle "<1" scores → replace with 0
    # Google Trends sometimes returns "<1" for very low search volume
    if df_long['score'].dtype == 'object':
        logger.info("Detected string scores, cleaning '<1' values...")
        
        # Count how many "<1" values
        less_than_one_count = (df_long['score'] == '<1').sum()
        if less_than_one_count > 0:
            logger.info(f"Replacing {less_than_one_count} '<1' values with 0")
        
        # Replace "<1" with 0
        df_long['score'] = df_long['score'].replace('<1', '0')
        
        # Also handle any other non-numeric strings
        df_long['score'] = pd.to_numeric(df_long['score'], errors='coerce')
    
    # 4b. Handle NaN/null values (can occur from multiple batches or missing data)
    nan_count = df_long['score'].isna().sum()
    if nan_count > 0:
        logger.info(f"Replacing {nan_count} NaN/null values with 0")
        df_long['score'] = df_long['score'].fillna(0)
    
    # 4c. Cast types
    df_long['score'] = df_long['score'].astype('int64')
    df_long['is_partial'] = df_long['is_partial'].astype(bool)
    
    # 4d. Ensure date is proper datetime then convert to date string for BigQuery
    df_long['date'] = pd.to_datetime(df_long['date']).dt.date
    
    logger.info(f"Data types after cleaning:")
    logger.info(f"   - date: {df_long['date'].dtype}")
    logger.info(f"   - keyword: {df_long['keyword'].dtype}")
    logger.info(f"   - score: {df_long['score'].dtype}")
    logger.info(f"   - is_partial: {df_long['is_partial'].dtype}")
    
    # ============================================================
    # STEP 5: Enrichment - Add inserted_at timestamp
    # ============================================================
    
    # BigQuery only supports microsecond precision, not nanoseconds
    # Use pd.Timestamp with floor to microseconds
    df_long['inserted_at'] = pd.Timestamp.now(tz='UTC').floor('us')
    
    logger.info(f"Added inserted_at: {df_long['inserted_at'].iloc[0]}")
    
    # ============================================================
    # STEP 6: Deduplication/Aggregation - Handle duplicate (date, keyword) pairs
    # ============================================================
    
    # Check for duplicates before aggregation
    duplicate_count = df_long.duplicated(subset=['date', 'keyword']).sum()
    if duplicate_count > 0:
        logger.info(f"Found {duplicate_count} duplicate (date, keyword) pairs, aggregating...")
    
    # Group by (date, keyword) and aggregate
    # - max(score): Choose the highest score (resolves 0 vs real score conflicts)
    # - max(is_partial): True if any batch marked as partial
    # - max(inserted_at): Use the latest timestamp
    df_aggregated = df_long.groupby(['date', 'keyword'], as_index=False).agg({
        'score': 'max',
        'is_partial': 'max',
        'inserted_at': 'max'
    })
    
    rows_before = len(df_long)
    rows_after = len(df_aggregated)
    
    if rows_before != rows_after:
        logger.info(f"Deduplication: {rows_before} → {rows_after} rows ({rows_before - rows_after} duplicates removed)")
    else:
        logger.info(f"No duplicates found, {rows_after} unique rows")
    
    # ============================================================
    # STEP 7: Final Selection - Reorder columns for BigQuery
    # ============================================================
    
    final_columns = ['date', 'keyword', 'score', 'is_partial', 'inserted_at']
    df_final = df_aggregated[final_columns]
    
    logger.info(f"Final schema: {df_final.columns.tolist()}")
    logger.info(f"Final shape: {df_final.shape}")
    
    # ============================================================
    # STEP 8: Save as Parquet (preserves data types)
    # ============================================================
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output filename with timestamp
    snapshot_date = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = os.path.join(output_dir, f"google_trends_{snapshot_date}.parquet")
    
    # Force timestamp to microsecond precision for BigQuery compatibility
    # Create PyArrow schema with explicit TIMESTAMP(us) type
    import pyarrow as pa
    schema = pa.schema([
        ('date', pa.date32()),
        ('keyword', pa.string()),
        ('score', pa.int64()),
        ('is_partial', pa.bool_()),
        ('inserted_at', pa.timestamp('us', tz='UTC'))  # Microsecond precision
    ])
    
    df_final.to_parquet(output_path, index=False, engine='pyarrow', schema=schema)
    
    logger.info(f"Saved Parquet: {output_path}")
    logger.info(f"File size: {os.path.getsize(output_path) / 1024:.2f} KB")
    
    logger.info("\n" + "="*50)
    logger.info("TRANSFORM SUMMARY")
    logger.info("="*50)
    logger.info(f"Total rows: {len(df_final)}")
    logger.info(f"Unique keywords: {df_final['keyword'].nunique()}")
    logger.info(f"Date range: {df_final['date'].min()} to {df_final['date'].max()}")
    logger.info(f"Score range: {df_final['score'].min()} to {df_final['score'].max()}")
    logger.info(f"Partial data rows: {df_final['is_partial'].sum()}")
    
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
    required_columns = ['date', 'keyword', 'score', 'is_partial', 'inserted_at']
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing columns: {missing_columns}")
    
    # Check data types
    assert df['score'].dtype == 'int64', f"score should be int64, got {df['score'].dtype}"
    assert df['is_partial'].dtype == 'bool', f"is_partial should be bool, got {df['is_partial'].dtype}"
    
    # Check no nulls in required fields
    null_counts = df[['date', 'keyword', 'score']].isnull().sum()
    if null_counts.sum() > 0:
        raise ValueError(f"Null values found: {null_counts.to_dict()}")
    
    logger.info("Parquet validation passed!")
    return True


# ============================================================
# AIRFLOW TASK WRAPPER
# ============================================================

def transform_trends_task(**context):
    """
    Airflow task wrapper for transform_trends_data.
    Reads input path from XCom, writes output path to XCom.
    """
    ti = context['ti']
    
    # Get raw file path from previous task (extract)
    raw_file_path = ti.xcom_pull(task_ids='fetch_trends_data', key='output_file')
    
    if not raw_file_path:
        raise ValueError("No raw file path found in XCom from fetch_trends_data task")
    
    logger.info(f"Received raw file from XCom: {raw_file_path}")
    
    parquet_path = transform_trends_data(raw_file_path)
    
    validate_parquet(parquet_path)
    
    ti.xcom_push(key='parquet_file', value=parquet_path)
    ti.xcom_push(key='row_count', value=pd.read_parquet(parquet_path).shape[0])
    
    logger.info(f"Pushed parquet path to XCom: {parquet_path}")
    
    return parquet_path


# ============================================================
# LOCAL TESTING
# ============================================================

if __name__ == "__main__":
    import sys
    
    # Test with sample file
    if len(sys.argv) > 1:
        test_file = sys.argv[1]
    else:
        # Default test file
        test_file = "trends_raw_output.csv"
    
    if os.path.exists(test_file):
        print(f"\n{'='*60}")
        print("TESTING TRANSFORM PIPELINE")
        print(f"{'='*60}\n")
        
        output_path = transform_trends_data(test_file, output_dir="./test_output")
        
        validate_parquet(output_path)
        
        print(f"\n{'='*60}")
        print("SAMPLE OUTPUT (first 10 rows)")
        print(f"{'='*60}")
        df = pd.read_parquet(output_path)
        print(df.head(10).to_string())
        
        print(f"\nTransform test completed successfully!")
        print(f"Output: {output_path}")
    else:
        print(f"Test file not found: {test_file}")
        print("Usage: python transform_google_trends.py [raw_csv_path]")
