"""
This script reads raw JSON files from GCS, cleans/transforms them,
and writes the result back to GCS (Clean Zone) in Parquet format.

Usage:
    python transform_tiki.py --date 2026-01-21
    python transform_tiki.py  # Uses today's date
"""

import os
import re
import sys
import json
import logging
import argparse
from datetime import datetime, date
from typing import Optional, List, Tuple

import pandas as pd
import numpy as np
from google.cloud import storage
from google.oauth2 import service_account

# =============================================================================
# CONFIGURATION
# =============================================================================

# GCS Configuration
GCS_CONFIG = {
    'bucket_name': os.getenv('GCS_BUCKET_NAME', 'gcs-bucket-name'),
    'credentials_path': os.getenv('GCP_CREDENTIALS_PATH', '/opt/airflow/config/google_cloud_credentials.json'),
    'raw_zone_prefix': 'raw_zone/tiki/products',
    'clean_zone_prefix': 'clean_zone/tiki/fact_daily_snapshot',
    # BigQuery settings (for reference)
    'bq_project': os.getenv('GCP_PROJECT_ID', 'gcp-project-id'),
    'bq_dataset': os.getenv('GCP_DATASET', 'TikiWarehouse'),
}

# Star Schema: Fact table (daily metrics)
FACT_SCHEMA = [
    'snapshot_date',
    'product_id',
    'current_price',
    'original_price',
    'discount_rate',
    'sales_volume_acc',
    'review_count',
    'rating_average',
    'inventory_status',
    'tiki_now',
    'extracted_at',
]

# Star Schema: Dimension table (products attributes)
DIM_PRODUCTS_SCHEMA = [
    'product_id',
    'sku',
    'name',
    'brand_name',
    'image_url',
    'product_url',
    'seller_id',
    'seller_name',
    'seller_logo',
    'category_id',          # LEAF category ID (deepest level)
    'root_category_id',     # ROOT category ID (Level 1) - NEW
    'category_depth',       # Category hierarchy depth - NEW
    'created_at',
    'updated_at',
]

# Star Schema: Dimension table (categories)
DIM_CATEGORIES_SCHEMA = [
    'category_id',
    'category_name',
    'category_level',       # 1=Root, 2=Sub, 3=Deep (Leaf)
    'full_path',            # Full path (e.g., "1789 > 1795")
    'url_key',
    'parent_id',            # ID of parent category
    'standard_category',    # Standardized category name (manual mapping)
]

# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging() -> logging.Logger:
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# =============================================================================
# GCS CLIENT
# =============================================================================

def get_gcs_client() -> storage.Client:
    """Initialize GCS client with service account credentials."""
    credentials_path = GCS_CONFIG['credentials_path']
    
    # Also check local development path
    if not os.path.exists(credentials_path):
        local_path = './config/google_cloud_credentials.json'
        if os.path.exists(local_path):
            credentials_path = local_path
        else:
            raise FileNotFoundError(
                f"GCS credentials not found at {GCS_CONFIG['credentials_path']} or {local_path}"
            )
    
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path
    )
    
    logger.info(f"[GCS] Initialized client with credentials from: {credentials_path}")
    return storage.Client(credentials=credentials, project=credentials.project_id)


def list_blobs_with_prefix(client: storage.Client, prefix: str) -> List[storage.Blob]:
    """List all blobs in bucket with given prefix."""
    bucket = client.bucket(GCS_CONFIG['bucket_name'])
    blobs = list(bucket.list_blobs(prefix=prefix))
    return [b for b in blobs if b.name.endswith('.json')]


def read_json_from_gcs(client: storage.Client, blob_name: str) -> List[dict]:
    """Read JSON file from GCS and return as list of dicts."""
    bucket = client.bucket(GCS_CONFIG['bucket_name'])
    blob = bucket.blob(blob_name)
    
    content = blob.download_as_text()
    data = json.loads(content)
    
    # Handle both single object and array of objects
    if isinstance(data, list):
        return data
    return [data]


def write_parquet_to_gcs(
    df: pd.DataFrame,
    client: storage.Client,
    destination_path: str,
    file_type: str = 'fact'
) -> str:
    """Write DataFrame to GCS as Parquet file with BigQuery-compatible timestamps."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    import io
    
    bucket = client.bucket(GCS_CONFIG['bucket_name'])
    blob = bucket.blob(destination_path)
    
    # Reset index to avoid __index_level_0__ column
    df = df.reset_index(drop=True)
    
    table = pa.Table.from_pandas(df)
    
    # Force timestamp columns to microseconds (BigQuery compatibility)
    schema_fields = []
    for field in table.schema:
        if pa.types.is_timestamp(field.type):
            # Convert all timestamps to microseconds with UTC timezone
            schema_fields.append(pa.field(field.name, pa.timestamp('us', tz='UTC')))
        else:
            schema_fields.append(field)
    
    new_schema = pa.schema(schema_fields)
    table = table.cast(new_schema)
    
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer, compression='snappy')
    parquet_buffer.seek(0)
    
    blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
    
    gcs_uri = f"gs://{GCS_CONFIG['bucket_name']}/{destination_path}"
    logger.info(f"[GCS] Written {file_type} Parquet ({len(df)} rows) to: {gcs_uri}")
    
    return gcs_uri

# =============================================================================
# TRANSFORMATION FUNCTIONS
# =============================================================================

def parse_sales_volume(value: Optional[str]) -> int:
    """
    Parse Vietnamese sales volume string to integer.
    
    Examples:
        "Đã bán 1.5k" -> 1500
        "Đã bán 100" -> 100
        "Đã bán 2tr" -> 2000000
        "Đã bán 1,5k" -> 1500
        "Đã bán 10.000" -> 10000 (thousand separator)
        None -> 0
    """
    if pd.isna(value) or value is None:
        return 0
    
    if isinstance(value, (int, float)):
        return int(value)
    
    text = str(value).lower().strip()
    
    # Check for suffix first (k, tr, m, etc.)
    suffix_pattern = r'(\d+(?:[.,]\d+)?)\s*(k|tr|m|trieu|triệu)'
    suffix_match = re.search(suffix_pattern, text)
    
    if suffix_match:
        # Has suffix - treat . or , as decimal separator
        number_str = suffix_match.group(1).replace(',', '.')
        suffix = suffix_match.group(2)
        
        try:
            number = float(number_str)
        except ValueError:
            return 0
        
        # Apply multiplier based on suffix
        multipliers = {
            'k': 1000,
            'tr': 1000000,
            'trieu': 1000000,
            'triệu': 1000000,
            'm': 1000000,
        }
        
        return int(number * multipliers.get(suffix, 1))
    
    # No suffix - check for thousand separator pattern (e.g., 10.000 or 1.000.000)
    # Vietnamese uses . as thousand separator
    thousand_sep_pattern = r'(\d{1,3}(?:\.\d{3})+)'
    thousand_match = re.search(thousand_sep_pattern, text)
    
    if thousand_match:
        number_str = thousand_match.group(1).replace('.', '')
        try:
            return int(number_str)
        except ValueError:
            return 0
    
    # Simple number without suffix or thousand separator
    simple_pattern = r'(\d+)'
    simple_match = re.search(simple_pattern, text)
    
    if simple_match:
        try:
            return int(simple_match.group(1))
        except ValueError:
            return 0
    
    return 0


def parse_discount_rate(value: Optional[str]) -> int:
    """
    Extract discount rate as integer from string.
    
    Examples:
        "-41%" -> 41
        "41%" -> 41
        "-25" -> 25
        None -> 0
    """
    if pd.isna(value) or value is None:
        return 0
    
    if isinstance(value, (int, float)):
        return abs(int(value))
    
    text = str(value).strip()
    match = re.search(r'(\d+)', text)
    
    if match:
        return int(match.group(1))
    
    return 0


def extract_category_id(url: Optional[str]) -> Optional[int]:
    """
    Extract category ID from Tiki category URL.
    
    Examples:
        "https://tiki.vn/dien-thoai-may-tinh-bang/c1789" -> 1789
        ".../c1789?page=2" -> 1789
        None -> None
    """
    if pd.isna(url) or url is None:
        return None
    
    # Pattern matches /c followed by digits
    pattern = r'/c(\d+)'
    match = re.search(pattern, str(url))
    
    if match:
        return int(match.group(1))
    
    return None


def clean_price(value) -> Optional[int]:
    """
    Clean price value and convert to integer.
    
    Handles:
        - Numeric values
        - Strings with currency symbols
        - Vietnamese thousand separator (.) e.g., 1.000.000 VND -> 1000000
        - Null values
    """
    if pd.isna(value) or value is None:
        return None
    
    if isinstance(value, (int, float)):
        return int(value) if not np.isnan(value) else None
    
    text = str(value).strip()
    
    # Check for Vietnamese thousand separator pattern (e.g., 1.000.000)
    # Pattern: numbers separated by dots where each group after first is exactly 3 digits
    thousand_sep_pattern = r'^[\s]*(\d{1,3}(?:\.\d{3})+)(?:\s*(?:đ|VND|₫))?[\s]*$'
    thousand_match = re.match(thousand_sep_pattern, text, re.IGNORECASE)
    
    if thousand_match:
        number_str = thousand_match.group(1).replace('.', '')
        try:
            return int(number_str)
        except ValueError:
            return None
    
    # Fallback: Remove non-numeric characters except decimal point
    cleaned = re.sub(r'[^\d.]', '', text)
    
    if not cleaned:
        return None
    
    try:
        return int(float(cleaned))
    except ValueError:
        return None


def parse_snapshot_date(extracted_at: Optional[str]) -> Optional[date]:
    """
    Parse snapshot date from _extracted_at timestamp.
    
    Examples:
        "2026-01-18T16:49:55.805Z" -> date(2026, 1, 18)
    """
    if pd.isna(extracted_at) or extracted_at is None:
        return None
    
    try:
        # Handle ISO format with Z suffix
        dt_str = str(extracted_at).replace('Z', '+00:00')
        dt = datetime.fromisoformat(dt_str.split('+')[0])
        return dt.date()
    except (ValueError, AttributeError):
        return None

# =============================================================================
# MAIN TRANSFORMATION PIPELINE
# =============================================================================

def load_raw_data(client: storage.Client, execution_date: str) -> pd.DataFrame:
    """
    Load all raw JSON files for a specific date from GCS.
    
    Args:
        client: GCS client
        execution_date: Date string in YYYY-MM-DD format
    
    Returns:
        Combined DataFrame of all products
    """
    prefix = f"{GCS_CONFIG['raw_zone_prefix']}/snapshot_date={execution_date}/"
    
    logger.info(f"[LOAD] Looking for files in: gs://{GCS_CONFIG['bucket_name']}/{prefix}")
    
    blobs = list_blobs_with_prefix(client, prefix)
    
    if not blobs:
        raise ValueError(
            f"No JSON files found in raw zone for date {execution_date}. "
            f"Path: gs://{GCS_CONFIG['bucket_name']}/{prefix}"
        )
    
    logger.info(f"[LOAD] Found {len(blobs)} JSON files to process")
    
    # Load and combine all JSON files
    all_records = []
    for blob in blobs:
        logger.info(f"[LOAD] Reading: {blob.name}")
        records = read_json_from_gcs(client, blob.name)
        all_records.extend(records)
        logger.info(f"[LOAD]   -> {len(records)} records")
    
    df = pd.DataFrame(all_records)
    logger.info(f"[LOAD] Total raw records loaded: {len(df)}")
    
    return df


def transform_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply Star Schema transformations to raw DataFrame.
    
    Args:
        df: Raw DataFrame
    
    Returns:
        Tuple of (fact_df, dim_products_df)
    """
    logger.info("[TRANSFORM] Starting Star Schema transformation...")
    initial_count = len(df)
    
    df = df.copy()
    
    # -------------------------------------------------------------------------
    # 1. Rename columns FIRST (before parsing)
    # -------------------------------------------------------------------------
    
    logger.info("[TRANSFORM] Renaming columns...")
    df = df.rename(columns={
        # DO NOT rename _category_name - it conflicts with category_name from crawler
        # '_category_name': 'category_name',  # REMOVED - keep as _category_name
        '_extracted_at': 'extracted_at',
        'thumbnail_url': 'image_url',   # Map thumbnail_url → image_url
        'seller': 'seller_name',         # Map seller → seller_name
        'brand': 'brand_name',           # Map brand → brand_name
        'quantity_sold': 'sales_volume', # Map quantity_sold → sales_volume
        'rating': 'rating_average',      # Map rating → rating_average
    })
    
    # -------------------------------------------------------------------------
    # 2. Parse and standardize columns
    # -------------------------------------------------------------------------
    
    logger.info("[TRANSFORM] Parsing snapshot_date from extracted_at...")
    df['snapshot_date'] = df['extracted_at'].apply(parse_snapshot_date)
    
    logger.info("[TRANSFORM] Converting product_id to Int64...")
    df['product_id'] = pd.to_numeric(df['product_id'], errors='coerce').astype('Int64')
    
    # Note: Crawler already provides 'sku' as string, no conversion needed
    
    logger.info("[TRANSFORM] Converting seller_id to Int64...")
    df['seller_id'] = pd.to_numeric(df['seller_id'], errors='coerce').astype('Int64')
    
    logger.info("[TRANSFORM] Cleaning price columns...")
    df['current_price'] = df['price'].apply(clean_price).astype('Int64')
    df['original_price'] = df['original_price'].apply(clean_price).astype('Int64')
    
    logger.info("[TRANSFORM] Parsing discount_rate...")
    df['discount_rate'] = df['discount_rate'].apply(parse_discount_rate).astype('Int64')
    
    logger.info("[TRANSFORM] Parsing sales_volume...")
    df['sales_volume_acc'] = df['sales_volume'].apply(parse_sales_volume).astype('Int64')
    
    logger.info("[TRANSFORM] Converting rating_average to float...")
    df['rating_average'] = pd.to_numeric(df['rating_average'], errors='coerce')
    
    logger.info("[TRANSFORM] Converting review_count to Int64...")
    df['review_count'] = pd.to_numeric(df['review_count'], errors='coerce').astype('Int64')
    
    logger.info("[TRANSFORM] Processing category fields...")
    
    # Helper function to parse category_path
    def parse_category_path(path_str):
        """Parse category_path string to extract IDs and depth.
        Example: "1815 > 28670 > 12296 > 4593" -> [1815, 28670, 12296, 4593]
        """
        try:
            if pd.isna(path_str) or not path_str:
                return []
            # Split by ' > ' and convert to int
            ids = [int(x.strip()) for x in str(path_str).split('>')]
            return ids
        except Exception:
            return []
    
    # Parse category_path if available
    if 'category_path' in df.columns:
        logger.info("  → Parsing category_path to extract hierarchy")
        df['_parsed_path'] = df['category_path'].apply(parse_category_path)
        
        # Extract leaf category_id (last element in path)
        def get_leaf_id(path_list):
            return path_list[-1] if path_list else None
        
        # Extract root category_id (first element in path)
        def get_root_id(path_list):
            return path_list[0] if path_list else None
        
        # Calculate depth (number of elements)
        def get_depth(path_list):
            return len(path_list) if path_list else 0
        
        # Apply extraction with fallback to existing values
        if 'category_id' not in df.columns or df['category_id'].isna().all():
            logger.info("  → Extracting leaf category_id from category_path")
            df['category_id'] = df['_parsed_path'].apply(get_leaf_id)
        else:
            logger.info("  → Using existing category_id, filling NULLs from category_path")
            extracted_ids = df['_parsed_path'].apply(get_leaf_id)
            df['category_id'] = df['category_id'].fillna(extracted_ids)
        
        if 'root_category_id' not in df.columns or df['root_category_id'].isna().all():
            logger.info("  → Extracting root_category_id from category_path")
            df['root_category_id'] = df['_parsed_path'].apply(get_root_id)
        else:
            logger.info("  → Using existing root_category_id, filling NULLs from category_path")
            extracted_roots = df['_parsed_path'].apply(get_root_id)
            df['root_category_id'] = df['root_category_id'].fillna(extracted_roots)
        
        if 'category_depth' not in df.columns or (df['category_depth'] == 0).all():
            logger.info("  → Calculating category_depth from category_path")
            df['category_depth'] = df['_parsed_path'].apply(get_depth)
        else:
            logger.info("  → Using existing category_depth, fixing zeros from category_path")
            extracted_depths = df['_parsed_path'].apply(get_depth)
            df.loc[df['category_depth'] == 0, 'category_depth'] = df.loc[df['category_depth'] == 0, '_parsed_path'].apply(get_depth)
        
        # Cleanup temp column
        df.drop(columns=['_parsed_path'], inplace=True)
    
    # Fallback: extract category_id from URL if still null
    if 'category_id' not in df.columns or df['category_id'].isna().any():
        logger.info("  → Fallback: Extracting category_id from URL for remaining NULLs")
        if 'category_id' not in df.columns:
            df['category_id'] = df['_category_url'].apply(extract_category_id)
        else:
            url_ids = df['_category_url'].apply(extract_category_id)
            df['category_id'] = df['category_id'].fillna(url_ids)
    
    # Convert to proper types
    logger.info("  → Converting category fields to Int64")
    df['category_id'] = pd.to_numeric(df['category_id'], errors='coerce').astype('Int64')
    if 'root_category_id' in df.columns:
        df['root_category_id'] = pd.to_numeric(df['root_category_id'], errors='coerce').astype('Int64')
    if 'category_depth' in df.columns:
        df['category_depth'] = pd.to_numeric(df['category_depth'], errors='coerce').astype('Int64')
    
    logger.info("[TRANSFORM] Determining inventory_status...")
    # Assume in stock if price exists and > 0
    df['inventory_status'] = (df['current_price'].notna()) & (df['current_price'] > 0)
    
    logger.info("[TRANSFORM] Deriving tiki_now from badges...")
    # Derive tiki_now: True if 'tiki_now' appears in badges array
    def has_tiki_now(badges):
        try:
            if pd.isna(badges):
                return False
            if badges is None:
                return False
            # Handle list, numpy array, or any iterable
            if hasattr(badges, '__iter__') and not isinstance(badges, str):
                return 'tiki_now' in list(badges)
            if isinstance(badges, str):
                return 'tiki_now' in badges.lower()
            return False
        except Exception:
            return False
    
    df['tiki_now'] = df['badges'].apply(has_tiki_now)
    
    # Convert extracted_at to datetime (was string from JSON)
    logger.info("[TRANSFORM] Converting extracted_at to datetime...")
    df['extracted_at'] = pd.to_datetime(df['extracted_at'], utc=True)
    
    # -------------------------------------------------------------------------
    # 3. Deduplication
    # -------------------------------------------------------------------------
    
    logger.info("[TRANSFORM] Deduplicating by product_id (keeping most recent)...")
    df = df.sort_values('extracted_at', ascending=False)
    before_dedup = len(df)
    df = df.drop_duplicates(subset=['product_id'], keep='first')
    duplicates_removed = before_dedup - len(df)
    logger.info(f"[TRANSFORM] Removed {duplicates_removed} duplicate records")
    
    # -------------------------------------------------------------------------
    # 4. Null validation
    # -------------------------------------------------------------------------
    
    logger.info("[TRANSFORM] Validating critical fields...")
    before_null_check = len(df)
    df = df.dropna(subset=['product_id', 'current_price'])
    nulls_removed = before_null_check - len(df)
    if nulls_removed > 0:
        logger.warning(f"[TRANSFORM] Dropped {nulls_removed} rows with NULL product_id or price")
    
    # -------------------------------------------------------------------------
    # 5. Split into Fact and Dimension tables
    # -------------------------------------------------------------------------
    
    logger.info("[TRANSFORM] Splitting into fact and dimension tables...")
    
    # FACT TABLE: Daily metrics
    fact_df = df[[
        'snapshot_date',
        'product_id',
        'current_price',
        'original_price',
        'discount_rate',
        'sales_volume_acc',
        'review_count',
        'rating_average',
        'inventory_status',
        'tiki_now',
        'extracted_at',
    ]].copy()
    
    # DIMENSION TABLE: Product attributes (deduplicated)
    dim_columns = ['product_id', 'sku', 'name', 'brand_name', 'image_url', 
                   'product_url', 'seller_id', 'seller_name', 'seller_logo', 
                   'category_id', 'category_name']
    
    if 'root_category_id' in df.columns:
        dim_columns.append('root_category_id')
    if 'category_depth' in df.columns:
        dim_columns.append('category_depth')
    
    dim_df = df[dim_columns].copy()
    
    # Convert sku to string (BigQuery schema requirement)
    dim_df['sku'] = dim_df['sku'].astype(str).replace('<NA>', None)
    
    dim_df['created_at'] = df['extracted_at']
    dim_df['updated_at'] = df['extracted_at']
    
    # DIMENSION TABLE: Categories (unique categories)
    logger.info("[TRANSFORM] Creating dim_categories...")
    
    # Select category columns and reset index to avoid alignment issues
    cat_cols = ['category_id', '_category_url']
    
    # Include category_name from crawler if available (from Category API)
    if 'category_name' in df.columns:
        cat_cols.append('category_name')
    
    if 'root_category_id' in df.columns:
        cat_cols.append('root_category_id')
    if 'category_depth' in df.columns:
        cat_cols.append('category_depth')
    
    # Create category DataFrame with selected columns
    cat_df = df[cat_cols].copy()
    logger.info(f"[DIM_CAT] Columns after copy: {list(cat_df.columns)}")
    cat_df = cat_df.reset_index(drop=True)
    
    cat_df = cat_df.dropna(subset=['category_id'])  # Remove null categories
    cat_df = cat_df.drop_duplicates(subset=['category_id'])  # Keep unique categories
    
    # Extract url_key from category URL (e.g., "dien-thoai-may-tinh-bang")
    def extract_url_key(url):
        if pd.isna(url) or url is None:
            return None
        # Pattern: /slug/c1789 -> extract "slug"
        pattern = r'/([^/]+)/c\d+'
        match = re.search(pattern, str(url))
        return match.group(1) if match else None
    
    cat_df['url_key'] = cat_df['_category_url'].apply(extract_url_key)
    
    # Ensure category_name exists (NULL if not from crawler)
    if 'category_name' not in cat_df.columns:
        cat_df['category_name'] = None
    
    # Build category_level (1=Root, 2=Sub, 3=Deep Leaf)
    if 'category_depth' in cat_df.columns:
        cat_df['category_level'] = cat_df['category_depth'].apply(lambda x: min(x, 3) if pd.notna(x) else 1).astype('Int64')
    else:
        cat_df['category_level'] = 1  # Default to root level
    
    # Build full_path (root_id > leaf_id) - using vectorized operations
    if 'root_category_id' in cat_df.columns:
        # Create mask for categories that have different root
        has_root = cat_df['root_category_id'].notna() & (cat_df['root_category_id'] != cat_df['category_id'])
        # Build path conditionally
        cat_df['full_path'] = cat_df['category_id'].astype(str)
        cat_df.loc[has_root, 'full_path'] = (
            cat_df.loc[has_root, 'root_category_id'].astype(int).astype(str) + 
            ' > ' + 
            cat_df.loc[has_root, 'category_id'].astype(int).astype(str)
        )
    else:
        cat_df['full_path'] = cat_df['category_id'].astype(str)
    
    # Build parent_id (root for sub/leaf categories, NULL for root) - using vectorized operations
    if 'root_category_id' in cat_df.columns and 'category_depth' in cat_df.columns:
        # parent_id is root_category_id for depth > 1, otherwise NULL
        cat_df['parent_id'] = None
        mask = (cat_df['category_depth'].notna()) & (cat_df['category_depth'] > 1) & (cat_df['root_category_id'].notna())
        cat_df.loc[mask, 'parent_id'] = cat_df.loc[mask, 'root_category_id'].astype('Int64')
    else:
        cat_df['parent_id'] = None
    
    # standard_category will be NULL (to be filled manually)
    cat_df['standard_category'] = None
    
    # Drop temporary columns and ensure only schema columns exist
    cat_df = cat_df.drop(columns=['_category_url'], errors='ignore')
    if 'root_category_id' in cat_df.columns and 'root_category_id' not in DIM_CATEGORIES_SCHEMA:
        cat_df = cat_df.drop(columns=['root_category_id'], errors='ignore')
    if 'category_depth' in cat_df.columns and 'category_depth' not in DIM_CATEGORIES_SCHEMA:
        cat_df = cat_df.drop(columns=['category_depth'], errors='ignore')
    
    # Select only schema columns in correct order (this removes any duplicates)
    available_cols = [col for col in DIM_CATEGORIES_SCHEMA if col in cat_df.columns]
    missing_cols = [col for col in DIM_CATEGORIES_SCHEMA if col not in cat_df.columns]
    
    # Add missing columns with NULL (only if truly missing)
    for col in missing_cols:
        if col not in cat_df.columns:  # Double-check to avoid duplicates
            logger.warning(f"[DIM_CAT] Missing column '{col}', adding as NULL")
            cat_df[col] = None
    
    # Final selection in schema order
    cat_df = cat_df[DIM_CATEGORIES_SCHEMA]
    
    # Ensure proper data types for BigQuery compatibility
    # STRING columns must be object dtype (not inferred from None/int)
    string_cols = ['category_name', 'full_path', 'url_key', 'standard_category']
    for col in string_cols:
        if col in cat_df.columns:
            cat_df[col] = cat_df[col].astype('object')  # Force STRING type in Parquet
    
    # INT64 columns (nullable integer)
    int_cols = ['category_id', 'category_level', 'parent_id']
    for col in int_cols:
        if col in cat_df.columns:
            cat_df[col] = cat_df[col].astype('Int64')  # Nullable integer
    
    if len(cat_df.columns) != len(set(cat_df.columns)):
        duplicate_cols = [col for col in cat_df.columns if list(cat_df.columns).count(col) > 1]
        logger.error(f"[DIM_CAT] DUPLICATE COLUMNS DETECTED: {list(cat_df.columns)}")
        logger.error(f"[DIM_CAT] Duplicates: {set(duplicate_cols)}")
        cat_df = cat_df.loc[:, ~cat_df.columns.duplicated()]
        logger.info(f"[DIM_CAT] After removing duplicates: {list(cat_df.columns)}")
    
    logger.info(f"[TRANSFORM] Extracted {len(cat_df)} unique categories")
    
    for col in FACT_SCHEMA:
        if col not in fact_df.columns:
            logger.warning(f"[FACT] Missing column '{col}', adding as NULL")
            fact_df[col] = None
    fact_df = fact_df[FACT_SCHEMA]
    
    for col in DIM_PRODUCTS_SCHEMA:
        if col not in dim_df.columns:
            logger.warning(f"[DIM] Missing column '{col}', adding as NULL")
            dim_df[col] = None
    dim_df = dim_df[DIM_PRODUCTS_SCHEMA]
    
    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    
    logger.info(f"[TRANSFORM] Transformation complete:")
    logger.info(f"[TRANSFORM]   - Initial records: {initial_count}")
    logger.info(f"[TRANSFORM]   - Duplicates removed: {duplicates_removed}")
    logger.info(f"[TRANSFORM]   - Nulls removed: {nulls_removed}")
    logger.info(f"[TRANSFORM]   - Fact records: {len(fact_df)}")
    logger.info(f"[TRANSFORM]   - Dimension records: {len(dim_df)}")
    logger.info(f"[TRANSFORM]   - Category records: {len(cat_df)}")
    
    return fact_df, dim_df, cat_df


def save_to_clean_zone(
    fact_df: pd.DataFrame,
    dim_df: pd.DataFrame,
    cat_df: pd.DataFrame,
    client: storage.Client,
    execution_date: str
) -> Tuple[str, str, str]:
    """
    Save transformed DataFrames to GCS Clean Zone as Parquet.
    
    Args:
        fact_df: Fact table DataFrame
        dim_df: Dimension table DataFrame
        cat_df: Categories table DataFrame
        client: GCS client
        execution_date: Date string in YYYY-MM-DD format
    
    Returns:
        Tuple of (fact_uri, dim_uri, cat_uri)
    """
    # Fact table with partition by snapshot_date
    fact_path = (
        f"{GCS_CONFIG['clean_zone_prefix']}/"
        f"snapshot_date={execution_date}/"
        f"part-001.parquet"
    )
    
    # Dimension table (no partition, or partition by date for SCD Type 2)
    dim_path = (
        f"clean_zone/tiki/dim_products/"
        f"snapshot_date={execution_date}/"
        f"part-001.parquet"
    )
    
    # Categories dimension table
    cat_path = (
        f"clean_zone/tiki/dim_categories/"
        f"snapshot_date={execution_date}/"
        f"part-001.parquet"
    )
    
    logger.info(f"[SAVE] Writing {len(fact_df)} fact records...")
    fact_uri = write_parquet_to_gcs(fact_df, client, fact_path, file_type='fact')
    
    logger.info(f"[SAVE] Writing {len(dim_df)} dimension records...")
    dim_uri = write_parquet_to_gcs(dim_df, client, dim_path, file_type='dimension')
    
    logger.info(f"[SAVE] Writing {len(cat_df)} category records...")
    cat_uri = write_parquet_to_gcs(cat_df, client, cat_path, file_type='categories')
    
    return fact_uri, dim_uri, cat_uri

# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def run_transformation(execution_date: str) -> dict:
    """
    Main transformation pipeline.
    
    Args:
        execution_date: Date to process in YYYY-MM-DD format
    
    Returns:
        Dict with results
    """
    logger.info("=" * 60)
    logger.info("TIKI DATA TRANSFORMATION PIPELINE")
    logger.info("Vietnam E-commerce Analytics Platform - Phase 1")
    logger.info("=" * 60)
    logger.info(f"Execution Date: {execution_date}")
    logger.info(f"Bucket: {GCS_CONFIG['bucket_name']}")
    logger.info("=" * 60)
    
    try:
        client = get_gcs_client()
        raw_df = load_raw_data(client, execution_date)
        fact_df, dim_df, cat_df = transform_data(raw_df)
        
        if fact_df.empty:
            raise ValueError("Transformation resulted in empty fact DataFrame")
        
        fact_uri, dim_uri, cat_uri = save_to_clean_zone(fact_df, dim_df, cat_df, client, execution_date)
        
        logger.info("=" * 60)
        logger.info("[SUCCESS] Pipeline completed successfully!")
        logger.info(f"[SUCCESS] Fact output: {fact_uri}")
        logger.info(f"[SUCCESS] Fact records: {len(fact_df)}")
        logger.info(f"[SUCCESS] Dimension output: {dim_uri}")
        logger.info(f"[SUCCESS] Dimension records: {len(dim_df)}")
        logger.info(f"[SUCCESS] Categories output: {cat_uri}")
        logger.info(f"[SUCCESS] Category records: {len(cat_df)}")
        logger.info("=" * 60)
        
        return {
            'status': 'success',
            'fact_uri': fact_uri,
            'dim_uri': dim_uri,
            'cat_uri': cat_uri,
            'fact_records': len(fact_df),
            'dim_records': len(dim_df),
            'cat_records': len(cat_df),
        }
        
    except Exception as e:
        import traceback
        logger.error(f"[ERROR] Pipeline failed: {str(e)}")
        logger.error(f"[ERROR] Traceback: {traceback.format_exc()}")
        raise


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Transform Tiki raw data from GCS to Parquet'
    )
    parser.add_argument(
        '--date',
        type=str,
        default=datetime.now().strftime('%Y-%m-%d'),
        help='Execution date in YYYY-MM-DD format (default: today)'
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_arguments()
    
    try:
        result = run_transformation(args.date)
        print(f"\nFact Output: {result['fact_uri']}")
        print(f"Fact Records: {result['fact_records']}")
        print(f"Dimension Output: {result['dim_uri']}")
        print(f"Dimension Records: {result['dim_records']}")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        print(f"\nERROR: {str(e)}", file=sys.stderr)
        sys.exit(1)
