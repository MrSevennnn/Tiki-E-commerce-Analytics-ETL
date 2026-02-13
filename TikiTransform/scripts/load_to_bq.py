"""
BigQuery Load Script for Tiki Daily Snapshots (Star Schema)
Vietnam E-commerce Analytics Platform - Phase 1: Load

This script loads daily Parquet files from GCS Clean Zone into BigQuery.
Handles both fact table (partitioned) and dimension table (merge).

Usage:
    python load_to_bq.py --execution_date 2026-01-21
    python load_to_bq.py  # Uses today's date
"""

import os
import sys
import logging
import argparse
from datetime import datetime, date

from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account

# =============================================================================
# CONFIGURATION
# =============================================================================

CONFIG = {
    # GCS Configuration
    'gcs_bucket': os.getenv('GCS_BUCKET_NAME', 'your-gcs-bucket-name'),
    'gcs_fact_zone_prefix': 'clean_zone/tiki/fact_daily_snapshot',
    'gcs_dim_zone_prefix': 'clean_zone/tiki/dim_products',
    
    # BigQuery Configuration
    'bq_project': os.getenv('GCP_PROJECT_ID', 'gcp-project-id'),
    'bq_dataset': os.getenv('GCP_DATASET', 'TikiWarehouse'),
    'bq_fact_table': 'fact_daily_snapshot',
    'bq_dim_table': 'dim_products',
    
    # Credentials
    'credentials_path': os.getenv('GCP_CREDENTIALS_PATH', '/opt/airflow/config/google_cloud_credentials.json'),
}

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
# CREDENTIALS & CLIENTS
# =============================================================================

def get_credentials():
    """Get Google Cloud credentials from service account file."""
    credentials_path = CONFIG['credentials_path']
    
    # Check for local development path
    if not os.path.exists(credentials_path):
        local_paths = [
            './config/google_cloud_credentials.json',
            '../config/google_cloud_credentials.json',
            '../../google_cloud_credentials.json',
        ]
        for local_path in local_paths:
            if os.path.exists(local_path):
                credentials_path = local_path
                break
        else:
            raise FileNotFoundError(
                f"Credentials not found at {CONFIG['credentials_path']} or local paths"
            )
    
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path
    )
    logger.info(f"[CREDENTIALS] Loaded from: {credentials_path}")
    return credentials


def get_gcs_client(credentials) -> storage.Client:
    """Initialize GCS client."""
    return storage.Client(credentials=credentials, project=credentials.project_id)


def get_bq_client(credentials) -> bigquery.Client:
    """Initialize BigQuery client."""
    return bigquery.Client(credentials=credentials, project=CONFIG['bq_project'])

# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================

def validate_gcs_path_has_files(gcs_client: storage.Client, gcs_uri: str) -> int:
    """
    Check if the GCS path contains any Parquet files.
    Returns the count of files found.
    Raises ValueError if no files found.
    """
    if not gcs_uri.startswith('gs://'):
        raise ValueError(f"Invalid GCS URI: {gcs_uri}")
    
    path = gcs_uri[5:]  # Remove 'gs://'
    bucket_name = path.split('/')[0]
    
    prefix_with_pattern = '/'.join(path.split('/')[1:])
    # Remove everything after last slash if it contains wildcard
    if '*' in prefix_with_pattern:
        prefix = '/'.join(prefix_with_pattern.split('/')[:-1]) + '/'
    else:
        prefix = prefix_with_pattern.rstrip('/')
    
    bucket = gcs_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    parquet_files = [b for b in blobs if b.name.endswith('.parquet')]
    
    if not parquet_files:
        raise ValueError(
            f"[ERROR] No Parquet files found at: {gcs_uri}\n"
            f"This indicates the upstream Transform job did not produce data for this date.\n"
            f"Please check the Transform logs before re-running."
        )
    
    logger.info(f"[VALIDATION] Found {len(parquet_files)} Parquet file(s) at {gcs_uri}")
    for f in parquet_files:
        logger.info(f"  - {f.name} ({f.size:,} bytes)")
    
    return len(parquet_files)

# =============================================================================
# FACT TABLE LOAD (Partitioned with WRITE_TRUNCATE)
# =============================================================================

def load_fact_table(
    bq_client: bigquery.Client,
    gcs_client: storage.Client,
    execution_date: str
) -> dict:
    """
    Load fact table to BigQuery partition using WRITE_TRUNCATE (idempotent).
    
    Args:
        bq_client: BigQuery client
        gcs_client: GCS client
        execution_date: Date string in YYYY-MM-DD format
    
    Returns:
        dict with job statistics
    """
    exec_date = datetime.strptime(execution_date, '%Y-%m-%d').date()
    partition_date = exec_date.strftime('%Y%m%d')
    
    gcs_source_uri = (
        f"gs://{CONFIG['gcs_bucket']}/"
        f"{CONFIG['gcs_fact_zone_prefix']}/"
        f"snapshot_date={execution_date}/*.parquet"
    )
    
    # Partition decorator: table$YYYYMMDD
    bq_table_ref = (
        f"{CONFIG['bq_project']}."
        f"{CONFIG['bq_dataset']}."
        f"{CONFIG['bq_fact_table']}${partition_date}"
    )
    
    logger.info("=" * 60)
    logger.info("[FACT] Loading fact_daily_snapshot")
    logger.info("=" * 60)
    logger.info(f"[FACT] Source: {gcs_source_uri}")
    logger.info(f"[FACT] Target: {bq_table_ref}")
    logger.info(f"[FACT] Mode: WRITE_TRUNCATE (Idempotent)")
    
    validate_gcs_path_has_files(gcs_client, gcs_source_uri)
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    
    logger.info("[FACT] Starting load job...")
    load_job = bq_client.load_table_from_uri(
        gcs_source_uri,
        bq_table_ref,
        job_config=job_config,
    )
    
    logger.info(f"[FACT] Job ID: {load_job.job_id}")
    load_job.result()  # Wait for completion
    
    # Get row count for this partition
    count_query = f"""
    SELECT COUNT(*) as row_count
    FROM `{CONFIG['bq_project']}.{CONFIG['bq_dataset']}.{CONFIG['bq_fact_table']}`
    WHERE snapshot_date = '{execution_date}'
    """
    count_result = bq_client.query(count_query).result()
    rows_loaded = list(count_result)[0].row_count
    
    logger.info(f"[FACT] ✓ Loaded {rows_loaded:,} rows to partition")
    
    return {
        'table': 'fact_daily_snapshot',
        'rows_loaded': rows_loaded,
        'job_id': load_job.job_id,
    }

# =============================================================================
# DIMENSION TABLE LOAD (Merge/Upsert)
# =============================================================================

def load_dimension_table(
    bq_client: bigquery.Client,
    gcs_client: storage.Client,
    execution_date: str
) -> dict:
    """
    Load dimension table using MERGE (upsert) strategy.
    
    Args:
        bq_client: BigQuery client
        gcs_client: GCS client
        execution_date: Date string in YYYY-MM-DD format
    
    Returns:
        dict with job statistics
    """
    gcs_source_uri = (
        f"gs://{CONFIG['gcs_bucket']}/"
        f"{CONFIG['gcs_dim_zone_prefix']}/"
        f"snapshot_date={execution_date}/*.parquet"
    )
    
    temp_table_id = f"{CONFIG['bq_dataset']}.temp_dim_products_{execution_date.replace('-', '')}"
    target_table_id = f"{CONFIG['bq_dataset']}.{CONFIG['bq_dim_table']}"
    
    logger.info("=" * 60)
    logger.info("[DIM] Loading dim_products")
    logger.info("=" * 60)
    logger.info(f"[DIM] Source: {gcs_source_uri}")
    logger.info(f"[DIM] Target: {CONFIG['bq_project']}.{target_table_id}")
    logger.info(f"[DIM] Mode: MERGE (Upsert)")
    
    validate_gcs_path_has_files(gcs_client, gcs_source_uri)
    
    # Step 1: Load to temporary table
    logger.info(f"[DIM] Step 1: Loading to temp table {temp_table_id}...")
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    
    load_job = bq_client.load_table_from_uri(
        gcs_source_uri,
        temp_table_id,
        job_config=job_config,
    )
    load_job.result()
    logger.info(f"[DIM] ✓ Loaded to temp table")
    
    # Step 2: MERGE into target table
    logger.info("[DIM] Step 2: Merging into target table...")
    merge_query = f"""
    MERGE `{CONFIG['bq_project']}.{target_table_id}` T
    USING `{CONFIG['bq_project']}.{temp_table_id}` S
    ON T.product_id = S.product_id
    WHEN MATCHED THEN
        UPDATE SET
            sku = S.sku,
            name = S.name,
            brand_name = S.brand_name,
            image_url = S.image_url,
            product_url = S.product_url,
            seller_id = S.seller_id,
            seller_name = S.seller_name,
            seller_logo = S.seller_logo,
            category_id = S.category_id,
            updated_at = S.updated_at
    WHEN NOT MATCHED THEN
        INSERT (
            product_id, sku, name, brand_name, image_url, product_url,
            seller_id, seller_name, seller_logo, category_id,
            created_at, updated_at
        )
        VALUES (
            S.product_id, S.sku, S.name, S.brand_name, S.image_url, S.product_url,
            S.seller_id, S.seller_name, S.seller_logo, S.category_id,
            S.created_at, S.updated_at
        )
    """
    
    merge_job = bq_client.query(merge_query)
    merge_result = merge_job.result()
    
    logger.info(f"[DIM] ✓ Merge completed")
    
    # Step 3: Cleanup temp table
    logger.info("[DIM] Step 3: Cleaning up temp table...")
    bq_client.delete_table(temp_table_id, not_found_ok=True)
    logger.info("[DIM] ✓ Cleanup complete")
    
    count_query = f"""
    SELECT COUNT(*) as row_count
    FROM `{CONFIG['bq_project']}.{target_table_id}`
    """
    count_result = bq_client.query(count_query).result()
    total_rows = list(count_result)[0].row_count
    
    logger.info(f"[DIM] ✓ Total products in dimension: {total_rows:,}")
    
    return {
        'table': 'dim_products',
        'total_rows': total_rows,
        'merge_job_id': merge_job.job_id,
    }


def load_categories_table(
    bq_client: bigquery.Client,
    gcs_client: storage.Client,
    execution_date: str
) -> dict:
    """
    Load dim_categories using MERGE (upsert).
    
    Args:
        bq_client: BigQuery client
        gcs_client: GCS client
        execution_date: Date string YYYY-MM-DD
    
    Returns:
        dict with results
    """
    exec_date = datetime.strptime(execution_date, '%Y-%m-%d').date()
    date_partition = exec_date.strftime('%Y%m%d')
    
    gcs_source_uri = (
        f"gs://{CONFIG['gcs_bucket']}/clean_zone/tiki/dim_categories/"
        f"snapshot_date={execution_date}/*.parquet"
    )
    target_table_id = f"{CONFIG['bq_dataset']}.dim_categories"
    temp_table_id = f"{CONFIG['bq_dataset']}.temp_dim_categories_{date_partition}"
    
    logger.info("=" * 60)
    logger.info("[CAT] Loading dim_categories")
    logger.info("=" * 60)
    logger.info(f"[CAT] Source: {gcs_source_uri}")
    logger.info(f"[CAT] Target: {CONFIG['bq_project']}.{target_table_id}")
    logger.info(f"[CAT] Mode: MERGE (Upsert)")
    
    validate_gcs_path_has_files(gcs_client, gcs_source_uri)
    
    # Step 1: Load to temporary table
    logger.info(f"[CAT] Step 1: Loading to temp table {temp_table_id}...")
    
    # Define explicit schema to avoid type inference issues
    schema = [
        bigquery.SchemaField("category_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("category_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category_level", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("full_path", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("url_key", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("parent_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("standard_category", "STRING", mode="NULLABLE"),
    ]
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,  # Explicit schema
    )
    
    load_job = bq_client.load_table_from_uri(
        gcs_source_uri,
        temp_table_id,
        job_config=job_config,
    )
    load_job.result()
    logger.info(f"[CAT] ✓ Loaded to temp table")
    
    # Step 2: MERGE into target table (HYBRID: prioritize manual category_name)
    logger.info("[CAT] Step 2: Merging into target table (HYBRID mode)...")
    merge_query = f"""
    MERGE `{CONFIG['bq_project']}.{target_table_id}` T
    USING `{CONFIG['bq_project']}.{temp_table_id}` S
    ON T.category_id = S.category_id
    WHEN MATCHED THEN
        UPDATE SET
            -- HYBRID: Keep manual name if exists, otherwise use crawler name
            category_name = COALESCE(T.category_name, S.category_name),
            category_level = S.category_level,
            full_path = S.full_path,
            url_key = S.url_key,
            parent_id = S.parent_id,
            -- Keep manual standard_category (crawler never provides this)
            standard_category = COALESCE(T.standard_category, S.standard_category)
    WHEN NOT MATCHED THEN
        INSERT (
            category_id, category_name, category_level, full_path, 
            url_key, parent_id, standard_category
        )
        VALUES (
            S.category_id, S.category_name, S.category_level, S.full_path,
            S.url_key, S.parent_id, S.standard_category
        )
    """
    
    merge_job = bq_client.query(merge_query)
    merge_result = merge_job.result()
    
    logger.info(f"[CAT] ✓ Merge completed")
    
    # Step 3: Cleanup temp table
    logger.info("[CAT] Step 3: Cleaning up temp table...")
    bq_client.delete_table(temp_table_id, not_found_ok=True)
    logger.info("[CAT] ✓ Cleanup complete")
    
    count_query = f"""
    SELECT COUNT(*) as row_count
    FROM `{CONFIG['bq_project']}.{target_table_id}`
    """
    count_result = bq_client.query(count_query).result()
    total_rows = list(count_result)[0].row_count
    
    logger.info(f"[CAT] ✓ Total categories in dimension: {total_rows:,}")
    
    return {
        'table': 'dim_categories',
        'total_rows': total_rows,
        'merge_job_id': merge_job.job_id,
    }

# =============================================================================
# MAIN LOAD ORCHESTRATION
# =============================================================================

def load_all_tables(
    execution_date: str,
    dry_run: bool = False
) -> dict:
    """
    Load both fact and dimension tables.
    
    Args:
        execution_date: Date string in YYYY-MM-DD format
        dry_run: If True, validate but don't execute load
    
    Returns:
        dict with results
    """
    # Validate date format
    try:
        exec_date = datetime.strptime(execution_date, '%Y-%m-%d').date()
    except ValueError:
        raise ValueError(f"Invalid date format: {execution_date}. Expected YYYY-MM-DD")
    
    logger.info("=" * 60)
    logger.info("[LOAD] BigQuery Star Schema Load")
    logger.info("=" * 60)
    logger.info(f"[CONFIG] Execution Date: {execution_date}")
    logger.info(f"[CONFIG] Project: {CONFIG['bq_project']}")
    logger.info(f"[CONFIG] Dataset: {CONFIG['bq_dataset']}")
    logger.info(f"[CONFIG] Bucket: {CONFIG['gcs_bucket']}")
    logger.info("=" * 60)
    
    credentials = get_credentials()
    gcs_client = get_gcs_client(credentials)
    bq_client = get_bq_client(credentials)
    
    if dry_run:
        logger.info("[DRY-RUN] Validation mode - no data will be loaded")
        # Just validate files exist
        fact_uri = f"gs://{CONFIG['gcs_bucket']}/{CONFIG['gcs_fact_zone_prefix']}/snapshot_date={execution_date}/*.parquet"
        dim_uri = f"gs://{CONFIG['gcs_bucket']}/{CONFIG['gcs_dim_zone_prefix']}/snapshot_date={execution_date}/*.parquet"
        validate_gcs_path_has_files(gcs_client, fact_uri)
        validate_gcs_path_has_files(gcs_client, dim_uri)
        logger.info("[DRY-RUN] ✓ Validation passed")
        return {'status': 'dry_run'}
    
    results = {}
    
    try:
        fact_result = load_fact_table(bq_client, gcs_client, execution_date)
        results['fact'] = fact_result
    except Exception as e:
        logger.error(f"[FACT] Load failed: {e}")
        raise
    
    try:
        dim_result = load_dimension_table(bq_client, gcs_client, execution_date)
        results['dimension'] = dim_result
    except Exception as e:
        logger.error(f"[DIM] Load failed: {e}")
        raise
    
    try:
        cat_result = load_categories_table(bq_client, gcs_client, execution_date)
        results['categories'] = cat_result
    except Exception as e:
        logger.error(f"[CAT] Load failed: {e}")
        raise
    
    logger.info("=" * 60)
    logger.info("[SUCCESS] All tables loaded successfully!")
    logger.info("=" * 60)
    logger.info(f"[FACT] {results['fact']['rows_loaded']:,} rows loaded to partition")
    logger.info(f"[DIM] {results['dimension']['total_rows']:,} total products")
    logger.info(f"[CAT] {results['categories']['total_rows']:,} total categories")
    logger.info("=" * 60)
    
    return {
        'status': 'success',
        'execution_date': execution_date,
        'results': results,
    }

# =============================================================================
# CLI INTERFACE
# =============================================================================

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Load Tiki daily snapshot from GCS to BigQuery (Star Schema)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Load specific date
    python load_to_bq.py --execution_date 2026-01-21
    
    # Load today's data
    python load_to_bq.py
    
    # Dry run (validate without loading)
    python load_to_bq.py --execution_date 2026-01-21 --dry-run
        """
    )
    
    parser.add_argument(
        '--execution_date', '-d',
        type=str,
        default=date.today().strftime('%Y-%m-%d'),
        help='Execution date in YYYY-MM-DD format (default: today)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate source files without executing load'
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_arguments()
    
    logger.info("[START] BigQuery Load Script")
    logger.info(f"[PARAM] execution_date: {args.execution_date}")
    logger.info(f"[PARAM] dry_run: {args.dry_run}")
    
    try:
        result = load_all_tables(
            execution_date=args.execution_date,
            dry_run=args.dry_run,
        )
        
        logger.info("[COMPLETE] Load finished successfully")
        sys.exit(0)
            
    except ValueError as e:
        logger.error(f"[FAILED] Validation error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"[FAILED] Unexpected error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == '__main__':
    main()
