"""
BigQuery Load Integration Test
Vietnam E-commerce Analytics Platform - Phase 1: Load Validation

This script tests the GCS -> BigQuery load pipeline using a temporary
test table to ensure connectivity and permissions before production use.

IMPORTANT: This script does NOT touch the production fact_daily_snapshot table.

Usage:
    python test_load_integration.py
"""

import os
import sys
import uuid
import logging
import io
from datetime import date
from typing import Optional

import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account

# =============================================================================
# CONFIGURATION
# =============================================================================

CONFIG = {
    # GCS Configuration
    'gcs_bucket': os.getenv('GCS_BUCKET_NAME', 'your-gcs-bucket-name'),
    'gcs_test_zone_prefix': 'test_zone',
    
    # BigQuery Configuration
    'bq_project': os.getenv('GCP_PROJECT_ID', 'your-gcp-project-id'),  # Real project ID from credentials
    'bq_dataset': os.getenv('GCP_DATASET', 'TikiWarehouse'),  # Real dataset name from BigQuery
    'bq_test_table': 'debug_load_test',  # Temporary test table
    
    # Credentials
    'credentials_path': os.getenv('GCP_CREDENTIALS_PATH', '/home/airflow_admin/tiki-airflow/config/google_cloud_credentials.json'),
}

# Schema matching Star Schema fact_daily_snapshot
TEST_FACT_SCHEMA = [
    bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("product_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("current_price", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("original_price", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("discount_rate", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("sales_volume_acc", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("review_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("rating_average", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("inventory_status", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("tiki_now", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="NULLABLE"),
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
            os.path.join(os.path.dirname(__file__), '../../google_cloud_credentials.json'),
        ]
        for local_path in local_paths:
            abs_path = os.path.abspath(local_path)
            if os.path.exists(abs_path):
                credentials_path = abs_path
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
# TEST DATA GENERATION
# =============================================================================

def generate_test_data() -> pd.DataFrame:
    """
    Generate a tiny DataFrame (1 row) with Star Schema fact table schema.
    """
    test_data = {
        'snapshot_date': [date.today()],
        'product_id': [999999999],  # Unique test ID
        'current_price': [1000000],
        'original_price': [1500000],
        'discount_rate': [33],
        'sales_volume_acc': [100],
        'review_count': [50],
        'rating_average': [4.5],
        'inventory_status': [True],
        'tiki_now': [True],
        'extracted_at': [pd.Timestamp.now(tz='UTC')],
    }
    
    df = pd.DataFrame(test_data)
    
    # Ensure correct types
    df['product_id'] = df['product_id'].astype('int64')
    df['current_price'] = df['current_price'].astype('int64')
    df['original_price'] = df['original_price'].astype('int64')
    df['discount_rate'] = df['discount_rate'].astype('int64')
    df['sales_volume_acc'] = df['sales_volume_acc'].astype('int64')
    df['rating_average'] = df['rating_average'].astype('float64')
    df['review_count'] = df['review_count'].astype('int64')
    df['inventory_status'] = df['inventory_status'].astype('bool')
    df['tiki_now'] = df['tiki_now'].astype('bool')
    
    logger.info(f"[TEST DATA] Generated {len(df)} row(s)")
    logger.info(f"[TEST DATA] Schema: {list(df.columns)}")
    
    return df

# =============================================================================
# GCS OPERATIONS
# =============================================================================

def upload_test_parquet_to_gcs(
    gcs_client: storage.Client,
    df: pd.DataFrame,
    test_id: str
) -> str:
    """
    Upload test DataFrame as Parquet to GCS test zone.
    Returns the GCS URI.
    """
    blob_name = f"{CONFIG['gcs_test_zone_prefix']}/temp_load_test_{test_id}.parquet"
    gcs_uri = f"gs://{CONFIG['gcs_bucket']}/{blob_name}"
    
    # Convert timestamp to microseconds (BigQuery compatibility)
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    # Create Arrow Table with TIMESTAMP_MICROS
    table = pa.Table.from_pandas(df)
    
    # Convert extracted_at column to microseconds
    schema_fields = []
    for field in table.schema:
        if field.name == 'extracted_at' and pa.types.is_timestamp(field.type):
            # Force microsecond precision
            schema_fields.append(pa.field('extracted_at', pa.timestamp('us', tz='UTC')))
        else:
            schema_fields.append(field)
    
    new_schema = pa.schema(schema_fields)
    table = table.cast(new_schema)
    
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer, compression='snappy')
    parquet_buffer.seek(0)
    
    bucket = gcs_client.bucket(CONFIG['gcs_bucket'])
    blob = bucket.blob(blob_name)
    blob.upload_from_file(parquet_buffer, content_type='application/octet-stream')
    
    logger.info(f"[GCS] Uploaded test Parquet to: {gcs_uri}")
    
    return gcs_uri


def delete_test_parquet_from_gcs(
    gcs_client: storage.Client,
    test_id: str
) -> bool:
    """Delete the temporary test Parquet file from GCS."""
    blob_name = f"{CONFIG['gcs_test_zone_prefix']}/temp_load_test_{test_id}.parquet"
    
    try:
        bucket = gcs_client.bucket(CONFIG['gcs_bucket'])
        blob = bucket.blob(blob_name)
        blob.delete()
        logger.info(f"[CLEANUP] Deleted GCS file: {blob_name}")
        return True
    except Exception as e:
        logger.warning(f"[CLEANUP] Failed to delete GCS file: {e}")
        return False

# =============================================================================
# BIGQUERY OPERATIONS
# =============================================================================

def load_test_data_to_bigquery(
    bq_client: bigquery.Client,
    gcs_uri: str
) -> str:
    """
    Load test Parquet file into a temporary BigQuery table.
    Returns the full table reference.
    """
    table_ref = f"{CONFIG['bq_project']}.{CONFIG['bq_dataset']}.{CONFIG['bq_test_table']}"
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=TEST_FACT_SCHEMA,
    )
    
    logger.info(f"[BQ] Loading test data to: {table_ref}")
    
    # Execute load job
    load_job = bq_client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config,
    )
    
    logger.info(f"[BQ] Job ID: {load_job.job_id}")
    load_job.result()  # Waits for job to finish
    
    logger.info(f"[BQ] Load job completed successfully")
    
    return table_ref


def verify_loaded_data(bq_client: bigquery.Client) -> int:
    """
    Query the test table to verify data was loaded correctly.
    Returns the row count.
    """
    table_ref = f"{CONFIG['bq_project']}.{CONFIG['bq_dataset']}.{CONFIG['bq_test_table']}"
    
    query = f"SELECT COUNT(*) as row_count FROM `{table_ref}`"
    
    logger.info(f"[VERIFY] Running count query on: {table_ref}")
    
    result = bq_client.query(query).result()
    row_count = list(result)[0].row_count
    
    logger.info(f"[VERIFY] Row count: {row_count}")
    
    return row_count


def delete_test_table(bq_client: bigquery.Client) -> bool:
    """Delete the temporary test table from BigQuery."""
    table_ref = f"{CONFIG['bq_project']}.{CONFIG['bq_dataset']}.{CONFIG['bq_test_table']}"
    
    try:
        bq_client.delete_table(table_ref, not_found_ok=True)
        logger.info(f"[CLEANUP] Deleted BigQuery table: {table_ref}")
        return True
    except Exception as e:
        logger.warning(f"[CLEANUP] Failed to delete BigQuery table: {e}")
        return False

# =============================================================================
# MAIN TEST FUNCTION
# =============================================================================

def run_integration_test() -> bool:
    """
    Run the full integration test.
    
    Steps:
    1. Generate dummy data
    2. Upload to GCS
    3. Load into BigQuery test table
    4. Verify row count
    5. Cleanup (delete table and GCS file)
    
    Returns True if all tests pass, False otherwise.
    """
    test_id = str(uuid.uuid4())[:8]
    gcs_uri: Optional[str] = None
    test_passed = False
    
    logger.info("=" * 60)
    logger.info("[TEST] BigQuery Load Integration Test")
    logger.info("=" * 60)
    logger.info(f"[TEST] Test ID: {test_id}")
    logger.info(f"[TEST] Bucket: {CONFIG['gcs_bucket']}")
    logger.info(f"[TEST] Project: {CONFIG['bq_project']}")
    logger.info(f"[TEST] Dataset: {CONFIG['bq_dataset']}")
    logger.info(f"[TEST] Test Table: {CONFIG['bq_test_table']}")
    logger.info("=" * 60)
    
    try:
        # Step 0: Initialize clients
        logger.info("\n[STEP 0] Initializing GCS and BigQuery clients...")
        credentials = get_credentials()
        gcs_client = get_gcs_client(credentials)
        bq_client = get_bq_client(credentials)
        logger.info("[STEP 0] Clients initialized successfully")
        
        # Step 1: Generate test data
        logger.info("\n[STEP 1] Generating test data...")
        df = generate_test_data()
        logger.info("[STEP 1] Test data generated")
        
        # Step 2: Upload to GCS
        logger.info("\n[STEP 2] Uploading test Parquet to GCS...")
        gcs_uri = upload_test_parquet_to_gcs(gcs_client, df, test_id)
        logger.info(f"[STEP 2] Upload complete: {gcs_uri}")
        
        # Step 3: Load into BigQuery
        logger.info("\n[STEP 3] Loading data into BigQuery test table...")
        table_ref = load_test_data_to_bigquery(bq_client, gcs_uri)
        logger.info(f"[STEP 3] Load complete: {table_ref}")
        
        # Step 4: Verify
        logger.info("\n[STEP 4] Verifying loaded data...")
        row_count = verify_loaded_data(bq_client)
        
        if row_count == 1:
            logger.info("[STEP 4] Verification PASSED: Row count = 1")
            test_passed = True
        else:
            logger.error(f"[STEP 4] Verification FAILED: Expected 1 row, got {row_count}")
            test_passed = False
        
    except Exception as e:
        logger.error(f"[TEST FAILED] Error during test: {e}")
        logger.exception("Full traceback:")
        test_passed = False
    
    finally:
        # Step 5: Cleanup (always runs)
        logger.info("\n[STEP 5] Cleaning up test resources...")
        
        try:
            credentials = get_credentials()
            gcs_client = get_gcs_client(credentials)
            bq_client = get_bq_client(credentials)
            
            # Delete BigQuery table
            delete_test_table(bq_client)
            
            # Delete GCS file
            delete_test_parquet_from_gcs(gcs_client, test_id)
            
            logger.info("[STEP 5] Cleanup complete")
        except Exception as cleanup_error:
            logger.warning(f"[STEP 5] Cleanup error (non-fatal): {cleanup_error}")
    
    # Final result
    logger.info("\n" + "=" * 60)
    if test_passed:
        logger.info("[SUCCESS] BigQuery Connection & Load OK")
        logger.info("=" * 60)
        logger.info("")
        logger.info("  All integration tests passed!")
        logger.info("  The GCS -> BigQuery load pipeline is working correctly.")
        logger.info("  You can now safely run the production load script.")
        logger.info("")
    else:
        logger.error("[FAILED] Integration test did not pass")
        logger.info("=" * 60)
        logger.error("")
        logger.error("  Please check the error messages above.")
        logger.error("  Common issues:")
        logger.error("    - Invalid credentials")
        logger.error("    - Missing BigQuery permissions")
        logger.error("    - Dataset does not exist")
        logger.error("    - GCS bucket access denied")
        logger.error("")
    logger.info("=" * 60)
    
    return test_passed


def main():
    """Main entry point."""
    success = run_integration_test()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
