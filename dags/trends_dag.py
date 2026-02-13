
"""

Features:
- Dynamic keyword fetching from BigQuery
- Smart anti-block protection (exponential backoff, jitter, User-Agent rotation)
- Batch processing (5 keywords per batch - Google Trends limit)
- Partial success handling (skip failed batches, continue with rest)
- Transform: Wide to Long format, handle '<1' scores, type casting
- GCS staging (Parquet format) + BigQuery loading
- Comprehensive error handling and logging

Workflow:
  get_keywords → fetch_trends_data → transform_trends → upload_to_gcs → load_to_staging → merge_to_fact

Upsert Pattern (Staging & MERGE):
  - Rolling window fetch (30 days) can cause duplicates with simple APPEND
  - Solution: Load to staging table (TRUNCATE) → MERGE into fact table
  - MERGE handles: UPDATE existing (date, keyword) pairs, INSERT new ones
  - This ensures idempotent loads and allows Google to revise historical scores
"""

import os
import sys
import time
import random
import hashlib
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any

import pandas as pd
from pytrends.request import TrendReq

sys.path.insert(0, '/opt/airflow/dags')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

scripts_path = '/opt/airflow/scripts/TikiTransform/scripts'
if os.path.exists(scripts_path):
    sys.path.insert(0, scripts_path)
    logging.info(f"Added to sys.path: {scripts_path}")
else:
    logging.warning(f"Path not found: {scripts_path}")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

TRANSFORM_AVAILABLE = False
try:
    from transform_google_trends import transform_trends_data as external_transform
    TRANSFORM_AVAILABLE = True
    logging.info("External transform script loaded successfully")
except ImportError as e:
    logging.error(f"Failed to import transform script: {e}")
    logging.error(f"Current sys.path: {sys.path}")
    # List available files in TikiTransform directory for debugging
    tiki_transform_base = '/opt/airflow/TikiTransform'
    if os.path.exists(tiki_transform_base):
        logging.info(f"Contents of {tiki_transform_base}:")
        for root, dirs, files in os.walk(tiki_transform_base):
            logging.info(f"  Dir: {root}")
            logging.info(f"  Files: {files}")
    else:
        logging.error(f"Directory does not exist: {tiki_transform_base}")
        for root, dirs, files in os.walk(tiki_transform_base):
            logging.info(f"  {root}: {dirs} | {files}")


# =============================================================================
# CONFIGURATION
# =============================================================================

# GCP Configuration
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'gcp-project-id')  # Set in docker-compose.yaml or .env
GCP_CONN_ID = 'google_cloud_default'
GCS_BUCKET = os.getenv('GCS_BUCKET_NAME', 'gcs-bucket-name')  # Set in docker-compose.yaml or .env
GCS_PREFIX = 'raw_zone/google_trends'

# BigQuery Configuration
BQ_DATASET = 'TikiWarehouse'
BQ_KEYWORD_TABLE = f'{BQ_DATASET}.dim_keyword_mapping'
BQ_TRENDS_TABLE = f'{BQ_DATASET}.fact_google_trends'
BQ_STAGING_TABLE = f'{BQ_DATASET}.staging_google_trends'  # Staging table for MERGE pattern

# Google Trends Configuration
TRENDS_TIMEFRAME = 'today 1-m'  # Last 30 days
TRENDS_GEO = 'VN'  # Vietnam
BATCH_SIZE = 5  # Google Trends limit per request

# Anti-Block Configuration
ANTI_BLOCK_CONFIG = {
    # Retry settings
    'max_retries': 5,
    'base_delay': 60,           # Base delay in seconds for exponential backoff
    'max_delay': 600,           # Max delay (10 minutes)
    'jitter_range': (5, 15),    # Random jitter in seconds
    
    # Rate limiting
    'batch_delay': (60, 90),    # Random delay between batches (seconds)
    'request_delay': (10, 30),  # Random delay between requests (seconds)
    
    # User-Agent rotation
    'user_agents': [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    ],
    
    # Caching (local)
    'cache_enabled': True,
    'cache_dir': '/tmp/google_trends_cache',
    'cache_ttl': 21600,  # 6 hours
}


# =============================================================================
# ANTI-BLOCK HELPER FUNCTIONS
# =============================================================================

def get_random_user_agent() -> str:
    """Get random User-Agent from pool."""
    return random.choice(ANTI_BLOCK_CONFIG['user_agents'])


def smart_delay(retry_count: int = 0, delay_type: str = 'request') -> None:
    """
    Smart delay with exponential backoff and jitter.
    
    Args:
        retry_count: Current retry attempt (0 for first request)
        delay_type: 'request' for between-request delay, 'batch' for between-batch delay
    """
    if retry_count == 0:
        if delay_type == 'batch':
            delay = random.uniform(*ANTI_BLOCK_CONFIG['batch_delay'])
        else:
            delay = random.uniform(*ANTI_BLOCK_CONFIG['request_delay'])
    else:
        # Exponential backoff: base_delay * 2^retry_count
        backoff = min(
            ANTI_BLOCK_CONFIG['base_delay'] * (2 ** (retry_count - 1)),
            ANTI_BLOCK_CONFIG['max_delay']
        )
        jitter = random.uniform(*ANTI_BLOCK_CONFIG['jitter_range'])
        delay = backoff + jitter
    
    logging.info(f"Smart delay: waiting {delay:.1f} seconds...")
    time.sleep(delay)


def get_cache_key(keywords: List[str], timeframe: str, geo: str) -> str:
    """Generate cache key from query parameters."""
    data = f"{','.join(sorted(keywords))}_{timeframe}_{geo}"
    return hashlib.md5(data.encode()).hexdigest()


def load_from_cache(cache_key: str) -> Optional[pd.DataFrame]:
    """Load data from cache if valid."""
    if not ANTI_BLOCK_CONFIG['cache_enabled']:
        return None
    
    cache_dir = Path(ANTI_BLOCK_CONFIG['cache_dir'])
    cache_file = cache_dir / f"{cache_key}.json"
    
    if not cache_file.exists():
        return None
    
    # Check cache age
    cache_age = time.time() - cache_file.stat().st_mtime
    if cache_age > ANTI_BLOCK_CONFIG['cache_ttl']:
        logging.info(f"Cache expired (age: {cache_age/3600:.1f} hours)")
        return None
    
    try:
        with open(cache_file, 'r') as f:
            cached = json.load(f)
        df = pd.read_json(cached['data'], orient='split')
        logging.info(f"Cache hit (age: {cache_age/60:.1f} minutes)")
        return df
    except Exception as e:
        logging.warning(f"Cache read error: {e}")
        return None


def save_to_cache(cache_key: str, df: pd.DataFrame) -> None:
    """Save data to cache."""
    if not ANTI_BLOCK_CONFIG['cache_enabled']:
        return
    
    cache_dir = Path(ANTI_BLOCK_CONFIG['cache_dir'])
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    cache_file = cache_dir / f"{cache_key}.json"
    
    try:
        cached = {
            'timestamp': datetime.now().isoformat(),
            'data': df.to_json(orient='split', date_format='iso'),
        }
        with open(cache_file, 'w') as f:
            json.dump(cached, f)
        logging.info("Data cached successfully")
    except Exception as e:
        logging.warning(f"Cache write error: {e}")


# =============================================================================
# CORE FETCH FUNCTION WITH ANTI-BLOCK
# =============================================================================

def fetch_batch_with_retry(keywords: List[str], timeframe: str, geo: str) -> Optional[pd.DataFrame]:
    """
    Fetch Google Trends data for a batch of keywords with retry mechanism.
    
    Args:
        keywords: List of keywords (max 5)
        timeframe: Timeframe string
        geo: Geographic region
    
    Returns:
        DataFrame or None if all retries failed
    """
    cache_key = get_cache_key(keywords, timeframe, geo)
    cached_df = load_from_cache(cache_key)
    if cached_df is not None:
        return cached_df
    
    max_retries = ANTI_BLOCK_CONFIG['max_retries']
    
    for attempt in range(max_retries):
        try:
            logging.info(f"Attempt {attempt + 1}/{max_retries} for keywords: {keywords}")
            
            # Add delay before request (except first attempt)
            if attempt > 0:
                smart_delay(retry_count=attempt)
            
            user_agent = get_random_user_agent()
            logging.info(f"Using User-Agent: {user_agent[:50]}...")
            
            pytrends = TrendReq(
                hl='vi-VN',
                tz=420,  # Vietnam timezone
                timeout=(10, 30),
                retries=2,
                backoff_factor=1,
            )
            
            # Override User-Agent
            pytrends.requests_args = {'headers': {'User-Agent': user_agent}}
            
            # Small random delay before building payload
            time.sleep(random.uniform(1, 3))
            
            # Build payload
            pytrends.build_payload(kw_list=keywords, timeframe=timeframe, geo=geo)
            
            # Small random delay before fetching
            time.sleep(random.uniform(1, 3))
            
            df = pytrends.interest_over_time()
            
            if df is not None and not df.empty:
                logging.info(f"Successfully fetched {len(df)} rows for batch")
                save_to_cache(cache_key, df)
                return df
            else:
                logging.warning(f"Empty response on attempt {attempt + 1}")
                
        except Exception as e:
            error_msg = str(e)
            
            if '429' in error_msg or 'Too Many Requests' in error_msg:
                logging.warning(f"Rate limited (429) on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    continue  # Will apply backoff on next iteration
                else:
                    logging.error("Max retries reached for batch")
                    return None
            else:
                logging.error(f"Unexpected error: {error_msg}")
                if attempt < max_retries - 1:
                    continue
                else:
                    return None
    
    return None


# =============================================================================
# AIRFLOW TASK FUNCTIONS
# =============================================================================

def get_keywords(**context) -> List[str]:
    """
    Task 1: Get active keywords from BigQuery dim_keyword_mapping table.
    
    Returns:
        List of active keywords
    """
    logging.info("=" * 60)
    logging.info("TASK 1: Get Keywords from BigQuery")
    logging.info("=" * 60)
    
    try:
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
        
        query = f"""
            SELECT DISTINCT trend_keyword 
            FROM `{GCP_PROJECT_ID}.{BQ_KEYWORD_TABLE}` 
            WHERE is_active = TRUE
            ORDER BY trend_keyword
        """
        
        logging.info(f"Executing query:\n{query}")
        
        result = bq_hook.get_pandas_df(sql=query)
        
        if result.empty:
            raise ValueError(
                "No active keywords found in BigQuery!\n"
                f"Table: {GCP_PROJECT_ID}.{BQ_KEYWORD_TABLE}\n"
                "Please ensure the table exists and has rows with is_active = TRUE"
            )
        
        keywords = result['trend_keyword'].tolist()
        logging.info(f"Found {len(keywords)} active keywords: {keywords}")
        
        context['task_instance'].xcom_push(key='keywords', value=keywords)
        
        return keywords
        
    except Exception as e:
        logging.error(f"Failed to get keywords from BigQuery: {str(e)}")
        raise ValueError(
            f"BigQuery keyword fetch failed: {str(e)}\n"
            f"Table: {GCP_PROJECT_ID}.{BQ_KEYWORD_TABLE}\n"
            "Please check:\n"
            "1. Table exists in BigQuery\n"
            "2. Service account has BigQuery Data Viewer role\n"
            "3. google_cloud_default connection is configured correctly"
        ) from e


def fetch_trends_data(**context) -> str:
    """
    Task 2: Fetch Google Trends data for all keywords with batching and anti-block.
    
    Returns:
        Path to the output CSV file
    """
    logging.info("=" * 60)
    logging.info("TASK 2: Fetch Google Trends Data")
    logging.info("=" * 60)
    
    ti = context['task_instance']
    execution_date = context['ds']
    
    keywords = ti.xcom_pull(task_ids='get_keywords', key='keywords')
    
    if not keywords:
        raise ValueError("No keywords found in XCom from get_keywords task")
    
    logging.info(f"Keywords to fetch: {keywords}")
    logging.info(f"Total keywords: {len(keywords)}")
    logging.info(f"Batch size: {BATCH_SIZE}")
    
    batches = [keywords[i:i + BATCH_SIZE] for i in range(0, len(keywords), BATCH_SIZE)]
    logging.info(f"Number of batches: {len(batches)}")
    
    all_results = []
    successful_batches = 0
    failed_batches = 0
    
    for batch_idx, batch in enumerate(batches, 1):
        logging.info(f"\n{'='*40}")
        logging.info(f"Processing batch {batch_idx}/{len(batches)}: {batch}")
        logging.info('='*40)
        
        # Add delay between batches (except first)
        if batch_idx > 1:
            smart_delay(delay_type='batch')
        
        try:
            df = fetch_batch_with_retry(
                keywords=batch,
                timeframe=TRENDS_TIMEFRAME,
                geo=TRENDS_GEO
            )
            
            if df is not None and not df.empty:
                # Keep RAW data (WIDE format) for transform task
                # Reset index to get date as column
                df_processed = df.reset_index()
                
                # Ensure date column exists (reset_index might create 'index' column)
                if 'date' not in df_processed.columns and 'index' in df_processed.columns:
                    df_processed = df_processed.rename(columns={'index': 'date'})
                
                # Store as-is (WIDE format: date, iPhone 15, Samsung S24, Tiki, isPartial)
                # Transform task will convert to LONG format
                all_results.append(df_processed)
                successful_batches += 1
                logging.info(f"Batch {batch_idx} fetched: {len(df_processed)} rows (WIDE format)")
            else:
                failed_batches += 1
                logging.warning(f"Batch {batch_idx} returned no data")
                
        except Exception as e:
            failed_batches += 1
            logging.error(f"Batch {batch_idx} failed: {str(e)}")
            # Continue with next batch (partial success)
            continue
    
    # Summary
    logging.info(f"\n{'='*60}")
    logging.info("FETCH SUMMARY")
    logging.info(f"{'='*60}")
    logging.info(f"Total batches: {len(batches)}")
    logging.info(f"Successful: {successful_batches}")
    logging.info(f"Failed: {failed_batches}")
    
    if not all_results:
        raise ValueError("No data fetched from any batch. All batches failed.")
    
    final_df = pd.concat(all_results, ignore_index=True)
    final_df['date'] = pd.to_datetime(final_df['date']).dt.strftime('%Y-%m-%d')
    
    logging.info(f"\nFinal DataFrame: {len(final_df)} rows, {len(final_df.columns)} columns (WIDE format)")
    logging.info(f"Columns: {list(final_df.columns)}")
    logging.info(f"Date range: {final_df['date'].min()} to {final_df['date'].max()}")
    
    # Save RAW data as CSV (WIDE format for transform task)
    output_dir = Path('/tmp/google_trends')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = output_dir / f"trends_{execution_date}.csv"
    final_df.to_csv(output_file, index=False)
    
    logging.info(f"Saved RAW CSV (WIDE format): {output_file}")
    
    ti.xcom_push(key='output_file', value=str(output_file))
    ti.xcom_push(key='row_count', value=len(final_df))
    ti.xcom_push(key='successful_batches', value=successful_batches)
    ti.xcom_push(key='failed_batches', value=failed_batches)
    
    return str(output_file)


def transform_trends_data(**context) -> str:
    """
    Task 3: Transform raw CSV from wide format to long format.
    Uses external transform script: TikiTransform/scripts/transform_google_trends.py
    
    Returns:
        Path to transformed Parquet file
    """
    ti = context['task_instance']
    
    logging.info("=" * 60)
    logging.info("TASK 3: Transform Trends Data (External Script)")
    logging.info("=" * 60)
    
    if not TRANSFORM_AVAILABLE:
        raise ImportError(
            "Transform script not found!\n"
            "Please ensure TikiTransform/scripts/transform_google_trends.py is available.\n"
            "Expected path: /opt/airflow/TikiTransform/scripts/transform_google_trends.py\n"
            "Check DAG logs for import error details and directory listing."
        )
    
    raw_file = ti.xcom_pull(task_ids='fetch_trends_data', key='output_file')
    
    if not raw_file or not os.path.exists(raw_file):
        raise ValueError(f"Raw file not found: {raw_file}")
    
    logging.info(f"Input: {raw_file}")
    logging.info(f"Using: TikiTransform/Scripts/transform_google_trends.py")
    
    parquet_file = external_transform(
        raw_file_path=raw_file,
        output_dir='/tmp/google_trends'
    )
    
    if not os.path.exists(parquet_file):
        raise ValueError(f"Transform failed: output file not found at {parquet_file}")
    
    file_size_kb = os.path.getsize(parquet_file) / 1024
    logging.info(f"Output: {parquet_file} ({file_size_kb:.2f} KB)")
    
    df_final = pd.read_parquet(parquet_file)
    
    logging.info("\n" + "=" * 50)
    logging.info("TRANSFORM SUMMARY")
    logging.info("=" * 50)
    logging.info(f"Total rows: {len(df_final)}")
    logging.info(f"Unique keywords: {df_final['keyword'].nunique()}")
    logging.info(f"Date range: {df_final['date'].min()} to {df_final['date'].max()}")
    logging.info(f"Score range: {df_final['score'].min()} to {df_final['score'].max()}")
    logging.info(f"Partial data rows: {df_final['is_partial'].sum()}")
    
    ti.xcom_push(key='parquet_file', value=str(parquet_file))
    ti.xcom_push(key='transform_row_count', value=len(df_final))
    
    return str(parquet_file)


def prepare_gcs_upload(**context) -> Dict[str, str]:
    """
    Task 4: Prepare GCS upload parameters for Parquet file.
    Returns dict with src and dst paths.
    """
    ti = context['task_instance']
    execution_date = context['ds']
    
    parquet_file = ti.xcom_pull(task_ids='transform_trends_data', key='parquet_file')
    
    if not parquet_file:
        raise ValueError("No parquet file found in XCom from transform_trends_data")
    
    gcs_path = f"{GCS_PREFIX}/snapshot_date={execution_date}/trends.parquet"
    
    logging.info(f"Local file: {parquet_file}")
    logging.info(f"GCS destination: gs://{GCS_BUCKET}/{gcs_path}")
    
    ti.xcom_push(key='gcs_path', value=gcs_path)
    ti.xcom_push(key='local_file', value=parquet_file)
    
    return {
        'src': parquet_file,
        'dst': gcs_path
    }


def log_success(**context):
    """
    Task 6: Log success and summary.
    """
    ti = context['task_instance']
    execution_date = context['ds']
    
    raw_row_count = ti.xcom_pull(task_ids='fetch_trends_data', key='row_count')
    transform_row_count = ti.xcom_pull(task_ids='transform_trends_data', key='transform_row_count')
    successful_batches = ti.xcom_pull(task_ids='fetch_trends_data', key='successful_batches')
    failed_batches = ti.xcom_pull(task_ids='fetch_trends_data', key='failed_batches')
    gcs_path = ti.xcom_pull(task_ids='prepare_gcs_upload', key='gcs_path')
    
    logging.info("=" * 60)
    logging.info("GOOGLE TRENDS DAG COMPLETED SUCCESSFULLY")
    logging.info("=" * 60)
    logging.info(f"Execution Date: {execution_date}")
    logging.info(f"Raw Rows Extracted: {raw_row_count}")
    logging.info(f"Rows After Transform: {transform_row_count}")
    logging.info(f"Successful Batches: {successful_batches}")
    logging.info(f"Failed Batches: {failed_batches}")
    logging.info(f"GCS Location: gs://{GCS_BUCKET}/{gcs_path}")
    logging.info(f"BigQuery Table: {GCP_PROJECT_ID}.{BQ_TRENDS_TABLE}")
    logging.info(f"Format: Parquet")
    logging.info("=" * 60)


# =============================================================================
# DAG DEFINITION
# =============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=2),  # Allow time for backoff retries
}

with DAG(
    dag_id='tiki_google_trends_analytics',
    default_args=default_args,
    description='Fetch Google Trends data for Vietnam E-commerce Analytics - Phase 2',
    schedule_interval='0 3 * * *',  # Daily at 3 AM Vietnam Time (UTC+7 = 20:00 UTC)
    start_date=datetime(2026, 1, 28),
    catchup=False,
    tags=['phase-2', 'google-trends', 'external-data'],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:
    
    # Task 1: Get keywords from BigQuery
    task_get_keywords = PythonOperator(
        task_id='get_keywords',
        python_callable=get_keywords,
        provide_context=True,
    )
    
    # Task 2: Fetch trends data with anti-block protection
    task_fetch_trends = PythonOperator(
        task_id='fetch_trends_data',
        python_callable=fetch_trends_data,
        provide_context=True,
    )
    
    # Task 3: Transform data (Wide -> Long, handle '<1', type casting)
    task_transform = PythonOperator(
        task_id='transform_trends_data',
        python_callable=transform_trends_data,
        provide_context=True,
    )
    
    # Task 4: Prepare GCS upload
    task_prepare_upload = PythonOperator(
        task_id='prepare_gcs_upload',
        python_callable=prepare_gcs_upload,
        provide_context=True,
    )
    
    # Task 5: Upload Parquet to GCS
    task_upload_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src="{{ task_instance.xcom_pull(task_ids='transform_trends_data', key='parquet_file') }}",
        dst="{{ task_instance.xcom_pull(task_ids='prepare_gcs_upload', key='gcs_path') }}",
        bucket=GCS_BUCKET,
        gcp_conn_id=GCP_CONN_ID,
    )
    
    # Task 6: Load Parquet to Staging Table (TRUNCATE first)
    # This clears staging before loading new batch - prevents accumulation
    task_load_staging = GCSToBigQueryOperator(
        task_id='load_to_staging',
        bucket=GCS_BUCKET,
        source_objects=["{{ task_instance.xcom_pull(task_ids='prepare_gcs_upload', key='gcs_path') }}"],
        destination_project_dataset_table=f'{GCP_PROJECT_ID}.{BQ_STAGING_TABLE}',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',  # CRITICAL: Clean staging before load
        create_disposition='CREATE_IF_NEEDED',
        gcp_conn_id=GCP_CONN_ID,
    )
    
    # Task 7: MERGE from Staging to Fact Table (Upsert Pattern)
    # This handles:
    # - UPDATE: If (date, keyword) exists → update score, is_partial, inserted_at
    # - INSERT: If (date, keyword) is new → insert the row
    # Result: No duplicates, historical data preserved, allows Google score revisions
    merge_query = f"""
    MERGE `{GCP_PROJECT_ID}.{BQ_TRENDS_TABLE}` AS target
    USING `{GCP_PROJECT_ID}.{BQ_STAGING_TABLE}` AS source
    ON target.date = source.date AND target.keyword = source.keyword
    
    WHEN MATCHED THEN
        UPDATE SET
            score = source.score,
            is_partial = source.is_partial,
            inserted_at = source.inserted_at
    
    WHEN NOT MATCHED THEN
        INSERT (date, keyword, score, is_partial, inserted_at)
        VALUES (source.date, source.keyword, source.score, source.is_partial, source.inserted_at)
    """
    
    task_merge_to_fact = BigQueryInsertJobOperator(
        task_id='merge_to_fact',
        configuration={
            'query': {
                'query': merge_query,
                'useLegacySql': False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )
    
    # Task 8: Log success
    task_log_success = PythonOperator(
        task_id='log_success',
        python_callable=log_success,
        provide_context=True,
    )
    
    # get_keywords -> fetch_trends -> transform -> prepare_upload -> upload_gcs -> load_staging -> merge_to_fact -> log_success
    task_get_keywords >> task_fetch_trends >> task_transform >> task_prepare_upload >> task_upload_gcs >> task_load_staging >> task_merge_to_fact >> task_log_success
