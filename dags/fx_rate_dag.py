"""
Features:
- Fetches rate from open.er-api.com (free public API)
- Fallback to hardcoded rate if API fails (resilience)
- Idempotent loading (DELETE before INSERT)
- Uses Airflow logical date for backfill capability
- BigQuery partitioned by date

Source: https://open.er-api.com/v6/latest/USD
Target: TikiWarehouse.dim_exchange_rate

Workflow:
  fetch_fx_rate → transform_fx_data → delete_existing_data → load_to_bigquery
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple

import requests
import pandas as pd

sys.path.insert(0, '/opt/airflow/dags')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

scripts_path = '/opt/airflow/scripts/TikiTransform/scripts'
if os.path.exists(scripts_path):
    sys.path.insert(0, scripts_path)
    logging.info(f"Added to sys.path: {scripts_path}")

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.dates import days_ago

TRANSFORM_AVAILABLE = False
try:
    from transform_fx_rate import transform_fx_rate, validate_parquet
    TRANSFORM_AVAILABLE = True
    logging.info("FX Rate transform script loaded successfully")
except ImportError as e:
    logging.error(f"Failed to import FX transform script: {e}")


# =============================================================================
# CONFIGURATION
# =============================================================================

# GCP Configuration
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'gcp-project-id')  # Set in docker-compose.yaml or .env
GCP_CONN_ID = 'google_cloud_default'

# BigQuery Configuration
BQ_DATASET = 'TikiWarehouse'
BQ_TABLE = f'{BQ_DATASET}.dim_exchange_rate'
BQ_FULL_TABLE = f'{GCP_PROJECT_ID}.{BQ_TABLE}'

# API Configuration
API_URL = 'https://open.er-api.com/v6/latest/USD'
API_TIMEOUT = 10  # seconds

# Fallback Configuration
FALLBACK_RATE = 25400.0
TARGET_CURRENCY = 'VND'
BASE_CURRENCY = 'USD'


# =============================================================================
# CORE FUNCTIONS
# =============================================================================

def fetch_exchange_rate() -> Tuple[float, str]:
    """
    Fetch USD/VND exchange rate from API with fallback.
    
    Returns:
        Tuple of (rate, source) where source is 'open.er-api.com' or 'Fallback'
    """
    logging.info("=" * 60)
    logging.info("STEP 1: Fetching Exchange Rate")
    logging.info("=" * 60)
    
    try:
        logging.info(f"Calling API: {API_URL}")
        logging.info(f"Timeout: {API_TIMEOUT} seconds")
        
        response = requests.get(API_URL, timeout=API_TIMEOUT)
        response.raise_for_status()
        
        data = response.json()
        
        # Validate response structure
        if data.get('result') != 'success':
            raise ValueError(f"API returned non-success result: {data.get('result')}")
        
        rates = data.get('rates', {})
        
        if TARGET_CURRENCY not in rates:
            raise ValueError(f"Target currency {TARGET_CURRENCY} not found in response")
        
        rate = float(rates[TARGET_CURRENCY])
        source = 'open.er-api.com'
        
        logging.info(f"API Success!")
        logging.info(f"Rate: 1 {BASE_CURRENCY} = {rate:,.2f} {TARGET_CURRENCY}")
        logging.info(f"API Time: {data.get('time_last_update_utc', 'N/A')}")
        
        return rate, source
        
    except requests.exceptions.Timeout:
        logging.warning("API timeout! Using fallback rate.")
        return FALLBACK_RATE, 'Fallback'
        
    except requests.exceptions.RequestException as e:
        logging.warning(f"API request failed: {str(e)}")
        logging.warning("Using fallback rate.")
        return FALLBACK_RATE, 'Fallback'
        
    except (ValueError, KeyError) as e:
        logging.warning(f"API response parsing failed: {str(e)}")
        logging.warning("Using fallback rate.")
        return FALLBACK_RATE, 'Fallback'
        
    except Exception as e:
        logging.warning(f"Unexpected error: {str(e)}")
        logging.warning("Using fallback rate.")
        return FALLBACK_RATE, 'Fallback'


def delete_existing_data(logical_date: str) -> int:
    """
    Delete existing data for the given date (idempotent operation).
    
    Args:
        logical_date: Date string in 'YYYY-MM-DD' format
        
    Returns:
        Number of rows deleted
    """
    logging.info("=" * 60)
    logging.info("STEP 2: Delete Existing Data (Idempotent)")
    logging.info("=" * 60)
    
    try:
        bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
        
        # Delete query for idempotent insert
        delete_sql = f"""
            DELETE FROM `{BQ_FULL_TABLE}`
            WHERE date = '{logical_date}'
        """
        
        logging.info(f"Executing DELETE for date: {logical_date}")
        logging.info(f"SQL: {delete_sql.strip()}")
        
        bq_hook.run_query(
            sql=delete_sql,
            use_legacy_sql=False
        )
        
        logging.info(f"DELETE completed for date {logical_date}")
        return 1  # Success
        
    except Exception as e:
        # If table doesn't exist yet, that's OK - first run
        if "Not found: Table" in str(e):
            logging.info("Table doesn't exist yet - will be created on first insert")
            return 0
        else:
            logging.error(f"DELETE failed: {str(e)}")
            raise


def load_to_bigquery(parquet_path: str, logical_date: str) -> None:
    """
    Load Parquet file to BigQuery using pandas-gbq.
    
    Args:
        parquet_path: Path to the Parquet file
        logical_date: Date for logging purposes
    """
    logging.info("=" * 60)
    logging.info("STEP 4: Load to BigQuery")
    logging.info("=" * 60)
    
    df = pd.read_parquet(parquet_path)
    
    logging.info(f"Loading {len(df)} row(s) to BigQuery")
    logging.info(f"Target table: {BQ_FULL_TABLE}")
    
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    credentials = bq_hook.get_credentials()
    
    # Load using pandas-gbq (keep original dtypes from Parquet)
    # Parquet already has correct schema: date32, timestamp[us, tz=UTC], etc.
    df.to_gbq(
        destination_table=BQ_TABLE,
        project_id=GCP_PROJECT_ID,
        credentials=credentials,
        if_exists='append',
        # No need to specify table_schema - pandas-gbq will infer from Parquet
    )
    
    logging.info(f"Successfully loaded to BigQuery!")
    logging.info(f"Date: {logical_date}")
    logging.info(f"Rate: {df['rate'].iloc[0]:,.2f}")
    logging.info(f"Source: {df['source'].iloc[0]}")


# =============================================================================
# AIRFLOW TASK FUNCTIONS
# =============================================================================

def fetch_fx_rate(**context) -> Dict[str, Any]:
    """
    Task 1: Fetch USD/VND exchange rate from API with fallback.
    
    Returns:
        Dict with rate and source
    """
    ti = context['task_instance']
    logical_date = context['ds']
    
    logging.info("=" * 60)
    logging.info("TASK 1: FETCH EXCHANGE RATE")
    logging.info("=" * 60)
    logging.info(f"Logical Date: {logical_date}")
    
    rate, source = fetch_exchange_rate()
    
    result = {
        'rate': rate,
        'source': source,
        'logical_date': logical_date
    }
    
    ti.xcom_push(key='fx_data', value=result)
    
    logging.info(f"Fetched: {rate:,.2f} {TARGET_CURRENCY} from {source}")
    
    return result


def transform_fx_data(**context) -> str:
    """
    Task 2: Transform exchange rate data to Parquet format.
    
    Returns:
        Path to Parquet file
    """
    ti = context['task_instance']
    logical_date = context['ds']
    
    logging.info("=" * 60)
    logging.info("TASK 2: TRANSFORM DATA")
    logging.info("=" * 60)
    
    if not TRANSFORM_AVAILABLE:
        raise ImportError(
            "FX Transform script not found!\n"
            "Expected path: /opt/airflow/scripts/TikiTransform/scripts/transform_fx_rate.py"
        )
    
    fx_data = ti.xcom_pull(task_ids='fetch_fx_rate', key='fx_data')
    
    if not fx_data:
        raise ValueError("No FX data found from fetch_fx_rate task")
    
    rate = fx_data['rate']
    source = fx_data['source']
    
    logging.info(f"Input: Rate={rate:,.2f}, Source={source}")
    
    parquet_path = transform_fx_rate(
        rate=rate,
        logical_date=logical_date,
        source=source,
        output_dir='/tmp/fx_rate'
    )
    
    validate_parquet(parquet_path)
    
    ti.xcom_push(key='parquet_path', value=parquet_path)
    
    logging.info(f"Transformed: {parquet_path}")
    
    return parquet_path


def delete_existing_fx_data(**context) -> int:
    """
    Task 3: Delete existing data for the date (idempotent operation).
    
    Returns:
        Number of rows deleted (1 for success, 0 if table doesn't exist)
    """
    logical_date = context['ds']
    
    logging.info("=" * 60)
    logging.info("TASK 3: DELETE EXISTING DATA")
    logging.info("=" * 60)
    
    rows_deleted = delete_existing_data(logical_date)
    
    return rows_deleted


def load_fx_to_bigquery(**context) -> None:
    """
    Task 4: Load Parquet file to BigQuery.
    """
    ti = context['task_instance']
    logical_date = context['ds']
    
    logging.info("=" * 60)
    logging.info("TASK 4: LOAD TO BIGQUERY")
    logging.info("=" * 60)
    
    parquet_path = ti.xcom_pull(task_ids='transform_fx_data', key='parquet_path')
    
    if not parquet_path:
        raise ValueError("No parquet path found from transform_fx_data task")
    
    load_to_bigquery(parquet_path, logical_date)
    
    fx_data = ti.xcom_pull(task_ids='fetch_fx_rate', key='fx_data')
    
    logging.info("\n" + "=" * 60)
    logging.info("FX RATE INGESTION - COMPLETE")
    logging.info("=" * 60)
    logging.info(f"Date: {logical_date}")
    logging.info(f"Rate: 1 {BASE_CURRENCY} = {fx_data['rate']:,.2f} {TARGET_CURRENCY}")
    logging.info(f"Source: {fx_data['source']}")
    logging.info(f"Table: {BQ_FULL_TABLE}")


# =============================================================================
# LEGACY FUNCTION (Kept for reference, not used in DAG)
# =============================================================================

def fetch_and_load_fx_rate(**context) -> Dict[str, Any]:
    """
    Main task: Fetch exchange rate and load to BigQuery.
    
    This is an idempotent operation:
    1. Fetch rate from API (or use fallback)
    2. Delete existing data for the date
    3. Transform and validate
    4. Load to BigQuery
    
    Args:
        context: Airflow context with logical date
        
    Returns:
        Dict with execution metadata
    """
    ti = context['task_instance']
    logical_date = context['ds']  # Airflow logical date (YYYY-MM-DD)
    
    logging.info("=" * 60)
    logging.info("FX RATE INGESTION - START")
    logging.info("=" * 60)
    logging.info(f"Logical Date: {logical_date}")
    logging.info(f"Target: {BQ_FULL_TABLE}")
    
    if not TRANSFORM_AVAILABLE:
        raise ImportError(
            "FX Transform script not found!\n"
            "Expected path: /opt/airflow/scripts/TikiTransform/scripts/transform_fx_rate.py"
        )
    
    # Step 1: Fetch exchange rate
    rate, source = fetch_exchange_rate()
    
    # Step 2: Delete existing data (idempotent)
    delete_existing_data(logical_date)
    
    # Step 3: Transform
    logging.info("=" * 60)
    logging.info("STEP 3: Transform Data")
    logging.info("=" * 60)
    
    parquet_path = transform_fx_rate(
        rate=rate,
        logical_date=logical_date,
        source=source,
        output_dir='/tmp/fx_rate'
    )
    
    validate_parquet(parquet_path)
    
    # Step 4: Load to BigQuery
    load_to_bigquery(parquet_path, logical_date)
    
    # Summary
    logging.info("\n" + "=" * 60)
    logging.info("FX RATE INGESTION - COMPLETE")
    logging.info("=" * 60)
    logging.info(f"Date: {logical_date}")
    logging.info(f"Rate: 1 {BASE_CURRENCY} = {rate:,.2f} {TARGET_CURRENCY}")
    logging.info(f"Source: {source}")
    logging.info(f"Table: {BQ_FULL_TABLE}")
    
    result = {
        'date': logical_date,
        'rate': rate,
        'source': source,
        'table': BQ_FULL_TABLE
    }
    
    ti.xcom_push(key='fx_result', value=result)
    
    return result


# =============================================================================
# DAG DEFINITION
# =============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=10),
}

with DAG(
    dag_id='tiki_exchange_rate_ingest',
    default_args=default_args,
    description='Daily USD/VND exchange rate ingestion for price normalization',
    schedule_interval='0 0 * * *',  # Daily at 07:00 AM Vietnam Time (00:00 UTC)
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=['fx', 'dimension', 'daily'],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:
    
    # Task 1: Fetch exchange rate from API
    task_fetch = PythonOperator(
        task_id='fetch_fx_rate',
        python_callable=fetch_fx_rate,
        provide_context=True,
    )
    
    # Task 2: Transform to Parquet
    task_transform = PythonOperator(
        task_id='transform_fx_data',
        python_callable=transform_fx_data,
        provide_context=True,
    )
    
    # Task 3: Delete existing data (idempotent)
    task_delete = PythonOperator(
        task_id='delete_existing_data',
        python_callable=delete_existing_fx_data,
        provide_context=True,
    )
    
    # Task 4: Load to BigQuery
    task_load = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_fx_to_bigquery,
        provide_context=True,
    )
    
    task_fetch >> task_transform >> task_delete >> task_load
