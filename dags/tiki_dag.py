
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# =============================================================================
# DAG Configuration
# =============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': [' '],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id='tiki_etl_pipeline',
    default_args=default_args,
    description='Daily Tiki product data: Crawl → Transform → Load to BigQuery',
    schedule_interval='0 9 * * *',  # 09:00 UTC = 16:00 Vietnam
    start_date=datetime(2026, 1, 23),  # Updated to today
    catchup=False,
    max_active_runs=1,
    tags=['tiki', 'etl', 'bigquery', 'daily'],
    doc_md="""
    ## Tiki ETL Pipeline
    
    **Schedule**: Daily at 09:00 UTC (16:00 Vietnam Time)
    
    ### Tasks:
    1. **crawl_tiki_products**: Crawl product data from Tiki using Puppeteer
    2. **transform_to_parquet**: Transform raw JSON → Clean Parquet (Star Schema)
    3. **load_to_bigquery**: Load to BigQuery (Fact + Dimension tables)
    
    ### Output Tables:
    - `TikiWarehouse.fact_daily_snapshot` (partitioned by date)
    - `TikiWarehouse.dim_products` (MERGE/upsert)
    """,
) as dag:

    # -------------------------------------------------------------------------
    # Start Task
    # -------------------------------------------------------------------------
    start = EmptyOperator(
        task_id='start',
    )

    # -------------------------------------------------------------------------
    # Task 1: Crawl Tiki Products
    # -------------------------------------------------------------------------
    crawl_tiki_products = BashOperator(
        task_id='crawl_tiki_products',
        bash_command="""
            set -e
            echo "=========================================="
            echo "[CRAWL] Starting Tiki Product Crawler"
            echo "[CRAWL] Date: {{ ds }}"
            echo "=========================================="
            
            cd /opt/airflow/scripts/TikiCrawler/puppeteer
            
            if [ ! -d "node_modules" ]; then
                echo "[CRAWL] Installing npm dependencies..."
                npm install
            fi
            
            echo "[CRAWL] Running crawler..."
            npm start -- --date {{ ds }}
            
            echo "[CRAWL] ✓ Crawler completed successfully"
        """,
        env={
            'PUPPETEER_SKIP_CHROMIUM_DOWNLOAD': 'true',
            'PUPPETEER_EXECUTABLE_PATH': '/usr/bin/google-chrome-stable',
        },
    )

    # -------------------------------------------------------------------------
    # Task 2: Transform to Parquet (Star Schema)
    # -------------------------------------------------------------------------
    transform_to_parquet = BashOperator(
        task_id='transform_to_parquet',
        bash_command="""
            set -e
            echo "=========================================="
            echo "[TRANSFORM] Starting Data Transformation"
            echo "[TRANSFORM] Date: {{ ds }}"
            echo "=========================================="
            
            cd /opt/airflow/scripts/TikiTransform/scripts
            

            python transform_tiki.py --date {{ ds }}
            
            echo "[TRANSFORM] ✓ Transform completed successfully"
        """,
    )

    # -------------------------------------------------------------------------
    # Task 3: Load to BigQuery
    # -------------------------------------------------------------------------
    load_to_bigquery = BashOperator(
        task_id='load_to_bigquery',
        bash_command="""
            set -e
            echo "=========================================="
            echo "[LOAD] Starting BigQuery Load"
            echo "[LOAD] Date: {{ ds }}"
            echo "=========================================="
            
            cd /opt/airflow/scripts/TikiTransform/scripts
            

            python load_to_bq.py --execution_date {{ ds }}
            
            echo "[LOAD] ✓ BigQuery load completed successfully"
        """,
    )

    # -------------------------------------------------------------------------
    # End Task
    # -------------------------------------------------------------------------
    end = EmptyOperator(
        task_id='end',
    )

    # -------------------------------------------------------------------------
    # Task Dependencies
    # -------------------------------------------------------------------------
    start >> crawl_tiki_products >> transform_to_parquet >> load_to_bigquery >> end
