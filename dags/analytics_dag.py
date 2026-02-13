"""
Features:
- Golden Join: Combines product snapshots, trends, and FX rates
- Delete-Write pattern for idempotent daily processing
- Data quality checks with ASSERT statements
- Price normalization (VND → USD using real-time FX rates)
- Trend signal mapping for market analysis

Source Tables (Core Layer):
- fact_product_snapshot: Daily product data from Tiki
- dim_product: Product master data
- dim_keyword_mapping: Maps categories to Google Trends keywords
- fact_google_trends: Daily trend scores
- dim_exchange_rate: Daily USD/VND rates

Target Table (Analytics Layer):
- analytics_product_market_daily: Aggregated market insights

Workflow:
  build_daily_mart → check_data_quality
"""

import os
import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago


# =============================================================================
# CONFIGURATION
# =============================================================================

# GCP Configuration
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'gcp-project-id')  # Set in docker-compose.yaml or .env
GCP_CONN_ID = 'google_cloud_default'

# BigQuery Configuration
BQ_DATASET = 'TikiWarehouse'

# Source Tables (Core Layer)
FACT_PRODUCT_SNAPSHOT = f'{GCP_PROJECT_ID}.{BQ_DATASET}.fact_daily_snapshot'
DIM_PRODUCT = f'{GCP_PROJECT_ID}.{BQ_DATASET}.dim_products'  # Note: plural
DIM_CATEGORY = f'{GCP_PROJECT_ID}.{BQ_DATASET}.dim_categories'  # For category_name lookup
DIM_KEYWORD_MAPPING = f'{GCP_PROJECT_ID}.{BQ_DATASET}.dim_keyword_mapping'
FACT_GOOGLE_TRENDS = f'{GCP_PROJECT_ID}.{BQ_DATASET}.fact_google_trends'
DIM_EXCHANGE_RATE = f'{GCP_PROJECT_ID}.{BQ_DATASET}.dim_exchange_rate'

# Target Table (Analytics Layer)
ANALYTICS_TABLE = f'{GCP_PROJECT_ID}.{BQ_DATASET}.analytics_product_market_daily'

# Fallback FX Rate (if no rate available for the date)
FALLBACK_FX_RATE = 25400.0


# =============================================================================
# SQL QUERIES
# =============================================================================

# Delete existing data for the execution date (idempotent)
DELETE_SQL = f"""
DELETE FROM `{ANALYTICS_TABLE}`
WHERE date = '{{{{ ds }}}}'
"""

# The Golden Join: Combine all data sources into analytics table
INSERT_SQL = f"""
INSERT INTO `{ANALYTICS_TABLE}` (
    date,
    product_id,
    product_name,
    category_name,
    category_level,
    
    -- Price metrics (VND)
    price_vnd_real,
    price_vnd_list,
    discount_percentage,
    
    -- Price metrics (USD - normalized)
    price_usd_real,
    fx_rate,
    
    -- Trend metrics
    trend_keyword,
    google_trend_score,
    trend_signal_status,
    
    -- Metadata
    inserted_at
)

SELECT
    -- Date
    f.snapshot_date AS date,
    
    -- Product info
    CAST(f.product_id AS STRING) AS product_id,  -- Convert INT64 to STRING
    p.name AS product_name,
    c.category_name,  -- From dim_categories
    c.category_level,  -- Track category depth (should be 3 for leaf categories)
    
    -- Price metrics (VND)
    CAST(f.current_price AS FLOAT64) AS price_vnd_real,
    CAST(f.original_price AS FLOAT64) AS price_vnd_list,
    CAST(f.discount_rate AS FLOAT64) AS discount_percentage,
    
    -- Price metrics (USD - normalized with FX rate)
    ROUND(f.current_price / COALESCE(e.rate, {FALLBACK_FX_RATE}), 2) AS price_usd_real,
    COALESCE(e.rate, {FALLBACK_FX_RATE}) AS fx_rate,
    
    -- Trend metrics
    m.trend_keyword,
    CAST(t.score AS INT64) AS google_trend_score,
    
    -- Trend signal status (for analysis quality indicator)
    CASE
        WHEN m.trend_keyword IS NULL THEN 'Unmapped'
        WHEN t.score IS NULL THEN 'No Trend Data'
        ELSE 'Full Data'
    END AS trend_signal_status,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS inserted_at

FROM `{FACT_PRODUCT_SNAPSHOT}` f

-- Join product dimensions
LEFT JOIN `{DIM_PRODUCT}` p
    ON f.product_id = p.product_id

-- Join category dimensions (for category_name)
-- NOTE: Products should have leaf category_id (level 3) from crawler extraction
LEFT JOIN `{DIM_CATEGORY}` c
    ON p.category_id = c.category_id

-- Join keyword mapping (category → trend keyword)
-- Keyword mappings are defined for leaf categories (level 3)
LEFT JOIN `{DIM_KEYWORD_MAPPING}` m
    ON p.category_id = m.tiki_category_id
    AND m.is_active = TRUE

-- Join Google Trends data
LEFT JOIN `{FACT_GOOGLE_TRENDS}` t
    ON m.trend_keyword = t.keyword
    AND f.snapshot_date = t.date

-- Join FX rate
LEFT JOIN `{DIM_EXCHANGE_RATE}` e
    ON f.snapshot_date = e.date
    AND e.from_currency = 'USD'
    AND e.to_currency = 'VND'

-- Process only the execution date (incremental)
WHERE f.snapshot_date = '{{{{ ds }}}}'
"""

# Combined Delete + Insert query
BUILD_MART_SQL = f"""
-- Step 1: Delete existing data for idempotency
{DELETE_SQL};

-- Step 2: Insert fresh data
{INSERT_SQL}
"""

# Data Quality Check SQL with ASSERT statements
DATA_QUALITY_SQL = f"""
-- Data Quality Checks for analytics_product_market_daily
-- Fails the task if any assertion fails

-- Check 1: No invalid prices (price must be positive)
ASSERT (
    SELECT COUNT(*)
    FROM `{ANALYTICS_TABLE}`
    WHERE date = '{{{{ ds }}}}'
    AND price_vnd_real <= 0
) = 0
AS 'QUALITY_CHECK_FAILED: Found products with invalid price (price_vnd_real <= 0)';

-- Check 2: No NULL product_ids
ASSERT (
    SELECT COUNT(*)
    FROM `{ANALYTICS_TABLE}`
    WHERE date = '{{{{ ds }}}}'
    AND product_id IS NULL
) = 0
AS 'QUALITY_CHECK_FAILED: Found rows with NULL product_id';

-- Check 3: FX rate is always present (either real or fallback)
ASSERT (
    SELECT COUNT(*)
    FROM `{ANALYTICS_TABLE}`
    WHERE date = '{{{{ ds }}}}'
    AND fx_rate IS NULL
) = 0
AS 'QUALITY_CHECK_FAILED: Found rows with NULL fx_rate';

-- Check 4: At least some data was inserted
ASSERT (
    SELECT COUNT(*)
    FROM `{ANALYTICS_TABLE}`
    WHERE date = '{{{{ ds }}}}'
) > 0
AS 'QUALITY_CHECK_FAILED: No data found for execution date - possible upstream failure';

-- Check 5: USD price is reasonable (sanity check)
ASSERT (
    SELECT COUNT(*)
    FROM `{ANALYTICS_TABLE}`
    WHERE date = '{{{{ ds }}}}'
    AND price_usd_real > 100000  -- $100k is unrealistic for Tiki products
) = 0
AS 'QUALITY_CHECK_FAILED: Found products with unrealistic USD price (> $100,000)';

-- Check 6: At least SOME products have keyword mapping (warn if 0% mapped)
-- NOTE: We only have ~18 keyword mappings vs hundreds of categories, so low % is normal
ASSERT (
    SELECT COUNTIF(trend_signal_status = 'Full Data')
    FROM `{ANALYTICS_TABLE}`
    WHERE date = '{{{{ ds }}}}'
) > 0
AS 'QUALITY_CHECK_WARNING: No products have Google Trends data - check keyword_mapping table';

-- If all assertions pass, return success summary
SELECT
    '{{{{ ds }}}}' AS check_date,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT product_id) AS unique_products,
    COUNTIF(trend_signal_status = 'Full Data') AS with_trend_data,
    COUNTIF(trend_signal_status = 'No Trend Data') AS missing_trend,
    COUNTIF(trend_signal_status = 'Unmapped') AS unmapped_category,
    'ALL_CHECKS_PASSED' AS status
FROM `{ANALYTICS_TABLE}`
WHERE date = '{{{{ ds }}}}'
"""


# =============================================================================
# LOGGING FUNCTION
# =============================================================================

def log_analytics_summary(**context):
    """
    Log summary of analytics build.
    """
    execution_date = context['ds']
    
    logging.info("=" * 60)
    logging.info("ANALYTICS DAG COMPLETED SUCCESSFULLY")
    logging.info("=" * 60)
    logging.info(f"Execution Date: {execution_date}")
    logging.info(f"Target Table: {ANALYTICS_TABLE}")
    logging.info(f"Data Quality Checks: PASSED")
    logging.info("=" * 60)


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
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    dag_id='analytics_market_insight',
    default_args=default_args,
    description='Daily market analytics aggregation - Phase 3',
    schedule_interval='0 10 * * *',  # Daily at 10:00 UTC (17:00 Vietnam) - Runs AFTER all dependencies
    start_date=datetime(2026, 2, 2),
    catchup=False,
    tags=['analytics', 'reporting', 'phase3'],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:
    
    # =========================================================================
    # Task 1: Build Daily Analytics Mart
    # =========================================================================
    # Uses Delete-Write pattern for idempotency:
    # 1. DELETE existing data for the execution date
    # 2. INSERT fresh data from the Golden Join
    #
    # The Golden Join combines:
    # - fact_product_snapshot: Daily product data
    # - dim_product: Product master data
    # - dim_keyword_mapping: Category → Trend keyword mapping
    # - fact_google_trends: Daily trend scores
    # - dim_exchange_rate: Daily FX rates
    # =========================================================================
    
    task_build_mart = BigQueryInsertJobOperator(
        task_id='build_daily_mart',
        configuration={
            'query': {
                'query': BUILD_MART_SQL,
                'useLegacySql': False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )
    
    # =========================================================================
    # Task 2: Data Quality Checks
    # =========================================================================
    # Validates the data inserted in Task 1:
    # - No invalid prices (price <= 0)
    # - No NULL product_ids
    # - FX rate is always present
    # - At least some data exists
    # - USD price is reasonable
    #
    # Uses BigQuery ASSERT statements - fails task if any check fails
    # =========================================================================
    
    task_check_quality = BigQueryInsertJobOperator(
        task_id='check_data_quality',
        configuration={
            'query': {
                'query': DATA_QUALITY_SQL,
                'useLegacySql': False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
        trigger_rule='all_success',  # Only run if build succeeds
    )
    
    # =========================================================================
    # Task 3: Log Summary
    # =========================================================================
    
    task_log_summary = PythonOperator(
        task_id='log_summary',
        python_callable=log_analytics_summary,
        provide_context=True,
        trigger_rule='all_success',
    )
    
    # =========================================================================
    # Task Dependencies
    # =========================================================================
    # build_daily_mart → check_data_quality → log_summary
    
    task_build_mart >> task_check_quality >> task_log_summary
