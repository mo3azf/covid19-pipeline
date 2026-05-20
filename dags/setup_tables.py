"""
DAG: 01_setup_covid_tables
Purpose: Initialize the Star Schema tables in PostgreSQL using inline SQL.
This DAG should be triggered manually once to set up the database.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'covid_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# SQL for all tables, embedded directly to avoid file path issues
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    week_of_year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_id SERIAL PRIMARY KEY,
    country_region VARCHAR(100) NOT NULL,
    province_state VARCHAR(100),
    lat DECIMAL(10, 6),
    long DECIMAL(10, 6),
    combined_key VARCHAR(200) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_covid_cases (
    case_id SERIAL PRIMARY KEY,
    location_id INT NOT NULL REFERENCES dim_location(location_id) ON DELETE CASCADE,
    date_id DATE NOT NULL REFERENCES dim_date(date_id) ON DELETE CASCADE,
    confirmed_cumulative INT DEFAULT 0,
    deaths_cumulative INT DEFAULT 0,
    recovered_cumulative INT DEFAULT 0,
    new_confirmed INT DEFAULT 0,
    new_deaths INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(location_id, date_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_location_date ON fact_covid_cases(location_id, date_id);
CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_covid_cases(date_id);

CREATE TABLE IF NOT EXISTS etl_log (
    log_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100),
    task_id VARCHAR(100),
    execution_date TIMESTAMP,
    status VARCHAR(20),
    records_processed INT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

with DAG(
    dag_id='01_setup_covid_tables',
    default_args=default_args,
    description='Create dim and fact tables for COVID-19 data',
    schedule_interval=None,
    catchup=False,
    tags=['setup', 'postgres'],
) as dag:

    create_tables = PostgresOperator(
        task_id='create_star_schema_tables',
        postgres_conn_id='postgres_default',
        sql=CREATE_TABLES_SQL       # Inline SQL - no file path needed
    )

    create_tables