import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.models import Variable

default_args = {
    'owner': 'samik',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='market_pipeline_daily',
    default_args=default_args,
    description='Daily BSE stock pipeline: Extract → Load → Transform → Test',
    schedule='0 6 * * 1-5',
    start_date=pendulum.datetime(2026, 6, 1, tz='Asia/Kolkata'),
    catchup=False,
    tags=['market', 'snowflake', 'dbt', 'production'],
) as dag:

    extract_and_load = BashOperator(
        task_id='extract_and_load',
        bash_command='cd /opt/airflow/market_pipeline && python src/pipeline.py',
        env={
            'ALPHA_VANTAGE_KEY': Variable.get('ALPHA_VANTAGE_KEY'),
            'SNOWFLAKE_ACCOUNT': Variable.get('SNOWFLAKE_ACCOUNT'),
            'SNOWFLAKE_USER': Variable.get('SNOWFLAKE_USER'),
            'SNOWFLAKE_PASSWORD': Variable.get('SNOWFLAKE_PASSWORD'),
            'SNOWFLAKE_DATABASE': Variable.get('SNOWFLAKE_DATABASE'),
            'SNOWFLAKE_WAREHOUSE': Variable.get('SNOWFLAKE_WAREHOUSE'),
            'SNOWFLAKE_ROLE': Variable.get('SNOWFLAKE_ROLE'),
        }
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            'cd /opt/airflow/market_pipeline/market_dbt && '
            'dbt clean && '
            'dbt run --profiles-dir /opt/airflow/dbt/.dbt'
        ),
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=(
            'cd /opt/airflow/market_pipeline/market_dbt && '
            'dbt test --profiles-dir /opt/airflow/dbt/.dbt'
        ),
    )

    extract_and_load >> dbt_run >> dbt_test