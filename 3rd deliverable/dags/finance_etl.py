from datetime import timedelta

from airflow.models import DAG
from airflow.operators.papermill_operator import PapermillOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'Juan Lacasa',
    'start_date': days_ago(2)
}

with DAG(
    dag_id='finance_etl_papermill',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=10)
) as dag:
    # [START howto_operator_papermill]
    run_this = PapermillOperator(
        task_id="run_complete_etl",
        input_nb="/opt/airflow/dags/entrega_ETL.ipynb",
        output_nb="/opt/airflow/logs/out-{{ execution_date }}.ipynb",
        parameters={"msgs": "Ran from Airflow at {{ execution_date }}!"}
    )