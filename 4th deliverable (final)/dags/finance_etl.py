from datetime import timedelta

from airflow.models import DAG
from airflow.operators.papermill_operator import PapermillOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import json
import smtplib

from dotenv.main import dotenv_values

default_args = {
    'owner': 'Juan Lacasa',
    'start_date': days_ago(2)
}

smtp_keys = dotenv_values('/opt/airflow/keys/smtp.env')

def enviar(ti):
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login(smtp_keys['username'],smtp_keys['password']) # Cambia tu contraseña !!!!!!!!
        subject='Data download report'
        xcom_output=ti.xcom_pull(task_ids='run_etl')
        body_text=json.loads(xcom_output)['body_text']
        message='Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail('juanmlacasa@gmail.com','juanmlacasa@gmail.com',message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')

with DAG(
    dag_id='finance_etl_papermill',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=10)
) as dag:
    # run entrega_ETL.py
    task_etl = BashOperator(
        task_id='run_etl',
        bash_command='python3 /opt/airflow/dags/entrega_ETL.py',
        do_xcom_push=True,
        dag=dag
    )
    # xcom_etl = task_etl.xcom_pull(task_ids=['run_etl'])
    # message = json.loads(xcom_etl)
    # send email
    task_email = PythonOperator(
        task_id='send_email',
        python_callable=enviar,
        provide_context=True,
        dag=dag
    )

    task_etl >> task_email