from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

inicio = 'Iniciando'
fim = 'finalizada'
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 1),
    'retries': 1,
}

with DAG(
        'simple_test_dag',
        default_args=default_args,
        description='Uma DAG simples para teste',
        schedule='@daily',
        catchup=False,
) as dag:
    start_task = BashOperator(
        task_id='start',
        bash_command=f'echo {inicio} a DAG'
    )

    end_task = BashOperator(
        task_id='end',
        bash_command=f'echo DAG {fim}'
    )

start_task >> end_task