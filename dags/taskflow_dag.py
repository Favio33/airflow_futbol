from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
import subprocess

default_args = {
    'owner':'FVR',
    'retries':'2',
    'retry_delay':timedelta(minutes=1)
}

@dag(
    dag_id= 'taskflow_v1',
    default_args= default_args,
    tags= ['pandas', 'snowflake', 'aws'],
    start_date= datetime(2023, 8, 1, 0),
    schedule_interval= '@daily',
    template_searchpath="/usr/local/airflow/include"
)
def main_etl():
    
    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'Pepo',
            'last_name': 'Varillas'
        }
    
    @task()
    def get_age():
        return 60
    
    @task()
    def greet(first_name:str, last_name:str, age:int):
        print(f"Hi {first_name} {last_name}, your age is {age}")


    task_pg1 = SQLExecuteQueryOperator(
        task_id = 'create_pg_table',
        conn_id = 'postgres_localhost',
        sql="/templates/sql/create.sql"
    )

    task_pg2 = SQLExecuteQueryOperator(
        task_id = 'insert_values',
        conn_id = 'postgres_localhost',
        sql = "/templates/sql/insert.sql"
    )

    task_bash1 = BashOperator(
        task_id = 'list_dir',
        bash_command='echo "$(ls /usr/local/airflow)"'
    )

    name_dict = get_name()
    age = get_age()
    greet = greet(name_dict['first_name'], name_dict['last_name'], age) 
    age >> task_pg1 >> task_pg2 >> task_bash1

greet_dag = main_etl()