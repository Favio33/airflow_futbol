from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

default_args = {
    'owner':'FVR',
    'retries':'2',
    'retry_delay':timedelta(minutes=2)
}

def greet(ti, age:int):
    name = ti.xcom_pull(task_ids='get_name_task', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name_task', key='last_name')
    print(f"Hola cpp {name}, {last_name}:{age} es {datetime.now()}")

def get_name(ti):
    ti.xcom_push(key = 'first_name', value = 'Yerry')
    ti.xcom_push(key = 'last_name', value = 'Pepales')


with DAG(
    dag_id="test",
    default_args=default_args,
    tags=['pandas', 'snowflake', 'aws'],
    start_date=datetime(2023, 7, 31, 0),
    schedule_interval="@daily"
    ) as dag:

    task1 = BashOperator(
        task_id = 'first_task',
        bash_command='echo hello world! This is my first task.'
    )

    task2 = BashOperator(
        task_id = 'second_task',
        bash_command= "echo Second Task! Raaaaaa"
    )

    task3 = BashOperator(
        task_id = 'third_task',
        bash_command= "echo Task 3 maz nah!"
    )

    task4 = PythonOperator(
        task_id = "greet_task",
        python_callable= greet,
        op_kwargs={'age':12}
    )

    task5 = PythonOperator(
        task_id = 'get_name_task',
        python_callable=get_name
    )

    # Serial
    task1.set_downstream(task2) ## or task1 >> task2
    task2.set_downstream(task3) ## or task2 >> task3

    # Paralelizar
    #task1 >> [task2, task3] 
    task1.set_downstream([task2, task3])

    task4 << [task2, task3]
    task5 >> task4
    pass
