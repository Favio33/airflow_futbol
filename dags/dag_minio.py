from datetime import datetime, timedelta
import csv

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    'owner':'FVR',
    'retries':'2',
    'retry_delay':timedelta(minutes=1)
}

@dag(
    dag_id= 'taskflow_v2',
    default_args= default_args,
    tags= ['pandas', 'aws', 'minio'],
    start_date= datetime(2023, 8, 4, 0),
    schedule_interval= '@daily'
)
def minio_etl():
    
    task1 = S3KeySensor(
        task_id = 'sensor_minio_s3',
        bucket_name = 'airflow',
        bucket_key = 'data.csv',
        aws_conn_id = 'minio_conn',
        mode = 'poke',
        poke_interval = 5,
        timeout = 30
    )

    @task
    def postgres_to_s3(**context):
        # Step 1: Query PG table
        ds = str(context['ds'])
        next_ds = str(context['next_ds'])
        hook = PostgresHook(postgres_conn_id = 'postgres_localhost')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM airflow.orders WHERE date >= %s AND date <= %s",
                       (ds, next_ds))
        with open(f"include/data/get_orders_{ds}.txt", "w") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow([i[0] for i in cursor.description])
            csv_writer.writerows(cursor)
        cursor.close()
        conn.close()
        s3hook = S3Hook(aws_conn_id="minio_conn")
        s3hook.load_file(
            filename = f"/usr/local/airflow/include/data/get_orders_{ds}.txt",
            key=f"orders/{next_ds}.txt",
            bucket_name = 'airflow',
            replace = True
        )


    postgres_to_s3 = postgres_to_s3()
    task1 >> postgres_to_s3

dag_minio = minio_etl()