from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'process_sword_messages',
    default_args=default_args,
    description='Process Protocol Buffer messages from bin file',
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'protobuf', 'sword'],
)

process_sword_messages = SparkSubmitOperator(
    task_id='process_sword_messages',
    application='/opt/bitnami/spark/jobs/sword_processor_job.py',
    conn_id='spark-ebsim',
    application_args=[
        # '--input-file', '/opt/bitnami/spark/jobs/test_sword_messages.bin'
        '--input-file', '/opt/bitnami/spark/jobs/session-proto_1.pb'
    ],
    dag=dag,
)

process_sword_messages