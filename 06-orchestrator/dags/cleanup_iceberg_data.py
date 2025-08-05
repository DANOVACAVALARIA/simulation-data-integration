from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
import logging

# Configurações
AWS_CONN_ID = 'minio-ebsim'
NESSIE_CONN_ID = 'nessie-postgres'

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 30),
}

dag = DAG("cleanup_data",
          default_args=default_args,
          schedule_interval=None,
          tags=["nessie", "minio"])

def delete_bucket_data(bucket_name):
    """Apaga todos os dados de um bucket"""
    try:
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        
        # Lista todos os objetos do bucket
        objects = s3_hook.list_keys(bucket_name=bucket_name)
        
        if objects:
            # Deleta todos os objetos
            for obj_key in objects:
                try:
                    s3_hook.delete_objects(bucket=bucket_name, keys=[obj_key])
                    logging.info(f"Deleted object: {obj_key}")
                except Exception as e:
                    logging.error(f"Error deleting {obj_key}: {str(e)}")
            
            logging.info(f"🗑️ All data deleted from bucket '{bucket_name}'")
        else:
            logging.info(f"Bucket '{bucket_name}' is already empty")
            
    except Exception as e:
        logging.error(f"Error deleting data from bucket '{bucket_name}': {str(e)}")
        raise

def delete_warehouse_data():
    """Apaga dados do bucket warehouse"""
    delete_bucket_data('warehouse')

def delete_bronze_data():
    """Apaga dados do bucket bronze"""
    delete_bucket_data('bronze')

# Task definitions
start_dag = DummyOperator(task_id='start_cleanup', dag=dag)

delete_warehouse_task = PythonOperator(
    task_id='delete_warehouse_data',
    python_callable=delete_warehouse_data,
    dag=dag
)

delete_bronze_task = PythonOperator(
    task_id='delete_bronze_data',
    python_callable=delete_bronze_data,
    dag=dag
)

def reset_nessie_metadata():
    """Reset adequado dos metadados do Nessie"""
    try:
        hook = PostgresHook(postgres_conn_id=NESSIE_CONN_ID)
        
        # Primeiro, deleta apenas os objetos (dados das tabelas), mantendo as refs
        hook.run("DELETE FROM public.objs;")
        logging.info("🗑️ Nessie objects deleted")
        
        # Reset do sequence dos objetos
        hook.run("ALTER SEQUENCE public.objs_c_id_seq RESTART WITH 1;")
        logging.info("🔄 Nessie objects sequence reset")
        
        logging.info("✅ Nessie metadata reset successfully")
    except Exception as e:
        logging.error(f"Error resetting Nessie metadata: {str(e)}")
        raise

reset_nessie_task = PythonOperator(
    task_id='reset_nessie_metadata',
    python_callable=reset_nessie_metadata,
    dag=dag
)

end_dag = DummyOperator(task_id='cleanup_complete', dag=dag)

# Dependencies
start_dag >> [delete_warehouse_task, delete_bronze_task, reset_nessie_task] >> end_dag