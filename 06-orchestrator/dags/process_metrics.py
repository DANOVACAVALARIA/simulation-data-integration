from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging
import json
import os
import requests
import statistics

# Configurações
NGINX_LOG_PATH = '/opt/airflow/nginx/access.log'
SPARK_CONN_ID = 'spark-ebsim'
AWS_CONN_ID = 'minio-ebsim'
SPARK_MASTER_URL = 'http://spark-master:8078'
DREMIO_BASE_URL = 'http://dremio-app:9047'
DREMIO_USERNAME = 'admin'
DREMIO_PASSWORD = 'passw0rd'

JARS = (
    '/opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.8.1.jar,'
    '/opt/bitnami/spark/jars/nessie-spark-extensions-3.5_2.12-0.103.2.jar,'
    '/opt/bitnami/spark/jars/nessie-client-0.103.2.jar,'
    '/opt/bitnami/spark/jars/hadoop-aws-3.3.2.jar,'
    '/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar,'
)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 30),
}

dag = DAG("process_metrics",
          default_args=default_args,
          schedule_interval=None,
          tags=["nginx", "airflow", "spark", "dremio", "minio"])

def get_default_metrics():
    """Retorna métricas padrão zeradas"""
    return {
        'avg_mem_use_perc': 0.0,
        'std_mem_use_perc': 0.0,
        'avg_mem_use_gb': 0.0,
        'std_mem_use_gb': 0.0,
        'avg_cpu_use_perc': 0.0,
        'std_cpu_use_perc': 0.0,
    }

def extract_process_exercises_metrics():
    """Extrai métricas da DAG 'process_exercises'"""
    try:
        hook = PostgresHook(postgres_conn_id='airflow-postgres')
        query = """
        SELECT state, EXTRACT(EPOCH FROM (end_date - start_date)) as duration_seconds
        FROM dag_run 
        WHERE dag_id = 'process_exercises' AND start_date IS NOT NULL
        ORDER BY start_date DESC LIMIT 30
        """
        
        executions = hook.get_records(query)
        if not executions:
            logging.warning("No executions found for 'process_exercises'")
            return {**get_default_metrics(), 'avg_success_perc': 0.0, 'avg_success_time': 0.0, 
                    'total_executions': 0, 'successful_executions': 0}
        
        successful = sum(1 for exec in executions if exec[0] == 'success')
        total = len(executions)
        durations = [float(exec[1]) / 60 for exec in executions if exec[1] and float(exec[1]) > 0]
        
        success_perc = (successful / total) * 100 if total > 0 else 0.0
        avg_time = statistics.mean(durations) if durations else 0.0
        std_time = statistics.stdev(durations) if len(durations) > 1 else 0.0
        
        logging.info(f"📊 Process Exercises: {successful}/{total} successful, avg time: {avg_time:.2f}min")
        
        return {
            **get_default_metrics(),
            'avg_success_perc': float(success_perc),
            'avg_success_time': float(avg_time),
            'std_success_time': float(std_time),
            'total_executions': int(total),
            'successful_executions': int(successful)
        }
        
    except Exception as e:
        logging.error(f"Error extracting Airflow metrics: {str(e)}")
        raise

def collect_nginx_metrics():
    """Coleta métricas do nginx"""
    if not os.path.exists(NGINX_LOG_PATH):
        logging.warning(f"Nginx log not found: {NGINX_LOG_PATH}")
        return {'avg_req_success_perc': 0.0, 'avg_req_time': 0.0, 'avg_res_time': 0.0,
                'avg_req_res_total_time': 0.0, 'total_requests': 0, 'successful_requests': 0}
    
    try:
        total_requests = 0
        successful_requests = 0
        request_times = []
        response_times = []
        
        with open(NGINX_LOG_PATH, 'r') as file:
            for line in file:
                if not line.strip():
                    continue
                
                try:
                    log_entry = json.loads(line)
                    total_requests += 1
                    
                    # Success check
                    status = int(log_entry.get('status', 0))
                    if 200 <= status < 300:
                        successful_requests += 1
                    
                    # Times
                    req_time = log_entry.get('response_time')
                    if req_time and str(req_time) != '-':
                        try:
                            request_times.append(float(req_time))
                        except (ValueError, TypeError):
                            pass
                    
                    upstream_time = log_entry.get('upstream_response_time')
                    if upstream_time and str(upstream_time) != '-':
                        try:
                            response_times.append(float(upstream_time))
                        except (ValueError, TypeError):
                            pass
                
                except json.JSONDecodeError:
                    continue
        
        # Calculate metrics
        req_success_perc = (successful_requests / total_requests) * 100 if total_requests > 0 else 0.0
        avg_req_time = statistics.mean(request_times) if request_times else 0.0
        avg_res_time = statistics.mean(response_times) if response_times else 0.0
        
        # Combined time
        combined_times = [req + res for req, res in zip(request_times, response_times) if req and res]
        avg_total_time = statistics.mean(combined_times) if combined_times else 0.0
        
        logging.info(f"🌐 Nginx: {successful_requests}/{total_requests} successful requests")
        
        return {
            'avg_req_success_perc': float(req_success_perc),
            'avg_req_time': float(avg_req_time),
            'avg_res_time': float(avg_res_time),
            'avg_req_res_total_time': float(avg_total_time),
            'total_requests': int(total_requests),
            'successful_requests': int(successful_requests)
        }
        
    except Exception as e:
        logging.error(f"Error collecting nginx metrics: {str(e)}")
        raise

def collect_minio_metrics():
    """Coleta métricas do MinIO"""
    try:
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        bronze_size_gb = 0.0
        warehouse_size_gb = 0.0
        
        for bucket_name in ['bronze', 'warehouse']:
            try:
                objects = s3_hook.list_keys(bucket_name=bucket_name)
                total_size_bytes = 0
                
                if objects:
                    for obj_key in objects:
                        try:
                            obj_info = s3_hook.head_object(key=obj_key, bucket_name=bucket_name)
                            if 'ContentLength' in obj_info:
                                total_size_bytes += obj_info['ContentLength']
                        except Exception:
                            continue
                
                size_gb = total_size_bytes / (1024 * 1024 * 1024)
                if bucket_name == 'bronze':
                    bronze_size_gb = size_gb
                else:
                    warehouse_size_gb = size_gb
                    
                logging.info(f"📊 Bucket {bucket_name}: {size_gb:.3f} GB")
                
            except Exception as e:
                logging.warning(f"Could not access bucket {bucket_name}: {str(e)}")
        
        logging.info(f"🗄️ MinIO: Bronze {bronze_size_gb:.3f}GB, Warehouse {warehouse_size_gb:.3f}GB")
        
        return {
            **get_default_metrics(),
            'bronze_storage_used_gb': float(bronze_size_gb),
            'warehouse_storage_used_gb': float(warehouse_size_gb)
        }
        
    except Exception as e:
        logging.error(f"Error collecting MinIO metrics: {str(e)}")
        raise

def collect_spark_metrics():
    """Coleta métricas do Spark"""
    try:
        response = requests.get(f"{SPARK_MASTER_URL}/json", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            cores = int(data.get('cores', 0))
            memory = int(data.get('memory', 0))
            workers = data.get('workers', [])
            alive_workers = len([w for w in workers if w.get('state') == 'ALIVE']) if isinstance(workers, list) else 0
            
            logging.info(f"⚡ Spark: {cores} cores, {memory}MB memory, {alive_workers} alive workers")
            return get_default_metrics()
        else:
            logging.warning(f"Spark API unavailable: HTTP {response.status_code}")
            raise Exception(f"Spark API returned {response.status_code}")
        
    except Exception as e:
        logging.error(f"Error collecting Spark metrics: {str(e)}")
        raise

def get_dremio_auth_token():
    """Obtém token de autenticação do Dremio"""
    try:
        auth_url = f"{DREMIO_BASE_URL}/apiv2/login"
        payload = {"userName": DREMIO_USERNAME, "password": DREMIO_PASSWORD}
        
        response = requests.post(auth_url, json=payload, 
                               headers={'Content-Type': 'application/json'}, timeout=30)
        
        if response.status_code == 200:
            token = response.json().get('token')
            if token:
                logging.info("🔑 Dremio authentication successful")
                return token
        
        logging.error(f"Dremio authentication failed: HTTP {response.status_code}")
        return None
        
    except Exception as e:
        logging.error(f"Error authenticating with Dremio: {str(e)}")
        return None

def collect_dremio_metrics():
    """Coleta métricas do Dremio"""
    try:
        auth_token = get_dremio_auth_token()
        if not auth_token:
            raise Exception("Failed to authenticate with Dremio")
        
        headers = {
            'Authorization': f'Bearer {auth_token}',
            'Content-Type': 'application/json'
        }
        
        jobs_url = f"{DREMIO_BASE_URL}/apiv2/jobs"
        jobs_response = requests.get(f"{jobs_url}?limit=100", headers=headers, timeout=30)
        
        if jobs_response.status_code == 200:
            jobs_data = jobs_response.json()
            jobs = jobs_data.get('jobs', [])
            
            if jobs:
                successful_jobs = sum(1 for job in jobs if job.get('jobState') == 'COMPLETED')
                total_jobs = len(jobs)
                success_rate = (successful_jobs / total_jobs) * 100 if total_jobs > 0 else 0.0
                
                # Calculate average time
                durations = []
                for job in jobs:
                    start_time = job.get('startTime')
                    end_time = job.get('endTime')
                    if start_time and end_time:
                        duration_seconds = (end_time - start_time) / 1000
                        if duration_seconds > 0:
                            durations.append(duration_seconds)
                
                avg_query_time = statistics.mean(durations) if durations else 0.0
                std_query_time = statistics.stdev(durations) if len(durations) > 1 else 0.0
                
                logging.info(f"🔍 Dremio: {successful_jobs}/{total_jobs} successful jobs, avg time: {avg_query_time:.2f}s")
                
                return {
                    **get_default_metrics(),
                    'avg_query_success_perc': float(success_rate),
                    'avg_query_time': float(avg_query_time),
                    'std_query_time': float(std_query_time)
                }
            else:
                logging.warning("No Dremio jobs found")
                raise Exception("No jobs found")
        else:
            logging.error(f"Failed to fetch Dremio jobs: HTTP {jobs_response.status_code}")
            raise Exception(f"Dremio API returned {jobs_response.status_code}")
        
    except Exception as e:
        logging.error(f"Error collecting Dremio metrics: {str(e)}")
        raise

# Task definitions
start_dag = DummyOperator(task_id='start_metrics_collection', dag=dag)

# Collect tasks
collect_tasks = [
    PythonOperator(task_id='collect_nginx_metrics', python_callable=collect_nginx_metrics, dag=dag),
    PythonOperator(task_id='collect_airflow_metrics', python_callable=extract_process_exercises_metrics, dag=dag),
    PythonOperator(task_id='collect_minio_metrics', python_callable=collect_minio_metrics, dag=dag),
    PythonOperator(task_id='collect_spark_metrics', python_callable=collect_spark_metrics, dag=dag),
    PythonOperator(task_id='collect_dremio_metrics', python_callable=collect_dremio_metrics, dag=dag)
]

# Save tasks
save_tasks = []
metrics_types = ['nginx', 'airflow', 'minio', 'spark', 'dremio']

for i, metrics_type in enumerate(metrics_types):
    save_task = SparkSubmitOperator(
        task_id=f'save_{metrics_type}_metrics_iceberg',
        application='/opt/bitnami/spark/jobs/process_metrics_job.py',
        conn_id=SPARK_CONN_ID,
        jars=JARS,
        application_args=[f"{{{{ task_instance.xcom_pull(task_ids='collect_{metrics_type}_metrics') | tojson }}}}", metrics_type],
        dag=dag,
    )
    save_tasks.append(save_task)
    
    # Dependencies: each collect task feeds into its save task
    collect_tasks[i] >> save_task

# Set dependencies
start_dag >> collect_tasks

# Chain save tasks sequentially to avoid resource conflicts
for i in range(len(save_tasks) - 1):
    save_tasks[i] >> save_tasks[i + 1]