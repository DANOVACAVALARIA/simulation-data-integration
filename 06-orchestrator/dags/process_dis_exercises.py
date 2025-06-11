from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import json

# Configurações
AWS_CONN_ID = 'minio-ebsim'
SOURCE_BUCKET = 'bronze'
SPARK_CONN_ID = 'spark-ebsim'

# JARs necessários para Iceberg/Nessie + PDF processing
JARS = ('/opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.8.1.jar,'
        '/opt/bitnami/spark/jars/nessie-spark-extensions-3.5_2.12-0.103.2.jar,'
        '/opt/bitnami/spark/jars/nessie-client-0.103.2.jar,'
        '/opt/bitnami/spark/jars/hadoop-aws-3.3.2.jar,'
        '/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar,'
        '/opt/bitnami/spark/jars/pdfbox-2.0.24.jar,'
        '/opt/bitnami/spark/jars/pdfbox-tools-2.0.24.jar,'
        '/opt/bitnami/spark/jars/fontbox-2.0.24.jar')

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 30),
}

dag = DAG("process_dis_exercises", default_args=default_args, schedule_interval=None)

def identify_ready_exercises(**context):
    """Identifica exercícios prontos para processamento (com documentos E simulações)"""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    
    try:
        # Carrega manifest.json
        manifest_content = s3_hook.read_key(
            key='exercises/manifest.json',
            bucket_name=SOURCE_BUCKET
        )
        manifest = json.loads(manifest_content)
        
        ready_exercises = []
        
        # Verifica exercícios em "created"
        for exercise in manifest.get('created', []):
            exercise_id = exercise['id']
            
            try:
                # Carrega metadados do exercício
                metadata_content = s3_hook.read_key(
                    key=f'exercises/{exercise_id}/exercise-metadata.json',
                    bucket_name=SOURCE_BUCKET
                )
                metadata = json.loads(metadata_content)
                
                # Verifica se tem documentos E simulações
                has_documents = len(metadata.get('documents', [])) > 0
                has_simulations = len(metadata.get('simulations', [])) > 0
                
                if has_documents and has_simulations:
                    ready_exercises.append({
                        'id': exercise_id,
                        'metadata': metadata,
                        'manifest_entry': exercise
                    })
                    print(f"Exercício pronto: {exercise_id}")
                    
            except Exception as e:
                print(f"Erro ao verificar exercício {exercise_id}: {e}")
                continue
        
        print(f"Total de exercícios prontos: {len(ready_exercises)}")
        return ready_exercises
        
    except Exception as e:
        print(f"Erro ao carregar manifest: {e}")
        return []

def update_manifest_to_processed(**context):
    """Move exercícios processados de 'created' para 'processed'"""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    
    processed_exercises = context['task_instance'].xcom_pull(task_ids='identify_ready_exercises')
    
    if not processed_exercises:
        print("Nenhum exercício para atualizar no manifest")
        return
    
    try:
        # Carrega manifest atual
        manifest_content = s3_hook.read_key(
            key='exercises/manifest.json',
            bucket_name=SOURCE_BUCKET
        )
        manifest = json.loads(manifest_content)
        
        processed_ids = {ex['id'] for ex in processed_exercises}
        
        # Remove exercícios processados de 'created'
        manifest['created'] = [
            ex for ex in manifest.get('created', []) 
            if ex['id'] not in processed_ids
        ]
        
        # Adiciona exercícios em 'processed'
        if 'processed' not in manifest:
            manifest['processed'] = []
        
        for exercise in processed_exercises:
            processed_entry = exercise['manifest_entry'].copy()
            processed_entry['data']['processedAt'] = datetime.now().isoformat()
            manifest['processed'].append(processed_entry)
        
        # Salva manifest atualizado
        updated_manifest = json.dumps(manifest, indent=2)
        s3_hook.load_string(
            string_data=updated_manifest,
            key='exercises/manifest.json',
            bucket_name=SOURCE_BUCKET,
            replace=True
        )
        
        print(f"Manifest atualizado: {len(processed_exercises)} exercícios movidos para 'processed'")
        
    except Exception as e:
        print(f"Erro ao atualizar manifest: {e}")
        raise

# Task 1: Identifica exercícios prontos
identify_exercises = PythonOperator(
    task_id='identify_ready_exercises',
    python_callable=identify_ready_exercises,
    dag=dag,
)

# Task 2: Processa exercícios para duas tabelas Iceberg separadas
process_exercises_separate = SparkSubmitOperator(
    task_id='process_exercises_to_separate_tables',
    application='/opt/bitnami/spark/jobs/process_dis_exercises_job.py',
    conn_id=SPARK_CONN_ID,
    jars=JARS,
    application_args=["{{ task_instance.xcom_pull(task_ids='identify_ready_exercises') | tojson }}"],
    dag=dag,
)

# Task 3: Atualiza manifest
update_manifest = PythonOperator(
    task_id='update_manifest_to_processed',
    python_callable=update_manifest_to_processed,
    dag=dag,
)

# Dependências
identify_exercises >> process_exercises_separate >> update_manifest