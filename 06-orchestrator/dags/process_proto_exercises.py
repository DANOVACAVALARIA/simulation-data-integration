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

# JARs necessários para Iceberg/Nessie + Protocol Buffers processing
JARS = ('/opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.8.1.jar,'
        '/opt/bitnami/spark/jars/nessie-spark-extensions-3.5_2.12-0.103.2.jar,'
        '/opt/bitnami/spark/jars/nessie-client-0.103.2.jar,'
        '/opt/bitnami/spark/jars/hadoop-aws-3.3.2.jar,'
        '/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar,'
        '/opt/bitnami/spark/jars/protobuf-java-3.21.12.jar,'
        '/opt/bitnami/spark/jars/pdfbox-2.0.24.jar,'
        '/opt/bitnami/spark/jars/pdfbox-tools-2.0.24.jar,'
        '/opt/bitnami/spark/jars/fontbox-2.0.24.jar')

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 30),
}

dag = DAG("process_proto_exercises", default_args=default_args, schedule_interval=None)

def identify_ready_protobuf_exercises(**context):
    """Identifica exercícios com dados Protocol Buffers e documentos PDF prontos"""
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
                
                # Verifica se tem documentos E simulações protobuf
                has_documents = len(metadata.get('documents', [])) > 0
                has_protobuf_simulations = False
                protobuf_simulations = []
                
                for sim in metadata.get('simulations', []):
                    sim_path = sim.get('path', '')
                    
                    # Verifica se a simulação contém arquivos protobuf/SWORD
                    try:
                        # Lista arquivos na pasta da simulação
                        sim_objects = s3_hook.list_keys(
                            bucket_name=SOURCE_BUCKET,
                            prefix=f"{sim_path}/",
                            delimiter=''
                        )
                        
                        protobuf_files = []
                        for obj in sim_objects:
                            # Busca por arquivos protobuf relacionados ao SWORD
                            if (obj.endswith(('.pb', '.protobuf', '.bin')) and 
                                ('sword' in obj.lower() or 'simtoclient' in obj.lower() or 
                                 'clienttosim' in obj.lower())):
                                protobuf_files.append(obj)
                        
                        if protobuf_files:
                            has_protobuf_simulations = True
                            sim['protobuf_files'] = protobuf_files
                            protobuf_simulations.append(sim)
                            
                    except Exception as e:
                        print(f"Erro ao verificar arquivos protobuf para simulação {sim.get('id', '')}: {e}")
                        continue
                
                # Precisa ter documentos E simulações protobuf
                if has_documents and has_protobuf_simulations:
                    ready_exercises.append({
                        'id': exercise_id,
                        'metadata': metadata,
                        'manifest_entry': exercise,
                        'protobuf_simulations': protobuf_simulations
                    })
                    print(f"Exercício com docs e protobuf pronto: {exercise_id} "
                          f"({len(protobuf_simulations)} simulações, {len(metadata.get('documents', []))} documentos)")
                    
            except Exception as e:
                print(f"Erro ao verificar exercício {exercise_id}: {e}")
                continue
        
        print(f"Total de exercícios com docs+protobuf prontos: {len(ready_exercises)}")
        return ready_exercises
        
    except Exception as e:
        print(f"Erro ao carregar manifest: {e}")
        return []

def update_manifest_to_processed(**context):
    """Move exercícios processados de 'created' para 'processed'"""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    
    processed_exercises = context['task_instance'].xcom_pull(task_ids='identify_ready_protobuf_exercises')
    
    if not processed_exercises:
        print("Nenhum exercício protobuf+docs para atualizar no manifest")
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
            processed_entry['data']['protobufProcessedAt'] = datetime.now().isoformat()
            processed_entry['data']['documentsProcessedAt'] = datetime.now().isoformat()
            manifest['processed'].append(processed_entry)
        
        # Salva manifest atualizado
        updated_manifest = json.dumps(manifest, indent=2)
        s3_hook.load_string(
            string_data=updated_manifest,
            key='exercises/manifest.json',
            bucket_name=SOURCE_BUCKET,
            replace=True
        )
        
        print(f"Manifest atualizado: {len(processed_exercises)} exercícios protobuf+docs movidos para 'processed'")
        
    except Exception as e:
        print(f"Erro ao atualizar manifest: {e}")
        raise

# Task 1: Identifica exercícios com dados protobuf e documentos prontos
identify_protobuf_exercises = PythonOperator(
    task_id='identify_ready_protobuf_exercises',
    python_callable=identify_ready_protobuf_exercises,
    dag=dag,
)

# Task 2: Processa dados protobuf e documentos para tabelas Iceberg separadas
process_protobuf_and_docs = SparkSubmitOperator(
    task_id='process_sword_protobuf_and_documents',
    application='/opt/bitnami/spark/jobs/process_proto_exercises_job.py',
    conn_id=SPARK_CONN_ID,
    jars=JARS,
    application_args=["{{ task_instance.xcom_pull(task_ids='identify_ready_protobuf_exercises') | tojson }}"],
    dag=dag,
)

# Task 3: Atualiza manifest
update_manifest = PythonOperator(
    task_id='update_manifest_to_processed',
    python_callable=update_manifest_to_processed,
    dag=dag,
)

# Dependências
identify_protobuf_exercises >> process_protobuf_and_docs >> update_manifest