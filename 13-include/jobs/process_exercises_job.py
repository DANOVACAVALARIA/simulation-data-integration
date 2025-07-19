import json
import sys
import io
import struct
from datetime import datetime
from typing import List, Dict, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, explode, lit, size, when
from pyspark.sql.types import (
    StringType, StructType, StructField, IntegerType, 
    FloatType, DoubleType, LongType, ArrayType
)

# ========== CONFIGURAÇÕES ==========
class Config:
    SOURCE_BUCKET = 'bronze'
    WAREHOUSE_BUCKET = 'warehouse'
    AWS_S3_ENDPOINT = 'http://minio-storage:9000'
    AWS_ACCESS_KEY = 'admin'
    AWS_SECRET_KEY = 'password'
    WAREHOUSE = f"s3a://{WAREHOUSE_BUCKET}/"
    NESSIE_URI = "http://nessie-app:19120/api/v1"
    MAX_TEXT_LENGTH = 1_000_000
    MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB

# ========== UTILITÁRIOS DE LOG ==========
def log_info(message: str):
    print(f"[PROCESS EXERCISES] ℹ️ {message}")

def log_success(message: str):
    print(f"[PROCESS EXERCISES] ✅ {message}")

def log_error(message: str):
    print(f"[PROCESS EXERCISES] ❌ {message}")

# ========== PROCESSAMENTO ==========
def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extrai texto de PDF"""
    if not pdf_bytes or len(pdf_bytes) == 0:
        return None
    
    if len(pdf_bytes) > Config.MAX_FILE_SIZE:
        return "[AVISO: Arquivo muito grande]"
    
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            max_pages = min(len(pdf.pages), 50)
            text = '\n'.join([pdf.pages[i].extract_text() or "" for i in range(max_pages)])
            clean_text = ' '.join(text.split())
            
            if len(clean_text) > Config.MAX_TEXT_LENGTH:
                clean_text = clean_text[:Config.MAX_TEXT_LENGTH] + "...[TRUNCADO]"
            
            return clean_text if clean_text else "[VAZIO]"
            
    except ImportError:
        try:
            import PyPDF2
            reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
            max_pages = min(len(reader.pages), 50)
            text = '\n'.join([reader.pages[i].extract_text() or "" for i in range(max_pages)])
            clean_text = ' '.join(text.split())
            
            if len(clean_text) > Config.MAX_TEXT_LENGTH:
                clean_text = clean_text[:Config.MAX_TEXT_LENGTH] + "...[TRUNCADO]"
                
            return clean_text if clean_text else "[VAZIO]"
        except Exception as e:
            return f"[ERRO PyPDF2: {str(e)[:100]}]"
    except Exception as e:
        return f"[ERRO: {str(e)[:100]}]"

def extract_dis_data(dis_bytes: bytes) -> List[Dict]:
    """Extrai dados de arquivo DIS"""
    if not dis_bytes or len(dis_bytes) == 0:
        return []
    
    try:
        from opendis.PduFactory import createPdu
        
        data = []
        buffer = dis_bytes
        offset = 0
        max_records = 10000
        
        while offset < len(buffer) - 12 and len(data) < max_records:
            try:
                pdu = createPdu(buffer[offset:])
                
                if pdu and hasattr(pdu, 'pduType') and pdu.pduType == 1:
                    data.append({
                        "EntityID": int(getattr(pdu.entityID, 'entityID', 0)),
                        "ForceID": int(getattr(pdu, 'forceId', 0)),
                        "Kind": int(getattr(pdu.entityType, 'entityKind', 0)),
                        "Latitude": float(getattr(pdu.entityLocation, 'x', 0.0)),
                        "Longitude": float(getattr(pdu.entityLocation, 'y', 0.0)),
                        "Altitude": float(getattr(pdu.entityLocation, 'z', 0.0)),
                        "Timestamp": int(getattr(pdu, 'timestamp', 0))
                    })
                
                offset += getattr(pdu, 'length', 12)
                
            except Exception:
                offset += 12
                continue
        
        return data
    
    except Exception as e:
        log_error(f"Erro ao extrair dados DIS: {str(e)}")
        return []

def decode_protobuf_messages(protobuf_bytes: bytes) -> List[Dict]:
    """Decodifica mensagens Protocol Buffer"""
    if not protobuf_bytes or len(protobuf_bytes) == 0:
        return []

    try:
        from simulation_client_pb2 import SimToClient
        
        messages = []
        buffer = protobuf_bytes
        index = 0
        max_messages = 10000
        
        while index < len(buffer) and len(messages) < max_messages:
            try:
                if index + 4 > len(buffer):
                    break
                
                length = struct.unpack(">I", buffer[index:index+4])[0]
                index += 4
                
                if length <= 0 or length > 1000000:
                    break
                
                if index + length > len(buffer):
                    break
                    
                msg_data = buffer[index:index+length]
                index += length
                
                msg = SimToClient()
                msg.ParseFromString(msg_data)
                
                message_record = {
                    "context": int(msg.context) if hasattr(msg, 'context') else None,
                    "client_id": int(msg.client_id) if hasattr(msg, 'client_id') else None,
                    "knowledge_id": None,
                    "knowledge_group_id": None,
                    "latitude": None,
                    "longitude": None,
                    "altitude": None,
                    "timestamp": None
                }
                
                if (hasattr(msg, 'message') and 
                    msg.message.HasField('unit_knowledge_update')):
                    
                    u = msg.message.unit_knowledge_update
                    message_record.update({
                        "knowledge_id": int(u.knowledge.id) if hasattr(u, 'knowledge') and hasattr(u.knowledge, 'id') else None,
                        "knowledge_group_id": int(u.knowledge_group.id) if hasattr(u, 'knowledge_group') and hasattr(u.knowledge_group, 'id') else None,
                        "latitude": float(u.position.latitude) if hasattr(u, 'position') and hasattr(u.position, 'latitude') else None,
                        "longitude": float(u.position.longitude) if hasattr(u, 'position') and hasattr(u.position, 'longitude') else None,
                        "altitude": float(u.height_f) if hasattr(u, 'height_f') else None,
                        "timestamp": int(u.pertinence) if hasattr(u, 'pertinence') else None
                    })
                
                messages.append(message_record)
                
            except Exception:
                index += 1
                continue
        
        return messages
        
    except Exception as e:
        log_error(f"Erro ao extrair dados protobuf: {str(e)}")
        return []

# ========== UDFs ==========
extract_pdf_udf = udf(extract_pdf_text, StringType())
extract_dis_udf = udf(extract_dis_data, ArrayType(StructType([
    StructField("EntityID", IntegerType()),
    StructField("ForceID", IntegerType()), 
    StructField("Kind", IntegerType()),
    StructField("Latitude", FloatType()),
    StructField("Longitude", FloatType()),
    StructField("Altitude", FloatType()),
    StructField("Timestamp", IntegerType())
])))

extract_protobuf_udf = udf(decode_protobuf_messages, ArrayType(StructType([
    StructField("context", IntegerType()),
    StructField("client_id", IntegerType()),
    StructField("knowledge_id", IntegerType()),
    StructField("knowledge_group_id", IntegerType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("altitude", DoubleType()),
    StructField("timestamp", LongType())
])))

# ========== PROCESSADOR ==========
class ExerciseProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def process_documents(self, exercise: Dict) -> Optional[DataFrame]:
        """Processa documentos PDF"""
        log_info("Processando documentos PDF")
        
        documents = exercise['metadata'].get('documents', [])
        if not documents:
            return None
        
        pdf_files = []
        doc_metadata = {}
        
        for doc in documents:
            if (doc.get('path') and doc.get('name', '').lower().endswith('.pdf')):
                pdf_path = f"s3a://{Config.SOURCE_BUCKET}/{doc['path']}"
                pdf_files.append(pdf_path)
                doc_metadata[pdf_path] = doc.get('name', doc['path'].split('/')[-1])
        
        if not pdf_files:
            return None
        
        all_records = []
        
        try:
            file_paths = ','.join(pdf_files)
            binary_df = self.spark.read.format("binaryFile").load(file_paths)
            
            processed_df = binary_df.withColumn("extracted_text", extract_pdf_udf(col("content"))) \
                .select(
                    lit(exercise['id']).alias("exercise_id"),
                    col("path").alias("source_file"),
                    col("extracted_text"),
                    when(col("extracted_text").startswith("[ERRO"), "ERROR").otherwise("SUCCESS").alias("status"),
                    lit(str(datetime.now())).alias("processed_at")
                ).filter(col("extracted_text").isNotNull())
            
            batch_results = processed_df.collect()
            
            for row in batch_results:
                file_path = row['source_file']
                document_name = doc_metadata.get(file_path, file_path.split('/')[-1])
                
                record = {
                    'exercise_id': row['exercise_id'],
                    'document_name': document_name,
                    'extracted_text': row['extracted_text'],
                    'status': row['status'],
                    'processed_at': row['processed_at']
                }
                all_records.append(record)
            
        except Exception as e:
            log_error(f"Erro no processamento de documentos: {str(e)}")
            return None
        
        if not all_records:
            return None
        
        result_df = self.spark.createDataFrame(all_records)
        log_success(f"PDFs processados: {result_df.count()} registros")
        
        return result_df

    def process_simulations(self, exercise: Dict) -> Tuple[Optional[DataFrame], str]:
        """Processa simulações"""
        log_info("Processando simulações")
        
        simulations = exercise['metadata'].get('simulations', [])
        if not simulations:
            return None, "NONE"
        
        sim_config = simulations[0] if isinstance(simulations, list) else simulations
        if not sim_config.get('path'):
            return None, "NONE"
        
        files, message_type = self._find_simulation_files(Config.SOURCE_BUCKET, sim_config['path'])
        
        if not files:
            return None, "NONE"
        
        try:
            if message_type == 'DIS':
                result_df = self._process_dis_files(files, exercise['id'])
                return result_df, message_type
            elif message_type == 'PROTO':
                result_df = self._process_protobuf_files(files, exercise['id'])
                return result_df, message_type
            else:
                raise Exception(f"Tipo de mensagem não suportado: {message_type}")
                
        except Exception as e:
            log_error(f"Erro no processamento de simulações: {str(e)}")
            raise

    def _process_dis_files(self, input_files: List[str], exercise_id: str) -> DataFrame:
        """Processa arquivos DIS"""
        try:
            file_paths = ','.join(input_files)
            binary_df = self.spark.read.format("binaryFile").load(file_paths)
            
            processed_df = binary_df.withColumn("pdu_data", extract_dis_udf(col("content"))) \
                .filter(col("pdu_data").isNotNull() & (size(col("pdu_data")) > 0)) \
                .select(
                    lit(exercise_id).alias("exercise_id"),
                    explode(col("pdu_data")).alias("pdu"),
                    lit(str(datetime.now())).alias("processed_at")
                ).select(
                    "exercise_id",
                    col("pdu.EntityID").alias("entity_id"),
                    col("pdu.ForceID").alias("force_id"),
                    col("pdu.Kind").alias("kind"),
                    col("pdu.Latitude").alias("latitude"),
                    col("pdu.Longitude").alias("longitude"),
                    col("pdu.Altitude").alias("altitude"),
                    col("pdu.Timestamp").alias("timestamp"),
                    "processed_at"
                )
            
            count = processed_df.count()
            if count > 0:
                log_success(f"Arquivos DIS processados: {count} registros")
                return processed_df
            else:
                raise Exception("Nenhum registro DIS extraído")
                
        except Exception as e:
            log_error(f"Erro ao processar arquivos DIS: {str(e)}")
            raise

    def _process_protobuf_files(self, input_files: List[str], exercise_id: str) -> DataFrame:
        """Processa arquivos Protobuf"""
        all_records = []
        
        for file_path in input_files:
            try:
                binary_data = self.spark.sparkContext.binaryFiles(file_path).collect()[0][1]
                
                if len(binary_data) == 0:
                    continue
                
                messages = decode_protobuf_messages(binary_data)
                
                for msg in messages:
                    record = {
                        'exercise_id': exercise_id,
                        'context': msg.get('context'),
                        'client_id': msg.get('client_id'),
                        'knowledge_id': msg.get('knowledge_id'),
                        'knowledge_group_id': msg.get('knowledge_group_id'),
                        'latitude': msg.get('latitude'),
                        'longitude': msg.get('longitude'),
                        'altitude': msg.get('altitude'),
                        'timestamp': msg.get('timestamp'),
                        'status': 'SUCCESS',
                        'processed_at': str(datetime.now())
                    }
                    all_records.append(record)
                
            except Exception as e:
                log_error(f"Erro ao processar arquivo {file_path}: {str(e)}")
                continue
        
        if not all_records:
            raise Exception("Nenhuma mensagem protobuf extraída")
        
        result_df = self.spark.createDataFrame(all_records)
        total_count = result_df.count()
        log_success(f"Protobuf processado: {total_count} registros")
        
        return result_df

    def save_table(self, df: DataFrame, table_name: str):
        """Salva DataFrame como tabela Iceberg"""
        try:
            count = df.count()
            log_info(f"Salvando {count} registros na tabela {table_name}")
            
            if count > 0:
                table_exists = self._table_exists(table_name)
                mode = "append" if table_exists else "overwrite"
                
                df.write.format("iceberg").mode(mode).saveAsTable(table_name)
                log_success(f"Tabela {table_name} salva com sucesso")
            else:
                log_error(f"DataFrame vazio para tabela {table_name}")
                
        except Exception as e:
            log_error(f"Erro ao salvar tabela {table_name}: {str(e)}")
            raise

    def _find_simulation_files(self, bucket: str, sim_path: str) -> Tuple[List[str], str]:
        """Busca arquivos de simulação"""
        try:
            full_path = f"s3a://{bucket}/{sim_path.rstrip('/')}/"
            
            all_files = self.spark.read.option("recursiveFileLookup", "true") \
                .format("binaryFile").load(f"{full_path}**") \
                .select("path", "length").collect()

            bin_files = []
            pb_files = []
            
            for row in all_files:
                file_path = row.path
                
                if row.length > Config.MAX_FILE_SIZE:
                    continue
                    
                if file_path.lower().endswith('.bin'):
                    bin_files.append(file_path)
                elif file_path.lower().endswith('.pb'):
                    pb_files.append(file_path)

            if bin_files:
                return bin_files, "DIS"
            elif pb_files:
                return pb_files, "PROTO"
            else:
                raise Exception("Nenhum arquivo de simulação encontrado")
            
        except Exception as e:
            log_error(f"Erro ao buscar arquivos: {str(e)}")
            raise

    def _table_exists(self, table_name: str) -> bool:
        """Verifica se tabela existe"""
        try:
            self.spark.sql(f"DESCRIBE TABLE {table_name}")
            return True
        except Exception:
            return False

def create_spark_session() -> SparkSession:
    """Cria sessão Spark"""
    log_info("Criando sessão Spark")
    
    try:
        configs = {
            # S3
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.access.key": Config.AWS_ACCESS_KEY,
            "spark.hadoop.fs.s3a.secret.key": Config.AWS_SECRET_KEY,
            "spark.hadoop.fs.s3a.endpoint": Config.AWS_S3_ENDPOINT,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            
            # Iceberg/Nessie
            "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            "spark.sql.catalog.nessie.uri": Config.NESSIE_URI,
            "spark.sql.catalog.nessie.ref": "main",
            "spark.sql.catalog.nessie.warehouse": Config.WAREHOUSE,
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        }
        
        builder = SparkSession.builder.appName("ProcessExercises")
        for key, value in configs.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        log_success("Sessão Spark criada com sucesso")
        return spark
        
    except Exception as e:
        log_error(f"Erro ao criar sessão Spark: {str(e)}")
        raise

def main():
    """Função principal"""
    log_info("Iniciando processamento de exercícios")
    
    if len(sys.argv) < 2:
        log_error("Parâmetros insuficientes")
        sys.exit(1)
    
    spark = None
    try:
        selected_exercise = json.loads(sys.argv[1])
        log_info(f"Exercício: {selected_exercise.get('id', 'ID não encontrado')}")
        
        spark = create_spark_session()
        processor = ExerciseProcessor(spark)
        
        # Criar namespace
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.exercises")
        log_success("Namespace criado/verificado")
        
        # Processar documentos
        documents_df = processor.process_documents(selected_exercise)
        if documents_df:
            processor.save_table(documents_df, "nessie.exercises.pdf_documents")
        
        # Processar simulações
        has_simulations = bool(selected_exercise['metadata'].get('simulations', []))
        simulation_processed = False
        
        if has_simulations:
            simulations_df, message_type = processor.process_simulations(selected_exercise)
            
            if simulations_df:
                if message_type == "DIS":
                    processor.save_table(simulations_df, "nessie.exercises.dis_messages")
                elif message_type == "PROTO":
                    processor.save_table(simulations_df, "nessie.exercises.protobuf_messages")
                simulation_processed = True
            else:
                raise Exception("Falha no processamento de simulações")
        else:
            simulation_processed = True
        
        if has_simulations and not simulation_processed:
            raise Exception("Falha crítica no processamento de simulações")
        
        log_success("Processamento concluído com sucesso!")
        
    except Exception as e:
        log_error(f"Erro crítico: {str(e)}")
        sys.exit(1)
        
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()