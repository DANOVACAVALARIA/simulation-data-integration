import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, lit
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, FloatType, ArrayType
from datetime import datetime

# Configurações
SOURCE_BUCKET = 'bronze'
WAREHOUSE_BUCKET = 'warehouse'
AWS_S3_ENDPOINT = 'http://minio-storage:9000'
AWS_ACCESS_KEY = 'A3wqz7zjUeDcScdBIoGE'
AWS_SECRET_KEY = 'GPm2xmcElO0yMeW8T6nizC8Bzhtmjr2mFzwGdfqF'
WAREHOUSE = f"s3a://{WAREHOUSE_BUCKET}/"
NESSIE_URI = "http://nessie-app:19120/api/v1"

def extract_text_from_pdf(pdf_bytes):
    """Extrai texto de PDF usando PDFBox"""
    if not pdf_bytes:
        return None
    
    try:
        from py4j.java_gateway import java_import
        
        java_import(spark._jvm, "org.apache.pdfbox.pdmodel.PDDocument")
        java_import(spark._jvm, "org.apache.pdfbox.text.PDFTextStripper")
        java_import(spark._jvm, "java.io.ByteArrayInputStream")
        
        byte_array = spark._jvm.java.io.ByteArrayInputStream(pdf_bytes)
        document = spark._jvm.org.apache.pdfbox.pdmodel.PDDocument.load(byte_array)
        stripper = spark._jvm.org.apache.pdfbox.text.PDFTextStripper()
        text = stripper.getText(document)
        document.close()
        
        if text:
            text = ' '.join(text.split())
            if len(text) > 1000000:
                text = text[:1000000]
        
        return text.strip() if text else None
        
    except Exception as e:
        return f"[ERRO: {str(e)}]"

def read_pdu_data(pdu_bytes):
    """Extrai dados PDU do buffer binário"""
    if not pdu_bytes:
        return []
    
    try:
        from opendis.PduFactory import createPdu
        
        data = []
        buffer = pdu_bytes
        offset = 0
        
        while offset < len(buffer):
            pdu = createPdu(buffer[offset:])
            
            if pdu.pduType == 1:  # Entity State PDU
                data.append({
                    "EntityID": int(pdu.entityID.entityID),
                    "ForceID": int(pdu.forceId),
                    "Kind": int(pdu.entityType.entityKind),
                    "Domain": int(pdu.entityType.domain),
                    "Country": int(pdu.entityType.country),
                    "Category": int(pdu.entityType.category),
                    "Timestamp": int(pdu.timestamp),
                    "Latitude": float(pdu.entityLocation.x),
                    "Longitude": float(pdu.entityLocation.y),
                    "Altitude": float(pdu.entityLocation.z)
                })
            
            offset += pdu.length
            if offset >= len(buffer):
                break
                
    except Exception as e:
        print(f"Erro processando PDU: {e}")
    
    return data

# UDFs
extract_pdf_text_udf = udf(extract_text_from_pdf, StringType())

pdu_schema = ArrayType(StructType([
    StructField("EntityID", IntegerType()),
    StructField("ForceID", IntegerType()),
    StructField("Kind", IntegerType()),
    StructField("Domain", IntegerType()),
    StructField("Country", IntegerType()),
    StructField("Category", IntegerType()),
    StructField("Timestamp", IntegerType()),
    StructField("Latitude", FloatType()),
    StructField("Longitude", FloatType()),
    StructField("Altitude", FloatType())
]))

read_pdu_udf = udf(read_pdu_data, pdu_schema)

def find_bin_files(spark, sim_path):
    """Busca arquivos .bin"""
    try:
        full_path = f"s3a://{SOURCE_BUCKET}/{sim_path}/"
        patterns = [f"{full_path}*.bin", f"{full_path}**/*.bin"]
        
        for pattern in patterns:
            try:
                df = spark.read.format("binaryFile").load(pattern).select("path").limit(5)
                files = [row.path for row in df.collect()]
                if files:
                    return files
            except:
                continue
        return []
    except:
        return []

def process_documents(spark, exercises):
    """Processa documentos e retorna DataFrame"""
    
    document_records = []
    
    for exercise in exercises:
        exercise_id = exercise['id']
        metadata = exercise['metadata']
        
        print(f"Processando documentos do exercício: {exercise_id}")
        
        for doc in metadata.get('documents', []):
            if not doc.get('path'):
                continue
            
            try:
                doc_path = f"s3a://{SOURCE_BUCKET}/{doc['path']}"
                is_pdf = doc.get('name', '').lower().endswith('.pdf')
                
                extracted_text = None
                text_length = 0
                extraction_status = "NOT_PDF"
                
                if is_pdf:
                    try:
                        pdf_df = spark.read.format("binaryFile").load(doc_path)
                        text_result = pdf_df.select(
                            extract_pdf_text_udf(col("content")).alias("text")
                        ).collect()[0]['text']
                        
                        extracted_text = text_result
                        text_length = len(text_result) if text_result else 0
                        
                        if text_result and text_result.startswith("[ERRO"):
                            extraction_status = "ERROR"
                        elif text_length == 0:
                            extraction_status = "EMPTY"
                        else:
                            extraction_status = "SUCCESS"
                            
                    except Exception as e:
                        print(f"Erro PDF {doc.get('name', '')}: {e}")
                        extracted_text = f"[ERRO: {str(e)}]"
                        extraction_status = "ERROR"
                
                document_records.append({
                    'exercise_id': exercise_id,
                    'document_id': doc.get('id', ''),
                    'document_name': doc.get('name', ''),
                    'source_path': doc.get('path', ''),
                    'mime_type': doc.get('mimetype', ''),
                    'extracted_text': extracted_text,
                    'text_length': text_length,
                    'extraction_status': extraction_status,
                    'processed_at': datetime.now()
                })
                
                print(f"  Doc: {doc.get('name', '')} - {extraction_status} - {text_length} chars")
                
            except Exception as e:
                print(f"Erro processando doc {doc.get('name', '')}: {e}")
    
    if document_records:
        return spark.createDataFrame(document_records)
    else:
        # Retorna DataFrame vazio com schema correto
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
        
        schema = StructType([
            StructField("exercise_id", StringType()),
            StructField("document_id", StringType()),
            StructField("document_name", StringType()),
            StructField("source_path", StringType()),
            StructField("mime_type", StringType()),
            StructField("extracted_text", StringType()),
            StructField("text_length", IntegerType()),
            StructField("extraction_status", StringType()),
            StructField("processed_at", TimestampType())
        ])
        return spark.createDataFrame([], schema)

def process_simulations(spark, exercises):
    """Processa simulações e retorna DataFrame"""
    
    simulation_records = []
    
    for exercise in exercises:
        exercise_id = exercise['id']
        metadata = exercise['metadata']
        
        print(f"Processando simulações do exercício: {exercise_id}")
        
        for sim in metadata.get('simulations', []):
            if not sim.get('path'):
                continue
            
            try:
                sim_id = sim.get('id', '')
                sim_name = sim.get('name', sim_id)
                bin_files = find_bin_files(spark, sim['path'])
                
                if not bin_files:
                    print(f"  Sim {sim_id}: nenhum arquivo .bin")
                    continue
                
                print(f"  Sim {sim_id}: {len(bin_files)} arquivos")
                
                # Processa PDUs
                bin_df = spark.read.format("binaryFile").load(bin_files)
                
                pdu_df = bin_df.withColumn("pdu_data", read_pdu_udf(col("content"))) \
                    .filter(col("pdu_data").isNotNull()) \
                    .select(
                        lit(exercise_id).alias("exercise_id"),
                        lit(sim_id).alias("simulation_id"),
                        lit(sim_name).alias("simulation_name"),
                        lit(sim.get('path', '')).alias("source_path"),
                        explode(col("pdu_data")).alias("pdu"),
                        lit(datetime.now()).alias("processed_at")
                    ).select(
                        col("exercise_id"),
                        col("simulation_id"),
                        col("simulation_name"),
                        col("source_path"),
                        col("pdu.EntityID").alias("entity_id"),
                        col("pdu.ForceID").alias("force_id"),
                        col("pdu.Kind").alias("kind"),
                        col("pdu.Domain").alias("domain"),
                        col("pdu.Country").alias("country"),
                        col("pdu.Category").alias("category"),
                        col("pdu.Timestamp").alias("timestamp"),
                        col("pdu.Latitude").alias("latitude"),
                        col("pdu.Longitude").alias("longitude"),
                        col("pdu.Altitude").alias("altitude"),
                        col("processed_at")
                    )
                
                pdu_count = pdu_df.count()
                print(f"  Sim {sim_id}: {pdu_count} PDUs")
                
                # Adiciona à lista de DataFrames
                simulation_records.append(pdu_df)
                
            except Exception as e:
                print(f"Erro processando sim {sim.get('id', '')}: {e}")
    
    # Une todos os DataFrames de simulação
    if simulation_records:
        result_df = simulation_records[0]
        for df in simulation_records[1:]:
            result_df = result_df.union(df)
        return result_df
    else:
        # Retorna DataFrame vazio com schema correto
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
        
        schema = StructType([
            StructField("exercise_id", StringType()),
            StructField("simulation_id", StringType()),
            StructField("simulation_name", StringType()),
            StructField("source_path", StringType()),
            StructField("entity_id", IntegerType()),
            StructField("force_id", IntegerType()),
            StructField("kind", IntegerType()),
            StructField("domain", IntegerType()),
            StructField("country", IntegerType()),
            StructField("category", IntegerType()),
            StructField("timestamp", IntegerType()),
            StructField("latitude", FloatType()),
            StructField("longitude", FloatType()),
            StructField("altitude", FloatType()),
            StructField("processed_at", TimestampType())
        ])
        return spark.createDataFrame([], schema)

def main():
    global spark
    
    # Inicializa Spark
    spark = SparkSession.builder \
        .appName("ProcessExercisesSeparateTables") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", AWS_S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.uri", NESSIE_URI) \
        .config("spark.sql.catalog.nessie.ref", "main") \
        .config("spark.sql.catalog.nessie.warehouse", WAREHOUSE) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .getOrCreate()

    try:
        # Parse argumentos
        if len(sys.argv) < 2:
            print("Erro: Nenhuma lista de exercícios fornecida!")
            sys.exit(1)
        
        exercises = json.loads(sys.argv[1])
        print(f"Processando {len(exercises)} exercícios")
        
        if not exercises:
            return
        
        # Cria namespace
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.exercises")
        
        # Processa documentos
        print("\n=== PROCESSANDO DOCUMENTOS ===")
        documents_df = process_documents(spark, exercises)
        
        if documents_df.count() > 0:
            print(f"Salvando {documents_df.count()} documentos...")
            
            documents_df.write \
                .format("iceberg") \
                .mode("overwrite") \
                .option("write.parquet.compression-codec", "snappy") \
                .partitionBy("exercise_id") \
                .saveAsTable("nessie.exercises.documents")
            
            print("✅ Tabela de documentos salva: nessie.exercises.documents")
        else:
            print("⚠️ Nenhum documento processado")
        
        # Processa simulações
        print("\n=== PROCESSANDO SIMULAÇÕES ===")
        simulations_df = process_simulations(spark, exercises)
        
        if simulations_df.count() > 0:
            print(f"Salvando {simulations_df.count()} PDUs...")
            
            simulations_df.write \
                .format("iceberg") \
                .mode("overwrite") \
                .option("write.parquet.compression-codec", "snappy") \
                .partitionBy("exercise_id", "simulation_id") \
                .saveAsTable("nessie.exercises.simulation_pdus")
            
            print("✅ Tabela de simulações salva: nessie.exercises.simulation_pdus")
        else:
            print("⚠️ Nenhuma simulação processada")
        
        # Mostra estatísticas finais
        print("\n=== ESTATÍSTICAS FINAIS ===")
        
        # Estatísticas de documentos
        if documents_df.count() > 0:
            doc_stats = documents_df.groupBy("exercise_id", "extraction_status").count()
            print("Documentos por exercício e status:")
            doc_stats.show()
        
        # Estatísticas de simulações
        if simulations_df.count() > 0:
            sim_stats = simulations_df.groupBy("exercise_id", "simulation_id").count()
            print("PDUs por exercício e simulação:")
            sim_stats.show()
        
        print("🎉 PROCESSAMENTO CONCLUÍDO!")
        
    except Exception as e:
        print(f"Erro: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()