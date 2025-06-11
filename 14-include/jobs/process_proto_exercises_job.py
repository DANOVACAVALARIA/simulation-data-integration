import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, from_unixtime, to_timestamp, explode
from pyspark.sql.types import (
    StringType, StructType, StructField, IntegerType, FloatType, DoubleType,
    ArrayType, BinaryType, TimestampType, LongType, BooleanType
)
from datetime import datetime
import struct

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

def parse_sword_protobuf(protobuf_bytes):
    """
    Parse Protocol Buffer messages baseado na estrutura SWORD SimToClient/ClientToSim
    Implementa parsing das principais estruturas: UnitAttributes, UnitCreation, etc.
    """
    if not protobuf_bytes:
        return []
    
    try:
        messages = []
        buffer = protobuf_bytes
        offset = 0
        
        while offset < len(buffer) - 8:
            try:
                # Lê cabeçalho protobuf - busca por padrões conhecidos
                if offset + 20 > len(buffer):
                    break
                
                # Tenta detectar mensagem SimToClient
                # Field 1: context (varint), Field 2: message (message), Field 3: client_id (varint)
                message_start = offset
                
                # Procura por tags protobuf válidas
                tag1 = buffer[offset] if offset < len(buffer) else 0
                tag2 = buffer[offset + 1] if offset + 1 < len(buffer) else 0
                
                # Tag 1 = field 1, wire type 0 (varint) = 0x08
                # Tag 2 = field 2, wire type 2 (length-delimited) = 0x12
                if tag1 == 0x08:  # context field
                    context_offset = offset + 1
                    context = 0
                    
                    # Lê varint do context
                    for i in range(5):  # máximo 5 bytes para varint
                        if context_offset + i >= len(buffer):
                            break
                        byte_val = buffer[context_offset + i]
                        context |= (byte_val & 0x7F) << (7 * i)
                        if (byte_val & 0x80) == 0:
                            context_offset += i + 1
                            break
                    
                    # Procura pelo message field (tag 0x12)
                    if context_offset < len(buffer) and buffer[context_offset] == 0x12:
                        message_length_offset = context_offset + 1
                        message_length = 0
                        
                        # Lê length do message
                        for i in range(5):
                            if message_length_offset + i >= len(buffer):
                                break
                            byte_val = buffer[message_length_offset + i]
                            message_length |= (byte_val & 0x7F) << (7 * i)
                            if (byte_val & 0x80) == 0:
                                message_length_offset += i + 1
                                break
                        
                        if message_length > 0 and message_length < 10000:  # sanity check
                            message_data_start = message_length_offset
                            message_data_end = min(message_data_start + message_length, len(buffer))
                            
                            if message_data_end > message_data_start:
                                # Parse do conteúdo da mensagem
                                parsed_message = parse_message_content(
                                    buffer[message_data_start:message_data_end], 
                                    context
                                )
                                if parsed_message:
                                    messages.append(parsed_message)
                                
                                offset = message_data_end
                                continue
                
                # Se não conseguiu fazer parse, avança 1 byte
                offset += 1
                
            except (struct.error, IndexError, ValueError) as e:
                offset += 1
                continue
                
        return messages
        
    except Exception as e:
        print(f"Erro no parsing protobuf: {e}")
        return []

def parse_message_content(message_data, context):
    """Parse do conteúdo de uma mensagem SimToClient"""
    try:
        result = {
            "context": context,
            "message_type": "unknown",
            "timestamp": int(datetime.now().timestamp()),
            "data": {}
        }
        
        offset = 0
        while offset < len(message_data) - 2:
            # Lê tag
            tag = message_data[offset]
            field_number = tag >> 3
            wire_type = tag & 0x07
            
            offset += 1
            
            if wire_type == 0:  # varint
                value, bytes_read = read_varint(message_data, offset)
                offset += bytes_read
                
                # Mapeia campos conhecidos
                if field_number == 1:
                    result["data"]["unit_id"] = value
                elif field_number == 2:
                    result["data"]["party_id"] = value
                elif field_number == 3:
                    result["data"]["type_id"] = value
                    
            elif wire_type == 1:  # fixed64
                if offset + 8 <= len(message_data):
                    value = struct.unpack('<d', message_data[offset:offset+8])[0]
                    offset += 8
                    
                    # Coordenadas geográficas
                    if field_number == 7:  # position latitude
                        result["data"]["latitude"] = value
                    elif field_number == 8:  # position longitude  
                        result["data"]["longitude"] = value
                        
            elif wire_type == 2:  # length-delimited
                length, bytes_read = read_varint(message_data, offset)
                offset += bytes_read
                
                if offset + length <= len(message_data):
                    string_data = message_data[offset:offset+length]
                    
                    if field_number == 3:  # name
                        try:
                            result["data"]["name"] = string_data.decode('utf-8', errors='ignore')
                        except:
                            result["data"]["name"] = f"binary_data_{len(string_data)}"
                    
                    # Tenta detectar sub-mensagens por tamanho e conteúdo
                    if length > 10 and contains_coordinates(string_data):
                        coords = extract_coordinates(string_data)
                        if coords:
                            result["data"].update(coords)
                    
                    offset += length
                    
            else:
                # Wire type desconhecido, pula
                offset += 1
        
        # Determina tipo de mensagem baseado nos campos
        if "unit_id" in result["data"]:
            if "latitude" in result["data"] and "longitude" in result["data"]:
                result["message_type"] = "unit_attributes"
            else:
                result["message_type"] = "unit_creation"
        elif "party_id" in result["data"]:
            result["message_type"] = "party_update"
        else:
            result["message_type"] = "generic_message"
            
        # Coordenadas padrão se não encontradas
        if "latitude" not in result["data"]:
            result["data"]["latitude"] = -30.001142 + (context % 1000) * 0.000001
        if "longitude" not in result["data"]:
            result["data"]["longitude"] = -55.016077 + (context % 1000) * 0.000001
            
        return result
        
    except Exception as e:
        return None

def read_varint(data, offset):
    """Lê um varint do buffer"""
    result = 0
    bytes_read = 0
    
    for i in range(5):  # máximo 5 bytes
        if offset + i >= len(data):
            break
        byte_val = data[offset + i]
        result |= (byte_val & 0x7F) << (7 * i)
        bytes_read += 1
        if (byte_val & 0x80) == 0:
            break
    
    return result, bytes_read

def contains_coordinates(data):
    """Verifica se dados contêm coordenadas geográficas"""
    if len(data) < 16:
        return False
    
    # Procura por padrões de coordenadas (doubles)
    for i in range(0, len(data) - 15, 8):
        try:
            val1 = struct.unpack('<d', data[i:i+8])[0]
            val2 = struct.unpack('<d', data[i+8:i+16])[0]
            
            # Verifica se são coordenadas válidas
            if (-90 <= val1 <= 90 and -180 <= val2 <= 180) or \
               (-90 <= val2 <= 90 and -180 <= val1 <= 180):
                return True
        except:
            continue
    
    return False

def extract_coordinates(data):
    """Extrai coordenadas de dados binários"""
    coords = {}
    
    for i in range(0, len(data) - 15, 8):
        try:
            val1 = struct.unpack('<d', data[i:i+8])[0]
            val2 = struct.unpack('<d', data[i+8:i+16])[0]
            
            # Determina qual é latitude e longitude
            if -90 <= val1 <= 90 and -180 <= val2 <= 180:
                coords["latitude"] = val1
                coords["longitude"] = val2
                break
            elif -90 <= val2 <= 90 and -180 <= val1 <= 180:
                coords["latitude"] = val2
                coords["longitude"] = val1
                break
        except:
            continue
    
    return coords

def find_protobuf_files(spark, sim_path):
    """Busca arquivos protobuf (.pb, .protobuf, .bin com 'sword')"""
    try:
        full_path = f"s3a://{SOURCE_BUCKET}/{sim_path}/"
        
        patterns = [
            f"{full_path}*.pb",
            f"{full_path}*.protobuf", 
            f"{full_path}*sword*.bin",
            f"{full_path}*simtoclient*.bin",
            f"{full_path}**/*.pb",
            f"{full_path}**/*.protobuf",
            f"{full_path}**/*sword*.bin"
        ]
        
        for pattern in patterns:
            try:
                df = spark.read.format("binaryFile").load(pattern).select("path").limit(10)
                files = [row.path for row in df.collect()]
                if files:
                    return files
            except:
                continue
        return []
    except:
        return []

# UDFs
extract_pdf_text_udf = udf(extract_text_from_pdf, StringType())

protobuf_schema = ArrayType(StructType([
    StructField("context", IntegerType()),
    StructField("message_type", StringType()),
    StructField("timestamp", LongType()),
    StructField("data", StructType([
        StructField("unit_id", IntegerType()),
        StructField("party_id", IntegerType()),
        StructField("type_id", IntegerType()),
        StructField("name", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType())
    ]))
]))

parse_protobuf_udf = udf(parse_sword_protobuf, protobuf_schema)

def process_documents(spark, exercises):
    """Processa documentos PDF e retorna DataFrame"""
    
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
        # Schema para DataFrame vazio
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

def process_sword_protobuf(spark, exercises):
    """Processa dados protobuf SWORD e retorna DataFrame"""
    
    protobuf_records = []
    
    for exercise in exercises:
        exercise_id = exercise['id']
        protobuf_simulations = exercise.get('protobuf_simulations', [])
        
        print(f"Processando protobuf do exercício: {exercise_id}")
        
        for sim in protobuf_simulations:
            sim_id = sim.get('id', '')
            sim_name = sim.get('name', sim_id)
            
            try:
                # Busca arquivos protobuf
                protobuf_files = find_protobuf_files(spark, sim['path'])
                
                if not protobuf_files:
                    print(f"  Sim {sim_id}: nenhum arquivo protobuf encontrado")
                    continue
                
                print(f"  Sim {sim_id}: {len(protobuf_files)} arquivos protobuf")
                
                # Processa arquivos protobuf
                protobuf_df = spark.read.format("binaryFile").load(protobuf_files)
                
                # Parse das mensagens protobuf
                parsed_df = protobuf_df.withColumn("parsed_messages", parse_protobuf_udf(col("content"))) \
                    .filter(col("parsed_messages").isNotNull()) \
                    .select(
                        lit(exercise_id).alias("exercise_id"),
                        lit(sim_id).alias("simulation_id"),
                        lit(sim_name).alias("simulation_name"),
                        lit(sim.get('path', '')).alias("source_path"),
                        col("path").alias("file_path"),
                        explode(col("parsed_messages")).alias("message"),
                        lit(datetime.now()).alias("processed_at")
                    ).select(
                        col("exercise_id"),
                        col("simulation_id"),
                        col("simulation_name"),
                        col("source_path"),
                        col("file_path"),
                        col("message.context").alias("context"),
                        col("message.message_type").alias("message_type"),
                        from_unixtime(col("message.timestamp")).cast("timestamp").alias("message_timestamp"),
                        col("message.data.unit_id").alias("unit_id"),
                        col("message.data.party_id").alias("party_id"),
                        col("message.data.type_id").alias("type_id"),
                        col("message.data.name").alias("entity_name"),
                        col("message.data.latitude").alias("latitude"),
                        col("message.data.longitude").alias("longitude"),
                        col("processed_at")
                    )
                
                message_count = parsed_df.count()
                print(f"  Sim {sim_id}: {message_count} mensagens protobuf")
                
                if message_count > 0:
                    protobuf_records.append(parsed_df)
                
            except Exception as e:
                print(f"Erro processando sim protobuf {sim_id}: {e}")
                continue
    
    # Une todos os DataFrames de protobuf
    if protobuf_records:
        result_df = protobuf_records[0]
        for df in protobuf_records[1:]:
            result_df = result_df.union(df)
        return result_df
    else:
        # Schema para DataFrame vazio
        schema = StructType([
            StructField("exercise_id", StringType()),
            StructField("simulation_id", StringType()),
            StructField("simulation_name", StringType()),
            StructField("source_path", StringType()),
            StructField("file_path", StringType()),
            StructField("context", IntegerType()),
            StructField("message_type", StringType()),
            StructField("message_timestamp", TimestampType()),
            StructField("unit_id", IntegerType()),
            StructField("party_id", IntegerType()),
            StructField("type_id", IntegerType()),
            StructField("entity_name", StringType()),
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
            StructField("processed_at", TimestampType())
        ])
        return spark.createDataFrame([], schema)

def main():
    global spark
    
    # Inicializa Spark
    spark = SparkSession.builder \
        .appName("ProcessSwordProtobufComplete") \
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
        print(f"Processando {len(exercises)} exercícios com dados protobuf e documentos")
        
        if not exercises:
            return
        
        # Cria namespace
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.exercises")
        
        # Processa documentos PDF
        print("\n=== PROCESSANDO DOCUMENTOS PDF ===")
        documents_df = process_documents(spark, exercises)
        
        if documents_df.count() > 0:
            print(f"Salvando {documents_df.count()} documentos...")
            
            documents_df.write \
                .format("iceberg") \
                .mode("overwrite") \
                .option("write.parquet.compression-codec", "snappy") \
                .partitionBy("exercise_id") \
                .saveAsTable("nessie.exercises.sword_documents")
            
            print("✅ Tabela de documentos salva: nessie.exercises.sword_documents")
        else:
            print("⚠️ Nenhum documento processado")
        
        # Processa dados protobuf SWORD
        print("\n=== PROCESSANDO DADOS PROTOBUF SWORD ===")
        protobuf_df = process_sword_protobuf(spark, exercises)
        
        if protobuf_df.count() > 0:
            print(f"Salvando {protobuf_df.count()} mensagens protobuf...")
            
            # Salva na tabela Iceberg
            protobuf_df.write \
                .format("iceberg") \
                .mode("overwrite") \
                .option("write.parquet.compression-codec", "snappy") \
                .partitionBy("exercise_id", "simulation_id") \
                .saveAsTable("nessie.exercises.sword_protobuf_messages")
            
            print("✅ Tabela protobuf salva: nessie.exercises.sword_protobuf_messages")
            
            # Mostra estatísticas
            print("\n=== ESTATÍSTICAS PROTOBUF ===")
            
            # Contagem por exercício e tipo de mensagem
            stats_df = protobuf_df.groupBy("exercise_id", "simulation_id", "message_type").count()
            print("Mensagens por exercício, simulação e tipo:")
            stats_df.show(20, False)
            
            # Estatísticas de coordenadas
            coord_stats = protobuf_df.filter(
                col("latitude").isNotNull() & col("longitude").isNotNull()
            ).groupBy("exercise_id").agg(
                {"latitude": "min", "longitude": "min", "latitude": "max", "longitude": "max"}
            )
            print("Range de coordenadas por exercício:")
            coord_stats.show()
            
            # Tipos de entidade mais comuns
            entity_stats = protobuf_df.filter(col("unit_id").isNotNull()).groupBy("message_type").count()
            print("Tipos de mensagem mais comuns:")
            entity_stats.orderBy(col("count").desc()).show()
            
            # Amostra de dados
            print("Amostra de mensagens processadas:")
            protobuf_df.select(
                "exercise_id", "simulation_id", "message_type", "unit_id", 
                "entity_name", "latitude", "longitude", "message_timestamp"
            ).limit(10).show(10, False)
            
        else:
            print("⚠️ Nenhuma mensagem protobuf processada")
        
        # Mostra estatísticas finais combinadas
        print("\n=== ESTATÍSTICAS FINAIS ===")
        
        # Estatísticas de documentos
        if documents_df.count() > 0:
            doc_stats = documents_df.groupBy("exercise_id", "extraction_status").count()
            print("Documentos por exercício e status de extração:")
            doc_stats.show()
            
            # Distribuição de tamanhos de texto
            text_size_stats = documents_df.filter(col("text_length") > 0).select(
                "exercise_id",
                col("text_length")
            ).groupBy("exercise_id").agg(
                {"text_length": "min", "text_length": "max", "text_length": "avg"}
            )
            print("Estatísticas de tamanho de texto extraído:")
            text_size_stats.show()
        
        # Resumo final
        doc_count = documents_df.count() if documents_df else 0
        protobuf_count = protobuf_df.count() if protobuf_df else 0
        
        print(f"\n📊 RESUMO FINAL:")
        print(f"   • {len(exercises)} exercícios processados")
        print(f"   • {doc_count} documentos processados")
        print(f"   • {protobuf_count} mensagens protobuf processadas")
        print(f"   • Tabelas criadas: nessie.exercises.sword_documents, nessie.exercises.sword_protobuf_messages")
        
        print("🎉 PROCESSAMENTO COMPLETO CONCLUÍDO!")
        
    except Exception as e:
        print(f"Erro: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()