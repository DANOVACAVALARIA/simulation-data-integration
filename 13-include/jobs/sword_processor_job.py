import argparse
import struct
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
from pyspark.sql.functions import col, lit


from simulation_client_pb2 import SimToClient
# # Importa o schema protobuf
# try:
#     # Tenta import via package instalado
#     from simulation_client_pb2 import SimToClient
#     print("Protobuf schema imported successfully from protobuf_schemas package")
# except ImportError as e:
#     print(f"Error importing from package: {e}")
#     try:
#         # Fallback para sys.path direto
#         import sys
#         import os
#         sys.path.insert(0, '/opt/bitnami/spark/jobs')
#         from simulation_client_pb2 import SimToClient
#         print("Protobuf schema imported successfully from jobs directory")
#     except ImportError as e2:
#         print(f"Error importing from jobs directory: {e2}")
#         print("Debugging information:")
#         print(f"Python path: {sys.path}")
#         print(f"Files in jobs dir: {os.listdir('/opt/bitnami/spark/jobs') if os.path.exists('/opt/bitnami/spark/jobs') else 'Jobs dir not found'}")
#         raise

def decode_messages_spark(spark, input_file):
    """
    Decodifica mensagens Protocol Buffer usando Spark
    """
    # Lê o arquivo binário
    binary_data = spark.sparkContext.binaryFiles(input_file).collect()[0][1]
    
    binary_data_2 = spark.read.format('binaryFile').load(input_file).collect('content')

    print(f"======binary_data:{binary_data}======")
    print(f"======binary_data_2:{binary_data_2}======")
    
    messages = []
    index = 0
    msg_num = 1
    
    while index < len(binary_data):
        # Lê tamanho (4 bytes)
        if index + 4 > len(binary_data):
            break
        
        length = struct.unpack(">I", binary_data[index:index+4])[0]
        index += 4
        
        # Lê mensagem
        if index + length > len(binary_data):
            break
            
        msg_data = binary_data[index:index+length]
        index += length
        
        # Decodifica
        try:
            msg = SimToClient()
            msg.ParseFromString(msg_data)
            
            # Extrai dados da mensagem
            row_data = {
                'message_num': msg_num,
                'context': msg.context,
                'client_id': msg.client_id,
                'knowledge_id': None,
                'group_id': None,
                'latitude': None,
                'longitude': None,
                'pertinence': None
            }
            
            # Verifica se tem unit_knowledge_update
            if msg.message.HasField('unit_knowledge_update'):
                u = msg.message.unit_knowledge_update
                row_data.update({
                    'knowledge_id': u.knowledge.id,
                    'group_id': u.knowledge_group.id,
                    'latitude': u.position.latitude,
                    'longitude': u.position.longitude,
                    'pertinence': u.pertinence
                })
            
            messages.append(row_data)
            
        except Exception as e:
            print(f"Erro ao decodificar mensagem {msg_num}: {e}")
        
        msg_num += 1
    
    # Define schema do DataFrame
    schema = StructType([
        StructField("message_num", IntegerType(), True),
        StructField("context", IntegerType(), True),
        StructField("client_id", IntegerType(), True),
        StructField("knowledge_id", IntegerType(), True),
        StructField("group_id", IntegerType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("pertinence", LongType(), True)
    ])
    
    # Cria DataFrame
    df = spark.createDataFrame(messages, schema)
    
    # Mostra estatísticas e dados
    print(f"Total de mensagens processadas: {df.count()}")
    print("Dados processados:")
    df.show(truncate=False)
    
    return df

def main():
    parser = argparse.ArgumentParser(description='Process Sword Protocol Buffer messages')
    parser.add_argument('--input-file', required=True, help='Input binary file path')
    
    args = parser.parse_args()
    
    # Inicializa Spark Session
    spark = SparkSession.builder \
        .appName("SwordMessageProcessor") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Processa as mensagens
        df = decode_messages_spark(spark, args.input_file)
        
        print("Processamento concluído com sucesso!")
        
    except Exception as e:
        print(f"Erro durante o processamento: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()