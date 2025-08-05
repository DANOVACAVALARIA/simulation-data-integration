import json
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Configuration
class Config:
    WAREHOUSE_BUCKET = 'warehouse'
    AWS_S3_ENDPOINT = 'http://minio-storage:9000'
    AWS_ACCESS_KEY = 'admin'
    AWS_SECRET_KEY = 'password'
    WAREHOUSE = f"s3a://{WAREHOUSE_BUCKET}/"
    NESSIE_URI = "http://nessie-app:19120/api/v1"

def log_info(message: str):
    print(f"[METRICS] ℹ️ {message}")

def log_success(message: str):
    print(f"[METRICS] ✅ {message}")

def log_error(message: str):
    print(f"[METRICS] ❌ {message}")

# Specific schemas matching existing tables exactly
SCHEMAS = {
    'airflow': StructType([
        StructField("dag_id", StringType(), True),
        StructField("analysis_date", TimestampType(), True),
        StructField("avg_mem_use_perc", DoubleType(), True),
        StructField("std_mem_use_perc", DoubleType(), True),
        StructField("avg_mem_use_gb", DoubleType(), True),
        StructField("std_mem_use_gb", DoubleType(), True),
        StructField("avg_cpu_use_perc", DoubleType(), True),
        StructField("std_cpu_use_perc", DoubleType(), True),
        StructField("avg_success_perc", DoubleType(), True),
        StructField("std_success_perc", DoubleType(), True),
        StructField("avg_success_time", DoubleType(), True),
        StructField("std_success_time", DoubleType(), True),
        StructField("total_executions", StringType(), True),
        StructField("successful_executions", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ]),
    
    'nginx': StructType([
        StructField("service_type", StringType(), True),
        StructField("analysis_date", TimestampType(), True),
        StructField("avg_req_success_perc", DoubleType(), True),
        StructField("std_req_success_perc", DoubleType(), True),
        StructField("avg_res_success_perc", DoubleType(), True),
        StructField("std_res_success_perc", DoubleType(), True),
        StructField("avg_req_time", DoubleType(), True),
        StructField("std_req_time", DoubleType(), True),
        StructField("avg_res_time", DoubleType(), True),
        StructField("std_res_time", DoubleType(), True),
        StructField("avg_req_res_total_time", DoubleType(), True),
        StructField("std_req_res_total_time", DoubleType(), True),
        StructField("total_requests", StringType(), True),
        StructField("successful_requests", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ]),
    
    'minio': StructType([
        StructField("service_type", StringType(), True),
        StructField("analysis_date", TimestampType(), True),
        StructField("avg_mem_use_perc", DoubleType(), True),
        StructField("std_mem_use_perc", DoubleType(), True),
        StructField("avg_mem_use_gb", DoubleType(), True),
        StructField("std_mem_use_gb", DoubleType(), True),
        StructField("avg_cpu_use_perc", DoubleType(), True),
        StructField("std_cpu_use_perc", DoubleType(), True),
        StructField("avg_read_speed_kb_per_sec", DoubleType(), True),
        StructField("std_read_speed_kb_per_sec", DoubleType(), True),
        StructField("avg_write_speed_kb_per_sec", DoubleType(), True),
        StructField("std_write_speed_kb_per_sec", DoubleType(), True),
        StructField("avg_bronze_received_mb", DoubleType(), True),
        StructField("std_bronze_received_mb", DoubleType(), True),
        StructField("avg_bronze_sent_mb", DoubleType(), True),
        StructField("std_bronze_sent_mb", DoubleType(), True),
        StructField("avg_warehouse_received_mb", DoubleType(), True),
        StructField("std_warehouse_received_mb", DoubleType(), True),
        StructField("avg_warehouse_sent_mb", DoubleType(), True),
        StructField("std_warehouse_sent_mb", DoubleType(), True),
        StructField("bronze_storage_used_gb", DoubleType(), True),
        StructField("warehouse_storage_used_gb", DoubleType(), True),
        StructField("created_at", TimestampType(), True),
    ]),
    
    'spark': StructType([
        StructField("service_type", StringType(), True),
        StructField("analysis_date", TimestampType(), True),
        StructField("avg_mem_use_perc", DoubleType(), True),
        StructField("std_mem_use_perc", DoubleType(), True),
        StructField("avg_mem_use_gb", DoubleType(), True),
        StructField("std_mem_use_gb", DoubleType(), True),
        StructField("avg_cpu_use_perc", DoubleType(), True),
        StructField("std_cpu_use_perc", DoubleType(), True),
        StructField("avg_read_speed_kb_per_sec", DoubleType(), True),
        StructField("std_read_speed_kb_per_sec", DoubleType(), True),
        StructField("avg_write_speed_kb_per_sec", DoubleType(), True),
        StructField("std_write_speed_kb_per_sec", DoubleType(), True),
        StructField("avg_job_success_perc", DoubleType(), True),
        StructField("std_job_success_perc", DoubleType(), True),
        StructField("avg_job_success_time", DoubleType(), True),
        StructField("std_job_success_time", DoubleType(), True),
        StructField("created_at", TimestampType(), True),
    ]),
    
    'dremio': StructType([
        StructField("service_type", StringType(), True),
        StructField("analysis_date", TimestampType(), True),
        StructField("avg_mem_use_perc", DoubleType(), True),
        StructField("std_mem_use_perc", DoubleType(), True),
        StructField("avg_mem_use_gb", DoubleType(), True),
        StructField("std_mem_use_gb", DoubleType(), True),
        StructField("avg_cpu_use_perc", DoubleType(), True),
        StructField("std_cpu_use_perc", DoubleType(), True),
        StructField("avg_query_success_perc", DoubleType(), True),
        StructField("std_query_success_perc", DoubleType(), True),
        StructField("avg_query_time", DoubleType(), True),
        StructField("std_query_time", DoubleType(), True),
        StructField("created_at", TimestampType(), True),
    ])
}

def create_spark_session() -> SparkSession:
    """Create Spark session"""
    log_info("Creating Spark session")
    
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
        
        builder = SparkSession.builder.appName("ProcessMetrics")
        for key, value in configs.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        log_success("Spark session created")
        return spark
        
    except Exception as e:
        log_error(f"Error creating Spark session: {str(e)}")
        raise

def prepare_metrics_data(spark: SparkSession, metrics: dict, metrics_type: str):
    """Prepare metrics data based on type"""
    current_time = datetime.now()
    
    # Type-specific records with exact schema matching
    if metrics_type == 'airflow':
        record = {
            'dag_id': 'process_exercises',
            'analysis_date': current_time,
            'avg_mem_use_perc': float(metrics.get('avg_mem_use_perc', 0.0)),
            'std_mem_use_perc': float(metrics.get('std_mem_use_perc', 0.0)),
            'avg_mem_use_gb': float(metrics.get('avg_mem_use_gb', 0.0)),
            'std_mem_use_gb': float(metrics.get('std_mem_use_gb', 0.0)),
            'avg_cpu_use_perc': float(metrics.get('avg_cpu_use_perc', 0.0)),
            'std_cpu_use_perc': float(metrics.get('std_cpu_use_perc', 0.0)),
            'avg_success_perc': float(metrics.get('avg_success_perc', 0.0)),
            'std_success_perc': 0.0,
            'avg_success_time': float(metrics.get('avg_success_time', 0.0)),
            'std_success_time': float(metrics.get('std_success_time', 0.0)),
            'total_executions': str(metrics.get('total_executions', '0')),
            'successful_executions': str(metrics.get('successful_executions', '0')),
            'created_at': current_time
        }
    
    elif metrics_type == 'nginx':
        record = {
            'service_type': 'all',
            'analysis_date': current_time,
            'avg_req_success_perc': float(metrics.get('avg_req_success_perc', 0.0)),
            'std_req_success_perc': 0.0,
            'avg_res_success_perc': float(metrics.get('avg_req_success_perc', 0.0)),
            'std_res_success_perc': 0.0,
            'avg_req_time': float(metrics.get('avg_req_time', 0.0)),
            'std_req_time': 0.0,
            'avg_res_time': float(metrics.get('avg_res_time', 0.0)),
            'std_res_time': 0.0,
            'avg_req_res_total_time': float(metrics.get('avg_req_res_total_time', 0.0)),
            'std_req_res_total_time': 0.0,
            'total_requests': str(metrics.get('total_requests', '0')),
            'successful_requests': str(metrics.get('successful_requests', '0')),
            'created_at': current_time
        }
    
    elif metrics_type == 'minio':
        record = {
            'service_type': 'minio',
            'analysis_date': current_time,
            'avg_mem_use_perc': 0.0,
            'std_mem_use_perc': 0.0,
            'avg_mem_use_gb': 0.0,
            'std_mem_use_gb': 0.0,
            'avg_cpu_use_perc': 0.0,
            'std_cpu_use_perc': 0.0,
            'avg_read_speed_kb_per_sec': 0.0,
            'std_read_speed_kb_per_sec': 0.0,
            'avg_write_speed_kb_per_sec': 0.0,
            'std_write_speed_kb_per_sec': 0.0,
            'avg_bronze_received_mb': 0.0,
            'std_bronze_received_mb': 0.0,
            'avg_bronze_sent_mb': 0.0,
            'std_bronze_sent_mb': 0.0,
            'avg_warehouse_received_mb': 0.0,
            'std_warehouse_received_mb': 0.0,
            'avg_warehouse_sent_mb': 0.0,
            'std_warehouse_sent_mb': 0.0,
            'bronze_storage_used_gb': float(metrics.get('bronze_storage_used_gb', 0.0)),
            'warehouse_storage_used_gb': float(metrics.get('warehouse_storage_used_gb', 0.0)),
            'created_at': current_time
        }
    
    elif metrics_type == 'spark':
        record = {
            'service_type': 'spark',
            'analysis_date': current_time,
            'avg_mem_use_perc': 0.0,
            'std_mem_use_perc': 0.0,
            'avg_mem_use_gb': 0.0,
            'std_mem_use_gb': 0.0,
            'avg_cpu_use_perc': 0.0,
            'std_cpu_use_perc': 0.0,
            'avg_read_speed_kb_per_sec': 0.0,
            'std_read_speed_kb_per_sec': 0.0,
            'avg_write_speed_kb_per_sec': 0.0,
            'std_write_speed_kb_per_sec': 0.0,
            'avg_job_success_perc': float(metrics.get('avg_job_success_perc', 0.0)),
            'std_job_success_perc': 0.0,
            'avg_job_success_time': float(metrics.get('avg_job_success_time', 0.0)),
            'std_job_success_time': 0.0,
            'created_at': current_time
        }
    
    elif metrics_type == 'dremio':
        record = {
            'service_type': 'dremio',
            'analysis_date': current_time,
            'avg_mem_use_perc': 0.0,
            'std_mem_use_perc': 0.0,
            'avg_mem_use_gb': 0.0,
            'std_mem_use_gb': 0.0,
            'avg_cpu_use_perc': 0.0,
            'std_cpu_use_perc': 0.0,
            'avg_query_success_perc': float(metrics.get('avg_query_success_perc', 0.0)),
            'std_query_success_perc': 0.0,
            'avg_query_time': float(metrics.get('avg_query_time', 0.0)),
            'std_query_time': float(metrics.get('std_query_time', 0.0)),
            'created_at': current_time
        }
    
    return spark.createDataFrame([record], SCHEMAS[metrics_type])

def save_metrics_table(spark: SparkSession, df, table_name: str):
    """Save DataFrame to Iceberg table"""
    try:
        count = df.count()
        log_info(f"Saving {count} records to table {table_name}")
        
        if count > 0:
            df.write.format("iceberg").mode("append").saveAsTable(table_name)
            log_success(f"Metrics saved to table {table_name}")
            
            # Log sample data
            metrics_row = df.collect()[0]
            log_info(f"📊 Sample data: {metrics_row.asDict()}")
        else:
            log_error(f"Empty DataFrame for table {table_name}")
            
    except Exception as e:
        log_error(f"Error saving table {table_name}: {str(e)}")
        raise

def main():
    """Main function"""
    log_info("Starting metrics processing")
    
    if len(sys.argv) < 3:
        log_error("Insufficient parameters. Usage: script.py <metrics_data> <metrics_type>")
        sys.exit(1)
    
    spark = None
    try:
        # Get metrics and type from XCom
        metrics_data = json.loads(sys.argv[1])
        metrics_type = sys.argv[2]
        
        log_info(f"Processing metrics type: {metrics_type}")
        
        # Create Spark session
        spark = create_spark_session()
        
        # Create namespace if not exists
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.metrics")
        
        # Prepare and save metrics
        table_name = f'nessie.metrics.{metrics_type}'
        metrics_df = prepare_metrics_data(spark, metrics_data, metrics_type)
        save_metrics_table(spark, metrics_df, table_name)
        
        log_success(f"Metrics processing {metrics_type} completed!")
        
    except Exception as e:
        log_error(f"Critical error: {str(e)}")
        sys.exit(1)
        
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()