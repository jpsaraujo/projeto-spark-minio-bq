from pyspark.sql import SparkSession
import os

# CONFIGURAÇÕES
PROJECT_ID = "projeto-spark-minio" 
DATASET_ID = "ecommerce_analytics"
TABLE_ID = "product_metrics_gold"
GCP_KEY_PATH = os.path.abspath("config/gcp-key.json")

# Define a variável de ambiente para o Google Cloud (isso ajuda o driver Java)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_KEY_PATH

def create_spark_session():
    # Removi o gcs-connector para evitar conflitos, focando apenas no conector BQ
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.31.1"
    ]
    
    return (SparkSession.builder
        .appName("Silver-To-BigQuery")
        .config("spark.jars.packages", ",".join(packages))
        # Configurações MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate())

def load_to_bigquery():
    spark = create_spark_session()
    
    try:
        print("📖 Lendo dados da Silver Zone (MinIO)...")
        df_silver = spark.read.parquet("s3a://silver-zone/product_metrics")

        print(f"🚀 Enviando para o BigQuery via DIRECT WRITE...")
        
        # USANDO O MÉTODO DIRECT (Não precisa de bucket GCS)
        df_silver.write \
            .format("bigquery") \
            .option("parentProject", PROJECT_ID) \
            .option("project", PROJECT_ID) \
            .option("dataset", DATASET_ID) \
            .option("table", TABLE_ID) \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()

        print("✅ SUCESSO! Dados carregados no BigQuery.")
        
    except Exception as e:
        print(f"❌ Erro: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    load_to_bigquery()