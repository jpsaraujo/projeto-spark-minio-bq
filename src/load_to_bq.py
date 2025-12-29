from pyspark.sql import SparkSession
import os

# CONFIGURAÇÕES - AJUSTE AQUI
PROJECT_ID = "projeto-spark-minio"  # ID do teu projeto no GCP
DATASET_ID = "ecommerce_analytics"
TABLE_ID = "product_metrics_gold"
GCP_KEY_PATH = os.path.abspath("config/gcp-key.json")

def create_spark_session():
    # 1. Adicionamos o gcs-connector à lista de pacotes
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.31.1",
        "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.6" # Adicionado aqui
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
        # --- CORREÇÃO AQUI: Registrar o FileSystem 'gs' ---
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        # -------------------------------------------------
        # Configurações GCP/BigQuery
        .config("credentialsFile", GCP_KEY_PATH)
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCP_KEY_PATH)
        .getOrCreate())

def load_to_bigquery():
    spark = create_spark_session()
    
    try:
        # 1. Ler da Silver Zone (MinIO)
        print("📖 Lendo dados da Silver Zone...")
        df_silver = spark.read.parquet("s3a://silver-zone/product_metrics")

        # 2. Escrever no BigQuery
        # O Spark usa um bucket temporário no GCS ou escrita direta via API
        print(f"🚀 Enviando dados para o BigQuery: {DATASET_ID}.{TABLE_ID}...")
        
        df_silver.write \
            .format("bigquery") \
            .option("temporaryGcsBucket", " meu-bucket-temp")\
            .option("parentProject", PROJECT_ID) \
            .option("project", PROJECT_ID) \
            .option("dataset", DATASET_ID) \
            .option("table", TABLE_ID) \
            .mode("overwrite") \
            .save()

        print("✅ Dados carregados no BigQuery com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro no carregamento para o BigQuery: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    load_to_bigquery()