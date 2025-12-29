from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import os

def create_spark_session():
    # Definição dos pacotes necessários para S3A e MinIO
    # O spark.jars.packages fará o download automático na primeira execução
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]

    return SparkSession.builder \
        .appName("Ecom-Ingestion-Landing") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def run_ingestion():
    spark = create_spark_session()
    print("🚀 Sessão Spark iniciada...")

    # 1. Simulação de Dados (Poderia ser um CSV/JSON que você baixou)
    # Vamos criar um DataFrame de exemplo com eventos de e-commerce
    data = [
        ("prod_101", "view", 299.90, "user_A"),
        ("prod_102", "add_to_cart", 450.00, "user_B"),
        ("prod_101", "purchase", 299.90, "user_A"),
        ("prod_103", "view", 15.00, "user_C")
    ]
    
    columns = ["product_id", "event_type", "price", "user_id"]
    
    df = spark.createDataFrame(data, schema=columns)

    # 2. Adicionando metadados (Data de processamento)
    df_final = df.withColumn("ingestion_timestamp", current_timestamp())

    # 3. Escrita na Landing Zone (MinIO)
    # O Spark criará a pasta 'events' dentro do bucket 'landing-zone'
    path = "s3a://landing-zone/raw_events"
    
    print(f"📦 Gravando dados em {path}...")
    
    try:
        df_final.write \
            .mode("overwrite") \
            .parquet(path)
        print("✅ Ingestão concluída com sucesso no MinIO!")
    except Exception as e:
        print(f"❌ Erro na ingestão: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_ingestion()