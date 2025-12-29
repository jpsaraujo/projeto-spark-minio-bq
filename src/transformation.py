from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, when

def create_spark_session():
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]
    return SparkSession.builder \
        .appName("Ecom-Transformation-Silver") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def run_transformation():
    spark = create_spark_session()
    
    # 1. Leitura dos dados da Landing Zone
    print("📖 Lendo dados da Landing Zone...")
    df_raw = spark.read.parquet("s3a://landing-zone/raw_events")

    # 2. Transformação (Silver Layer)
    # Vamos criar uma tabela agregada: Total de vendas e quantidade de eventos por produto
    print("⚙️ Processando transformações...")
    df_silver = df_raw.groupBy("product_id").agg(
        sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("total_revenue"),
        count("event_type").alias("total_events"),
        count(when(col("event_type") == "purchase", 1)).alias("total_sales")
    )

    # 3. Escrita na Silver Zone
    path_silver = "s3a://silver-zone/product_metrics"
    print(f"📦 Gravando dados transformados em {path_silver}...")
    
    try:
        df_silver.write.mode("overwrite").parquet(path_silver)
        print("✅ Camada Silver atualizada com sucesso!")
        df_silver.show() # Para visualizares o resultado no terminal
    except Exception as e:
        print(f"❌ Erro na transformação: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_transformation()