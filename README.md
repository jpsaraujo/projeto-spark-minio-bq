# Multi-Cloud Data Lakehouse: Spark, MinIO & BigQuery

Este projeto demonstra um pipeline de dados completo utilizando uma arquitetura **Medallion**, integrando armazenamento de objetos local (MinIO) com processamento distribuído (Apache Spark) e Data Warehousing na nuvem (Google BigQuery).

## 🏗️ Arquitetura
O pipeline segue o fluxo:
1. **Landing Zone (MinIO)**: Ingestão de dados brutos em Parquet.
2. **Silver Layer (MinIO)**: Processamento e agregação de métricas via PySpark.
3. **Gold Layer (BigQuery)**: Exportação final via BigQuery Storage Write API para análise e BI.

## 🛠️ Tecnologias
* **Linguagem:** Python (PySpark)
* **Storage Local:** MinIO (S3-compatible)
* **Processing:** Apache Spark 3.4.1 (Dockerized)
* **Cloud DW:** Google BigQuery
* **Infra:** Docker & Docker Compose

## 🚀 Como Executar

### 1. Preparação do Ambiente
```bash
# Subir infraestrutura (MinIO + Spark Cluster)
docker-compose up -d

# Instalar dependências locais (opcional para desenvolvimento)
pip install -r requirements.txt
