"""
Transformación Bronze: Listings
Lee datos JSON de la capa Raw y aplica transformaciones básicas para la capa Bronze.
"""

import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, lower, regexp_replace,
    trim, when, input_file_name, explode
)
from pyspark.sql.types import DecimalType, IntegerType

# Agregar path raiz para imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.utils.spark_utils import get_spark_session


def normalize_column_names(df):
    """
    Normaliza nombres de columnas a snake_case lowercase.
    """
    for column in df.columns:
        # Convertir a lowercase y reemplazar espacios con guiones bajos
        normalized = column.lower().replace(" ", "_").replace("-", "_")
        df = df.withColumnRenamed(column, normalized)
    return df


def transform_listings(spark, raw_bucket, bronze_bucket, prefix, date_str):
    """
    Transforma datos de listings desde Raw a Bronze.
    # ... (omitted docs)
    """
    
    # Construir paths (Soporte para s3a:// o file:///)
    def build_path(bucket, partial_path):
        if "://" in bucket:
            return f"{bucket}/{partial_path}"
        return f"s3a://{bucket}/{partial_path}"

    raw_path = build_path(raw_bucket, f"{prefix}/api/listings/*{date_str}.json")
    bronze_path = build_path(bronze_bucket, f"{prefix}/listings/{date_str}/")
    
    print(f"[INFO] Leyendo datos desde Raw: {raw_path}")
    
    try:
        # Leer JSON desde Raw
        # Nota: DummyJSON devuelve un solo objeto JSON grande, se requiere multiLine=true
        raw_df = spark.read.option("multiLine", "true").json(raw_path)
        
        print(f"[INFO] Registros leídos desde Raw: {raw_df.count()}")
        print("[INFO] Esquema Raw:")
        raw_df.printSchema()
        
        # Expandir array de products si existe (DummyJSON retorna {products: []})
        if "products" in raw_df.columns:
            # Primero explotar el array para tener filas, luego seleccionar campos
            df = raw_df.select(explode(col("products")).alias("product"))
            df = df.select("product.*")
        else:
            df = raw_df
        
        # Normalizar nombres de columnas
        df = normalize_column_names(df)
        
        # Conversión de tipos (ajustar según esquema real)
        transformations = []
        
        # Si existen columnas comunes de listings
        if "id" in df.columns:
            df = df.withColumn("id", col("id").cast(IntegerType()))
        
        if "price" in df.columns:
            df = df.withColumn("price", col("price").cast(DecimalType(10, 2)))
        
        if "title" in df.columns:
            df = df.withColumn("title", trim(col("title")))
        
        # Eliminar duplicados por ID
        if "id" in df.columns:
            df = df.dropDuplicates(["id"])
        
        # Filtrar nulos en campos críticos
        if "id" in df.columns:
            df = df.filter(col("id").isNotNull())
        
        # Agregar campos de auditoría
        df = df.withColumn("_ingestion_timestamp", current_timestamp())
        df = df.withColumn("_source_file", input_file_name())
        df = df.withColumn("_processing_date", lit(date_str))
        
        print(f"[INFO] Registros después de transformaciones: {df.count()}")
        print("[INFO] Esquema Bronze:")
        df.printSchema()
        
        print("[INFO] Ejemplo de registros transformados (primeros 5):")
        df.show(5, truncate=False)
        
        # Escribir en Bronze como Parquet
        print(f"[INFO] Escribiendo en Bronze: {bronze_path}")
        df.write \
            .mode("overwrite") \
            .parquet(bronze_path)
        
        print("[SUCCESS] Transformación completada exitosamente")
        
        return df.count()
        
    except Exception as e:
        print(f"[ERROR] Error durante transformación: {str(e)}")
        raise


def main():
    """
    Función principal de ejecución.
    """
    # Leer argumentos
    raw_bucket = os.getenv("RAW_BUCKET", "data-lake-raw-henry")
    bronze_bucket = os.getenv("BRONZE_BUCKET", "data-lake-bronze-henry")
    prefix = os.getenv("RAW_PREFIX", "marketplace_rentals")
    date_str = datetime.now().strftime("%Y%m%d")
    
    # Permitir override por argumentos CLI
    if len(sys.argv) > 1:
        raw_bucket = sys.argv[1]
    if len(sys.argv) > 2:
        bronze_bucket = sys.argv[2]
    if len(sys.argv) > 3:
        date_str = sys.argv[3]
    
    print("=" * 80)
    print("TRANSFORMACIÓN BRONZE: LISTINGS")
    print("=" * 80)
    print(f"Raw Bucket: {raw_bucket}")
    print(f"Bronze Bucket: {bronze_bucket}")
    print(f"Prefix: {prefix}")
    print(f"Date: {date_str}")
    print("=" * 80)
    
    # Crear SparkSession usando utilidad centralizada
    spark = get_spark_session("Bronze_Transformation_Listings", execution_mode="local")
    
    try:
        record_count = transform_listings(spark, raw_bucket, bronze_bucket, prefix, date_str)
        print(f"\n[SUCCESS] Total de registros procesados: {record_count}")
        return 0
    except Exception as e:
        print(f"\n[ERROR] Transformación fallida: {str(e)}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
