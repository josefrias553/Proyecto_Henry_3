"""
Transformación Bronze: NYC
Lee datos CSV de NYC desde la capa Raw y aplica transformaciones básicas para la capa Bronze.
"""

import sys
import os
from datetime import datetime
from pyspark.sql.functions import (
    col, current_timestamp, lit, trim, input_file_name,
    when, regexp_replace
)
from pyspark.sql.types import (
    DecimalType, IntegerType, StringType, DoubleType, LongType
)

# Agregar path raiz para imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.utils.spark_utils import get_spark_session


def normalize_column_names(df):
    """Normaliza nombres de columnas a snake_case lowercase."""
    for column in df.columns:
        normalized = column.lower().replace(" ", "_").replace("-", "_")
        df = df.withColumnRenamed(column, normalized)
    return df


def transform_nyc(spark, raw_bucket, bronze_bucket, prefix, date_str):
    """
    Transforma datos de NYC (CSV) desde Raw a Bronze.
    
    Transformaciones aplicadas:
    - Normalización de nombres de columnas
    - Conversión de tipos de datos (id, price, latitude, longitude, etc.)
    - Limpieza de campos de texto
    - Eliminación de duplicados
    - Filtrado de registros inválidos
    - Agregado de campos de auditoría
    
    Args:
        spark: SparkSession
        raw_bucket: Bucket S3 de capa Raw
        bronze_bucket: Bucket S3 de capa Bronze
        prefix: Prefijo común (ej. marketplace_rentals)
        date_str: Fecha en formato YYYYMMDD
    """
    
    # Construir paths (Soporte para s3a:// o file:///)
    def build_path(bucket, partial_path):
        if "://" in bucket:
            return f"{bucket}/{partial_path}"
        return f"s3a://{bucket}/{partial_path}"

    raw_path = build_path(raw_bucket, f"{prefix}/csv/nyc/*{date_str}.csv")
    bronze_path = build_path(bronze_bucket, f"{prefix}/nyc/{date_str}/")
    
    print(f"[INFO] Leyendo datos de NYC desde Raw: {raw_path}")
    
    try:
        # Leer CSV desde Raw
        raw_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(raw_path)
        
        print(f"[INFO] Registros leídos desde Raw: {raw_df.count()}")
        print("[INFO] Esquema Raw:")
        raw_df.printSchema()
        
        # Normalizar nombres de columnas
        df = normalize_column_names(raw_df)
        
        # Conversión de tipos de datos
        # ID
        if "id" in df.columns:
            df = df.withColumn("id", col("id").cast(LongType()))
        
        # Coordenadas geográficas
        if "latitude" in df.columns:
            df = df.withColumn("latitude", col("latitude").cast(DoubleType()))
        
        if "longitude" in df.columns:
            df = df.withColumn("longitude", col("longitude").cast(DoubleType()))
        
        # Precio
        if "price" in df.columns:
            df = df.withColumn("price", col("price").cast(DecimalType(10, 2)))
        
        # Números enteros
        for int_col in ["minimum_nights", "number_of_reviews", 
                        "calculated_host_listings_count", "availability_365"]:
            if int_col in df.columns:
                df = df.withColumn(int_col, col(int_col).cast(IntegerType()))
        
        # Host ID
        if "host_id" in df.columns:
            df = df.withColumn("host_id", col("host_id").cast(LongType()))
        
        # Limpiar campos de texto
        text_fields = ["name", "host_name", "neighbourhood_group", 
                       "neighbourhood", "room_type"]
        for field in text_fields:
            if field in df.columns:
                df = df.withColumn(field, trim(col(field)))
        
        # Eliminar duplicados por ID
        if "id" in df.columns:
            initial_count = df.count()
            df = df.dropDuplicates(["id"])
            df = df.filter(col("id").isNotNull())
            final_count = df.count()
            print(f"[INFO] Duplicados eliminados: {initial_count - final_count}")
        
        # Filtrar registros inválidos
        # Latitud y longitud válidas para NYC
        if "latitude" in df.columns and "longitude" in df.columns:
            df = df.filter(
                (col("latitude").isNotNull()) &
                (col("longitude").isNotNull()) &
                (col("latitude").between(40.4, 41.0)) &  # NYC bounds
                (col("longitude").between(-74.3, -73.7))
            )
        
        # Precio válido (mayor que 0)
        if "price" in df.columns:
            df = df.filter((col("price").isNotNull()) & (col("price") > 0))
        
        # Agregar campos de auditoría
        df = df.withColumn("_ingestion_timestamp", current_timestamp())
        df = df.withColumn("_source_file", input_file_name())
        df = df.withColumn("_processing_date", lit(date_str))
        
        print(f"[INFO] Registros después de transformaciones: {df.count()}")
        print("[INFO] Esquema Bronze:")
        df.printSchema()
        
        print("[INFO] Ejemplo de registros transformados (primeros 5):")
        df.show(5, truncate=False)
        
        # Mostrar estadísticas descriptivas
        print("[INFO] Estadísticas de campos numéricos:")
        df.select("price", "minimum_nights", "number_of_reviews", 
                  "availability_365").describe().show()
        
        # Escribir en Bronze como Parquet
        print(f"[INFO] Escribiendo en Bronze: {bronze_path}")
        df.write \
            .mode("overwrite") \
            .parquet(bronze_path)
        
        print("[SUCCESS] Transformación NYC completada exitosamente")
        
        return df.count()
        
    except Exception as e:
        print(f"[ERROR] Error durante transformación NYC: {str(e)}")
        import traceback
        traceback.print_exc()
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
    print("TRANSFORMACIÓN BRONZE: NYC")
    print("=" * 80)
    print(f"Raw Bucket: {raw_bucket}")
    print(f"Bronze Bucket: {bronze_bucket}")
    print(f"Prefix: {prefix}")
    print(f"Date: {date_str}")
    print("=" * 80)
    
    # Crear SparkSession usando utilidad centralizada
    spark = get_spark_session("Bronze_Transformation_NYC", execution_mode="local")
    
    try:
        record_count = transform_nyc(spark, raw_bucket, bronze_bucket, prefix, date_str)
        print(f"\n[SUCCESS] Total de registros procesados: {record_count}")
        return 0
    except Exception as e:
        print(f"\n[ERROR] Transformación fallida: {str(e)}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
