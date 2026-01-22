"""
Script genérico de transformación Raw → Bronze.
Puede ser usado para cualquier entidad con configuraciones apropiadas.
"""

import sys
import os
import argparse
import logging
from datetime import datetime
from pyspark.sql.functions import (
    col, current_timestamp, lit, trim, input_file_name
)

# Agregar path raiz para imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.utils.spark_utils import get_spark_session

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("RawToBronze")

def normalize_column_names(df):
    """Normaliza nombres de columnas a snake_case lowercase."""
    for column in df.columns:
        normalized = column.lower().replace(" ", "_").replace("-", "_")
        df = df.withColumnRenamed(column, normalized)
    return df


def transform_raw_to_bronze(spark, raw_path, bronze_path, entity, date_str, file_format="json"):
    """
    Transformación genérica Raw → Bronze.
    
    Args:
        spark: SparkSession
        raw_path: Path S3 a archivos Raw
        bronze_path: Path S3 destino Bronze
        entity: Nombre de entidad
        date_str: Fecha YYYYMMDD
        file_format: Formato del archivo (json, csv)
    """
    
    logger.info(f"Transformando {entity}")
    logger.info(f"Raw: {raw_path}")
    logger.info(f"Bronze: {bronze_path}")
    
    try:
        # Leer según formato
        if file_format == "json":
            # Permissive mode para JSON mal formados si es necesario, 
            # pero por defecto failfast o permissive es mejor para data quality
            # MultiLine=true por seguridad con APIs que devuelven un solo objeto/array
            raw_df = spark.read.option("multiLine", "true").json(raw_path)
        elif file_format == "csv":
            raw_df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(raw_path)
        else:
            raise ValueError(f"Formato no soportado: {file_format}")
        
        # Verificar si el DataFrame está vacío
        if raw_df.isEmpty():
            logger.warning(f"El dataset {raw_path} está vacío. Saltando transformación.")
            return 0

        record_count_raw = raw_df.count()
        logger.info(f"Registros Raw: {record_count_raw}")
        
        # Normalizar columnas
        df = normalize_column_names(raw_df)
        
        # Eliminar duplicados si existe columna id
        if "id" in df.columns:
            df = df.dropDuplicates(["id"])
            df = df.filter(col("id").isNotNull())
        
        # Agregar metadatos
        df = df.withColumn("_ingestion_timestamp", current_timestamp())
        df = df.withColumn("_source_file", input_file_name())
        df = df.withColumn("_processing_date", lit(date_str))
        
        record_count_bronze = df.count()
        logger.info(f"Registros Bronze: {record_count_bronze}")
        
        # Validación de duplicados eliminados
        dropped_count = record_count_raw - record_count_bronze
        if dropped_count > 0:
            logger.info(f"Se eliminaron {dropped_count} registros duplicados o nulos.")

        # Escribir Parquet
        logger.info(f"Escribiendo datos en {bronze_path}")
        df.write \
            .mode("overwrite") \
            .parquet(bronze_path)
        
        logger.info(f"{entity} transformado exitosamente")
        return record_count_bronze
        
    except Exception as e:
        logger.error(f"Transformación fallida: {str(e)}", exc_info=True)
        raise


def main():
    parser = argparse.ArgumentParser(description="Transformación Raw → Bronze")
    parser.add_argument("--raw-bucket", required=True, help="Bucket Raw")
    parser.add_argument("--bronze-bucket", required=True, help="Bucket Bronze")
    parser.add_argument("--entity", required=True, help="Entidad a transformar")
    parser.add_argument("--source-type", default="api", help="Tipo de fuente: api, scraping, csv")
    parser.add_argument("--date", default=datetime.now().strftime("%Y%m%d"), help="Fecha YYYYMMDD")
    parser.add_argument("--prefix", default="marketplace_rentals", help="Prefijo S3")
    
    args = parser.parse_args()
    
    # Determinar formato
    file_format = "csv" if args.source_type == "csv" else "json"
    
    # Construir paths (Soporte para s3a:// o file:///)
    def build_path(bucket, partial_path):
        if "://" in bucket:
            return f"{bucket}/{partial_path}"
        return f"s3a://{bucket}/{partial_path}"

    # Nota: Usamos wildcard * para buscar el archivo específico o carpeta
    raw_path = build_path(args.raw_bucket, f"{args.prefix}/{args.source_type}/{args.entity}/*{args.date}.{file_format}*")
    bronze_path = build_path(args.bronze_bucket, f"{args.prefix}/{args.entity}/{args.date}/")
    
    logger.info("=" * 80)
    logger.info(f"TRANSFORMACIÓN BRONZE: {args.entity.upper()}")
    logger.info("=" * 80)
    
    spark = get_spark_session(f"Bronze_Transformation_{args.entity}", execution_mode="local")
    
    try:
        record_count = transform_raw_to_bronze(
            spark, raw_path, bronze_path, args.entity, args.date, file_format
        )
        logger.info(f"Proceso completado. Total registros procesados: {record_count}")
        return 0
    except Exception as e:
        logger.error(f"Fallo crítico en el job: {str(e)}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
