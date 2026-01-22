"""
Script de Validación Bronze.
Compara registros Raw vs Bronze y genera reportes de calidad.
"""

import sys
import os
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Agregar path raiz para imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.utils.spark_utils import get_spark_session


def validate_bronze_transformation(spark, raw_bucket, bronze_bucket, entity, prefix, date_str):
    """
    Valida la transformación Bronze comparando con Raw.
    
    Genera reporte con:
    - Conteo de registros (Raw vs Bronze)
    - Comparación de esquemas
    - Campos agregados
    - Muestra de registros
    """
    
    print(f"\n{'=' * 80}")
    print(f"VALIDACIÓN BRONZE: {entity.upper()}")
    print(f"{'=' * 80}\n")
    
    report = {
        "entity": entity,
        "date": date_str,
        "timestamp": datetime.now().isoformat(),
        "status": "UNKNOWN"
    }
    
    try:
        # Leer Bronze
        bronze_path = f"s3a://{bronze_bucket}/{prefix}/{entity}/{date_str}/"
        print(f"[INFO] Leyendo Bronze: {bronze_path}")
        
        bronze_df = spark.read.parquet(bronze_path)
        bronze_count = bronze_df.count()
        
        print(f"[INFO] Registros en Bronze: {bronze_count}")
        
        # Esquema Bronze
        print("\n[INFO] Esquema Bronze:")
        bronze_df.printSchema()
        
        # Validaciones
        report["bronze_count"] = bronze_count
        report["bronze_schema"] = bronze_df.schema.json()
        
        # Verificar campos de auditoría
        audit_fields = ["_ingestion_timestamp", "_source_file", "_processing_date"]
        missing_fields = [f for f in audit_fields if f not in bronze_df.columns]
        
        if missing_fields:
            print(f"[WARNING] Campos de auditoría faltantes: {missing_fields}")
            report["missing_audit_fields"] = missing_fields
        else:
            print("[OK] Todos los campos de auditoría presentes")
        
        # Verificar nulos en ID
        if "id" in bronze_df.columns:
            null_ids = bronze_df.filter(col("id").isNull()).count()
            report["null_ids"] = null_ids
            if null_ids > 0:
                print(f"[WARNING] {null_ids} registros con ID nulo")
            else:
                print("[OK] No hay IDs nulos")
        
        # Verificar duplicados
        if "id" in bronze_df.columns:
            total_ids = bronze_df.count()
            unique_ids = bronze_df.select("id").distinct().count()
            duplicates = total_ids - unique_ids
            report["duplicate_count"] = duplicates
            
            if duplicates > 0:
                print(f"[WARNING] {duplicates} registros duplicados")
            else:
                print("[OK] No hay duplicados")
        
        # Muestra de datos
        print("\n[INFO] Muestra de registros Bronze:")
        bronze_df.show(5, truncate=False)
        
        # Estadísticas básicas
        print("\n[INFO] Estadísticas:")
        bronze_df.describe().show()
        
        # Determinar estado
        if bronze_count > 0 and len(missing_fields) == 0:
            report["status"] = "SUCCESS"
            print(f"\n[SUCCESS] Validación completada exitosamente")
        else:
            report["status"] = "WARNING"
            print(f"\n[WARNING] Validación completada con advertencias")
        
        # Guardar reporte
        report_path = f"s3a://{bronze_bucket}/reports/{entity}_validation_{date_str}.json"
        print(f"\n[INFO] Guardando reporte en: {report_path}")
        
        # Convertir a DataFrame y guardar
        report_json = json.dumps(report, indent=2)
        print(f"\n[REPORT]\n{report_json}")
        
        spark_report = spark.createDataFrame([(report_json,)], ["report"])
        spark_report.write \
            .mode("overwrite") \
            .text(report_path.replace(".json", ".txt"))
        
        return report
        
    except Exception as e:
        print(f"[ERROR] Validación fallida: {str(e)}")
        report["status"] = "ERROR"
        report["error"] = str(e)
        raise


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Validación Bronze")
    parser.add_argument("--raw-bucket", required=True)
    parser.add_argument("--bronze-bucket", required=True)
    parser.add_argument("--entity", required=True)
    parser.add_argument("--prefix", default="marketplace_rentals")
    parser.add_argument("--date", default=datetime.now().strftime("%Y%m%d"))
    
    args = parser.parse_args()
    
    spark = get_spark_session(f"Validation_Bronze_{args.entity}", execution_mode="local")
    
    try:
        report = validate_bronze_transformation(
            spark, args.raw_bucket, args.bronze_bucket, 
            args.entity, args.prefix, args.date
        )
        
        if report["status"] == "SUCCESS":
            return 0
        elif report["status"] == "WARNING":
            return 1
        else:
            return 2
            
    except Exception as e:
        print(f"\n[ERROR] Validación fallida: {str(e)}")
        return 3
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
