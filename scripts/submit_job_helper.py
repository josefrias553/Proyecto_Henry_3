import argparse

def generate_spark_submit(bucket, prefix, entity, date, raw_bucket, bronze_bucket):
    """Genera el comando spark-submit para EMR."""
    
    # Paths en S3 donde vive el código
    main_script = f"s3://{bucket}/{prefix}/transformations/bronze/raw_to_bronze.py"
    # Para importar módulos custom (src), idealmente se empaquetan en un zip, 
    # pero si están en el worker python path o pasados como --py-files funciona.
    # Aquí asumimos que el script no depende de src/ para la transformación pura, 
    # o que src se sube y se pasa en --py-files.
    
    # Si transform_raw_to_bronze fuera dependiente de utils en src/, deberíamos zippearlo.
    # Por ahora, el script refactorizado es autocontenido o solo usa libs estándar + pyspark.
    
    cmd = (
        f"spark-submit --deploy-mode cluster \\\n"
        f"  --conf spark.yarn.appMasterEnv.PYTHONPATH=./ \\\n"
        f"  --py-files s3://{bucket}/{prefix}/src.zip \\\n"  # Placeholder si decidimos zippear
        f"  {main_script} \\\n"
        f"  --raw-bucket {raw_bucket} \\\n"
        f"  --bronze-bucket {bronze_bucket} \\\n"
        f"  --entity {entity} \\\n"
        f"  --date {date}"
    )
    
    return cmd

def main():
    parser = argparse.ArgumentParser(description="Generador de comandos EMR")
    parser.add_argument("--code-bucket", required=True, help="Bucket donde se subió el código")
    parser.add_argument("--code-prefix", default="emr-apps/m3-project", help="Prefijo del código")
    parser.add_argument("--raw-bucket", required=True, help="Bucket de datos Raw")
    parser.add_argument("--bronze-bucket", required=True, help="Bucket de datos Bronze")
    parser.add_argument("--entity", default="listings", help="Entidad a procesar")
    parser.add_argument("--date", default="20260122", help="Fecha YYYYMMDD")
    
    args = parser.parse_args()
    
    print("="*60)
    print("COMANDO SUGERIDO PARA EMR (SSH o Step)")
    print("="*60)
    cmd = generate_spark_submit(
        args.code_bucket, args.code_prefix, args.entity, args.date, 
        args.raw_bucket, args.bronze_bucket
    )
    print(cmd)
    print("="*60)
    print("NOTA: Asegúrate de haber ejecutado scripts/deploy_emr.py primero.")

if __name__ == "__main__":
    main()
