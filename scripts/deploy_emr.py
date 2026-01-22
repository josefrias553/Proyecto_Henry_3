import boto3
import os
import argparse
import sys
from botocore.exceptions import NoCredentialsError

def upload_to_s3(local_file, bucket, s3_file):
    """Sube un archivo a un bucket S3."""
    s3 = boto3.client('s3')
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print(f"Subido: {local_file} -> s3://{bucket}/{s3_file}")
        return True
    except FileNotFoundError:
        print(f"El archivo no fue encontrado: {local_file}")
        return False
    except NoCredentialsError:
        print("Credenciales no disponibles")
        return False
    except Exception as e:
        print(f"Error subiendo {local_file}: {e}")
        return False

def upload_directory(local_path, bucket, s3_prefix):
    """Sube un directorio recursivamente a S3."""
    for root, dirs, files in os.walk(local_path):
        for file in files:
            local_file = os.path.join(root, file)
            relative_path = os.path.relpath(local_file, local_path)
            s3_file = os.path.join(s3_prefix, relative_path).replace("\\", "/") # Windows fix
            
            upload_to_s3(local_file, bucket, s3_file)

def main():
    parser = argparse.ArgumentParser(description="Despliegue de código a S3 para EMR")
    parser.add_argument("--bucket", required=True, help="Bucket destino S3 (ej. my-emr-bucket)")
    parser.add_argument("--prefix", default="emr-apps/m3-project", help="Prefijo en S3")
    
    args = parser.parse_args()
    
    # 1. Subir requirements.txt
    print("--- Subiendo dependencias ---")
    upload_to_s3("requirements.txt", args.bucket, f"{args.prefix}/requirements.txt")
    
    # 2. Subir código fuente (src)
    print("\n--- Subiendo código fuente (src/) ---")
    upload_directory("src", args.bucket, f"{args.prefix}/src")
    
    # 3. Subir transformaciones
    print("\n--- Subiendo transformaciones ---")
    upload_directory("transformations", args.bucket, f"{args.prefix}/transformations")
    
    print("\n[SUCCESS] Despliegue completado.")
    print(f"Código base en: s3://{args.bucket}/{args.prefix}/")
    print(f"Script de bootstapping (si usas uno): s3://{args.bucket}/{args.prefix}/requirements.txt")

if __name__ == "__main__":
    main()
