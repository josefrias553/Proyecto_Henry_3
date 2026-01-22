import os
import sys
import argparse
import boto3
from botocore.exceptions import NoCredentialsError

def upload_directory(local_path, bucket_name, s3_prefix=""):
    """
    Sube recursivamente un directorio local a S3.
    """
    s3 = boto3.client('s3')
    
    # Normalizar path local
    local_path = os.path.normpath(local_path)
    
    print(f"Iniciando subida a s3://{bucket_name}/{s3_prefix}")
    
    files_uploaded = 0
    
    for root, dirs, files in os.walk(local_path):
        for file in files:
            # Path absoluto local
            local_file_path = os.path.join(root, file)
            
            # Path relativo desde el directorio base
            relative_path = os.path.relpath(local_file_path, local_path)
            
            # Key en S3 (reemplazar backslashes en Windows)
            s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")
            
            try:
                print(f"Subiendo {s3_key}...")
                s3.upload_file(local_file_path, bucket_name, s3_key)
                files_uploaded += 1
            except FileNotFoundError:
                print(f"Archivo no encontrado: {local_file_path}")
            except NoCredentialsError:
                print("Error: Credenciales AWS no encontradas.")
                return False
            except Exception as e:
                print(f"Error subiendo {local_file_path}: {e}")

    print(f"Subida completada. Total archivos: {files_uploaded}")
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload local directory to S3")
    parser.add_argument("local_path", help="Directorio local a subir")
    parser.add_argument("bucket_name", help="Nombre del bucket destino")
    parser.add_argument("--prefix", default="", help="Prefijo destino en S3")
    
    args = parser.parse_args()
    
    upload_directory(args.local_path, args.bucket_name, args.prefix)
