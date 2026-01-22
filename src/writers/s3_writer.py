"""
Módulo Writer de S3.
Escribe datos extraídos a la capa Raw de S3 con validación y convenciones de nombres.
"""

import json
import csv
import io
from datetime import datetime
from typing import List, Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError

from src.logger import logger


class S3Writer:
    """
    Writer para la Capa Raw de S3.
    
    Características:
    - Formatos de exportación JSON y CSV
    - Convención de nomenclatura estandarizada
    - Verificación de existencia de archivos (previene sobrescritura)
    - Validación antes de la carga
    """
    
    def __init__(
        self,
        bucket_name: str,
        prefix: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_region: str = "us-east-1"
    ):
        """
        Inicializa el writer de S3.
        
        Args:
            bucket_name: Nombre del bucket de S3
            prefix: Prefijo para las claves de S3 (ej. "marketplace_rentals")
            aws_access_key_id: Clave de acceso de AWS
            aws_secret_access_key: Clave secreta de AWS
            aws_region: Región de AWS
        """
        self.bucket_name = bucket_name
        self.prefix = prefix.rstrip("/")
        
        # Inicializar cliente S3
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
        
        logger.info(f"S3Writer inicializado para bucket: {bucket_name}")
    
    def write(
        self,
        data: List[Dict[str, Any]],
        domain: str,
        source: str,
        entity: str,
        file_format: str = "json",
        allow_overwrite: bool = False
    ) -> str:
        """
        Escribe datos en la Capa Raw de S3.
        
        Args:
            data: Datos a escribir (lista de diccionarios)
            domain: Nombre del dominio (ej. "marketplace_rentals")
            source: Tipo de fuente (ej. "api", "scraping")
            entity: Tipo de entidad (ej. "listings", "reviews")
            file_format: Formato de salida ("json" o "csv")
            allow_overwrite: Si se permite sobrescribir archivos existentes
            
        Returns:
            str: Clave S3 del archivo subido
            
        Raises:
            ValueError: Si los datos son inválidos o el archivo existe y no se permite sobrescribir
            ClientError: Si la carga a S3 falla
        """
        # Validar datos
        if not data:
            raise ValueError("No se pueden escribir datos vacíos en S3")
        
        if not isinstance(data, list):
            raise ValueError(f"Los datos deben ser una lista, se obtuvo {type(data)}")
        
        # Generar nombre de archivo
        date_str = datetime.now().strftime("%Y%m%d")
        filename = f"{domain}_{source}_{entity}_{date_str}.{file_format}"
        
        # Construir clave S3 con estructura jerárquica: {prefix}/{source}/{entity}/{filename}
        s3_path = f"{source}/{entity}"
        if self.prefix:
            s3_key = f"{self.prefix}/{s3_path}/{filename}"
        else:
            s3_key = f"{s3_path}/{filename}"
        
        # Verificar si el archivo existe
        if not allow_overwrite and self._file_exists(s3_key):
            raise ValueError(
                f"El archivo ya existe en S3: {s3_key}. "
                "Establezca allow_overwrite=True para reemplazarlo."
            )
        
        # Serializar datos
        if file_format == "json":
            content = self._to_json(data)
            content_type = "application/json"
        elif file_format == "csv":
            content = self._to_csv(data)
            content_type = "text/csv"
        else:
            raise ValueError(f"Formato no soportado: {file_format}")
        
        # Subir a S3
        try:
            logger.info(f"Subiendo a S3: s3://{self.bucket_name}/{s3_key}")
            logger.info(f"Tamaño del archivo: {len(content)} bytes")
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=content,
                ContentType=content_type
            )
            
            logger.info(f"Se subieron exitosamente {len(data)} registros a S3")
            logger.info(f"URI de S3: s3://{self.bucket_name}/{s3_key}")
            
            return s3_key
            
        except ClientError as e:
            logger.error(f"Falló la subida a S3: {str(e)}")
            raise
    
    def _file_exists(self, s3_key: str) -> bool:
        """
        Verifica si el archivo existe en S3.
        
        Args:
            s3_key: Clave del objeto S3
            
        Returns:
            bool: True si el archivo existe, False en caso contrario
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            logger.warning(f"El archivo ya existe: s3://{self.bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                # Otro error, loguear y asumir que no existe
                logger.warning(f"Error verificando existencia de archivo: {str(e)}")
                return False
    
    def _to_json(self, data: List[Dict[str, Any]]) -> bytes:
        """
        Convierte datos a formato JSON.
        
        Args:
            data: Lista de diccionarios
            
        Returns:
            bytes: Contenido JSON como bytes
        """
        try:
            # Pretty-print JSON para legibilidad
            json_str = json.dumps(data, indent=2, ensure_ascii=False)
            return json_str.encode("utf-8")
        except (TypeError, ValueError) as e:
            logger.error(f"Falló al serializar datos a JSON: {str(e)}")
            raise ValueError(f"Datos no son serializables a JSON: {str(e)}")
    
    def _to_csv(self, data: List[Dict[str, Any]]) -> bytes:
        """
        Convierte datos a formato CSV.
        
        Args:
            data: Lista de diccionarios
            
        Returns:
            bytes: Contenido CSV como bytes
        """
        if not data:
            return b""
        
        # Obtener todos los nombres de campos únicos
        fieldnames = set()
        for item in data:
            fieldnames.update(item.keys())
        fieldnames = sorted(fieldnames)
        
        # Escribir CSV a buffer de string
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        
        writer.writeheader()
        for item in data:
            writer.writerow(item)
        
        # Convertir a bytes
        csv_str = output.getvalue()
        return csv_str.encode("utf-8")
    
    def write_raw_file(
        self,
        file_content: bytes,
        domain: str,
        source: str,
        entity: str,
        file_extension: str,
        allow_overwrite: bool = False
    ) -> str:
        """
        Escribe un archivo crudo (raw) directamente en la Capa Raw de S3.
        Usado para fuentes file-based que no requieren transformación (ELT puro).
        
        Args:
            file_content: Contenido binario del archivo
            domain: Nombre del dominio (ej. "marketplace_rentals")
            source: Tipo de fuente (ej. "api", "scraping")
            entity: Tipo de entidad (ej. "listings")
            file_extension: Extensión del archivo (ej. "csv.gz", "csv", "json.gz")
            allow_overwrite: Si se permite sobrescribir archivos existentes
            
        Returns:
            str: Clave S3 del archivo subido
            
        Raises:
            ValueError: Si los datos son inválidos o el archivo existe
            ClientError: Si la carga a S3 falla
        """
        # Validar contenido
        if not file_content:
            raise ValueError("No se puede escribir un archivo vacío en S3")
        
        # Generar nombre de archivo con extensión original
        date_str = datetime.now().strftime("%Y%m%d")
        filename = f"{domain}_{source}_{entity}_{date_str}.{file_extension}"
        
        # Construir clave S3 con estructura jerárquica
        s3_path = f"{source}/{entity}"
        if self.prefix:
            s3_key = f"{self.prefix}/{s3_path}/{filename}"
        else:
            s3_key = f"{s3_path}/{filename}"
        
        # Verificar si el archivo existe
        if not allow_overwrite and self._file_exists(s3_key):
            raise ValueError(
                f"El archivo ya existe en S3: {s3_key}. "
                "Establezca allow_overwrite=True para reemplazarlo."
            )
        
        # Determinar Content-Type basado en la extensión
        if file_extension.endswith(".gz"):
            content_type = "application/gzip"
        elif file_extension.endswith(".csv"):
            content_type = "text/csv"
        elif file_extension.endswith(".json"):
            content_type = "application/json"
        else:
            content_type = "application/octet-stream"
        
        # Subir a S3
        try:
            logger.info(f"Subiendo archivo crudo a S3: s3://{self.bucket_name}/{s3_key}")
            logger.info(f"Tamaño del archivo: {len(file_content)} bytes")
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=file_content,
                ContentType=content_type
            )
            
            logger.info(f"Se subió exitosamente el archivo a S3")
            logger.info(f"URI de S3: s3://{self.bucket_name}/{s3_key}")
            
            return s3_key
            
        except ClientError as e:
            logger.error(f"Falló la subida a S3: {str(e)}")
            raise
