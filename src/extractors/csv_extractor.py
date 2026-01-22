"""
Módulo de Extractor de CSV.
Lee archivos CSV desde carpeta local y los prepara para carga en S3.
"""

import os
import glob
from typing import Optional
from pathlib import Path

from src.logger import logger


class CSVExtractor:
    """
    Extractor para archivos CSV locales.
    
    Características:
    - Lee archivos desde carpeta local (ej. datos/)
    - Valida encoding y delimitador
    - No modifica contenido (ELT puro)
    - Retorna bytes raw para escritura directa en S3
    """
    
    def __init__(self, base_folder: str = "datos"):
        """
        Inicializa el extractor de CSV.
        
        Args:
            base_folder: Carpeta base donde se encuentran los archivos CSV
        """
        self.base_folder = base_folder
        logger.info(f"CSVExtractor inicializado con carpeta base: {base_folder}")
    
    def extract(self, entity: str) -> bytes:
        """
        Extrae archivo CSV de la carpeta local.
        
        Busca archivos .csv en {base_folder}/{entity}/ y retorna el contenido
        del primer archivo encontrado. No modifica el contenido (paradigma ELT).
        
        Args:
            entity: Nombre de la entidad (ej. "productos", "clientes")
            
        Returns:
            bytes: Contenido raw del archivo CSV
            
        Raises:
            FileNotFoundError: Si no se encuentra ningún archivo CSV
            ValueError: Si hay problemas con el encoding del archivo
        """
        # Construir ruta a la carpeta de la entidad
        entity_folder = os.path.join(self.base_folder, entity)
        
        logger.info(f"Buscando archivos CSV en: {entity_folder}")
        
        # Verificar que la carpeta existe
        if not os.path.exists(entity_folder):
            raise FileNotFoundError(
                f"No existe la carpeta: {entity_folder}. "
                f"Crear carpeta {self.base_folder}/{entity}/ con archivos CSV."
            )
        
        # Buscar archivos .csv en la carpeta
        csv_pattern = os.path.join(entity_folder, "*.csv")
        csv_files = glob.glob(csv_pattern)
        
        if not csv_files:
            raise FileNotFoundError(
                f"No se encontraron archivos CSV en: {entity_folder}"
            )
        
        # Usar el primer archivo encontrado
        csv_file = csv_files[0]
        logger.info(f"Archivo CSV encontrado: {csv_file}")
        
        if len(csv_files) > 1:
            logger.warning(
                f"Se encontraron {len(csv_files)} archivos CSV. "
                f"Utilizando: {os.path.basename(csv_file)}"
            )
        
        # Validar encoding y delimitador
        self._validate_csv(csv_file)
        
        # Leer archivo como bytes (sin modificar contenido)
        try:
            with open(csv_file, 'rb') as f:
                content = f.read()
            
            logger.info(f"Archivo CSV leído exitosamente")
            logger.info(f"Tamaño: {len(content)} bytes")
            
            return content
            
        except Exception as e:
            logger.error(f"Error leyendo archivo CSV: {str(e)}")
            raise
    
    def _validate_csv(self, filepath: str) -> None:
        """
        Valida estructura básica del CSV.
        
        Verifica:
        - Encoding (UTF-8 o Latin-1)
        - Presencia de delimitador (,)
        - Al menos una línea de headers
        
        Args:
            filepath: Ruta al archivo CSV
            
        Raises:
            ValueError: Si la validación falla
        """
        logger.info("Validando estructura básica del CSV...")
        
        # Intentar detectar encoding
        encodings_to_try = ['utf-8', 'latin-1', 'iso-8859-1']
        content_decoded = None
        detected_encoding = None
        
        for encoding in encodings_to_try:
            try:
                with open(filepath, 'r', encoding=encoding) as f:
                    content_decoded = f.read(1024)  # Leer primeras líneas
                detected_encoding = encoding
                break
            except UnicodeDecodeError:
                continue
        
        if content_decoded is None:
            raise ValueError(
                f"No se pudo detectar encoding válido. "
                f"Intentados: {', '.join(encodings_to_try)}"
            )
        
        logger.info(f"✓ Encoding detectado: {detected_encoding}")
        
        # Validar presencia de delimitador
        lines = content_decoded.split('\n')
        if not lines:
            raise ValueError("El archivo CSV está vacío")
        
        header_line = lines[0]
        if ',' not in header_line and ';' not in header_line and '\t' not in header_line:
            logger.warning(
                "No se detectó delimitador estándar (,;\\t). "
                "El archivo podría no ser un CSV válido."
            )
        
        # Detectar delimitador más probable
        comma_count = header_line.count(',')
        semicolon_count = header_line.count(';')
        tab_count = header_line.count('\t')
        
        delimiter = ','
        if semicolon_count > comma_count and semicolon_count > tab_count:
            delimiter = ';'
        elif tab_count > comma_count and tab_count > semicolon_count:
            delimiter = '\\t'
        
        logger.info(f"✓ Delimitador detectado: '{delimiter}'")
        logger.info(f"✓ Headers encontrados: {len(header_line.split(delimiter))} columnas")
        
        # Log de advertencias si es necesario
        if len(lines) < 2:
            logger.warning("El archivo solo contiene headers (sin datos)")
