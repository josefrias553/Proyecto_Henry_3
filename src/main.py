"""
Script principal de orquestación para el Extractor ELT.
Coordina la extracción, validación y carga a S3.
"""

import sys
import os
import argparse
from typing import Optional, List, Dict, Any

from src.config import Config
from src.logger import logger
from src.extractors.api_extractor import APIExtractor
from src.extractors.web_scraper import WebScraper
from src.extractors.csv_extractor import CSVExtractor
from src.writers.s3_writer import S3Writer
from src.validators.data_validator import DataValidator


def parse_args() -> argparse.Namespace:
    """
    Parsea los argumentos de línea de comandos.
    
    Returns:
        argparse.Namespace: Argumentos parseados
    """
    parser = argparse.ArgumentParser(
        description="Extractor ELT - Extrae datos desde APIs y web scraping a la Capa Raw de S3"
    )
    
    parser.add_argument(
        "--source",
        type=str,
        choices=["api", "scraping", "csv"],
        help="Tipo de fuente de datos (api, scraping o csv). También puede configurarse via env var EXTRACTION_SOURCE."
    )
    
    parser.add_argument(
        "--entity",
        type=str,
        help="Entidad a extraer (ej. listings, reviews). También puede configurarse via env var EXTRACTION_ENTITY."
    )
    
    parser.add_argument(
        "--format",
        type=str,
        choices=["json", "csv"],
        default="json",
        help="Formato de salida (por defecto: json)"
    )
    
    parser.add_argument(
        "--allow-overwrite",
        action="store_true",
        help="Permitir sobrescribir archivos existentes en S3"
    )
    
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Nivel de logging (por defecto: INFO)"
    )
    
    return parser.parse_args()



def extract_from_api(config: Config, entity: str):
    """
    Extrae datos desde una fuente API.
    
    Soporta dos tipos de fuentes:
    1. API REST JSON: Retorna List[Dict] de registros
    2. File-based: Retorna bytes del archivo descargado
    
    Args:
        config: Objeto de configuración
        entity: Entidad a extraer
        
    Returns:
        Union[List[Dict], bytes]: Datos extraídos o archivo crudo
    """
    # Configuración de entidades file-based (datasets descargables)
    # Estas entidades se descargan como archivos sin transformar
    # NOTA: listings NO es file-based, usa REST JSON con DummyJSON como simulación
    FILE_BASED_ENTITIES = {
        "reviews": "reviews.csv.gz",
        "calendar": "calendar.csv.gz"
    }
    
    logger.info(f"Extrayendo {entity} desde API")
    
    extractor = APIExtractor(
        base_url=config.api_base_url,
        api_key=config.api_key,
        timeout=30,
        max_retries=3
    )
    
    # Detectar si es una entidad file-based
    if entity in FILE_BASED_ENTITIES:
        # Descarga directa de archivo (InsideAirbnb style)
        logger.info(f"Entidad '{entity}' es file-based, descargando archivo directamente")
        file_path = FILE_BASED_ENTITIES[entity]
        endpoint = f"/{file_path}"
        file_content = extractor.extract_file(endpoint)
        
        # Retornar tupla indicando que es un archivo
        return ("file", file_content, file_path.split(".")[-2:])  # (tipo, contenido, [extensión])
    
    else:
        # API REST JSON (comportamiento original)
        logger.info(f"Entidad '{entity}' es REST JSON, extrayendo como API estándar")
        
        # Configuración específica por entidad
        if entity == "listings":
            # listings: Simulación de marketplace usando DummyJSON
            # Las APIs reales (Airbnb, InsideAirbnb) están protegidas por anti-bot
            logger.info("Usando DummyJSON para simular dominio marketplace")
            endpoint = "/products" if config.api_base_url else "https://dummyjson.com/products"
            data = extractor.extract(endpoint, params={"limit": 100})
        elif entity == "reviews_json":
            endpoint = "/reviews" if config.api_base_url else "https://api.example.com/v1/reviews"
            data = extractor.extract(endpoint, params={"limit": 100})
        else:
            logger.warning(f"Entidad desconocida '{entity}', usando endpoint por defecto")
            endpoint = f"/{entity}"
            data = extractor.extract(endpoint)
        
        # Retornar tupla indicando que es JSON
        return ("json", data, None)


def extract_from_scraping(config: Config, entity: str) -> List[Dict[str, Any]]:
    """
    Extrae datos desde una fuente de web scraping.
    
    Args:
        config: Objeto de configuración
        entity: Entidad a extraer
        
    Returns:
        List[Dict]: Datos extraídos
    """
    logger.info(f"Scrapeando {entity} desde la web")
    
    scraper = WebScraper(
        base_url=config.scraping_base_url,
        timeout=30
    )
    
    # Ejemplo: Scrapear desde URL configurada
    # En implementación real, los selectores serían específicos por entidad
    if entity == "listings":
        selectors = {
            "title": "h2.listing-title",
            "price": "span.price",
            "location": "div.location",
            "rating": "span.rating"
        }
        container_selector = "div.listing-card"
        url = "/listings" if config.scraping_base_url else "https://example.com/listings"
        
    elif entity == "reviews":
        selectors = {
            "author": "span.author-name",
            "rating": "span.rating",
            "comment": "p.review-text",
            "date": "time.review-date"
        }
        container_selector = "div.review-card"
        url = "/reviews" if config.scraping_base_url else "https://example.com/reviews"
        
    elif entity == "neighborhoods":
        # Configuración para scraping de barrios (neighborhoods)
        selectors = {
            "barrio": "td:nth-of-type(1) a",
            "superficie": "td:nth-of-type(2)",
            "habitantes": "td:nth-of-type(3)",
            "densidad": "td:nth-of-type(4)"
        }
        container_selector = "table.wikitable tbody tr"
        url = "/wiki/Anexo:Barrios_de_la_ciudad_de_Buenos_Aires" if config.scraping_base_url else "https://es.wikipedia.org/wiki/Anexo:Barrios_de_la_ciudad_de_Buenos_Aires"
        
    else:
        logger.error(f"Scraping no configurado para entidad: {entity}")
        raise ValueError(f"Entidad desconocida para scraping: {entity}")
    
    data = scraper.scrape(url, selectors, container_selector)
    return data


def extract_from_csv(config: Config, entity: str):
    """
    Extrae datos desde archivos CSV locales.
    
    Lee archivos desde la carpeta datos/{entity}/ y retorna el contenido
    raw como bytes para ingesta ELT pura.
    
    Args:
        config: Objeto de configuración
        entity: Entidad a extraer (nombre de subcarpeta)
        
    Returns:
        tuple: ("csv", bytes, "csv") - tipo, contenido, extensión
    """
    logger.info(f"Extrayendo {entity} desde archivos CSV locales")
    
    # Carpeta base para archivos CSV (default: datos/)
    csv_folder = os.getenv("CSV_DATA_FOLDER", "datos")
    
    extractor = CSVExtractor(base_folder=csv_folder)
    
    try:
        csv_content = extractor.extract(entity)
        
        # Retornar tupla indicando que es un archivo CSV
        return ("csv", csv_content, "csv")
        
    except FileNotFoundError as e:
        logger.error(f"Error: {str(e)}")
        logger.error(f"Asegúrese de que la carpeta {csv_folder}/{entity}/ existe y contiene archivos .csv")
        raise


def main() -> int:
    """
    Función principal de ejecución.
    
    Returns:
        int: código de salida (0 para éxito, 1 para error)
    """
    args = parse_args()
    
    # Actualizar nivel de logger
    logger.setLevel(args.log_level)
    for handler in logger.handlers:
        handler.setLevel(args.log_level)
    
    logger.info("=" * 60)
    logger.info("Extractor ELT v1.0.0")
    logger.info("=" * 60)
    
    try:
        # Cargar configuración
        logger.info("Cargando configuración desde variables de entorno...")
        config = Config.from_env()
        config.validate()
        
        # Determinar fuente y entidad (CLI args sobrescriben env vars)
        source = args.source or config.extraction_source
        entity = args.entity or config.extraction_entity
        
        if not source or not entity:
            logger.error("Fuente y entidad deben ser especificadas vía argumentos CLI o variables de entorno")
            logger.error("Ejemplo: python main.py --source api --entity listings")
            return 1
        
        logger.info(f"Configuración cargada:")
        logger.info(f"  - Dominio: {config.domain}")
        logger.info(f"  - Fuente: {source}")
        logger.info(f"  - Entidad: {entity}")
        logger.info(f"  - Bucket S3: {config.raw_bucket}")
        logger.info(f"  - Prefijo S3: {config.raw_prefix}")
        logger.info(f"  - Formato de Salida: {args.format}")
        
        # Extracción de datos
        logger.info(f"\n{'=' * 60}")
        logger.info("FASE DE EXTRACCIÓN")
        logger.info(f"{'=' * 60}")
        
        if source == "api":
            extraction_result = extract_from_api(config, entity)
            extraction_type, data, file_info = extraction_result
        elif source == "scraping":
            data = extract_from_scraping(config, entity)
            extraction_type = "json"
            file_info = None
        elif source == "csv":
            extraction_result = extract_from_csv(config, entity)
            extraction_type, data, file_info = extraction_result
        else:
            logger.error(f"Fuente desconocida: {source}")
            return 1
        
        # Manejar según el tipo de extracción
        if extraction_type in ["file", "csv"]:
            # File-based ingestion: subir archivo directo sin validación
            logger.info(f"Modo: Ingesta file-based (archivo crudo)")
            logger.info(f"Tamaño del archivo: {len(data)} bytes")
            
            # Escribir archivo raw a S3
            logger.info(f"\n{'=' * 60}")
            logger.info("FASE DE CARGA A S3 (ARCHIVO CRUDO)")
            logger.info(f"{'=' * 60}")
            
            writer = S3Writer(
                bucket_name=config.raw_bucket,
                prefix=config.raw_prefix,
                aws_access_key_id=config.aws_access_key_id,
                aws_secret_access_key=config.aws_secret_access_key,
                aws_region=config.aws_region
            )
            
            # Determinar extensión del archivo
            if extraction_type == "csv":
                file_extension = "csv"
            else:
                file_extension = ".".join(file_info) if file_info else "dat"
            
            s3_key = writer.write_raw_file(
                file_content=data,
                domain=config.domain,
                source=source,
                entity=entity,
                file_extension=file_extension,
                allow_overwrite=args.allow_overwrite
            )
            
            # Resumen de éxito
            logger.info(f"\n{'=' * 60}")
            logger.info("✓ INGESTA EXITOSA (ARCHIVO CRUDO)")
            logger.info(f"{'=' * 60}")
            logger.info(f"Tamaño del archivo: {len(data)} bytes")
            logger.info(f"Ubicación S3: s3://{config.raw_bucket}/{s3_key}")
            logger.info(f"Tipo: Archivo crudo (.{file_extension})")
            logger.info("=" * 60)
            
        else:
            # JSON/REST ingestion: flujo estándar con validación
            logger.info(f"Modo: Ingesta REST JSON (estructurado)")
            logger.info(f"Se extrajeron {len(data)} registros")
            
            # Validar datos
            logger.info(f"\n{'=' * 60}")
            logger.info("FASE DE VALIDACIÓN")
            logger.info(f"{'=' * 60}")
            
            if not DataValidator.validate_all(data, entity, strict_schema=False):
                logger.error("La validación de datos falló, abortando carga")
                return 1
            
            # Escribir a S3
            logger.info(f"\n{'=' * 60}")
            logger.info("FASE DE CARGA A S3")
            logger.info(f"{'=' * 60}")
            
            writer = S3Writer(
                bucket_name=config.raw_bucket,
                prefix=config.raw_prefix,
                aws_access_key_id=config.aws_access_key_id,
                aws_secret_access_key=config.aws_secret_access_key,
                aws_region=config.aws_region
            )
            
            s3_key = writer.write(
                data=data,
                domain=config.domain,
                source=source,
                entity=entity,
                file_format=args.format,
                allow_overwrite=args.allow_overwrite
            )
            
            # Resumen de éxito
            logger.info(f"\n{'=' * 60}")
            logger.info("✓ EXTRACCIÓN EXITOSA")
            logger.info(f"{'=' * 60}")
            logger.info(f"Registros extraídos: {len(data)}")
            logger.info(f"Ubicación S3: s3://{config.raw_bucket}/{s3_key}")
            logger.info(f"Formato: {args.format}")
            logger.info("=" * 60)
        
        return 0
        
    except ValueError as e:
        logger.error(f"Error de configuración: {str(e)}")
        return 1
    except Exception as e:
        logger.error(f"La extracción falló: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
