"""
Módulo de logging para el Extractor ELT.
Proporciona logging estructurado para observabilidad.
"""

import logging
import sys
from typing import Optional


def setup_logger(name: str = "etl_extractor", level: str = "INFO") -> logging.Logger:
    """
    Configura y retorna una instancia de logger.
    
    Args:
        name: Nombre del logger
        level: Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
    Returns:
        logging.Logger: Logger configurado
    """
    logger = logging.getLogger(name)
    
    # Convertir string de nivel a constante de logging
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(numeric_level)
    
    # Eliminar handlers existentes para evitar duplicados
    logger.handlers.clear()
    
    # Crear console handler (stdout para compatibilidad con Docker)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(numeric_level)
    
    # Crear formatter
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    
    # Agregar handler al logger
    logger.addHandler(handler)
    
    # Evitar propagación al root logger
    logger.propagate = False
    
    return logger


# Crear instancia de logger por defecto
logger = setup_logger()
