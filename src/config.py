"""
Módulo de configuración para el Extractor ELT.
Carga y valida variables de entorno.
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    """Configuración cargada desde variables de entorno."""
    
    # Credenciales de AWS
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str
    
    # Capa Raw de S3
    raw_bucket: str
    raw_prefix: str
    
    # Parámetros de dominio y extracción
    domain: str
    extraction_source: Optional[str] = None
    extraction_entity: Optional[str] = None
    
    # Configuración de API (opcional)
    api_base_url: Optional[str] = None
    api_key: Optional[str] = None
    
    # Configuración de Scraping (opcional)
    scraping_base_url: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> "Config":
        """
        Carga la configuración desde variables de entorno.
        Si existe un archivo .env en el directorio raíz, intenta cargarlo.
        Valida que las variables requeridas estén presentes.
        
        Returns:
            Config: Instancia de configuración
            
        Raises:
            ValueError: Si faltan variables de entorno requeridas
        """
        # Intentar cargar .env manualmente para no depender de python-dotenv
        env_path = os.path.join(os.getcwd(), ".env")
        if os.path.exists(env_path):
            with open(env_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line:
                        key, value = line.split("=", 1)
                        if not os.getenv(key):  # No sobrescribir si ya existe
                            os.environ[key] = value.strip()

        # Variables requeridas
        required_vars = [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "RAW_BUCKET"
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(
                f"Faltan variables de entorno requeridas: {', '.join(missing_vars)}"
            )
        
        return cls(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            aws_region=os.getenv("AWS_REGION", "us-east-1"),
            raw_bucket=os.getenv("RAW_BUCKET"),
            raw_prefix=os.getenv("RAW_PREFIX", "marketplace_rentals"),
            domain=os.getenv("DOMAIN", "marketplace_rentals"),
            extraction_source=os.getenv("EXTRACTION_SOURCE"),
            extraction_entity=os.getenv("EXTRACTION_ENTITY"),
            api_base_url=os.getenv("API_BASE_URL"),
            api_key=os.getenv("API_KEY"),
            scraping_base_url=os.getenv("SCRAPING_BASE_URL")
        )
    
    def validate(self) -> None:
        """
        Valida los valores de configuración.
        
        Raises:
            ValueError: Si la configuración es inválida
        """
        if not self.raw_bucket:
            raise ValueError("RAW_BUCKET no puede estar vacío")
        
        if self.extraction_source and self.extraction_source not in ["api", "scraping"]:
            raise ValueError(
                f"EXTRACTION_SOURCE debe ser 'api' o 'scraping', se obtuvo: {self.extraction_source}"
            )
