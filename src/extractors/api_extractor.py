"""
Módulo de Extractor de API.
Extrae datos desde APIs REST con lógica de reintento y manejo de errores.
"""

import time
from typing import Dict, List, Optional, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.logger import logger


# Headers de navegador estándar para evitar detección de bot
# Usados en descargas file-based (InsideAirbnb, datasets públicos)
DEFAULT_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
    "Referer": "https://insideairbnb.com/",
    "Connection": "keep-alive"
}


class APIExtractor:
    """
    Extractor para fuentes de datos API REST.
    
    Características:
    - Reintentos automáticos con backoff exponencial
    - Manejo de tiempos de espera (timeout)
    - Reintentos automáticos
    - Logging de peticiones
    - Manejo de errores para códigos de estado HTTP
    """
    
    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3
    ):
        """
        Inicializa el extractor de API.
        
        Args:
            base_url: URL base para los endpoints de la API
            api_key: Clave de API para autenticación (si es requerida)
            timeout: Tiempo de espera de la petición en segundos
            max_retries: Número máximo de intentos de reintento
        """
        self.base_url = base_url
        self.api_key = api_key
        self.timeout = timeout
        self.max_retries = max_retries
        
        # Configurar sesión con estrategia de reintento
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """
        Crea una sesión de requests con configuración de reintentos.
        
        Returns:
            requests.Session: Sesión configurada
        """
        session = requests.Session()
        
        # Configurar estrategia de reintento
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=1,  # demoras de 1s, 2s, 4s
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def extract(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        method: str = "GET"
    ) -> List[Dict[str, Any]]:
        """
        Extrae datos de un endpoint de API.
        
        Args:
            endpoint: Ruta del endpoint de la API (relativa a base_url si está establecida)
            params: Parámetros de consulta (query params)
            headers: Encabezados HTTP
            method: Método HTTP (GET o POST)
            
        Returns:
            List[Dict]: Datos extraídos como lista de diccionarios
            
        Raises:
            requests.RequestException: Si la extracción falla después de los reintentos
        """
        # Construir URL completa
        if self.base_url:
            url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        else:
            url = endpoint
        
        # Agregar API key a los headers si está configurada
        if headers is None:
            headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        
        logger.info(f"Iniciando extracción desde API: {url}")
        logger.debug(f"Método: {method}, Params: {params}")
        
        try:
            # Realizar petición
            if method.upper() == "GET":
                response = self.session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout
                )
            elif method.upper() == "POST":
                response = self.session.post(
                    url,
                    json=params,
                    headers=headers,
                    timeout=self.timeout
                )
            else:
                raise ValueError(f"Método HTTP no soportado: {method}")
            
            # Verificar estado de la respuesta
            response.raise_for_status()
            
            # Parsear respuesta JSON
            data = response.json()
            
            # Normalizar datos a lista de diccionarios
            if isinstance(data, dict):
                # Verificar patrones comunes de paginación/wrapper
                if "data" in data and isinstance(data["data"], list):
                    data = data["data"]
                elif "results" in data and isinstance(data["results"], list):
                    data = data["results"]
                elif "items" in data and isinstance(data["items"], list):
                    data = data["items"]
                else:
                    # Objeto único, envolver en lista
                    data = [data]
            elif not isinstance(data, list):
                logger.warning(f"Tipo de respuesta inesperado: {type(data)}, envolviendo en lista")
                data = [data]
            
            logger.info(f"Se extrajeron exitosamente {len(data)} registros de {url}")
            logger.info(f"Estado de respuesta: {response.status_code}")
            
            return data
            
        except requests.exceptions.Timeout:
            logger.error(f"Tiempo de espera agotado después de {self.timeout}s: {url}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error HTTP {e.response.status_code}: {url}")
            logger.error(f"Respuesta: {e.response.text[:200]}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"La petición falló: {url} - {str(e)}")
            raise
        except ValueError as e:
            logger.error(f"Falló al parsear respuesta JSON: {str(e)}")
            raise
    
    def extract_with_pagination(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        page_param: str = "page",
        max_pages: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Extrae datos con soporte de paginación.
        
        Args:
            endpoint: Endpoint de la API
            params: Parámetros de consulta base
            page_param: Nombre del parámetro de página
            max_pages: Máximo de páginas a obtener (None = todas)
            
        Returns:
            List[Dict]: Todos los datos extraídos de todas las páginas
        """
        if params is None:
            params = {}
        
        all_data = []
        page = 1
        
        while max_pages is None or page <= max_pages:
            params[page_param] = page
            
            try:
                page_data = self.extract(endpoint, params=params)
                
                if not page_data:
                    logger.info(f"No hay más datos en la página {page}, deteniento paginación")
                    break
                
                all_data.extend(page_data)
                logger.info(f"Página {page} obtenida: {len(page_data)} registros")
                
                page += 1
                
                # Rate limiting simple
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Falló al obtener página {page}: {str(e)}")
                break
        
        logger.info(f"Total de registros obtenidos en {page - 1} páginas: {len(all_data)}")
        return all_data
    
    def extract_file(
        self,
        endpoint: str,
        headers: Optional[Dict[str, str]] = None
    ) -> bytes:
        """
        Descarga un archivo directamente desde una URL.
        Usado para fuentes file-based (CSV, GZIP, etc.) como InsideAirbnb.
        
        Este método NO parsea el contenido, retorna los bytes crudos para
        mantener fidelidad total con el archivo original (paradigma ELT).
        
        IMPORTANTE: Usa headers de navegador estándar para evitar bloqueos
        anti-bot (HTTP 403). Esto es una práctica común en ingesta automatizada
        de datasets públicos.
        
        Args:
            endpoint: URL del archivo a descargar (puede ser relativa a base_url)
            headers: Encabezados HTTP opcionales (se combinan con headers de navegador)
            
        Returns:
            bytes: Contenido crudo del archivo descargado
            
        Raises:
            requests.RequestException: Si la descarga falla
        """
        # Construir URL completa
        if self.base_url:
            url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        else:
            url = endpoint
        
        # Preparar headers: combinar headers de navegador con headers personalizados
        # Los headers de navegador previenen bloqueos 403 en datasets públicos
        request_headers = DEFAULT_BROWSER_HEADERS.copy()
        
        # Agregar headers personalizados si se proporcionan
        if headers:
            request_headers.update(headers)
        
        # Agregar API key si está configurada (poco común en file-based, pero soportado)
        if self.api_key:
            request_headers["Authorization"] = f"Bearer {self.api_key}"
        
        logger.info(f"Descargando archivo desde: {url}")
        logger.info("Usando headers de navegador estándar (anti-bot)")
        
        try:
            # Realizar descarga directa con headers de navegador
            response = self.session.get(
                url,
                headers=request_headers,
                timeout=self.timeout,
                stream=True  # Usar stream para archivos grandes
            )
            
            # Verificar estado de la respuesta
            response.raise_for_status()
            
            # Obtener contenido crudo
            file_content = response.content
            
            logger.info(f"Se descargó exitosamente el archivo de {url}")
            logger.info(f"Tamaño del archivo: {len(file_content)} bytes")
            logger.info(f"Estado de respuesta: {response.status_code}")
            
            return file_content
            
        except requests.exceptions.Timeout:
            logger.error(f"Tiempo de espera agotado después de {self.timeout}s: {url}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error HTTP {e.response.status_code}: {url}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"La descarga falló: {url} - {str(e)}")
            raise
