"""
Módulo de Web Scraper.
Extrae datos desde páginas HTML usando BeautifulSoup.
"""

from typing import Dict, List, Optional, Any
import requests
from bs4 import BeautifulSoup
import time

from src.logger import logger


class WebScraper:
    """
    Web scraper para fuentes de datos HTML.
    
    Características:
    - Parsing de HTML con BeautifulSoup
    - Selectores CSS configurables
    - Manejo básico de errores
    - Gestión de timeouts
    
    Nota: Respeta robots.txt (es responsabilidad del invocador verificarlo)
    """
    
    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: int = 30,
        user_agent: Optional[str] = None
    ):
        """
        Inicializa el web scraper.
        
        Args:
            base_url: URL base para el scraping
            timeout: Tiempo de espera de la petición en segundos
            user_agent: String de user agent personalizado
        """
        self.base_url = base_url
        self.timeout = timeout
        self.user_agent = user_agent or (
            "Mozilla/5.0 (compatible; ELT-Extractor/1.0; +https://example.com/bot)"
        )
        
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})
    
    def scrape(
        self,
        url: str,
        selectors: Dict[str, str],
        container_selector: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Realiza scraping de datos de una página web.
        
        Args:
            url: URL a scrapear (absoluta o relativa a base_url)
            selectors: Diccionario mapeando nombres de campos a selectores CSS
            container_selector: Selector CSS para elementos contenedores (opcional)
                              Si se proporciona, extrae múltiples ítems
            
        Returns:
            List[Dict]: Datos extraídos como lista de diccionarios
            
        Ejemplo:
            scraper.scrape(
                url="/listings",
                selectors={
                    "title": "h2.listing-title",
                    "price": "span.price",
                    "location": "div.location"
                },
                container_selector="div.listing-card"
            )
        """
        # Construir URL completa
        if self.base_url and not url.startswith(("http://", "https://")):
            url = f"{self.base_url.rstrip('/')}/{url.lstrip('/')}"
        
        logger.info(f"Realizando scraping: {url}")
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            # Parsear HTML
            soup = BeautifulSoup(response.content, "lxml")
            
            data = []
            
            if container_selector:
                # Extraer múltiples ítems
                containers = soup.select(container_selector)
                logger.info(f"Se encontraron {len(containers)} contenedores coincidiendo con '{container_selector}'")
                
                for idx, container in enumerate(containers):
                    item = self._extract_item(container, selectors)
                    if item:
                        data.append(item)
                    
                    if idx > 0 and idx % 10 == 0:
                        logger.debug(f"Procesados {idx} contenedores...")
            else:
                # Extraer ítem único de la página completa
                item = self._extract_item(soup, selectors)
                if item:
                    data.append(item)
            
            logger.info(f"Se extrajeron exitosamente {len(data)} ítems de {url}")
            return data
            
        except requests.exceptions.Timeout:
            logger.error(f"Tiempo de espera agotado después de {self.timeout}s: {url}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error HTTP {e.response.status_code}: {url}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Fallo en la petición: {url} - {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Fallo en el scraping: {url} - {str(e)}")
            raise
    
    def _extract_item(
        self,
        soup: BeautifulSoup,
        selectors: Dict[str, str]
    ) -> Optional[Dict[str, Any]]:
        """
        Extrae un único ítem usando selectores.
        
        Args:
            soup: Objeto BeautifulSoup (página o contenedor)
            selectors: Diccionario mapeando nombres de campos a selectores CSS
            
        Returns:
            Dict or None: Datos extraídos o None si todos los campos están vacíos
        """
        item = {}
        
        for field, selector in selectors.items():
            try:
                element = soup.select_one(selector)
                if element:
                    # Extraer texto y limpiarlo
                    value = element.get_text(strip=True)
                    item[field] = value
                else:
                    item[field] = None
                    logger.debug(f"Selector '{selector}' no encontrado para campo '{field}'")
            except Exception as e:
                logger.warning(f"Error extrayendo campo '{field}': {str(e)}")
                item[field] = None
        
        # Retornar None si todos los valores son vacíos o None
        if all(v is None or v == "" for v in item.values()):
            return None
        
        return item
    
    def scrape_multiple_pages(
        self,
        url_template: str,
        selectors: Dict[str, str],
        container_selector: str,
        page_range: range,
        delay: float = 1.0
    ) -> List[Dict[str, Any]]:
        """
        Realiza scraping de múltiples páginas con paginación.
        
        Args:
            url_template: Plantilla de URL con marcador {page}
            selectors: Selectores de campos
            container_selector: Selector de contenedor
            page_range: Rango de páginas a scrapear (ej. range(1, 6))
            delay: Retardo entre peticiones en segundos
            
        Returns:
            List[Dict]: Todos los datos scrapeados
            
        Ejemplo:
            scraper.scrape_multiple_pages(
                url_template="/listings?page={page}",
                selectors={...},
                container_selector="div.listing",
                page_range=range(1, 4),
                delay=2.0
            )
        """
        all_data = []
        
        for page in page_range:
            url = url_template.format(page=page)
            
            try:
                page_data = self.scrape(url, selectors, container_selector)
                
                if not page_data:
                    logger.info(f"No hay más datos en la página {page}, deteniento")
                    break
                
                all_data.extend(page_data)
                logger.info(f"Página {page}: se extrajeron {len(page_data)} ítems")
                
                # Rate limiting
                if page < page_range.stop - 1:
                    time.sleep(delay)
                    
            except Exception as e:
                logger.error(f"Falló al scrapear página {page}: {str(e)}")
                break
        
        logger.info(f"Total de ítems scrapeados: {len(all_data)}")
        return all_data
