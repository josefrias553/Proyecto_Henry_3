# Marketplace Rentals ELT Pipeline

**Versión:** 1.3.0  
**Estado:** Activo (Fase Bronze)

Este proyecto implementa un pipeline ELT (Extract, Load, Transform) robusto y modular para la ingesta y procesamiento de datos de un marketplace de alquileres. Soporta múltiples fuentes de datos (APIs, Web Scraping, Archivos CSV locales) y utiliza **PySpark** para transformaciones escalables en AWS S3.

---

## 🏗️ Arquitectura

El pipeline sigue una arquitectura de Data Lake por capas:

![alt text](img\image.png)

### Capas del Data Lake
- **Raw:** Datos crudos tal cual llegan de la fuente (JSON, CSV, HTML).
- **Bronze:** Datos limpios, tipados, deduplicados y en formato **Parquet**.
- **Silver (En progreso):** Datos enriquecidos, joineados y agregados.
- **Gold (Futuro):** Modelos dimensionales y KPIs de negocio.

---

## 🚀 Instalación y Configuración

### Pre-requisitos
- **Python 3.11+**
- **Java 17** (Requerido para PySpark)
- **AWS CLI** configurado
- **Docker** (Opcional, para ejecución contenerizada)

### 1. Configurar Entorno Virtual

```bash
# Crear entorno
python -m venv venv

# Activar (Windows)
.\venv\Scripts\Activate

# Activar (Linux/Mac)
source venv/bin/activate
```

### 2. Instalar Dependencias

```bash
pip install -r requirements.txt
```

### 3. Variables de Entorno (.env)

Crear un archivo `.env` basado en `.env.example`:

```ini
# AWS Credentials
AWS_ACCESS_KEY_ID=tu_access_key
AWS_SECRET_ACCESS_KEY=tu_secret_key
AWS_REGION=us-east-1

# S3 Buckets (Producción)
RAW_BUCKET=data-lake-raw-henry
BRONZE_BUCKET=data-lake-bronze-henry

# S3 Buckets (Verificación)
RAW_BUCKET_VERIFICATION=data-lake-raw-verification-bucket
BRONZE_BUCKET_VERIFICATION=data-lake-bronze-verification-bucket

# Configuración General
EXECUTION_MODE=verification  # Controls data validation rules (verification=strict, production=lenient)
```

---

## 📦 Entidades Soportadas

| Entidad | Fuente | Tipo | Descripción | Ruta S3 |
|---------|--------|------|-------------|---------|
| `listings` | DummyJSON | API REST | Listings de propiedades | `api/listings/` |
| `posts` | DummyJSON | API REST | Blog posts de usuarios | `api/posts/` |
| `neighborhoods` | Wikipedia | Scraping | Datos de barrios | `scraping/neighborhoods/` |
| `productos` | CSV Local | Archivo | Inventario de productos | `csv/productos/` |
| **`nyc`** | CSV Local | Archivo | **Airbnb NYC (48K+ rows)** | `csv/nyc/` |

---

## 💻 Ejemplos de Ejecución

## Ingesta de Datos (Capa Raw)

El proceso de extracción (API/Scraping/CSV) también se puede ejecutar vía Docker para consistencia.

### 1. Ingesta desde API (Listings & Posts)
```bash
# Listings (DummyJSON)
docker-compose run --rm --entrypoint python elt-spark -m src.main --source api --entity listings

# Posts (JSONPlaceholder)
docker-compose run --rm --entrypoint python elt-spark -m src.main --source api --entity posts --api-url https://jsonplaceholder.typicode.com/posts
```

### 2. Ingesta desde CSV (NYC)
Los archivos CSV deben estar en la carpeta `datos/nyc` local, la cual se debe montar o copiar. 
*Nota: Para carga simple de CSVs, se recomienda subir directamente a S3 o usar el script `src.main` localmente si se tiene Python instalado.*

```bash
python -m src.main --source csv --entity nyc
```

---

## 📊 Preguntas de Negocio (Caso NYC)

El pipeline de NYC está diseñado para responder:

1.  **Precios:** ¿Cuál es el precio promedio por distrito? (Manhattan vs Bronx).
2.  **Oferta:** ¿Qué tipo de habitación domina el mercado?
3.  **Top Hosts:** ¿Quiénes son los anfitriones con más propiedades?
4.  **Ocupación:** Segmentación de listings por disponibilidad anual (High/Low demand).
5.  **Geografía:** Barrios con mayor densidad de oferta activa.

---

## 📂 Estructura del Proyecto

```text
M3/
├── bronze_output/          # Salida local de transformaciones local (Mapped to /app/bronze_output in Docker)
├── datos/                  # Datos de entrada locales (CSVs)
├── scripts/                # Scripts de utilidad
│   ├── deploy_emr.py       # Deploy en EMR
│   └── upload_to_s3.py     # Carga de resultados locales a S3
├── src/                    # Código fuente
│   ├── extractors/         # Lógica de extracción (API, CSV, Scraping)
│   ├── utils/              # Utilidades compartidas (Spark, Logs)
│   └── main.py             # Punto de entrada principal
├── transformations/        # Scripts Spark de transformación
│   ├── bronze/             # Lógica Raw -> Bronze (NYC, Listings, Posts)
│   └── validation/         # Scripts de validación de datos
├── Dockerfile              # Definición de imagen (Python 3.11 + Java 17)
├── docker-compose.yml      # Orquestación de contenedores
├── requirements.txt        # Dependencias de Python
└── README.md               # Documentación del proyecto
```

---

## 🛠️ Desarrollo Tecnológico

-   **Lenguaje:** Python 3.11
-   **Procesamiento:** Apache Spark (PySpark 3.5.2)
-   **Cloud:** AWS S3 (Storage)
-   **Librerías Clave:** `boto3`, `pandas`, `requests`, `beautifulsoup4`
-   **Formato Datos:**
    -   Raw: JSON, CSV
    -   Bronze: Parquet (Snappy compressed)

## Ejecución del Pipeline (Docker Flow)

Este flujo garantiza la integridad de los datos y evita problemas de permisos al separar el procesamiento de la carga.

> **Nota para usuarios Windows (sin Docker):**
> Si decides ejecutar Spark nativamente en Windows, necesitarás configurar `HADOOP_HOME` y descargar `winutils.exe` compatible con Hadoop 3.3. Sin esto, la escritura de archivos Parquet fallará. **Se recomienda encarecidamente usar Docker.**

### Paso 1: Transformación (Generación Local)
Ejecuta los scripts Spark para procesar los datos crudos y generar la capa Bronze localmente.

```bash
# A. Transformar NYC Data (Fuente: CSV)
docker-compose run --rm --entrypoint python elt-spark transformations/bronze/transform_nyc.py data-lake-raw-henry file:///app/bronze_output

# B. Transformar Listings (Fuente: DummyJSON API)
docker-compose run --rm --entrypoint python elt-spark transformations/bronze/transform_listings.py data-lake-raw-henry file:///app/bronze_output

# C. Transformar Posts (Fuente: JSONPlaceholder API)
docker-compose run --rm --entrypoint python elt-spark transformations/bronze/raw_to_bronze.py --raw-bucket data-lake-raw-henry --bronze-bucket file:///app/bronze_output --entity posts --source-type api
```

### Paso 2: Validación Local (Opcional)
Verifica que los archivos Parquet se hayan generado correctamente en tu carpeta `M3/bronze_output`.

```bash
# En Windows PowerShell (Listar archivos)
dir bronze_output\marketplace_rentals\*\*\*
```

**Tip: Inspeccionar con Pandas (Python local)**
```python
import pandas as pd
# Asegúrate de tener pyarrow o fastparquet instalado
df = pd.read_parquet("bronze_output/marketplace_rentals/nyc/20260122/")
print(df.head())
print(df.info())
```

### Paso 3: Carga al Data Lake
Sube los datos procesados y validados al bucket de producción en S3.

```bash
docker-compose run --rm --entrypoint python elt-spark scripts/upload_to_s3.py /app/bronze_output data-lake-bronze-henry
```
*(Reemplaza `data-lake-bronze-henry` por tu bucket definido en `.env` si es diferente)*

### Paso 4: Verificación Final
Ejecuta el script de validación para confirmar que los datos en S3 son legibles y consistentes.

```bash
docker-compose run --rm --entrypoint python elt-spark transformations/validation/validate_bronze.py --raw-bucket data-lake-raw-henry --bronze-bucket data-lake-bronze-henry --entity nyc
```

---

**Autor:** Ingeniero Jose David Frias
**Licencia:** MIT
