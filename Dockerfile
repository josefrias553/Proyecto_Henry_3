# Usar imagen base Python 3.11 slim (coincide con entorno local)
FROM python:3.11-slim

# Variables de entorno para PySpark
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH=/app

# Instalación de dependencias del sistema
# - default-jre-headless: Instala Java compatible (11 o 17) de forma segura en Debian
# - procps: Comandos como 'ps' usados por Spark
# - libxml2/libxslt: Requerido por lxml
# mkdir -p /usr/share/man/man1 es un fix conocido para instalar Java en imagenes slim
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jre-headless \
    procps \
    libxml2 \
    libxslt1.1 \
    && rm -rf /var/lib/apt/lists/*

# Establecer directorio de trabajo
WORKDIR /app

# Copiar requirements primero para aprovechar cache de Docker
COPY requirements.txt .

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY . .

# Comando por defecto (sobrescrito por docker-compose o argumentos CLI)
ENTRYPOINT ["python", "-m", "src.main"]
CMD ["--help"]
