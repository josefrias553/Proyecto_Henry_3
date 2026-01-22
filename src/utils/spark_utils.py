import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark import SparkConf

# Cargar variables de entorno si existen (.env)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Configurar logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark_session(app_name, execution_mode="local"):
    """
    Crea y retorna una SparkSession configurada.
    
    Args:
        app_name (str): Nombre de la aplicación Spark.
        execution_mode (str): 'local' o 'cluster'. Por defecto 'local'.
    
    Returns:
        SparkSession: Sesión de Spark activa.
    """
    logger.info(f"Iniciando SparkSession para {app_name} en modo {execution_mode}")

    # -------------------------------------------------------------------------
    # FIX CRÍTICO PARA EJECUCIÓN LOCAL
    # -------------------------------------------------------------------------
    # 1. SPARK_HOME: Si existe una instalación global, causa conflictos de versión (Py4J error).
    #    La eliminamos para forzar el uso del entorno virtual bundled.
    if 'SPARK_HOME' in os.environ:
        logger.warning(f"SPARK_HOME detectado ({os.environ['SPARK_HOME']}). Desactivándolo para evitar conflictos.")
        del os.environ['SPARK_HOME']
        
    # Detener sesiones activas para evitar conflictos en testing local
    try:
        if 'spark' in globals():
            globals()['spark'].stop()
    except:
        pass

    try:
        # Construcción secuencial estándar
        builder = SparkSession.builder \
            .appName(app_name) \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.adaptive.enabled", "true")
            
        # Configuración específica para Local
        if execution_mode == "local" or "SUBMIT_ARGS" not in os.environ:
            logger.info("Configurando para ejecución LOCAL")
            builder = builder.master("local[*]")
            
            # Dependencias S3
            builder = builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.532")
            builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            builder = builder.config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            
            if os.name == 'nt':
                # Windows: Usar 'file' committer (legacy)
                logger.info("Sistema Windows detectado: Usando 'file' committer para S3A")
                builder = builder.config("spark.hadoop.fs.s3a.committer.name", "file")
                builder = builder.config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            else:
                # Linux/Docker: Usar 'magic' committer (moderno/performante)
                logger.info("Sistema Linux detectado: Usando 'magic' committer para S3A")
                builder = builder.config("spark.hadoop.fs.s3a.committer.name", "magic")
                builder = builder.config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
            
            # FIX COMMIT S3 ON WINDOWS: Usar "file" committer (legacy/simple) para evitar fallos de rename/magic
            # Solo aplicar si estamos en Windows nativo
            if os.name == 'nt':
                logger.info("Sistema Windows detectado: Usando 'file' committer para S3A")
                builder = builder.config("spark.hadoop.fs.s3a.committer.name", "file")
                builder = builder.config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            
            # FIX WINDOWS: Propagar HADOOP_HOME explícitamente si existe
            if 'HADOOP_HOME' in os.environ:
                builder = builder.config("spark.systemProperty.hadoop.home.dir", os.environ['HADOOP_HOME'])
                
                # Propagar al PATH para asegurar que winutils/dll se encuentren
                hadoop_bin = os.path.join(os.environ['HADOOP_HOME'], 'bin')
                if hadoop_bin not in os.environ['PATH']:
                    logger.info(f"Agregando {hadoop_bin} al PATH")
                    os.environ['PATH'] += os.pathsep + hadoop_bin
            
        spark = builder.getOrCreate()
        
        # Log version details
        logger.info(f"Spark Version: {spark.version}")
        logger.info(f"Java Version: {spark.conf.get('spark.driver.host')}") # Indirect check
        
        return spark
    except Exception as e:
        logger.error(f"Error fatal iniciando SparkSession: {str(e)}")
        raise e
