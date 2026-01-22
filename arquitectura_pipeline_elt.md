# Arquitectura de Pipeline ELT
## Documento Técnico de Diseño

---

## 1. Objetivo del Pipeline

### Problema de Negocio
En el contexto empresarial actual, las organizaciones enfrentan la creciente necesidad de consolidar datos dispersos en múltiples sistemas transaccionales, APIs externas, archivos planos y fuentes no estructuradas. La falta de una vista unificada y confiable de los datos **impide la toma de decisiones estratégicas basadas en evidencia**, genera inconsistencias en reportes, y limita la capacidad analítica de la organización.

**Dominio de Aplicación:** El pipeline se diseña para integrar datos de plataformas de alojamientos, incluyendo propiedades, anfitriones, precios, disponibilidad y reseñas, pudiendo extenderse a otras fuentes transaccionales o de eventos en el futuro.

El pipeline ELT propuesto resuelve este problema mediante:

- **Consolidación centralizada** de datos de múltiples fuentes heterogéneas
- **Persistencia histórica** que permite análisis temporal y auditoría
- **Transformación desacoplada** que habilita iteraciones rápidas sin afectar la extracción
- **Acceso democratizado** a datos de calidad para equipos de negocio, analytics y ciencia de datos

### Decisiones Habilitadas

Este sistema habilita capacidades analíticas estratégicas y operacionales:

**Analítica Estratégica:**
- Identificación de patrones de comportamiento de clientes a lo largo del tiempo
- Análisis de rentabilidad por producto, región, canal de venta
- Proyecciones de demanda basadas en datos históricos consolidados
- Evaluación de eficiencia operacional y optimización de procesos

**Analítica Operacional:**
- Monitoreo de KPIs en tiempo casi real
- Detección de anomalías en transacciones o comportamiento
- Reportería regulatoria y compliance
- Análisis de calidad de datos y confiabilidad de fuentes

---

## 2. Descripción de las Etapas ELT

### 2.1 Extract — Extracción

**Objetivo:** Obtener datos de múltiples fuentes con mínima transformación, preservando fidelidad con el origen.

#### Tipos de Fuentes

| Tipo de Fuente | Ejemplos | Método de Extracción | Frecuencia Típica |
|----------------|----------|----------------------|-------------------|
| **Bases de datos transaccionales** | PostgreSQL, MySQL, SQL Server, Oracle | CDC (Change Data Capture) o Full/Incremental snapshots | Incremental (cada 15-60 min) |
| **Archivos planos** | CSV, JSON, Parquet, XML | API de Object Storage o file watchers | Batch diario/horario |
| **APIs REST** | CRM, ERP externo, servicios de pago | HTTP polling o webhooks | Near real-time o batch |
| **Streaming** | Kafka, event logs, clickstream | Consumidores de streams | Real-time |
| **Web scraping** | Portales públicos, competencia | Scrapers programados | Batch diario/semanal |

#### Enfoques de Extracción

**Full Load (Carga Completa):**
- Justificado para tablas pequeñas (< 1M registros) o sin timestamp de modificación
- Permite detección de deletes implícitos
- Mayor costo computacional y de red

**Incremental Load (Carga Incremental):**
- Basado en columnas de control: `updated_at`, `created_at`, o secuencias incrementales
- Reduce volumen de datos transferidos significativamente
- Requiere gestión de deletes explícita (soft deletes o tablas de log)

**Change Data Capture (CDC):**
- Captura cambios directamente del transaction log de la BD origen
- Latencia mínima (segundos a minutos)
- Requiere permisos especiales y herramientas especializadas (Debezium, AWS DMS, Airbyte)

**Near Real-Time:**
- Para decisiones operacionales críticas (fraud detection, inventory management)
- Balance entre latencia y costo de infraestructura

#### Principios de Diseño en Extracción

1. **Idempotencia:** Reejecutar una extracción no debe producir duplicados ni inconsistencias
2. **Mínima transformación:** Solo tipado básico, sin lógica de negocio
3. **Metadata enriquecida:** Capturar `extracted_at`, `source_system`, `extraction_id` para auditoría
4. **Resiliencia:** Manejo de reconexión, timeouts, y retry con backoff exponencial
5. **Observabilidad:** Métricas de volumen extraído, latencia, y tasa de errores

---

### 2.2 Load — Carga

**Objetivo:** Persistir datos crudos en un almacenamiento escalable y de bajo costo, optimizado para escritura masiva.

#### Estrategia de Carga

**Destino:** Data Lake / Lakehouse sobre Object Storage (S3, Azure Data Lake Storage, Google Cloud Storage)

**Formato de Persistencia:**
- **Parquet:** Columnar, compresión eficiente, compatible con motores analíticos
- **Delta Lake / Apache Iceberg:** Formatos table con capacidades ACID, time travel, schema evolution
- **JSON/CSV:** Solo para datos semi-estructurados o casos específicos

**Patrón de Escritura:**
- **Particionado temporal:** `year=YYYY/month=MM/day=DD/` para facilitar filtrado y retención
- **Particionado por fuente:** `source_system=crm/table=customers/`
- **Compresión:** Snappy o ZSTD según balance velocidad/tamaño

**Modos de Escritura:**

| Modo | Descripción | Caso de Uso |
|------|-------------|-------------|
| **Append** | Agregar datos sin sobrescribir | Logs, eventos, incremental loads |
| **Overwrite** | Reemplazar partición completa | Full loads, reprocesos |
| **Merge (Upsert)** | Actualizar registros existentes o insertar nuevos | CDC, actualizaciones incrementales con Delta Lake |

#### Consideraciones de Rendimiento

- **Batch sizing:** Escribir en micro-batches de 100k-1M registros para balance latencia/throughput
- **Compactación:** Procesos periódicos para consolidar archivos pequeños (evitar "small files problem")
- **Indexing:** Z-ordering o clustering keys en Delta Lake/Iceberg para mejorar query performance

---

### 2.3 Transform — Transformación

**Objetivo:** Convertir datos crudos en modelos analíticos consumibles, aplicando lógica de negocio, calidad y conformación.

#### Enfoque ELT vs ETL

| Aspecto | ETL (Tradicional) | ELT (Moderno) |
|---------|-------------------|---------------|
| **Transformación** | Antes de cargar (en herramienta externa) | Después de cargar (en el destino) |
| **Escalabilidad** | Limitada por servidor ETL | Escalabilidad horizontal del Lakehouse/DW |
| **Flexibilidad** | Rígido, requiere re-extracción para cambios | Iterativo, re-transformar sin re-extraer |
| **Costo** | Hardware dedicado para ETL | Compute on-demand |

**Adoptamos ELT** porque:
- Aprovecha poder computacional distribuido (Spark, SQL engines)
- Permite auditoría completa manteniendo datos raw
- Facilita reprocesos y correcciones históricas
- Desarrollo más ágil con herramientas como dbt

#### Capas de Transformación

La transformación se estructura en capas progresivas (detalladas en sección 3):

1. **Raw → Staging:** Estandarización mínima, tipado, deduplicación
2. **Staging → Core/Intermediate:** Limpieza, normalización, integración entre fuentes
3. **Core → Gold/Marts:** Modelado dimensional, agregaciones, métricas de negocio

#### Herramientas de Transformación

**dbt (data build tool):**
- Transformaciones SQL versionadas en Git
- Testing integrado (unique, not_null, relationships)
- Documentación auto-generada
- Lineage visual de dependencias

**Apache Spark:**
- Procesamiento distribuido para grandes volúmenes (>TB)
- Soporte para transformaciones complejas en Python/Scala
- Integración nativa con Delta Lake

**SQL Engines (Snowflake, BigQuery, Databricks SQL):**
- Transformaciones declarativas SQL
- Escalabilidad automática
- Costos basados en uso

#### Principios de Transformación

1. **Desacople:** Cada capa es independiente, no se salta niveles
2. **Testing:** Cada transformación tiene tests de contrato (schema) y calidad
3. **Incrementalidad:** Procesamiento incremental donde sea posible (dbt incremental models)
4. **Reproducibilidad:** Mismos inputs → mismos outputs, sin efectos laterales
5. **Documentación:** Cada modelo documenta lógica de negocio, owner, y SLA

---

## 3. Definición de Capas del Data Warehouse

### 3.1 Raw / Staging Layer

**Propósito:**
Almacenar datos **tal como llegan de la fuente**, sin transformaciones de negocio, actuando como fuente de verdad única y habilitando auditoría completa.

**Características:**

| Aspecto | Especificación |
|---------|----------------|
| **Formato** | Parquet, Delta Lake, JSON (según origen) |
| **Schema** | Schema-on-read o schema evolution habilitada |
| **Transformaciones permitidas** | Solo tipado básico, deduplicación técnica, enriquecimiento metadata |
| **Particionado** | Por fecha de ingesta y fuente |
| **Retención** | 90 días a 2 años según compliance y costo |

**Responsabilidades:**

- ✅ Persistencia fiel a la fuente (incluyendo datos "sucios")
- ✅ Metadata de auditoría: `ingestion_timestamp`, `source_file`, `extraction_batch_id`
- ✅ Soporte para reprocesos completos
- ✅ Detección de schema drift

**Límites (NO debe hacer):**

- ❌ Aplicar lógica de negocio (cálculos, categorizaciones)
- ❌ Joins entre fuentes
- ❌ Filtrado de datos "malos" (excepto duplicados técnicos obvios)

**Ejemplo de Naming:**
```
raw/
├── source_system=erp_sap/
│   ├── table=sales_orders/
│   │   └── year=2026/month=01/day=21/
│   │       └── part-00000.parquet
├── source_system=api_stripe/
│   └── endpoint=payments/
```

---

### 3.2 Core / Intermediate Layer

**Propósito:**
Aplicar **limpieza, normalización e integración** de datos cross-source, generando datasets confiables y reutilizables para múltiples casos de uso.

**Características:**

| Aspecto | Especificación |
|---------|----------------|
| **Formato** | Delta Lake / Iceberg (ACID necesario) |
| **Schema** | Fuertemente tipado, con contratos validados |
| **Transformaciones** | Limpieza, deduplicación semántica, conformación dimensional |
| **Particionado** | Por fecha de negocio (order_date, transaction_date) |
| **Retención** | 2-7 años, según necesidades analíticas |

**Responsabilidades:**

- ✅ **Limpieza de datos:** nulls handling, valores fuera de rango, corrección de tipos
- ✅ **Deduplicación semántica:** Eliminar duplicados basados en lógica de negocio
- ✅ **Conformación:** Estandarización de códigos (country codes, currency codes)
- ✅ **Slowly Changing Dimensions (SCD):** Tracking de cambios históricos en dimensiones (Type 2)
- ✅ **Integración cross-source:** Joins entre CRM + ERP + logs para crear entidades unificadas
- ✅ **Enriquecimiento:** Cálculos derivados que son reutilizables (días desde primera compra, segmentación RFM)

**Límites:**

- ❌ Agregaciones pesadas (sumas por trimestre, promedios móviles complejos)
- ❌ Modelado específico para un dashboard (eso va en Gold)

**Ejemplo de Modelos:**

```
core/
├── dim_customer (SCD Type 2)
│   ├── customer_key (surrogate)
│   ├── customer_id (business key)
│   ├── email, name, segment
│   ├── valid_from, valid_to, is_current
├── dim_product
├── dim_date (pre-generada, 10 años)
└── fct_transactions (event-level, no agregado)
    ├── transaction_id
    ├── customer_key (FK)
    ├── product_key (FK)
    ├── amount, quantity, discount
    └── transaction_date
```

---

### 3.3 Gold / Consumption Layer (Data Marts)

**Propósito:**
Proveer **modelos listos para consumo**, optimizados para casos de uso específicos (BI, reporting, ML), con agregaciones pre-calculadas y lógica de negocio final.

**Características:**

| Aspecto | Especificación |
|---------|----------------|
| **Formato** | Vistas materializadas, tablas agregadas, cachés |
| **Schema** | Orientado a negocio, nombres no-técnicos |
| **Transformaciones** | Agregaciones, window functions, métricas calculadas |
| **Optimización** | Indexes, clustering, caching según herramienta |
| **Retención** | Según SLA de reportería (usualmente 1-3 años) |

**Responsabilidades:**

- ✅ **Agregaciones pre-calculadas:** Ventas por mes/región, métricas rolling de 7/30/90 días
- ✅ **Métricas de negocio:** LTV, AOV, churn rate, conversion funnels
- ✅ **Star/Snowflake schemas** optimizados para herramientas BI específicas
- ✅ **Denormalización estratégica:** Para mejorar query performance
- ✅ **Row-level security (RLS):** Filtros de acceso por usuario/rol

**Límites:**

- ❌ Almacenar datos granulares que ya están en Core (evitar duplicación)
- ❌ Lógica de transformación base (ya debe estar en Core)

**Ejemplos de Marts:**

```
gold/
├── mart_sales_performance
│   ├── Métricas mensuales por producto/región
│   ├── YoY growth, trends
├── mart_customer_360
│   ├── Vista unificada de cliente (transacciones + soporte + marketing)
│   ├── Segmentación, LTV, engagement score
├── mart_inventory_optimization
└── mart_executive_dashboard
```

**Consumidores Típicos:**

- Dashboards de BI (Tableau, Power BI, Looker)
- APIs de reportería
- Modelos de ML (feature engineering puede partir de Gold)
- Exportes para reguladores

---

## 4. Diagrama Técnico de Arquitectura

### Descripción Conceptual del Flujo de Datos

A continuación se describe la arquitectura lógica del sistema, que puede ser plasmada visualmente en herramientas como Draw.io, Lucidchart o arquitecture diagrams tools.

![alt text](img\image2.png)

### Componentes Detallados

#### **1. Data Sources (Fuentes de Datos)**

| Fuente | Descripción | Método de Acceso |
|--------|-------------|------------------|
| **OLTP Databases** | Bases transaccionales (PostgreSQL, MySQL, SQL Server, Oracle) | CDC, JDBC connection |
| **External APIs** | CRM (Salesforce), Payment gateways (Stripe), Ads platforms | REST/GraphQL clients |
| **File Storage** | Data dumps, exports legacy, partner files | S3/ADLS API, SFTP |
| **Event Streams** | Clickstream, IoT sensors, application logs | Kafka consumers |
| **Web Scraping** | Datos públicos de competencia, portales regulatorios | Custom scrapers (Scrapy, Selenium) |

#### **2. Extraction Layer**

| Herramienta | Propósito | Ventajas |
|-------------|-----------|----------|
| **Debezium / AWS DMS** | CDC para bases de datos | Latencia baja, captura deletes |
| **Airbyte / Fivetran** | Conectores pre-built para SaaS APIs | Reducción de boilerplate, mantenimiento |
| **Python/Spark Scripts** | Extractores custom | Flexibilidad total |
| **Kafka Connect** | Ingesta de streams | Escalabilidad horizontal, fault-tolerance |

#### **3. Data Lake / Lakehouse (Object Storage)**

**Tecnología:** S3, ADLS Gen2, GCS con formato Delta Lake / Apache Iceberg

**Capacidades clave:**
- ✅ ACID transactions
- ✅ Time travel (consultar versión histórica de datos)
- ✅ Schema evolution
- ✅ Costo bajo (~$0.023/GB/mes en S3 Standard)

#### **4. Transformation Engine**

| Herramienta | Caso de Uso | Fortalezas |
|-------------|-------------|------------|
| **dbt** | Transformaciones SQL estándar | Version control, testing, documentación |
| **Apache Spark** | Transformaciones complejas, grandes volúmenes | Escalabilidad, UDFs en Python/Scala |
| **Orchestrator (Airflow/Dagster)** | Coordinación de pipelines | Retry logic, sensores, lineage visual |

#### **5. Consumption Layer**

| Consumer | Latencia Aceptable | Patrón de Acceso |
|----------|-------------------|------------------|
| **BI Dashboards** | Minutos a horas | Interactive queries, caching |
| **APIs** | Segundos | Pre-computed aggregates, caching Redis |
| **ML Training** | Batch diario/semanal | Bulk exports, Parquet snapshots |
| **Reports** | Batch nocturno | Scheduled exports (PDF, Excel) |

---

## 5. Justificación de Herramientas y Tecnologías

### Criterios de Selección

| Criterio | Peso | Justificación |
|----------|------|---------------|
| **Escalabilidad horizontal** | ⭐⭐⭐⭐⭐ | El sistema debe crecer de GB a TB a PB sin rediseño |
| **Costo Total de Ownership (TCO)** | ⭐⭐⭐⭐ | Balance entre costo de infraestructura y productividad del equipo |
| **Facilidad de integración** | ⭐⭐⭐⭐ | Ecosistema compatible, APIs estándar |
| **Community & Soporte** | ⭐⭐⭐ | Documentación, troubleshooting, talent availability |
| **Vendor Lock-in** | ⭐⭐⭐ | Preferencia por tecnologías open-source o multi-cloud |

---

### Stack Técnico Propuesto

#### **Storage Layer**

**Tecnología:** Object Storage (S3 / Azure Data Lake Storage / GCS) + Delta Lake

| Aspecto | Justificación |
|---------|---------------|
| **Escalabilidad** | Petabytes sin límite práctico, escalabilidad automática |
| **Costo** | 10-50x más barato que bases de datos tradicionales para datos fríos |
| **Compatibilidad** | Compatible con Spark, dbt, Trino, Presto, Athena, BigQuery (external tables) |
| **ACID con Delta Lake** | Transacciones garantizadas, time travel, schema versioning |
| **Separación compute-storage** | Múltiples motores pueden leer/escribir sin duplicar datos |

**Alternativa:** Apache Iceberg (mismas ventajas, mayor adopción en Snowflake/Databricks ecosystems)

---

#### **Transformation Layer**

**Tecnología Principal:** dbt (data build tool)

| Aspecto | Justificación |
|---------|---------------|
| **SQL-first** | 80% de transformaciones son SQL puro, accesible para analistas |
| **Version control** | Transformaciones en Git, code review, CI/CD integrado |
| **Testing integrado** | Tests de schema, unicidad, relaciones, custom tests |
| **Documentación auto-generada** | Lineage graph, column descriptions, metadata catalogs |
| **Incrementalidad** | Procesamiento incremental nativo reduce costos 10-100x |
| **Ecosistema** | Compatible con Snowflake, BigQuery, Databricks, Redshift, Spark |

**Tecnología Complementaria:** Apache Spark (PySpark)

| Aspecto | Justificación |
|---------|---------------|
| **Cálculos complejos** | ML feature engineering, graph processing, iterative algorithms |
| **Grandes volúmenes** | Procesamiento distribuido de TB en minutos (vs horas en SQL tradicional) |
| **UDFs custom** | Lógica no expresable en SQL (parsing, ML inference inline) |
| **Integración con Delta Lake** | API nativa para merge, optimize, vacuum |

---

#### **Orchestration**

**Tecnología Seleccionada:** Apache Airflow

**Apache Airflow** será utilizado como orquestador del pipeline ELT en fases posteriores de implementación. Airflow permite coordinar la ejecución de tareas de extracción, transformación y validación mediante DAGs (Directed Acyclic Graphs), proporcionando:

| Aspecto | Justificación |
|---------|---------------|
| **DAG-based scheduling** | Dependencias complejas manejadas declarativamente |
| **Retry logic** | Manejo automático de fallos transitorios |
| **Monitoring** | Alertas, SLA tracking, visualización de cuellos de botella |
| **Extensibilidad** | Operadores custom para cualquier herramienta |
| **Madurez** | Gran comunidad, amplia adopción enterprise, muchos operators pre-built |

**Alternativas evaluadas:**

| Herramienta | Fortalezas | Debilidades |
|-------------|------------|-------------|
| **Airflow** | Maduro, gran community, muchos operators | Curva de aprendizaje, UI legacy |
| **Dagster** | Modern, data-aware, testing fácil | Menos maduro, menos operators |
| **Prefect** | Híbrido (cloud + local), UX excelente | Menos adopción enterprise |

**CI/CD:** **GitHub Actions** será utilizado para automatizar pipelines de integración y despliegue continuo, incluyendo tests de dbt, validaciones de calidad de código, y despliegues automatizados de transformaciones.

---

#### **Compute Engine**

**Opción A:** Databricks (Lakehouse Platform)

| Aspecto | Beneficio |
|---------|----------|
| **Unified platform** | Spark + Delta Lake + dbt + notebooks + ML en una plataforma |
| **Auto-scaling** | Clusters escalan automáticamente según carga |
| **Unity Catalog** | Governance, lineage, access control integrado |
| **Photon engine** | Queries vectorizadas, 3-5x más rápidas que Spark estándar |

**Opción B:** Snowflake (Cloud Data Warehouse)

| Aspecto | Beneficio |
|---------|----------|
| **Zero management** | No hay clusters que configurar, escalabilidad automática |
| **Query performance** | Optimizaciones automáticas, resultados cacheados |
| **Data sharing** | Cross-company data sharing sin ETL |
| **Time travel** | Recuperar datos de hasta 90 días atrás |

**Opción C:** Self-managed Spark on Kubernetes

| Aspecto | Beneficio |
|---------|----------|
| **Costo** | Más barato en escala (no markup de vendor) |
| **Control total** | Customización completa de configuración |
| **Multi-cloud** | Portabilidad entre AWS/Azure/GCP |

**Debilidades:** Requiere expertise DevOps, overhead de mantenimiento

---

#### **BI & Analytics**

**Recomendación:** Herramienta según perfil de usuario

| Usuario | Herramienta | Justificación |
|---------|-------------|---------------|
| **Ejecutivos** | Tableau / Power BI | Dashboards visuales, interactividad |
| **Analistas** | Looker / Metabase | Self-service, SQL lightweight |
| **Data Scientists** | Jupyter / Hex / Deepnote | Notebooks, Python/R, colaboración |
| **Developers** | APIs (GraphQL/REST) | Integración con aplicaciones |

---

#### **Data Catalog & Governance**

**Tecnología:** Unity Catalog (Databricks) / Datahub (Open Source)

| Aspecto | Beneficio |
|---------|----------|
| **Discovery** | Search de datasets por keywords, tags, owners |
| **Lineage** | Visualización de dependencias tabla→transformación→dashboard |
| **Access control** | RBAC, attribute-based access, PII masking |
| **Metadata management** | Descriptions, SLAs, data quality scores |

---

### Consideraciones de Escalabilidad

| Volumen de Datos | Latencia de Procesamiento | Arquitectura Recomendada |
|------------------|---------------------------|--------------------------|
| **< 100 GB** | Batch diario | PostgreSQL + dbt Cloud + Metabase |
| **100 GB - 10 TB** | Batch horario | Snowflake/BigQuery + dbt + Looker |
| **10 TB - 1 PB** | Batch cada 15-60 min | Databricks + Delta Lake + Spark |
| **> 1 PB** | Streaming + batch | Lakehouse multi-layer + Kafka + Spark Streaming |

---

## 6. Identificación y Análisis de Fuentes de Datos

### Metodología de Análisis

Para cada pregunta de negocio, se realiza un análisis inverso:

1. **Definir la métrica o insight requerido**
2. **Identificar las entidades de negocio involucradas** (clientes, productos, transacciones)
3. **Mapear a fuentes de datos técnicas** (tablas, APIs, archivos)
4. **Evaluar calidad y confiabilidad**
5. **Determinar frecuencia de actualización necesaria**

---

### Preguntas de Negocio y Fuentes Asociadas

#### **Pregunta 1: ¿Cuál es el comportamiento de compra de clientes a lo largo del tiempo?**

**Fuentes Requeridas:**

| Fuente | Datos Provistos | Tipo | Frecuencia |
|--------|----------------|------|-----------|
| **ERP (SAP/Oracle)** | Transacciones de venta: order_id, customer_id, product_id, amount, quantity, date | Base de datos transaccional | Incremental (CDC cada 15 min) |
| **CRM (Salesforce)** | Información de cliente: customer_id, segment, lifetime_value, first_purchase_date | API REST | Batch diario |
| **Web Analytics (Google Analytics)** | Clickstream: session_id, customer_id, page_views, events, timestamps | API / BigQuery export | Batch diario |
| **Loyalty Program DB** | Puntos acumulados, tier, redemptions | Base de datos interna | Incremental |

**Relación Fuente → Métrica:**
- `dim_customer` (de CRM + ERP) → Segmentación
- `fct_transactions` (de ERP) → Frecuencia, recencia, monto
- `fct_web_events` (de Analytics) → Journey pre-compra

**Consideraciones de Calidad:**
- ⚠️ `customer_id` puede no estar linkeado entre web y ERP si no hay login → requiere identity resolution
- ✅ ERP es fuente de verdad para transacciones finales
- ⚠️ CRM puede tener desfase de 24h respecto a ERP

---

#### **Pregunta 2: ¿Qué productos tienen mejor margen de rentabilidad por región?**

**Fuentes Requeridas:**

| Fuente | Datos Provistos | Tipo | Frecuencia |
|--------|----------------|------|-----------|
| **ERP - Sales Module** | Ventas por producto/región: revenue, units_sold | DB transaccional | Incremental |
| **ERP - Inventory Module** | Costos de producto: product_id, cogs (cost of goods sold) | DB transaccional | Batch diario |
| **Logistics System** | Costos de envío por región | API / CSV exports | Batch semanal |
| **Master Data (MDM)** | Jerarquías de producto, catálogo de regiones | CSV / DB | Full load semanal |

**Relación Fuente → Métrica:**
- `fct_sales` → Revenue por producto/región
- `dim_product` → Categorías, jerarquías, COGS
- `dim_geography` → Regiones, países, costos logísticos
- **Métrica derivada:** `gross_margin = (revenue - cogs - shipping_cost) / revenue`

**Consideraciones de Calidad:**
- ✅ COGS es confiable si el ERP está bien configurado
- ⚠️ Costos de envío pueden estar agregados a nivel país, no región específica
- ⚠️ Productos sin `cogs` en MDM → requiere imputación o exclusión

---

#### **Pregunta 3: ¿Cómo se compara nuestro pricing vs. competencia?**

**Fuentes Requeridas:**

| Fuente | Datos Provistos | Tipo | Frecuencia |
|--------|----------------|------|-----------|
| **ERP - Pricing Module** | Precios internos: product_id, price, effective_date | DB transaccional | Incremental |
| **Web Scraping - Competidores** | Precios de competencia: competitor, product (matched), price, scraped_at | Web scraping (custom) | Batch diario |
| **Market Data Provider (externo)** | Índices de mercado, precios sugeridos | API REST | Batch semanal |
| **MDM** | Product matching: sku_interno ↔ competitor_sku | Manual curation / ML matching | Full load mensual |

**Relación Fuente → Métrica:**
- `dim_product_pricing` → Histórico de precios internos
- `fct_competitor_pricing` → Scraping agregado
- **Métrica derivada:** `price_index = our_price / avg_competitor_price`

**Consideraciones de Calidad:**
- ⚠️ Web scraping puede fallar si sitios cambian estructura HTML
- ⚠️ Product matching entre SKUs internos y externos es complejo → requiere fuzzy matching o ML
- ✅ Scraping debe respetar `robots.txt` y términos de servicio
- 🔒 Legal: validar compliance antes de scrapear competencia

---

#### **Pregunta 4: ¿Cuál es la tasa de churn de clientes y factores asociados?**

**Fuentes Requeridas:**

| Fuente | Datos Provistos | Tipo | Frecuencia |
|--------|----------------|------|-----------|
| **CRM - Customer DB** | Customer profile, segment, status (active/churned) | DB / API | Incremental |
| **Subscription System** | Subscriptions: customer_id, start_date, end_date, plan, payment_status | DB transaccional | CDC |
| **Support Tickets** | Tickets: customer_id, category, resolution_time, sentiment | DB transaccional | Incremental |
| **Email Marketing Platform** | Engagement: customer_id, email_opens, clicks, unsubscribes | API | Batch diario |
| **Payment Gateway (Stripe)** | Payment failures, retry attempts | Webhook → Kafka | Real-time |

**Relación Fuente → Métrica:**
- `dim_customer` → Atributos demográficos, firmográficos
- `fct_subscriptions` → Lifecycle de suscripción, churn events
- `fct_support` → Indicadores de insatisfacción
- `fct_engagement` → Engagement score
- **Métrica derivada:** `churn_rate = churned_customers / total_customers (monthly cohorts)`

**Consideraciones de Calidad:**
- ✅ Subscription system es fuente de verdad para churn
- ⚠️ Definición de churn puede variar: cancelación explícita vs. inactividad vs. no pago
- ⚠️ Support tickets pueden tener `customer_id` null si usuario no estaba logueado → linking manual o por email

---

#### **Pregunta 5: ¿Cuál es la eficiencia operacional de nuestros centros de distribución?**

**Fuentes Requeridas:**

| Fuente | Datos Provistos | Tipo | Frecuencia |
|--------|----------------|------|-----------|
| **Warehouse Management System (WMS)** | Order fulfillment: order_id, warehouse_id, picked_at, packed_at, shipped_at | DB transaccional | Incremental |
| **IoT Sensors** | Equipment uptime, temperature, alerts | MQTT / Kafka | Real-time |
| **HR System** | Staffing levels, shifts, labor hours | API / CSV | Batch diario |
| **Transportation Management System** | Delivery times, carrier performance | DB transaccional | Incremental |

**Relación Fuente → Métrica:**
- `fct_fulfillment` → Tiempo desde order → ship, error rates
- `fct_equipment` → Downtime por warehouse
- `dim_warehouse` → Capacidad, ubicación
- **Métrica derivada:** `efficiency = orders_shipped / (labor_hours + equipment_downtime_penalty)`

**Consideraciones de Calidad:**
- ✅ WMS timestamps son confiables si hay SLA de registro
- ⚠️ IoT sensors pueden tener gaps por connectivity issues → requiere interpolation
- ⚠️ Staffing data puede estar en diferentes zonas horarias → normalización crítica

---

### Evaluación de Confiabilidad de Fuentes

| Fuente | Confiabilidad | Justificación | Mitigación de Riesgos |
|--------|---------------|---------------|----------------------|
| **ERP (SAP/Oracle)** | ⭐⭐⭐⭐⭐ Alta | Sistema de record, transacciones ACID | Monitoreo de schema changes |
| **CRM (Salesforce)** | ⭐⭐⭐⭐ Alta | Datos curados por sales team | Validar contra ERP para customer_id |
| **Web Scraping** | ⭐⭐ Baja | Sujeto a cambios de HTML, bloqueos | Alertas de fallo, fallback a fuentes alternativas |
| **APIs Externas** | ⭐⭐⭐ Media | Depende de SLA del proveedor | Rate limiting handling, caching |
| **IoT Sensors** | ⭐⭐⭐ Media | Ruido, connectivity gaps | Outlier detection, interpolation |
| **Archivos CSV manuales** | ⭐⭐ Baja | Errores humanos, formatos inconsistentes | Schema validation en ingesta, alertas |

---

### Matriz de Valor Analítico

| Fuente | Costo de Integración | Frecuencia de Uso | Valor de Negocio | Prioridad |
|--------|----------------------|-------------------|------------------|-----------|
| **ERP Transactions** | Alto (CDC setup) | Diario | Crítico | **P0** |
| **CRM Data** | Medio (API estable) | Diario | Alto | **P0** |
| **Web Analytics** | Bajo (export nativo) | Diario | Alto | **P1** |
| **Competitor Pricing (scraping)** | Alto (custom, frágil) | Semanal | Medio | **P2** |
| **IoT Sensors** | Alto (streaming setup) | Real-time | Medio (operacional) | **P2** |
| **Social Media APIs** | Medio | Diario | Bajo (nice-to-have) | **P3** |

**Recomendación:** Comenzar con fuentes P0 (ERP, CRM), validar pipeline, luego iterar con P1-P2.

---

## 7. Consideraciones de Implementación Futura

> [!NOTE]
> Los siguientes temas están **fuera de alcance** en esta fase de diseño, pero deben ser considerados en fases posteriores de implementación.

### 7.1 Data Quality & Observability

**Frameworks a considerar:**
- **Great Expectations:** Data testing framework (asserts on data)
- **Monte Carlo / Datadog:** Anomaly detection, lineage, alerting
- **dbt tests:** Contratos de schema, business rules

**Métricas clave:**
- Freshness (SLA de actualización)
- Completeness (% nulls en columnas críticas)
- Accuracy (validación contra fuente de verdad)
- Consistency (joins exitosos entre tablas)

---

### 7.2 Security & Compliance

**GDPR / CCPA:**
- Right to erasure → hard deletes en Lakehouse (Delta Lake GDPR compliance)
- Data minimization → no almacenar PII innecesario
- Encryption at rest (S3 KMS) y in transit (TLS)

**Access Control:**
- RBAC en Data Catalog
- Column-level masking para PII
- Audit logs de accesos

---

### 7.3 Cost Optimization

**Estrategias:**
- Lifecycle policies en S3 (Standard → IA → Glacier)
- Partitioning inteligente para evitar full scans
- Spot instances para jobs de Spark no críticos
- Query result caching en Snowflake/BigQuery

**Monitoreo:**
- Cost attribution por equipo/proyecto
- Alertas de budget overruns

---

### 7.4 Disaster Recovery

**Backups:**
- Snapshots de Delta Lake (time travel integrado)
- Cross-region replication de S3 para datos críticos

**RPO/RTO:**
- Recovery Point Objective: máximo 1 día de pérdida de datos
- Recovery Time Objective: restauración en < 4 horas

---

## 8. Conclusiones y Próximos Pasos

### Resumen Ejecutivo

Este documento define la arquitectura base de un pipeline ELT moderno que:

✅ **Escala horizontalmente** de gigabytes a petabytes sin rediseño arquitectónico  
✅ **Separa responsabilidades** en capas claras (Raw → Core → Gold)  
✅ **Habilita iteración rápida** mediante ELT (transformar sin re-extraer)  
✅ **Garantiza auditoría completa** preservando datos crudos  
✅ **Soporta múltiples consumidores** (BI, ML, APIs) desde Gold layer  

### Decisiones Arquitectónicas Clave

| Decisión | Justificación |
|----------|---------------|
| **ELT sobre ETL** | Aprovecha compute distribuido del Lakehouse, flexibilidad de reprocesos |
| **Delta Lake / Iceberg** | ACID transactions, time travel, schema evolution en Object Storage barato |
| **Capas Raw-Core-Gold** | Separación de concerns, reutilización, auditoría |
| **dbt como transformation engine** | Testing integrado, version control, SQL-first para democratización |
| **Orchestration desacoplada** | Orquestador (Airflow/Dagster) coordina pero no ejecuta transformaciones |

### Riesgos y Mitigaciones

| Riesgo | Probabilidad | Impacto | Mitigación |
|--------|--------------|---------|------------|
| **Crecimiento de volumen no anticipado** | Media | Alto | Arquitectura cloud-native con auto-scaling |
| **Schema drift en fuentes** | Alta | Medio | Schema evolution en Delta Lake, alertas de cambios |
| **Calidad de datos en fuentes externas** | Alta | Alto | Data quality tests en Staging, alertas, fallbacks |
| **Vendor lock-in** | Baja | Medio | Preferencia por stacks open-source (Spark, dbt, Iceberg) |

### Roadmap Sugerido

#### **Fase 1: MVP (Mes 1-2)**
- ✅ Setup de infraestructura base (S3 + Delta Lake)
- ✅ Integración de 2-3 fuentes prioritarias (ERP, CRM)
- ✅ Implementación de capas Raw y Core
- ✅ Dashboard básico de monitoreo de pipeline

#### **Fase 2: Producción (Mes 3-4)**
- ✅ Integración de fuentes restantes (P1-P2)
- ✅ Implementación de Gold layer y primeros marts
- ✅ Orquestación con Airflow/Dagster
- ✅ Primeros dashboards de BI conectados

#### **Fase 3: Madurez (Mes 5-6)**
- ✅ Data quality framework (Great Expectations)
- ✅ Data Catalog y lineage (Datahub)
- ✅ Incrementalidad en dbt models
- ✅ Cost optimization y performance tuning

#### **Fase 4: Advanced Analytics (Mes 7+)**
- ✅ Feature engineering para ML
- ✅ Real-time streaming layer (Kafka → Delta Live Tables)
- ✅ Advanced governance (PII masking, GDPR compliance)

---

## Anexos

### Glosario Técnico

| Término | Definición |
|---------|------------|
| **CDC (Change Data Capture)** | Técnica para capturar cambios incrementales de una base de datos mediante lectura del transaction log |
| **Delta Lake** | Storage layer open-source que provee ACID transactions sobre data lakes (Parquet) |
| **ELT** | Extract-Load-Transform: cargar datos crudos primero, transformar después en el destino |
| **Lakehouse** | Arquitectura que combina flexibilidad del Data Lake con estructura del Data Warehouse |
| **SCD Type 2** | Slowly Changing Dimension Type 2: tracking de cambios históricos con validez temporal |
| **Idempotencia** | Propiedad de una operación que produce el mismo resultado si se ejecuta múltiples veces |

### Referencias

- [Delta Lake Documentation](https://docs.delta.io/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)
- [Databricks Lakehouse Architecture](https://www.databricks.com/product/data-lakehouse)
- [Kimball Dimensional Modeling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/)
- [Great Expectations Documentation](https://docs.greatexpectations.io/)

---

**Documento creado por:** Ingeniero Jose David Frias 
**Versión:** 1.0  
**Fecha:** Enero 2026
