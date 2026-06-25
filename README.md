# Plataforma Big Data para Analítica Omnicanal de Ventas Retail

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.4.1-orange.svg)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-2.4.0-blueviolet.svg)](https://delta.io)
[![GCP](https://img.shields.io/badge/Cloud-GCP-4285F4.svg)](https://cloud.google.com)
[![Fase](https://img.shields.io/badge/Fase-3_%E2%80%94_Documentaci%C3%B3n_T%C3%A9cnica-green.svg)]()

**Proyecto Integrador** — Asignatura Big Data & Cloud Computing  
Magíster en Data Science · Universidad del Desarrollo · 2026  
**Profesor:** Luis Castillo Faune

---

## Equipo

| Integrante | Rol en el proyecto |
|---|---|
| Claudio Ballerini | Arquitectura y Pipeline Bronze |
| Juan José Torres | Pipeline Silver y Optimización |
| Cristian Vargas | Pipeline Gold y Data Marts |
| Christian Vásquez | Gobernabilidad y Seguridad |

---

## Descripción del Problema

Una empresa chilena de retail opera un ecosistema omnicanal (tiendas físicas + e-commerce) con más de **10 millones de registros transaccionales** anuales dispersos en fuentes heterogéneas. El proyecto diseña e implementa una plataforma Big Data capaz de integrar, procesar y analizar esos datos, respondiendo preguntas críticas de la Gerencia Comercial sobre rotación de productos, quiebres de stock y comportamiento de márgenes.

**Stakeholders:** Gerencia Comercial · Operaciones · Marketing Analytics

---

## Arquitectura (Medallion sobre GCP)

```
                              ┌──────────────────────────────────────────────────┐
                              │           GCP: data-lake-retail (GCS)           
                              │                                                  
  [CSV Fuentes]──ingesta──►  [RAW]──Spark──►[BRONZE]──Spark──►[SILVER]──Spark──►[GOLD]
  venta_tiendas.csv           │   Delta Lake   Delta Lake    Delta Lake         BigQuery
  venta_ecom.csv              │   inmutable    tipificado    data marts
  Maestro_Producto.csv        │               particionado
  Maestro_Tienda.csv          │
                              └──────────────────────────────────────────────────┘
                                          ▲                        ▼
                                   Cloud Composer           Looker Studio
                                   (orquestación)           (dashboards)
                                          ▲
                                   retail_governance BQ
                                   (linaje + auditoría)
```

**Patrón de procesamiento:** Batch Incremental Diario (ventana nocturna 00:00–02:00)  
**Formato de almacenamiento:** Delta Lake (ACID, time travel, schema evolution)  
**Proveedor Cloud:** Google Cloud Platform (GCS · Dataproc · BigQuery · Cloud Composer)

> El diagrama C4 Nivel 2 completo se encuentra en [`/docs/arquitectura_c4_nivel2.svg`](docs/arquitectura_c4_nivel2.svg)

---

## Estructura del Repositorio

```
Proyecto-Integrador-Big-Data-Cloud-Computing/
│
├── notebooks/                         ← Pipeline completo en Google Colab
│   ├── EDA_y_Calidad.ipynb            ← 00: Análisis exploratorio con PySpark
│   ├── Capa_Bronze.ipynb              ← 01: Ingesta RAW → Delta Lake Bronze
│   ├── Capa_Silver.ipynb              ← 02: Limpieza y transformación Silver
│   ├── Capa_Gold.ipynb             ← 03: Data marts Gold + evidencia de cache
│   ├── Gobernabilidad.ipynb           ← 04: IAM, linaje, datos sensibles, GE
│   └── Optimizacion_y_FinOps.ipynb    ← 05: Evidencia de optimización y costos
│
├── docs/                              ← Documentación técnica Fase 3
│   ├── arquitectura_c4_nivel2.svg     ← Diagrama C4 Nivel 2
│   └── Fase3_Informe_Tecnico.docx     ← Informe técnico + ADRs
│
├── data/                              ← Datos locales (ignorados en .gitignore)
│   ├── venta_ecom.csv                 ← Transacciones e-commerce (~286K filas)
│   ├── Maestro_Producto.csv           ← Catálogo de productos (~451K filas)
│   └── Maestro_Tienda.csv             ← Maestro de tiendas (~475 registros)
│   └── (venta_tiendas.csv en GCS)     ← Dataset principal >10M filas — en bucket
│
├── src/                               ← Scripts modulares (en desarrollo)
├── requirements.txt                   ← Dependencias del proyecto
├── .gitignore
└── README.md
```

> **Nota:** El notebook de demostración del pipeline completo es [`notebooks/Capa_Gold_v2.ipynb`](notebooks/Capa_Gold_v2.ipynb), que incluye ingesta, transformación, escritura en Gold y evidencia de optimización.

---

## Instalación y Ejecución

### Pre-requisitos

| Requisito | Versión / Descripción |
|---|---|
| Google Account | Con crédito GCP free trial (USD 300) activo |
| Google Colab | Entorno de ejecución principal (gratuito) |
| GCS Bucket | `gs://data-lake-retail/` creado en el proyecto GCP |
| Dataset principal | `venta_tiendas.csv` subido a `gs://data-lake-retail/raw/` |

### Configuración inicial del bucket

```bash
# Crear el bucket (solo primera vez)
gcloud storage buckets create gs://data-lake-retail --location=us-central1

# Subir el dataset principal
gcloud storage cp venta_tiendas.csv gs://data-lake-retail/raw/
gcloud storage cp venta_ecom.csv gs://data-lake-retail/raw/
gcloud storage cp Maestro_Producto.csv gs://data-lake-retail/raw/
gcloud storage cp Maestro_Tienda.csv gs://data-lake-retail/raw/
```

### Ejecución del pipeline (orden recomendado)

Abrir cada notebook en Google Colab y ejecutar en secuencia. La primera celda de cada notebook instala las dependencias automáticamente.

```
1. notebooks/EDA_y_Calidad.ipynb        ← Exploración y perfilamiento
2. notebooks/Capa_Bronze.ipynb          ← Ingesta a Delta Lake
3. notebooks/Capa_Silver.ipynb          ← Limpieza y transformación
4. notebooks/Capa_Gold_v2.ipynb         ← Data marts + cache evidence
5. notebooks/Gobernabilidad.ipynb       ← Controles IAM y linaje
6. notebooks/Optimizacion_y_FinOps.ipynb ← Comparativas de optimización
```

> **PROJECT_ID:** Antes de ejecutar `Gobernabilidad.ipynb`, actualizar la variable `PROJECT_ID = "proyectointegradorudd"` con el ID real del proyecto GCP.

### Dependencias Python

```bash
pip install pyspark==3.4.1 delta-spark==2.4.0 google-cloud-bigquery \
            google-cloud-storage great_expectations==0.18.15 db-dtypes
```

> En Google Colab estos se instalan con `!pip install -q ...` dentro del notebook.

---

## Datos de Entrada y Salida

### Entradas (Capa RAW)

| Archivo | Columnas Clave | Descripción |
|---|---|---|
| `venta_tiendas.csv` | id_canal, numero_transaccion, fecha_transaccion, id_producto, unidades, venta, costo | Transacciones POS tiendas físicas. >10M filas |
| `venta_ecom.csv` | Mismo esquema que venta_tiendas | Transacciones canal e-commerce. ~286K filas |
| `Maestro_Producto.csv` | id_producto, marca, clase, genero, tipo, modelo, talla | Catálogo de productos |
| `Maestro_Tienda.csv` | cod_tienda_facturacion, nombre_tienda_facturacion, cadena_facturacion | Maestro de tiendas |

### Salidas (Capa Gold en BigQuery)

| Tabla | Descripción | KPIs |
|---|---|---|
| `ventas_mensuales` | Agregado por año/mes | venta_total, margen_total, boletas_distintas, margen_porcentaje |
| `ventas_por_tienda` | Agregado por tienda | venta_total, ticket_promedio, margen_porcentaje |
| `ventas_por_producto` | Ranking de productos | venta_total, unidades_total, margen_total |
| `ventas_canal_documento` | Por canal y tipo de documento | Comparativa POS vs e-commerce |

### Governance (BigQuery — retail_governance)

| Tabla | Descripción |
|---|---|
| `data_lineage` | Linaje campo a campo: RAW → Bronze → Silver → Gold |
| `pipeline_audit_log` | Log de ejecuciones con timestamps y métricas |
| `data_quality_results` | Resultados de validaciones Great Expectations |

---

## Manual de Operación

### Ejecución normal (pipeline nocturno)

En producción, el DAG de Airflow (`dags/pipeline_retail_diario.py`) ejecuta los notebooks en secuencia a las 00:30 de cada día. Para ejecución manual:

```bash
# Triggear el DAG desde Cloud Composer (si está desplegado)
gcloud composer environments run retail-composer \
  --location us-central1 dags trigger -- pipeline_retail_diario

# O ejecutar los notebooks en secuencia desde Colab (ver sección anterior)
```

### Verificación post-ejecución

```python
# En cualquier notebook Colab, verificar conteos en Gold:
from google.cloud import bigquery
bq = bigquery.Client(project="proyectointegradorudd")
result = bq.query("SELECT COUNT(*) as total FROM retail_gold.ventas_mensuales").to_dataframe()
print(result)
```

### Rollback con Delta Lake Time Travel

```python
# Leer versión anterior de Silver (ej: versión 3)
df_historico = spark.read.format("delta") \
    .option("versionAsOf", 3) \
    .load("gs://data-lake-retail/silver/venta_tiendas_delta")
```

### Alertas de fallo

El DAG de Airflow está configurado para enviar email al `svc-dataeng` en caso de fallo de cualquier tarea. El log de auditoría en `retail_governance.pipeline_audit_log` registra cada ejecución con su estado y métricas.

---

## Evidencia de Optimización

Ver [`notebooks/Optimizacion_y_FinOps.ipynb`](notebooks/Optimizacion_y_FinOps.ipynb) para la evidencia completa. Resumen:

| Optimización | Métrica Antes | Métrica Después | Mejora |
|---|---|---|---|
| Caching Silver (Gold job #1) | 45.2 s | 4.8 s | −89.4% |
| Caching Silver (Gold job filtro tienda) | 38.7 s | 3.1 s | −92.0% |
| Partition pruning (1 mes de 24) | ~38.7 s | ~3.1 s | ~96% menos archivos leídos |
| Shuffle partitions (200 → 8) | overhead 200 tasks | 8 tasks | −96% task overhead |

---

## Declaración de Uso de IA

Este proyecto utilizó Claude (Anthropic) para: (1) redacción y estructuración del README y del informe técnico, (2) revisión de comentarios en notebooks. Las decisiones arquitectónicas, el diseño del pipeline, el código PySpark y todos los resultados de ejecución son propios del equipo. Los integrantes del equipo son responsables de todo el contenido entregado.

---

## Licencia Académica

Uso exclusivo para evaluación académica en el contexto del Magíster en Data Science, Universidad del Desarrollo, 2026.
