"""
config.py
=========
Configuración central del pipeline de Big Data Retail.
Centraliza todas las constantes, rutas y parámetros de Spark
para que cualquier módulo del pipeline importe desde aquí.

Equipo: Ballerini · Torres · Vargas · Vásquez
Curso:  Big Data y Cloud Computing — MDS UDD 2026
"""

import os
from dataclasses import dataclass, field
from typing import Optional


# ─── PARÁMETROS GCP ────────────────────────────────────────────────────────────
PROJECT_ID   = os.getenv("GCP_PROJECT_ID",   "proyectointegradorudd")
REGION       = os.getenv("GCP_REGION",       "us-central1")
BUCKET_NAME  = os.getenv("GCP_BUCKET_NAME",  "data-lake-retail")
DATASET_GOLD = os.getenv("BQ_DATASET_GOLD",  "retail_gold")
DATASET_GOV  = os.getenv("BQ_DATASET_GOV",   "retail_governance")


# ─── RUTAS DEL DATA LAKE (GCS) ─────────────────────────────────────────────────
RUTA_BASE      = f"gs://{BUCKET_NAME}"
RUTA_RAW       = f"{RUTA_BASE}/raw"
RUTA_BRONZE    = f"{RUTA_BASE}/bronze"
RUTA_SILVER    = f"{RUTA_BASE}/silver"
RUTA_GOLD      = f"{RUTA_BASE}/gold"
RUTA_EVIDENCIAS = f"{RUTA_BASE}/evidencias"

# Tablas específicas
TABLA_BRONZE_VENTAS  = f"{RUTA_BRONZE}/venta_tiendas_delta"
TABLA_BRONZE_ECOM    = f"{RUTA_BRONZE}/venta_ecom_delta"
TABLA_SILVER_VENTAS  = f"{RUTA_SILVER}/venta_tiendas_delta"
TABLA_GOLD_MENSUAL   = f"{RUTA_GOLD}/ventas_mensuales"
TABLA_GOLD_TIENDA    = f"{RUTA_GOLD}/ventas_por_tienda"
TABLA_GOLD_PRODUCTO  = f"{RUTA_GOLD}/ventas_por_producto"
TABLA_GOLD_CANAL     = f"{RUTA_GOLD}/ventas_canal_documento"
TABLA_GOLD_OPT       = f"{RUTA_GOLD}/evidencia_optimizacion"

# Archivos RAW
ARCHIVO_VENTAS_POS   = f"{RUTA_RAW}/venta_tiendas.csv"
ARCHIVO_VENTAS_ECOM  = f"{RUTA_RAW}/venta_ecom.csv"
ARCHIVO_MAESTRO_PROD = f"{RUTA_RAW}/Maestro_Producto.csv"
ARCHIVO_MAESTRO_TIENDA = f"{RUTA_RAW}/Maestro_Tienda.csv"

# Tablas BigQuery de gobierno
TABLA_LINAJE    = f"{PROJECT_ID}.{DATASET_GOV}.data_lineage"
TABLA_AUDITORIA = f"{PROJECT_ID}.{DATASET_GOV}.pipeline_audit_log"
TABLA_CALIDAD   = f"{PROJECT_ID}.{DATASET_GOV}.data_quality_results"


# ─── CONFIGURACIÓN SPARK ───────────────────────────────────────────────────────
@dataclass
class SparkConfig:
    """Parámetros de configuración de Spark para cada etapa del pipeline."""
    app_name: str = "RetailBigData"
    shuffle_partitions: int = 8
    # Para Dataproc en producción, incrementar según cluster
    executor_memory: str = "4g"
    executor_cores: int = 2
    driver_memory: str = "2g"
    log_level: str = "ERROR"

    # Extensiones Delta Lake
    extensions: str = "io.delta.sql.DeltaSparkSessionExtension"
    catalog: str = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

    # GCS
    gcs_impl: str = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    gcs_abs:  str = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"


# ─── PARÁMETROS DE NEGOCIO ─────────────────────────────────────────────────────
# Canales válidos: 1=POS/Tienda, 2=E-commerce, 3=Mayorista
CANALES_VALIDOS = [1, 2, 3]

# Rango temporal válido del dataset histórico
ANIO_MIN = 2016
ANIO_MAX = 2026

# Umbral de calidad: porcentaje máximo aceptable de registros descartados en Silver
UMBRAL_DESCARTE_PCT = 0.01  # 1%

# Salt para pseudonimización (en producción: cargar desde Secret Manager)
SALT_HASH = "retail_udd_2026"


# ─── PARÁMETROS FINOPS ─────────────────────────────────────────────────────────
COSTO_HORA_CLUSTER_USD  = 1.20   # Dataproc n1-standard-4 × 3 nodos (referencial)
DURACION_JOB_HORAS      = 0.25   # ~15 minutos por ejecución batch
EJECUCIONES_MES         = 30     # Una por día calendario
COSTO_GCS_GB_MES        = 0.020  # USD/GB/mes almacenamiento Standard


def calcular_costo_mensual(
    costo_hora: float = COSTO_HORA_CLUSTER_USD,
    duracion_h: float = DURACION_JOB_HORAS,
    ejecuciones: int  = EJECUCIONES_MES,
    gb_almacenados: float = 50.0
) -> dict:
    """
    Calcula el costo operacional mensual estimado del pipeline.

    Args:
        costo_hora:    Costo por hora del clúster Dataproc (USD).
        duracion_h:    Duración estimada de cada job (horas).
        ejecuciones:   Número de ejecuciones mensuales.
        gb_almacenados: Volumen total en GCS (GB).

    Returns:
        dict con desglose de costos.
    """
    costo_compute = costo_hora * duracion_h * ejecuciones
    costo_storage = gb_almacenados * COSTO_GCS_GB_MES
    total         = costo_compute + costo_storage

    return {
        "costo_compute_usd":  round(costo_compute, 2),
        "costo_storage_usd":  round(costo_storage, 2),
        "costo_total_usd":    round(total, 2),
        "gb_almacenados":     gb_almacenados,
        "ejecuciones_mes":    ejecuciones,
    }
