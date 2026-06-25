"""
spark_session.py
================
Fábrica de SparkSession con soporte para Delta Lake y GCS.
Centraliza la inicialización de Spark para que todos los notebooks
y scripts del pipeline compartan la misma configuración base.

Uso típico:
    from src.spark_session import get_spark
    spark = get_spark(app_name="MiJob")

Equipo: Ballerini · Torres · Vargas · Vásquez
Curso:  Big Data y Cloud Computing — MDS UDD 2026
"""

import os
import pyspark
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from src.config import SparkConfig


def _descargar_conector_gcs() -> None:
    """
    Descarga el conector GCS al directorio de JARs de PySpark si no existe.
    Necesario en entornos Google Colab; en Dataproc el conector viene preinstalado.
    """
    JAR_URL  = "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.14.jar"
    jars_dir = os.path.join(pyspark.__path__[0], "jars")
    jar_path = os.path.join(jars_dir, "gcs-connector-hadoop3-2.2.14.jar")

    if not os.path.exists(jar_path):
        print("→ Descargando conector GCS...")
        os.system(f"wget -q {JAR_URL} -P {jars_dir}")
        print("✓ Conector GCS instalado.")
    else:
        print("✓ Conector GCS ya presente.")


def get_spark(
    app_name: str = "RetailBigData",
    cfg: SparkConfig = None,
    entorno: str = "colab"  # "colab" | "dataproc" | "local"
) -> SparkSession:
    """
    Crea y retorna una SparkSession configurada para Delta Lake y GCS.

    Args:
        app_name:  Nombre de la aplicación (aparece en Spark UI).
        cfg:       Objeto SparkConfig con parámetros de tuning.
                   Si es None, usa los valores por defecto.
        entorno:   'colab'    → descarga conector GCS, autentica con usuario.
                   'dataproc' → conector GCS preinstalado, usa SA del clúster.
                   'local'    → sin GCS, ideal para pruebas unitarias locales.

    Returns:
        SparkSession lista para usar.
    """
    if cfg is None:
        cfg = SparkConfig(app_name=app_name)

    # ── Paso 1: Preparar conector GCS (solo Colab) ─────────────────────────
    if entorno == "colab":
        # Desinstalar paquetes conflictivos que Colab trae por defecto
        os.system(
            "pip uninstall -y dataproc-spark-connect opentelemetry-api "
            "importlib-metadata pyspark delta-spark > /dev/null 2>&1"
        )
        os.system(
            "pip install -q importlib-metadata==8.0.0 pyspark==3.4.1 delta-spark==2.4.0"
        )
        _descargar_conector_gcs()

    # ── Paso 2: Construir SparkSession ──────────────────────────────────────
    builder = (
        SparkSession.builder
        .appName(cfg.app_name)
        .config("spark.sql.extensions",      cfg.extensions)
        .config("spark.sql.catalog.spark_catalog", cfg.catalog)
        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
    )

    # Configuración GCS (omitida en modo local)
    if entorno in ("colab", "dataproc"):
        builder = (
            builder
            .config("spark.hadoop.fs.gs.impl",            cfg.gcs_impl)
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", cfg.gcs_abs)
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        )

    # Tuning de recursos (solo en Dataproc; Colab tiene recursos fijos)
    if entorno == "dataproc":
        builder = (
            builder
            .config("spark.executor.memory", cfg.executor_memory)
            .config("spark.executor.cores",  str(cfg.executor_cores))
            .config("spark.driver.memory",   cfg.driver_memory)
        )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(cfg.log_level)

    print(f"✓ SparkSession iniciada | App: {cfg.app_name} | Spark {spark.version}")
    return spark


def stop_spark(spark: SparkSession) -> None:
    """Detiene la SparkSession de forma limpia."""
    spark.stop()
    print("✓ SparkSession detenida.")
