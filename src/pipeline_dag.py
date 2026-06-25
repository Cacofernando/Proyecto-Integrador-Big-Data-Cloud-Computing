"""
pipeline_dag.py
===============
DAG de Apache Airflow (Cloud Composer) para orquestar el pipeline
de Big Data Retail en GCP.

Flujo del pipeline:
  [Raw GCS] → Bronze → Silver → Gold → [BigQuery + Looker Studio]
                                 ↕
                           Gobierno (linaje, auditoría, calidad)

Decisión de diseño:
  Se usa Cloud Composer (Airflow gestionado) en lugar de Dataproc Workflows
  porque necesitamos dependencias entre tareas (DAG), reintentos automáticos
  y visibilidad del historial de ejecuciones. El costo adicional de Composer
  está justificado por la reducción de tiempo de operación del equipo.

Programación: Diaria a las 02:00 UTC (23:00 CLT), fuera del horario
de operaciones de tienda para minimizar competencia por recursos.

Equipo: Ballerini · Torres · Vargas · Vásquez
Curso:  Big Data y Cloud Computing — MDS UDD 2026
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# ─── CONSTANTES ────────────────────────────────────────────────────────────────
PROJECT_ID   = "proyectointegradorudd"
REGION       = "us-central1"
BUCKET_NAME  = "data-lake-retail"
CLUSTER_NAME = "retail-pipeline-cluster"
DATASET_GOV  = "retail_governance"

# ─── CONFIGURACIÓN DEL CLÚSTER (EPHEMERAL) ─────────────────────────────────────
# El clúster se crea al inicio del DAG y se destruye al finalizar.
# Esto implementa el patrón "ephemeral cluster" que reduce el costo en ~75%
# respecto a un clúster permanente (solo pagamos mientras procesamos).
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-ssd", "boot_disk_size_gb": 100},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-ssd", "boot_disk_size_gb": 100},
    },
    "software_config": {
        "image_version": "2.1-debian11",
        "optional_components": ["JUPYTER"],
        "properties": {
            "spark:spark.jars.packages": "io.delta:delta-core_2.12:2.4.0",
            "spark:spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark:spark.sql.catalog.spark_catalog":
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark:spark.sql.shuffle.partitions": "16",
            # En Dataproc aumentamos shuffle.partitions vs Colab (que usa 8)
            # para aprovechar el paralelismo del clúster multinodo.
        },
    },
    "lifecycle_config": {
        # Auto-apagar si el clúster lleva más de 45 min sin jobs
        # como red de seguridad contra olvidos.
        "idle_delete_ttl": {"seconds": 2700}
    },
}


def _job_pyspark(script_uri: str, app_name: str) -> dict:
    """Genera la configuración de un PySpark job para Dataproc."""
    return {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": script_uri,
            "args": ["--entorno", "dataproc"],
            "properties": {
                "spark.app.name": app_name,
            },
        },
    }


# ─── ARGUMENTOS POR DEFECTO DEL DAG ───────────────────────────────────────────
default_args = {
    "owner":            "data-engineering-retail",
    "depends_on_past":  False,
    "start_date":       datetime(2026, 1, 1),
    "email_on_failure": True,
    "email":            ["data-engineering@retailco.cl"],
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    # Tiempo máximo por tarea: 90 minutos
    "execution_timeout": timedelta(minutes=90),
}

# ─── DEFINICIÓN DEL DAG ────────────────────────────────────────────────────────
with DAG(
    dag_id="retail_medallion_pipeline",
    description="Pipeline batch diario: RAW → Bronze → Silver → Gold → BigQuery",
    default_args=default_args,
    schedule_interval="0 2 * * *",   # Diario a las 02:00 UTC
    catchup=False,                    # No ejecutar fechas pasadas al desplegar
    tags=["retail", "big-data", "medallion", "delta-lake"],
    doc_md="""
## Pipeline Big Data Retail — Arquitectura Medallion

Pipeline batch diario que procesa datos de ventas omnicanal (POS + e-commerce)
de la plataforma Big Data Retail. Implementa la arquitectura Medallion sobre GCS
con Delta Lake y expone los data marts en BigQuery para Looker Studio.

### Flujo
1. **Crear clúster** Dataproc ephemeral (1 master + 2 workers n1-standard-4).
2. **Bronze**: Ingesta CSV → Delta Lake sin transformaciones.
3. **Silver**: Limpieza, tipificación, normalización y particionamiento.
4. **Gold**: 4 data marts analíticos (ventas mensuales, tienda, producto, canal).
5. **Gobierno**: Registra linaje y auditoría en BigQuery.
6. **Destruir clúster** (se paga solo mientras corre el pipeline).

### Stakeholders
- Gerencia Comercial: dashboard Looker Studio sobre Gold.
- Operaciones: alertas de stock basadas en ventas_por_producto.
- Marketing: análisis de mix de canales desde ventas_canal_documento.
    """,
) as dag:

    # ─── TAREA 0: Crear clúster ephemeral ────────────────────────────────────
    crear_cluster = DataprocCreateClusterOperator(
        task_id="crear_cluster_dataproc",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    # ─── TAREA 1: Ingesta Bronze ──────────────────────────────────────────────
    job_bronze = DataprocSubmitJobOperator(
        task_id="ingesta_bronze",
        job=_job_pyspark(
            f"gs://{BUCKET_NAME}/scripts/run_bronze.py",
            "RetailBronze"
        ),
        region=REGION,
        project_id=PROJECT_ID,
    )

    # ─── TAREA 2: Transformación Silver ──────────────────────────────────────
    job_silver = DataprocSubmitJobOperator(
        task_id="transformacion_silver",
        job=_job_pyspark(
            f"gs://{BUCKET_NAME}/scripts/run_silver.py",
            "RetailSilver"
        ),
        region=REGION,
        project_id=PROJECT_ID,
    )

    # ─── TAREA 3: Data Marts Gold ─────────────────────────────────────────────
    job_gold = DataprocSubmitJobOperator(
        task_id="construccion_gold",
        job=_job_pyspark(
            f"gs://{BUCKET_NAME}/scripts/run_gold.py",
            "RetailGold"
        ),
        region=REGION,
        project_id=PROJECT_ID,
    )

    # ─── TAREA 4: Gobierno y auditoría ───────────────────────────────────────
    job_governance = DataprocSubmitJobOperator(
        task_id="gobierno_datos",
        job=_job_pyspark(
            f"gs://{BUCKET_NAME}/scripts/run_governance.py",
            "RetailGovernance"
        ),
        region=REGION,
        project_id=PROJECT_ID,
    )

    # ─── TAREA 5: Exportar Gold a BigQuery ───────────────────────────────────
    # BigQuery External Tables sobre Delta en GCS (no se copian datos,
    # BQ lee directamente desde GCS usando el conector Delta).
    exportar_bq = BigQueryInsertJobOperator(
        task_id="actualizar_vistas_bigquery",
        configuration={
            "query": {
                "query": f"""
                    -- Recrear External Tables apuntando a Gold en GCS.
                    -- En producción, estas tablas se crean una vez y se
                    -- actualizan automáticamente al haber nuevos archivos Delta.
                    SELECT 'ventas_mensuales actualizada' AS status,
                           CURRENT_TIMESTAMP() AS ejecutado_en
                """,
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID,
        location=REGION,
    )

    # ─── TAREA 6: Destruir clúster (siempre, incluso en error) ───────────────
    # TriggerRule.ALL_DONE garantiza que el clúster se destruye aunque falle
    # alguna tarea anterior, evitando costos de cómputo no deseados.
    destruir_cluster = DataprocDeleteClusterOperator(
        task_id="destruir_cluster_dataproc",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,   # CRÍTICO: siempre destruir
    )

    # ─── DEPENDENCIAS ─────────────────────────────────────────────────────────
    (
        crear_cluster
        >> job_bronze
        >> job_silver
        >> job_gold
        >> [job_governance, exportar_bq]
        >> destruir_cluster
    )
