# Plataforma Big Data para Analítica Omnicanal de Ventas Retail

> Fase 3 — Documentación Técnica  
> Big Data y Cloud Computing · Proyecto Integrador · MDS UDD 2026

---

## Equipo

| Integrante           | Rol                              |
|----------------------|----------------------------------|
| Claudio Ballerini    | Arquitectura y Pipeline Bronze   |
| Juan José Torres     | Pipeline Silver y Optimización   |
| Cristian Vargas      | Pipeline Gold y Data Marts       |
| Christian Vásquez    | Gobernabilidad y Seguridad       |
| **Prof. Luis Castillo Faune** | Docente evaluador       |

**Entrega Fase 3:** 26 de junio de 2026

---

## Descripción del Problema

Una empresa chilena de retail opera un ecosistema omnicanal con tiendas físicas
y e-commerce. Con más de **10 millones de registros transaccionales**, su
información comercial estaba dispersa sin integración, impidiendo responder
preguntas críticas de rotación, quiebres de stock y comportamiento de canal.

La solución implementa una **plataforma Big Data Medallion** sobre Google Cloud
Platform que procesa, limpia y expone los datos como data marts analíticos en
BigQuery + Looker Studio.

---

## Arquitectura

Patrón **Medallion** (Bronze → Silver → Gold) sobre GCP con procesamiento Batch
Incremental Diario en clúster efímero Dataproc. Ver diagrama completo en
`/docs/arquitectura_c4_nivel2.svg`.

```
[Sistemas fuente]      [GCS — Data Lake]                   [Consumo]
  CSV / ERP  ───►  RAW ──► Bronze ──► Silver ──► Gold ──► BigQuery ──► Looker
                         (Delta)    (Delta,      (Delta)   External    Studio
                                    anio/mes)    4 marts   Tables
                         ↕ Gobierno: retail_governance (BigQuery)
                         ↕ Orquestación: Cloud Composer / Airflow (diseñado)
```

### Stack tecnológico

| Componente         | Tecnología                     |
|--------------------|-------------------------------|
| Ingesta            | Python + PySpark               |
| Procesamiento      | Apache Spark 3.4.1 + Dataproc  |
| Formato de tabla   | Delta Lake 2.4.0               |
| Data Lake          | Google Cloud Storage           |
| Data Warehouse     | BigQuery (External Tables)     |
| Orquestación       | Airflow (diseñado; ver §6.1)   |
| Visualización      | Looker Studio                  |
| Gobierno           | BigQuery (`retail_governance`) |
| Seguridad          | GCP IAM + SHA-256              |

---

## Estructura del Repositorio

```
proyecto-retail-bigdata/
├── data/
│   ├── venta_ecom.csv           ← ~285.992 registros e-commerce
│   ├── Maestro_Producto.csv     ← ~451.000 productos
│   ├── Maestro_Tienda.csv       ← ~475 tiendas
│   ├── raw/ interim/ processed/ ← placeholders locales
│
├── notebooks/
│   ├── EDA_y_Calidad.ipynb      ← Exploración inicial con PySpark
│   ├── Capa_Bronze.ipynb        ← Ingesta CSV → Delta Lake
│   ├── Capa_Silver.ipynb        ← Limpieza + particionamiento
│   ├── Capa_Gold.ipynb       ← 4 data marts comerciales
│   └── Gobernabilidad.ipynb     ← IAM, linaje, calidad, seguridad
│
├── src/                         ← Módulos Python productizados (Fase 3)
│   ├── config.py                ← Rutas GCS, parámetros Spark, FinOps
│   ├── spark_session.py         ← Fábrica SparkSession (Colab/Dataproc)
│   ├── bronze.py                ← Ingesta Bronze modular
│   ├── silver.py                ← Transformaciones Silver
│   ├── gold.py                  ← Data marts + benchmark cache + FinOps
│   ├── governance.py            ← Linaje, auditoría, pseudonimización
│   └── pipeline_dag.py          ← DAG Airflow (diseñado, no desplegado)
│
├── docs/
│   ├── arquitectura_c4_nivel2.svg  ← Diagrama C4 Nivel 2
|.  ├── Informe_Tecnico_Fase3 ← Informe técnico de la Fase 3
│   └── adrs/
│       ├── ADR-001-delta-lake-formato-almacenamiento.md
│       ├── ADR-002-batch-vs-streaming.md
│       └── ADR-003-particionamiento-silver.md
│
├── requirements.txt
└── README.md
```

---

## Optimizaciones Implementadas

Documentadas en la **Sección 3** del Informe Técnico con métricas empíricas:

| Optimización | Métrica antes | Métrica después | Mejora |
|---|---|---|---|
| Cache Silver en Gold (ventas año/mes) | 45.2 s | 4.8 s | −89.4% |
| Cache Silver en Gold (ventas/tienda) | 38.7 s | 3.1 s | −92.0% |
| Particionamiento Silver por (anio,mes) | 100% datos leídos | ~4% datos leídos | ~96% menos scan |
| Shuffle partitions | 200 (default) | 8 (Colab 4 vCPU) | Sin overhead microtasks |

---

## Análisis de Costos (FinOps)

Principio rector: **clúster efímero** — Dataproc se crea al inicio del pipeline
y se destruye al terminar (0.25 h/día × 30 días = 7.5 h/mes).

| Componente | USD/mes |
|---|---|
| Dataproc n2-standard-4 efímero | 9.00 |
| Google Cloud Storage ~50 GB | 1.00 |
| BigQuery on-demand ~100 GB/mes | 0.50 |
| Cloud Composer (Airflow) | 31.00 |
| **Total con Cloud Composer** | **~41.50** |
| **Total con Cloud Workflows** | **~10.51** ← recomendado en etapa temprana |

---

## Limitaciones Conocidas (Sección 6.1 del Informe)

1. **E-commerce no integrado:** `venta_ecom.csv` (~286K filas) no está en el
   pipeline principal. Requiere resolver diferencia de esquema con
   `venta_tiendas.csv`.
2. **Airflow no desplegado:** el DAG fue diseñado pero no ejecutado en Cloud
   Composer productivo. La ejecución fue manual y secuencial en Colab.
3. **Gold sin enriquecimiento:** los data marts muestran códigos numéricos
   (cod_tienda, id_producto) en lugar de nombres. Pendiente JOIN con maestros.
4. **Spark UI manual:** las métricas de optimización se midieron con
   `time.time()`. El Spark UI Web requiere port-forwarding en Colab.

---

## Requisitos Previos

```bash
# Python 3.9+
pip install pyspark==3.4.1 delta-spark==2.4.0 importlib-metadata==8.0.0 \
            google-cloud-bigquery google-cloud-storage \
            great_expectations==0.18.15 db-dtypes apache-airflow
```

### APIs GCP necesarias

```bash
gcloud services enable storage.googleapis.com dataproc.googleapis.com \
  bigquery.googleapis.com composer.googleapis.com
```

---

## Instrucciones de Ejecución

### Opción 1 — Google Colab (demostración)

1. Crear bucket y subir datos RAW:
   ```bash
   gsutil mb -l us-central1 gs://data-lake-retail
   gsutil cp data/*.csv gs://data-lake-retail/raw/
   ```
2. Ejecutar notebooks **en orden**:
   `EDA_y_Calidad` → `Capa_Bronze` → `Capa_Silver` → `Capa_Gold_v2` → `Gobernabilidad`
3. Ajustar `PROJECT_ID` en la celda de configuración de cada notebook.

### Opción 2 — Dataproc (producción)

```bash
# Crear clúster
gcloud dataproc clusters create retail-pipeline-cluster \
  --region=us-central1 --master-machine-type=n2-standard-4 \
  --num-workers=2 --worker-machine-type=n2-standard-4 \
  --image-version=2.1-debian11 \
  --properties="spark:spark.jars.packages=io.delta:delta-core_2.12:2.4.0"

# Subir scripts
gsutil cp -r src/ gs://data-lake-retail/scripts/

# Ejecutar etapas (en orden)
gcloud dataproc jobs submit pyspark gs://data-lake-retail/scripts/run_bronze.py \
  --cluster=retail-pipeline-cluster --region=us-central1
# ... repetir para silver, gold, governance

# Destruir clúster al terminar (¡siempre!)
gcloud dataproc clusters delete retail-pipeline-cluster --region=us-central1
```

---

## Manual de Operación

### Verificar estado del Data Lake

```bash
gsutil ls gs://data-lake-retail/bronze/
gsutil ls gs://data-lake-retail/silver/
gsutil ls gs://data-lake-retail/gold/
gsutil du -sh gs://data-lake-retail/   # tamaño total
```

### Consultar data marts desde BigQuery

```sql
-- Ventas mensuales del último año disponible
SELECT anio, mes, venta_total, margen_porcentaje
FROM `proyectointegradorudd.retail_gold.ventas_mensuales`
ORDER BY anio DESC, mes DESC LIMIT 12;

-- Top 10 tiendas por venta
SELECT cod_tienda_facturacion, venta_total, ticket_promedio
FROM `proyectointegradorudd.retail_gold.ventas_por_tienda`
LIMIT 10;

-- Mix de canales
SELECT id_canal, tipo_documento, venta_total
FROM `proyectointegradorudd.retail_gold.ventas_canal_documento`
ORDER BY venta_total DESC;
```

### Revisar auditoría del pipeline

```sql
SELECT etapa, estado, registros_entrada, registros_salida, duracion_segundos
FROM `proyectointegradorudd.retail_governance.pipeline_audit_log`
ORDER BY timestamp_inicio DESC LIMIT 10;
```

### Revertir con Time Travel (Delta Lake)

```python
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "gs://data-lake-retail/silver/venta_tiendas_delta")
dt.history().show()          # ver versiones disponibles
dt.restoreToVersion(0)       # restaurar a versión anterior

# Ver historial de versiones disponibles
dt.history().show()
```

### Mantenimiento periódico (mensual recomendado)

Las tablas Delta Lake acumulan archivos pequeños y versiones antiguas con cada ejecución del pipeline. Ejecutar mensualmente para mantener el rendimiento de lectura:

```python
from delta.tables import DeltaTable

# 1. OPTIMIZE — compactar archivos pequeños en cada capa
for ruta in [
    "gs://data-lake-retail/bronze/venta_tiendas_delta",
    "gs://data-lake-retail/silver/venta_tiendas_delta",
    "gs://data-lake-retail/gold/ventas_mensuales",
]:
    dt = DeltaTable.forPath(spark, ruta)
    dt.optimize().executeCompaction()
    print(f"✓ OPTIMIZE completado: {ruta.split('/')[-1]}")

# 2. VACUUM — eliminar versiones antiguas (retener últimos 7 días = 168 horas)
# ⚠ No reducir por debajo de 168h sin deshabilitar la protección primero
for ruta in [
    "gs://data-lake-retail/bronze/venta_tiendas_delta",
    "gs://data-lake-retail/silver/venta_tiendas_delta",
]:
    dt = DeltaTable.forPath(spark, ruta)
    dt.vacuum(168)  # 168 horas = 7 días
    print(f"✓ VACUUM completado: {ruta.split('/')[-1]}")
```

> **Nota operacional:** OPTIMIZE es seguro de ejecutar en cualquier momento. VACUUM elimina permanentemente las versiones antiguas — después de ejecutarlo, el Time Travel solo estará disponible hasta la versión más antigua que quede dentro de la ventana de retención (7 días por defecto).

---

## Datos de Entrada y Salida

### Inputs — Capa RAW

| Archivo | Registros | Descripción |
|---|---|---|
| `venta_tiendas.csv` | >10.000.000 | Ventas POS (integrado en pipeline) |
| `venta_ecom.csv` | ~285.992 | Ventas e-commerce (**pendiente integración**) |
| `Maestro_Producto.csv` | ~451.000 | Catálogo SKU (**pendiente JOIN Gold**) |
| `Maestro_Tienda.csv` | ~475 | Catálogo tiendas (**pendiente JOIN Gold**) |

### Outputs — Capa Gold (data marts)

| Data Mart | Descripción |
|---|---|
| `ventas_mensuales` | KPIs por año-mes: venta, margen, boletas, productos |
| `ventas_por_tienda` | KPIs por tienda: venta, ticket promedio, margen |
| `ventas_por_producto` | KPIs por SKU: venta, margen, unidades |
| `ventas_canal_documento` | KPIs por canal × tipo de documento |
| `evidencia_optimizacion` | Benchmark cache: tiempos sin/con cache |

---

## Declaración de Uso de IA

Este informe y los módulos `src/` fueron desarrollados con asistencia de Claude
(Anthropic) y Google Gemini para estructuración y edición. Las decisiones
arquitectónicas, el análisis de trade-offs y todos los resultados de
implementación son propios del equipo. Los integrantes son capaces de explicar
y defender cada elemento documentado ante el panel evaluador.
