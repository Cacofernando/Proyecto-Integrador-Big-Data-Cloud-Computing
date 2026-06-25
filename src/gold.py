"""
gold.py
=======
Módulo de data marts — Capa Gold de la arquitectura Medallion.

Responsabilidades:
  - Leer desde Silver (datos limpios y tipificados).
  - Construir 4 data marts analíticos con KPIs comerciales.
  - Aplicar y evidenciar optimizaciones (cache, partición por predicado).
  - Estimar costo operacional (FinOps).
  - Escribir en Delta Lake para consumo desde BigQuery + Looker Studio.

Data marts generados:
  1. ventas_mensuales        — Evolución temporal de ventas/margen/boletas.
  2. ventas_por_tienda       — Ranking de tiendas por venta y ticket promedio.
  3. ventas_por_producto     — Productos con mayor rotación y margen.
  4. ventas_canal_documento  — Mix de canales POS vs e-commerce.

Equipo: Ballerini · Torres · Vargas · Vásquez
Curso:  Big Data y Cloud Computing — MDS UDD 2026
"""

import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, countDistinct, round as spark_round
)

from src import config as cfg


# ─── HELPER INTERNO ────────────────────────────────────────────────────────────
def _escribir_gold(df: DataFrame, nombre: str, ruta: str) -> dict:
    """Escribe un data mart en Gold y retorna métricas."""
    ruta_completa = f"{ruta}/{nombre}"
    inicio = time.time()
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(ruta_completa)
    )
    elapsed = round(time.time() - inicio, 2)
    count   = df.count()
    print(f"✓ Gold/{nombre}: {count:,} registros escritos en {elapsed}s")
    return {"nombre": nombre, "registros": count, "tiempo_seg": elapsed}


# ─── DATA MART 1: VENTAS MENSUALES ─────────────────────────────────────────────
def mart_ventas_mensuales(df_silver: DataFrame) -> DataFrame:
    """
    Agrega ventas por año-mes. Principal vista para análisis de estacionalidad
    y seguimiento de presupuesto mensual por la Gerencia Comercial.

    KPIs incluidos:
      - venta_total, costo_total, margen_total, unidades_total.
      - boletas_distintas: proxy del número de clientes atendidos.
      - productos_distintos: amplitud de surtido vendida.
      - margen_porcentaje: rentabilidad del período.
    """
    return (
        df_silver
        .groupBy("anio", "mes")
        .agg(
            spark_sum("venta").alias("venta_total"),
            spark_sum("costo").alias("costo_total"),
            spark_sum("margen").alias("margen_total"),
            spark_sum("unidades").alias("unidades_total"),
            countDistinct("numero_boleta").alias("boletas_distintas"),
            countDistinct("id_producto").alias("productos_distintos"),
        )
        .withColumn(
            "margen_porcentaje",
            spark_round(col("margen_total") / col("venta_total"), 4)
        )
        .orderBy("anio", "mes")
    )


# ─── DATA MART 2: VENTAS POR TIENDA ────────────────────────────────────────────
def mart_ventas_por_tienda(df_silver: DataFrame) -> DataFrame:
    """
    Agrega ventas por tienda física.
    Permite identificar las tiendas de mayor y menor rendimiento,
    y calcular el ticket promedio por punto de venta.

    KPIs incluidos:
      - ticket_promedio: venta_total / boletas_distintas.
        Indica el nivel de compra por visita al local.
    """
    return (
        df_silver
        .groupBy("cod_tienda_facturacion")
        .agg(
            spark_sum("venta").alias("venta_total"),
            spark_sum("costo").alias("costo_total"),
            spark_sum("margen").alias("margen_total"),
            spark_sum("unidades").alias("unidades_total"),
            countDistinct("numero_boleta").alias("boletas_distintas"),
            countDistinct("id_producto").alias("productos_distintos"),
        )
        .withColumn(
            "ticket_promedio",
            spark_round(col("venta_total") / col("boletas_distintas"), 2)
        )
        .withColumn(
            "margen_porcentaje",
            spark_round(col("margen_total") / col("venta_total"), 4)
        )
        .orderBy(col("venta_total").desc())
    )


# ─── DATA MART 3: VENTAS POR PRODUCTO ──────────────────────────────────────────
def mart_ventas_por_producto(df_silver: DataFrame) -> DataFrame:
    """
    Agrega ventas por SKU (id_producto).
    Permite identificar los productos de mayor rotación y los de mayor margen.
    Insumo para decisiones de surtido, compra y planificación de inventario.
    """
    return (
        df_silver
        .groupBy("id_producto")
        .agg(
            spark_sum("venta").alias("venta_total"),
            spark_sum("costo").alias("costo_total"),
            spark_sum("margen").alias("margen_total"),
            spark_sum("unidades").alias("unidades_total"),
            countDistinct("numero_boleta").alias("boletas_distintas"),
        )
        .withColumn(
            "margen_porcentaje",
            spark_round(col("margen_total") / col("venta_total"), 4)
        )
        .orderBy(col("venta_total").desc())
    )


# ─── DATA MART 4: VENTAS POR CANAL Y TIPO DOCUMENTO ───────────────────────────
def mart_ventas_canal_documento(df_silver: DataFrame) -> DataFrame:
    """
    Agrega ventas por canal (POS/ecom) y tipo de documento (boleta/factura/NC).
    Permite analizar el mix de canales y el peso de cada tipo de comprobante.
    Insumo clave para la estrategia omnicanal.
    """
    return (
        df_silver
        .groupBy("id_canal", "tipo_documento")
        .agg(
            spark_sum("venta").alias("venta_total"),
            spark_sum("costo").alias("costo_total"),
            spark_sum("margen").alias("margen_total"),
            spark_sum("unidades").alias("unidades_total"),
            countDistinct("numero_boleta").alias("boletas_distintas"),
        )
        .withColumn(
            "margen_porcentaje",
            spark_round(col("margen_total") / col("venta_total"), 4)
        )
        .orderBy("id_canal", "tipo_documento")
    )


# ─── EVIDENCIA DE OPTIMIZACIÓN ─────────────────────────────────────────────────
def benchmark_cache(df_silver: DataFrame, spark: SparkSession) -> dict:
    """
    Mide el impacto del cache de Spark sobre una consulta representativa.

    Metodología:
      1. Ejecutar la consulta de ventas mensuales SIN cache → medir tiempo.
      2. Cachear df_silver en memoria.
      3. Ejecutar la misma consulta CON cache → medir tiempo.
      4. Calcular mejora porcentual.

    Esta evidencia respalda la decisión arquitectónica de usar cache
    antes de construir múltiples data marts sobre el mismo DataFrame Silver.

    Returns:
        dict con tiempos y mejora porcentual.
    """
    # ── Sin cache ──────────────────────────────────────────────────────────
    inicio = time.time()
    df_silver.groupBy("anio", "mes").agg(spark_sum("venta").alias("total")).count()
    t_sin_cache = round(time.time() - inicio, 3)

    # ── Con cache ──────────────────────────────────────────────────────────
    df_cached = df_silver.cache()
    df_cached.count()  # Materializar el cache

    inicio = time.time()
    df_cached.groupBy("anio", "mes").agg(spark_sum("venta").alias("total")).count()
    t_con_cache = round(time.time() - inicio, 3)

    mejora_pct = round(((t_sin_cache - t_con_cache) / t_sin_cache) * 100, 2) \
                 if t_sin_cache > 0 else 0.0

    resultado = {
        "consulta":         "ventas_mensuales",
        "tiempo_sin_cache": t_sin_cache,
        "tiempo_con_cache": t_con_cache,
        "mejora_pct":       mejora_pct,
    }

    print(f"\n─── Benchmark Cache ─────────────────────────────────────────────")
    print(f"  Sin cache : {t_sin_cache}s")
    print(f"  Con cache : {t_con_cache}s")
    print(f"  Mejora    : {mejora_pct}%")

    # Guardar evidencia en Gold
    evidencia_df = spark.createDataFrame([
        ("ventas_mensuales", "sin_cache", float(t_sin_cache)),
        ("ventas_mensuales", "con_cache", float(t_con_cache)),
    ], ["consulta", "escenario", "tiempo_segundos"])

    _escribir_gold(evidencia_df, "evidencia_optimizacion", cfg.RUTA_GOLD)

    # Liberar cache para no consumir memoria innecesariamente
    df_cached.unpersist()

    return resultado


# ─── ESTIMACIÓN FINOPS ─────────────────────────────────────────────────────────
def estimar_costo_mensual(
    costo_hora_cluster: float = cfg.COSTO_HORA_CLUSTER_USD,
    duracion_horas: float     = cfg.DURACION_JOB_HORAS,
    ejecuciones_mes: int      = cfg.EJECUCIONES_MES,
    gb_almacenados: float     = 50.0
) -> dict:
    """
    Estima el costo mensual del pipeline (cómputo + almacenamiento).

    Supuestos:
      - Clúster Dataproc: 1 master + 2 workers n1-standard-4 → ~USD 1.20/h.
      - Duración job batch diario: 15 minutos (0.25h).
      - Almacenamiento GCS Standard: USD 0.020/GB/mes.
      - El clúster se apaga al finalizar cada job (ephemeral cluster).

    Returns:
        dict con desglose de costos en USD.
    """
    costo_compute = costo_hora_cluster * duracion_horas * ejecuciones_mes
    costo_storage = gb_almacenados * cfg.COSTO_GCS_GB_MES
    total         = costo_compute + costo_storage

    resumen = {
        "costo_compute_usd":  round(costo_compute, 2),
        "costo_storage_usd":  round(costo_storage, 2),
        "costo_total_usd":    round(total, 2),
        "gb_almacenados":     gb_almacenados,
        "ejecuciones_mes":    ejecuciones_mes,
        "duracion_horas":     duracion_horas,
    }

    print(f"\n─── Estimación FinOps — Mensual ─────────────────────────────────")
    print(f"  Cómputo Dataproc : USD {resumen['costo_compute_usd']:>6.2f}")
    print(f"  Almacenamiento   : USD {resumen['costo_storage_usd']:>6.2f}")
    print(f"  ─────────────────────────────────────────")
    print(f"  TOTAL ESTIMADO   : USD {resumen['costo_total_usd']:>6.2f}/mes")

    return resumen


# ─── PUNTO DE ENTRADA STANDALONE ───────────────────────────────────────────────
def run(spark: SparkSession) -> dict:
    """
    Ejecuta la construcción completa de la capa Gold:
    carga Silver → benchmark cache → 4 data marts → FinOps.

    Args:
        spark: SparkSession ya inicializada.

    Returns:
        dict con métricas de la ejecución.
    """
    # 1. Leer Silver
    print("→ Leyendo Silver...")
    df_silver = spark.read.format("delta").load(cfg.TABLA_SILVER_VENTAS)
    print(f"✓ {df_silver.count():,} registros en Silver")

    # 2. Benchmark de optimización
    bench = benchmark_cache(df_silver, spark)

    # 3. Cachear para construir múltiples data marts eficientemente
    df_silver_cached = df_silver.cache()
    df_silver_cached.count()

    resultados_gold = {}

    # 4. Construir y escribir data marts
    marts = {
        "ventas_mensuales":       mart_ventas_mensuales(df_silver_cached),
        "ventas_por_tienda":      mart_ventas_por_tienda(df_silver_cached),
        "ventas_por_producto":    mart_ventas_por_producto(df_silver_cached),
        "ventas_canal_documento": mart_ventas_canal_documento(df_silver_cached),
    }

    for nombre, df_mart in marts.items():
        resultados_gold[nombre] = _escribir_gold(df_mart, nombre, cfg.RUTA_GOLD)

    # 5. Liberar cache
    df_silver_cached.unpersist()

    # 6. FinOps
    finops = estimar_costo_mensual()

    return {
        "data_marts":  resultados_gold,
        "benchmark":   bench,
        "finops":      finops,
    }
