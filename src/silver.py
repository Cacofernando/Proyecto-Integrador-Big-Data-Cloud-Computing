"""
silver.py
=========
Módulo de transformación — Capa Silver de la arquitectura Medallion.

Responsabilidades:
  - Leer desde Bronze (datos crudos en STRING).
  - Aplicar limpieza, tipificación y normalización.
  - Escribir en Delta Lake particionado por (anio, mes).
  - Documentar reglas de calidad aplicadas.

Principio de diseño:
  Silver = datos confiables. Aquí se aplica el "contrato de datos":
  cualquier campo que llega a Gold debe cumplir las reglas definidas aquí.
  Las devoluciones (venta negativa) se CONSERVAN — son un hecho de negocio,
  no un error de datos.

Equipo: Ballerini · Torres · Vargas · Vásquez
Curso:  Big Data y Cloud Computing — MDS UDD 2026
"""

import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, regexp_replace, trim, to_timestamp, to_date,
    date_format, year, month, dayofmonth, when, lit,
    sum as spark_sum, current_timestamp
)

from src import config as cfg


# ─── REGLAS DE TRANSFORMACIÓN ──────────────────────────────────────────────────
# Documentadas aquí para que un nuevo Data Engineer entienda cada decisión.

FORMATO_FECHA_FUENTE = "dd/MM/yyyy hh:mm:ss a"
# Nota: el sufijo " CL" (zona horaria local chilena) se elimina antes del parseo.
# Ejemplo raw: "14/03/2016 12:00:00 AM CL" → timestamp → date 2016-03-14


def transformar_bronze_a_silver(df_bronze: DataFrame) -> DataFrame:
    """
    Aplica todas las transformaciones Silver sobre el DataFrame Bronze.

    Transformaciones aplicadas:
    1. Deduplicación por clave natural (canal, transacción, pos, boleta, producto).
    2. Casting de columnas numéricas desde STRING.
    3. Parseo de fecha_transaccion: elimina sufijo "CL", convierte a date.
    4. Creación de columnas derivadas: anio, mes, dia, margen, margen_porcentaje.
    5. Filtros de calidad: elimina registros sin fecha, sin producto y sin venta.
    6. Normalización de tipo_documento (trim de espacios).

    Args:
        df_bronze: DataFrame leído desde Bronze.

    Returns:
        DataFrame Silver listo para escribir.
    """
    # Eliminar sufijo " CL" antes del parseo de fecha
    fecha_sin_zona = regexp_replace(col("fecha_transaccion"), r" CL$", "")

    df_silver = (
        df_bronze

        # ── 1. Deduplicación ────────────────────────────────────────────────
        .dropDuplicates()

        # ── 2. Casting numérico ─────────────────────────────────────────────
        .withColumn("id_canal",               col("id_canal").cast("int"))
        .withColumn("numero_transaccion",     col("numero_transaccion").cast("long"))
        .withColumn("numero_pos",             col("numero_pos").cast("int"))
        .withColumn("numero_boleta",          col("numero_boleta").cast("long"))
        .withColumn("cod_tienda_facturacion", col("cod_tienda_facturacion").cast("int"))
        .withColumn("id_producto",            col("id_producto").cast("long"))
        .withColumn("unidades",               col("unidades").cast("int"))
        .withColumn("venta",                  col("venta").cast("double"))
        .withColumn("costo",                  col("costo").cast("double"))

        # ── 3. Normalización de texto ───────────────────────────────────────
        .withColumn("tipo_documento", trim(col("tipo_documento")))
        # Versión con guiones para compatibilidad con sistemas que no admiten "/"
        .withColumn(
            "fecha_transaccion_guion",
            regexp_replace(col("fecha_transaccion"), "/", "-")
        )

        # ── 4. Parseo de fecha ──────────────────────────────────────────────
        .withColumn("fecha_timestamp", to_timestamp(fecha_sin_zona, FORMATO_FECHA_FUENTE))
        .withColumn("fecha_venta",     to_date(col("fecha_timestamp")))
        .withColumn("fecha_venta_texto", date_format(col("fecha_venta"), "yyyy-MM-dd"))

        # ── 5. Variables temporales derivadas ───────────────────────────────
        .withColumn("anio", year(col("fecha_venta")))
        .withColumn("mes",  month(col("fecha_venta")))
        .withColumn("dia",  dayofmonth(col("fecha_venta")))

        # ── 6. Variables comerciales derivadas ──────────────────────────────
        .withColumn("margen", col("venta") - col("costo"))
        .withColumn(
            "margen_porcentaje",
            when(col("venta") != 0, (col("venta") - col("costo")) / col("venta"))
            .otherwise(None)
        )

        # ── 7. Filtros de calidad ───────────────────────────────────────────
        # Se eliminan SOLO registros con errores críticos.
        # Las devoluciones (venta < 0) se conservan: son hechos de negocio.
        .filter(col("fecha_venta").isNotNull())    # Fecha inválida = no parseable
        .filter(col("id_producto").isNotNull())    # Sin producto = registro huérfano
        .filter(col("venta").isNotNull())          # Sin monto = dato incompleto

        # ── 8. Columna de auditoría Silver ──────────────────────────────────
        .withColumn("_silver_timestamp", current_timestamp())
    )

    return df_silver


def calcular_metricas_calidad(df_bronze: DataFrame, df_silver: DataFrame) -> dict:
    """
    Calcula métricas de calidad antes y después de la transformación Silver.

    Args:
        df_bronze: DataFrame original desde Bronze.
        df_silver: DataFrame transformado Silver.

    Returns:
        dict con conteos y porcentaje de descarte.
    """
    n_bronze = df_bronze.count()
    n_silver = df_silver.count()
    n_descartados = n_bronze - n_silver
    pct_descarte = round((n_descartados / n_bronze) * 100, 4) if n_bronze > 0 else 0.0

    return {
        "registros_bronze":    n_bronze,
        "registros_silver":    n_silver,
        "registros_descartados": n_descartados,
        "pct_descarte":        pct_descarte,
        "umbral_ok":           pct_descarte <= (cfg.UMBRAL_DESCARTE_PCT * 100),
    }


def auditar_nulos_silver(df_silver: DataFrame) -> None:
    """
    Imprime un reporte de valores nulos por columna en la capa Silver.
    Útil para detectar regresiones de calidad en ejecuciones futuras.

    Args:
        df_silver: DataFrame Silver ya transformado.
    """
    print("\n─── Auditoría de Nulos — Silver ───────────────────────────────────")
    nulos = df_silver.select([
        spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df_silver.columns
        if not c.startswith("_")   # excluir columnas de auditoría interna
    ])
    nulos.show(truncate=False)


def escribir_silver(
    df_silver: DataFrame,
    ruta_destino: str,
    modo: str = "overwrite"
) -> dict:
    """
    Escribe el DataFrame Silver en Delta Lake particionado por (anio, mes).

    El particionamiento por (anio, mes) es la optimización clave de Silver:
    las consultas analíticas más frecuentes filtran por período temporal.
    Con ~12M registros y 2–3 años de datos, esto reduce el scan en ~12×.

    Args:
        df_silver:     DataFrame Silver transformado.
        ruta_destino:  Ruta gs:// en la capa Silver.
        modo:          'overwrite' para carga completa inicial.

    Returns:
        dict con métricas de escritura.
    """
    print(f"→ Escribiendo Silver particionado: {ruta_destino}")
    inicio = time.time()

    (
        df_silver.write
        .format("delta")
        .mode(modo)
        .partitionBy("anio", "mes")   # ← OPTIMIZACIÓN CLAVE
        .save(ruta_destino)
    )

    elapsed = round(time.time() - inicio, 2)
    print(f"✓ Silver escrito en {elapsed}s | Particionado por (anio, mes)")

    return {
        "ruta":      ruta_destino,
        "tiempo_seg": elapsed,
        "modo":       modo,
    }


# ─── PUNTO DE ENTRADA STANDALONE ───────────────────────────────────────────────
def run(spark: SparkSession) -> dict:
    """
    Ejecuta la transformación completa Bronze → Silver.

    Args:
        spark: SparkSession ya inicializada.

    Returns:
        dict con métricas de la ejecución.
    """
    # 1. Leer desde Bronze
    print("→ Leyendo desde Bronze...")
    df_bronze = spark.read.format("delta").load(cfg.TABLA_BRONZE_VENTAS)
    print(f"✓ {df_bronze.count():,} registros en Bronze")

    # 2. Transformar
    print("→ Aplicando transformaciones Silver...")
    inicio = time.time()
    df_silver = transformar_bronze_a_silver(df_bronze)
    tiempo_transform = round(time.time() - inicio, 2)

    # 3. Métricas de calidad
    metricas = calcular_metricas_calidad(df_bronze, df_silver)
    print(f"✓ Transformación completada en {tiempo_transform}s")
    print(f"  Bronze: {metricas['registros_bronze']:,} → Silver: {metricas['registros_silver']:,}")
    print(f"  Descartados: {metricas['registros_descartados']:,} ({metricas['pct_descarte']}%)")

    if not metricas["umbral_ok"]:
        raise ValueError(
            f"Tasa de descarte {metricas['pct_descarte']}% supera el umbral "
            f"{cfg.UMBRAL_DESCARTE_PCT * 100}%. Revisar reglas de filtrado."
        )

    # 4. Auditoría de nulos
    auditar_nulos_silver(df_silver)

    # 5. Escribir Silver
    resultado_escritura = escribir_silver(df_silver, cfg.TABLA_SILVER_VENTAS)

    return {
        "metricas_calidad": metricas,
        "escritura":        resultado_escritura,
        "tiempo_transform_seg": tiempo_transform,
    }
