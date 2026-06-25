"""
bronze.py
=========
Módulo de ingesta — Capa Bronze de la arquitectura Medallion.

Responsabilidades:
  - Leer archivos CSV crudos desde GCS (capa RAW).
  - Escribir en formato Delta Lake SIN transformaciones.
  - Preservar el dato original para garantizar trazabilidad y reproducibilidad.
  - Registrar linaje y auditoría en BigQuery.

Principio de diseño:
  Bronze = fuente de verdad inmutable. Cualquier error en capas superiores
  puede subsanarse releyendo desde Bronze sin necesidad de re-ingesta.

Equipo: Ballerini · Torres · Vargas · Vásquez
Curso:  Big Data y Cloud Computing — MDS UDD 2026
"""

import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit

from src import config as cfg


# ─── ESQUEMA DE LECTURA ────────────────────────────────────────────────────────
# Definimos el esquema explícito para evitar que Spark infiera tipos incorrectos
# y para documentar la estructura de la fuente.
SCHEMA_VENTAS = """
    id_canal               STRING,
    numero_transaccion     STRING,
    numero_pos             STRING,
    numero_boleta          STRING,
    fecha_transaccion      STRING,
    cod_tienda_facturacion STRING,
    tipo_documento         STRING,
    id_producto            STRING,
    unidades               STRING,
    venta                  STRING,
    costo                  STRING
"""


def leer_csv_raw(
    spark: SparkSession,
    ruta: str,
    schema: str = SCHEMA_VENTAS
) -> DataFrame:
    """
    Lee un CSV crudo desde GCS con schema explícito.
    Bronze lee TODO como STRING para preservar el dato original.

    Args:
        spark:  SparkSession activa.
        ruta:   Ruta gs:// del archivo CSV.
        schema: DDL string con el esquema de la tabla.

    Returns:
        DataFrame con los datos crudos.
    """
    print(f"→ Leyendo RAW: {ruta}")
    inicio = time.time()

    df = (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("multiline", "false")
        .schema(schema)
        .csv(ruta)
    )

    # Añadir metadatos de ingesta (columnas de auditoría Bronze)
    df = (
        df
        .withColumn("_ingesta_timestamp", current_timestamp())
        .withColumn("_archivo_origen", lit(ruta))
    )

    count = df.count()
    elapsed = round(time.time() - inicio, 2)
    print(f"✓ {count:,} registros leídos en {elapsed}s desde {ruta.split('/')[-1]}")
    return df


def escribir_bronze(
    df: DataFrame,
    ruta_destino: str,
    modo: str = "overwrite"
) -> dict:
    """
    Escribe un DataFrame en formato Delta Lake en la capa Bronze.

    Args:
        df:            DataFrame a escribir.
        ruta_destino:  Ruta gs:// de destino en Bronze.
        modo:          'overwrite' para carga inicial, 'append' para incremental.

    Returns:
        dict con métricas de escritura (tiempo, registros).
    """
    print(f"→ Escribiendo Bronze: {ruta_destino}")
    inicio = time.time()

    (
        df.write
        .format("delta")
        .mode(modo)
        # Bronze NO particiona: preserva el layout original para máxima compatibilidad
        .save(ruta_destino)
    )

    elapsed = round(time.time() - inicio, 2)
    print(f"✓ Bronze escrito en {elapsed}s | Modo: {modo}")

    return {
        "ruta": ruta_destino,
        "registros": df.count(),
        "tiempo_seg": elapsed,
        "modo": modo,
    }


def validar_bronze(spark: SparkSession, ruta: str) -> dict:
    """
    Valida que la escritura Bronze fue exitosa leyendo la tabla Delta
    y verificando el número de registros.

    Args:
        spark: SparkSession activa.
        ruta:  Ruta gs:// de la tabla Delta Bronze.

    Returns:
        dict con resultado de validación.
    """
    print(f"→ Validando Bronze: {ruta}")
    df = spark.read.format("delta").load(ruta)
    count = df.count()

    # Verificar que exista la columna de metadatos de ingesta
    tiene_metadata = "_ingesta_timestamp" in df.columns

    resultado = {
        "ruta":          ruta,
        "registros":     count,
        "tiene_metadata": tiene_metadata,
        "valido":        count > 0 and tiene_metadata,
    }

    estado = "✓ VÁLIDO" if resultado["valido"] else "✗ INVÁLIDO"
    print(f"{estado} | {count:,} registros | Metadata: {tiene_metadata}")
    return resultado


def documentar_linaje_bronze(ruta_origen: str, ruta_destino: str) -> dict:
    """
    Genera el registro de linaje para la capa Bronze.
    Retorna un dict listo para insertar en BigQuery.

    Args:
        ruta_origen:  Ruta del archivo RAW.
        ruta_destino: Ruta Delta en Bronze.

    Returns:
        dict con los campos del registro de linaje.
    """
    return {
        "dataset":         "venta_tiendas",
        "origen":          ruta_origen,
        "destino":         ruta_destino,
        "capa":            "BRONZE",
        "formato":         "Delta Lake (sin partición)",
        "transformaciones": "Sin transformaciones. Preserva datos originales incluyendo "
                            "tipos STRING y formato de fecha dd/MM/yyyy hh:mm:ss a CL.",
        "notebook_origen": "src/bronze.py → Capa_Bronze.ipynb",
    }


# ─── PUNTO DE ENTRADA STANDALONE ───────────────────────────────────────────────
def run(spark: SparkSession) -> dict:
    """
    Ejecuta la ingesta completa Bronze: POS + e-commerce.

    Args:
        spark: SparkSession ya inicializada.

    Returns:
        dict con métricas de la ejecución.
    """
    resultados = {}

    # 1. Ventas POS (tiendas físicas)
    df_pos = leer_csv_raw(spark, cfg.ARCHIVO_VENTAS_POS)
    resultados["pos"] = escribir_bronze(df_pos, cfg.TABLA_BRONZE_VENTAS)

    # 2. Ventas e-commerce
    df_ecom = leer_csv_raw(spark, cfg.ARCHIVO_VENTAS_ECOM)
    resultados["ecom"] = escribir_bronze(df_ecom, cfg.TABLA_BRONZE_ECOM)

    # 3. Validaciones
    resultados["validacion_pos"]  = validar_bronze(spark, cfg.TABLA_BRONZE_VENTAS)
    resultados["validacion_ecom"] = validar_bronze(spark, cfg.TABLA_BRONZE_ECOM)

    return resultados
