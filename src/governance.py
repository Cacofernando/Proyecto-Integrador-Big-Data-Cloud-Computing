"""
governance.py
=============
Módulo de Gobierno y Seguridad de Datos.

Responsabilidades:
  - Registrar el linaje de datos (RAW → Bronze → Silver → Gold).
  - Loguear cada ejecución del pipeline en una tabla de auditoría.
  - Pseudonimizar datos personales (SHA-256 con salt).
  - Enmascarar montos financieros sensibles a rangos.
  - Documentar la clasificación de columnas por nivel de sensibilidad.
  - Registrar resultados de validación Great Expectations.

Equipo: Ballerini · Torres · Vargas · Vásquez
Curso:  Big Data y Cloud Computing — MDS UDD 2026
"""

import uuid
import hashlib
import time as _time
from datetime import datetime, timezone
from typing import Optional

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from src import config as cfg


# ─── CLIENTE BIGQUERY ──────────────────────────────────────────────────────────
def get_bq_client() -> bigquery.Client:
    """Retorna un cliente BigQuery autenticado con las credenciales de entorno."""
    return bigquery.Client(project=cfg.PROJECT_ID)


# ─── UTILIDADES DE CREACIÓN ────────────────────────────────────────────────────
def _crear_dataset_si_no_existe(bq: bigquery.Client, dataset_id: str,
                                 descripcion: str = "") -> None:
    """Crea un dataset BigQuery si no existe."""
    ref = f"{cfg.PROJECT_ID}.{dataset_id}"
    try:
        bq.get_dataset(ref)
    except NotFound:
        ds = bigquery.Dataset(ref)
        ds.location    = cfg.REGION
        ds.description = descripcion
        bq.create_dataset(ds)
        print(f"✓ Dataset creado: {ref}")


def _crear_tabla_si_no_existe(bq: bigquery.Client, tabla_ref: str,
                               schema: list, descripcion: str = "") -> None:
    """Crea una tabla BigQuery con schema dado si no existe."""
    try:
        bq.get_table(tabla_ref)
    except NotFound:
        t = bigquery.Table(tabla_ref, schema=schema)
        t.description = descripcion
        bq.create_table(t)
        print(f"✓ Tabla creada: {tabla_ref}")


# ─── SCHEMAS BQ ────────────────────────────────────────────────────────────────
SCHEMA_LINAJE = [
    bigquery.SchemaField("id_linaje",          "STRING",    "REQUIRED",
        description="UUID único del registro"),
    bigquery.SchemaField("nombre_campo",       "STRING",    "REQUIRED"),
    bigquery.SchemaField("tabla_origen",       "STRING",    "REQUIRED"),
    bigquery.SchemaField("tabla_destino",      "STRING",    "REQUIRED"),
    bigquery.SchemaField("capa_origen",        "STRING",    "REQUIRED",
        description="RAW | BRONZE | SILVER | GOLD"),
    bigquery.SchemaField("capa_destino",       "STRING",    "REQUIRED"),
    bigquery.SchemaField("transformacion",     "STRING",    "NULLABLE"),
    bigquery.SchemaField("notebook_origen",    "STRING",    "NULLABLE"),
    bigquery.SchemaField("es_dato_sensible",   "BOOL",      "REQUIRED"),
    bigquery.SchemaField("tecnica_proteccion", "STRING",    "NULLABLE",
        description="HASH | MASK | SUPPRESS | NONE"),
    bigquery.SchemaField("fecha_registro",     "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("responsable",        "STRING",    "REQUIRED"),
]

SCHEMA_AUDITORIA = [
    bigquery.SchemaField("id_ejecucion",          "STRING",    "REQUIRED"),
    bigquery.SchemaField("etapa",                 "STRING",    "REQUIRED",
        description="BRONZE | SILVER | GOLD | GOVERNANCE"),
    bigquery.SchemaField("notebook",              "STRING",    "REQUIRED"),
    bigquery.SchemaField("estado",                "STRING",    "REQUIRED",
        description="INICIO | EXITO | ERROR"),
    bigquery.SchemaField("registros_entrada",     "INTEGER",   "NULLABLE"),
    bigquery.SchemaField("registros_salida",      "INTEGER",   "NULLABLE"),
    bigquery.SchemaField("registros_descartados", "INTEGER",   "NULLABLE"),
    bigquery.SchemaField("duracion_segundos",     "FLOAT64",   "NULLABLE"),
    bigquery.SchemaField("mensaje_error",         "STRING",    "NULLABLE"),
    bigquery.SchemaField("cuenta_ejecucion",      "STRING",    "REQUIRED"),
    bigquery.SchemaField("timestamp_inicio",      "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("timestamp_fin",         "TIMESTAMP", "NULLABLE"),
    bigquery.SchemaField("version_codigo",        "STRING",    "NULLABLE",
        description="Git commit hash para trazabilidad"),
]

SCHEMA_CALIDAD = [
    bigquery.SchemaField("id_validacion", "STRING",    "REQUIRED"),
    bigquery.SchemaField("timestamp_val", "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("capa",          "STRING",    "REQUIRED"),
    bigquery.SchemaField("expectativa",   "STRING",    "REQUIRED"),
    bigquery.SchemaField("justificacion", "STRING",    "NULLABLE"),
    bigquery.SchemaField("aprobada",      "BOOL",      "REQUIRED"),
    bigquery.SchemaField("n_fallidos",    "INTEGER",   "NULLABLE"),
    bigquery.SchemaField("pct_fallidos",  "FLOAT64",   "NULLABLE"),
]


# ─── INICIALIZACIÓN DE INFRAESTRUCTURA ────────────────────────────────────────
def inicializar_infraestructura_gobierno(bq: bigquery.Client) -> None:
    """
    Crea los datasets y tablas de gobierno si no existen.
    Idempotente: seguro de llamar en cada ejecución del pipeline.
    """
    _crear_dataset_si_no_existe(bq, cfg.DATASET_GOV,
        "Dataset de gobierno: linaje, auditoría y calidad de datos.")

    _crear_tabla_si_no_existe(bq, cfg.TABLA_LINAJE, SCHEMA_LINAJE,
        "Linaje campo a campo del pipeline RAW → GOLD.")

    _crear_tabla_si_no_existe(bq, cfg.TABLA_AUDITORIA, SCHEMA_AUDITORIA,
        "Log de auditoría por ejecución del pipeline ETL retail.")

    _crear_tabla_si_no_existe(bq, cfg.TABLA_CALIDAD, SCHEMA_CALIDAD,
        "Resultados de validación Great Expectations por ejecución.")

    print("✓ Infraestructura de gobierno verificada.")


# ─── CLASE AUDITOR PIPELINE ────────────────────────────────────────────────────
class AuditorPipeline:
    """
    Registra el inicio y fin de cada etapa del pipeline en BigQuery.

    Uso en cualquier módulo del pipeline:

        bq = get_bq_client()
        auditor = AuditorPipeline(bq, "SILVER", "src/silver.py")
        auditor.inicio()
        # ... procesamiento ...
        auditor.fin(registros_entrada=12_000_000,
                    registros_salida=11_987_340,
                    registros_descartados=12_660,
                    duracion_seg=45.7)
    """

    def __init__(self, bq: bigquery.Client, etapa: str, notebook: str,
                 cuenta: str = None, version_codigo: str = "HEAD"):
        self.bq             = bq
        self.id_ejecucion   = str(uuid.uuid4())
        self.etapa          = etapa
        self.notebook       = notebook
        self.cuenta         = cuenta or f"svc-dataeng@{cfg.PROJECT_ID}.iam.gserviceaccount.com"
        self.version_codigo = version_codigo
        self._ts_inicio     = None

    def inicio(self) -> "AuditorPipeline":
        self._ts_inicio = datetime.now(timezone.utc)
        fila = [{
            "id_ejecucion":          self.id_ejecucion,
            "etapa":                 self.etapa,
            "notebook":              self.notebook,
            "estado":                "INICIO",
            "registros_entrada":     None,
            "registros_salida":      None,
            "registros_descartados": None,
            "duracion_segundos":     None,
            "mensaje_error":         None,
            "cuenta_ejecucion":      self.cuenta,
            "timestamp_inicio":      self._ts_inicio.isoformat(),
            "timestamp_fin":         None,
            "version_codigo":        self.version_codigo,
        }]
        errores = self.bq.insert_rows_json(cfg.TABLA_AUDITORIA, fila)
        if not errores:
            print(f"[{self.etapa}] Auditoría iniciada — ID: {self.id_ejecucion[:8]}...")
        return self

    def fin(self, registros_entrada: int = None, registros_salida: int = None,
            registros_descartados: int = None, duracion_seg: float = None,
            error: str = None) -> None:
        ts_fin  = datetime.now(timezone.utc)
        estado  = "ERROR" if error else "EXITO"
        fila = [{
            "id_ejecucion":          self.id_ejecucion,
            "etapa":                 self.etapa,
            "notebook":              self.notebook,
            "estado":                estado,
            "registros_entrada":     registros_entrada,
            "registros_salida":      registros_salida,
            "registros_descartados": registros_descartados,
            "duracion_segundos":     duracion_seg,
            "mensaje_error":         error,
            "cuenta_ejecucion":      self.cuenta,
            "timestamp_inicio":      self._ts_inicio.isoformat() if self._ts_inicio else None,
            "timestamp_fin":         ts_fin.isoformat(),
            "version_codigo":        self.version_codigo,
        }]
        errores = self.bq.insert_rows_json(cfg.TABLA_AUDITORIA, fila)
        if not errores:
            icono = "✓" if estado == "EXITO" else "✗"
            print(f"{icono} [{self.etapa}] {estado} — "
                  f"Entrada: {registros_entrada:,} | Salida: {registros_salida:,} | "
                  f"Tiempo: {duracion_seg}s")


# ─── LINAJE DE DATOS ───────────────────────────────────────────────────────────
# Definición completa del linaje campo a campo del pipeline.
REGISTROS_LINAJE_PIPELINE = [
    # ── RAW → BRONZE ───────────────────────────────────────────────────────
    ("numero_transaccion", cfg.ARCHIVO_VENTAS_POS, cfg.TABLA_BRONZE_VENTAS,
     "RAW", "BRONZE",
     "Copia directa. Bronze preserva el dato crudo sin transformación alguna.",
     "src/bronze.py", False, "NONE"),

    ("fecha_transaccion", cfg.ARCHIVO_VENTAS_POS, cfg.TABLA_BRONZE_VENTAS,
     "RAW", "BRONZE",
     "Copia directa como STRING. Formato original: dd/MM/yyyy hh:mm:ss a CL.",
     "src/bronze.py", False, "NONE"),

    ("venta", cfg.ARCHIVO_VENTAS_POS, cfg.TABLA_BRONZE_VENTAS,
     "RAW", "BRONZE",
     "Copia directa como STRING. El cast a DOUBLE ocurre en Silver.",
     "src/bronze.py", False, "NONE"),

    ("id_producto", cfg.ARCHIVO_VENTAS_POS, cfg.TABLA_BRONZE_VENTAS,
     "RAW", "BRONZE",
     "Copia directa. Tipo LONG asignado en Silver.",
     "src/bronze.py", False, "NONE"),

    # ── BRONZE → SILVER ────────────────────────────────────────────────────
    ("fecha_venta", cfg.TABLA_BRONZE_VENTAS, cfg.TABLA_SILVER_VENTAS,
     "BRONZE", "SILVER",
     "Creada con to_date(to_timestamp(fecha_transaccion)). Sufijo ' CL' eliminado.",
     "src/silver.py", False, "NONE"),

    ("margen", cfg.TABLA_BRONZE_VENTAS, cfg.TABLA_SILVER_VENTAS,
     "BRONZE", "SILVER",
     "Campo derivado: venta - costo. Margen bruto por transacción.",
     "src/silver.py", False, "NONE"),

    ("margen_porcentaje", cfg.TABLA_BRONZE_VENTAS, cfg.TABLA_SILVER_VENTAS,
     "BRONZE", "SILVER",
     "(venta - costo) / venta cuando venta != 0, NULL en caso contrario.",
     "src/silver.py", False, "NONE"),

    ("venta", cfg.TABLA_BRONZE_VENTAS, cfg.TABLA_SILVER_VENTAS,
     "BRONZE", "SILVER",
     "Cast a DOUBLE. Se excluyen registros con venta NULL.",
     "src/silver.py", False, "NONE"),

    # ── SILVER → GOLD ──────────────────────────────────────────────────────
    ("venta_total", cfg.TABLA_SILVER_VENTAS, cfg.TABLA_GOLD_MENSUAL,
     "SILVER", "GOLD",
     "SUM(venta) GROUP BY anio, mes. Incluye devoluciones (venta < 0).",
     "src/gold.py", False, "NONE"),

    ("margen_porcentaje", cfg.TABLA_SILVER_VENTAS, cfg.TABLA_GOLD_MENSUAL,
     "SILVER", "GOLD",
     "ROUND(margen_total / venta_total, 4). NULL si venta_total = 0.",
     "src/gold.py", False, "NONE"),

    # Ejemplo de campo sensible (si se añade PII en el futuro)
    ("id_cliente_hash", cfg.TABLA_SILVER_VENTAS, cfg.TABLA_GOLD_TIENDA,
     "SILVER", "GOLD",
     "SHA-256 del id_cliente con salt. No reversible sin conocer el salt.",
     "src/governance.py", True, "HASH"),
]


def registrar_linaje_completo(bq: bigquery.Client) -> int:
    """
    Inserta todos los registros de linaje del pipeline en BigQuery.

    Returns:
        Número de registros insertados exitosamente.
    """
    TS_AHORA    = datetime.now(timezone.utc).isoformat()
    RESPONSABLE = f"svc-dataeng@{cfg.PROJECT_ID}.iam.gserviceaccount.com"

    filas = []
    for (campo, origen, destino, c_org, c_dst,
         transform, notebook, sensible, proteccion) in REGISTROS_LINAJE_PIPELINE:
        filas.append({
            "id_linaje":          str(uuid.uuid4()),
            "nombre_campo":       campo,
            "tabla_origen":       origen,
            "tabla_destino":      destino,
            "capa_origen":        c_org,
            "capa_destino":       c_dst,
            "transformacion":     transform,
            "notebook_origen":    notebook,
            "es_dato_sensible":   sensible,
            "tecnica_proteccion": proteccion,
            "fecha_registro":     TS_AHORA,
            "responsable":        RESPONSABLE,
        })

    errores = bq.insert_rows_json(cfg.TABLA_LINAJE, filas)
    if errores:
        print(f"⚠ Errores al insertar linaje: {errores}")
        return 0

    print(f"✓ {len(filas)} registros de linaje insertados en {cfg.TABLA_LINAJE}")
    return len(filas)


# ─── SEGURIDAD: PSEUDONIMIZACIÓN ───────────────────────────────────────────────
def pseudonimizar(valor: Optional[str], salt: str = cfg.SALT_HASH) -> Optional[str]:
    """
    Aplica SHA-256 con salt al valor para pseudonimizarlo.

    Características:
      - Determinista: mismo input → mismo hash. Permite joins entre tablas.
      - No reversible sin conocer el salt.
      - El salt en producción debe cargarse desde GCP Secret Manager.

    Args:
        valor: Valor a pseudonimizar (ej. RUT, email).
        salt:  Cadena secreta para dificultar ataques de diccionario.

    Returns:
        Hash SHA-256 hexadecimal de 64 caracteres, o None si valor es None.
    """
    if valor is None:
        return None
    entrada = f"{valor}{salt}".encode("utf-8")
    return hashlib.sha256(entrada).hexdigest()


def enmascarar_monto(monto: Optional[float]) -> str:
    """
    Transforma un monto exacto en un rango categórico (CLP).

    Uso: En la capa Gold, los analistas ven rangos en lugar de montos exactos
    por transacción. Los totales agregados (KPIs) no se enmascaran.

    Args:
        monto: Monto en pesos chilenos.

    Returns:
        String con el rango correspondiente.
    """
    if monto is None:
        return "DESCONOCIDO"
    if monto <= 0:
        return "DEVOLUCION_O_CERO"
    elif monto < 10_000:
        return "0-10k"
    elif monto < 50_000:
        return "10k-50k"
    elif monto < 100_000:
        return "50k-100k"
    else:
        return "100k+"


# ─── CLASIFICACIÓN DE COLUMNAS ─────────────────────────────────────────────────
CLASIFICACION_COLUMNAS = {
    "CONFIDENCIAL": {
        "descripcion":   "Dato financiero individual. Requiere rol analyst-financial.",
        "columnas":      ["numero_boleta", "id_cliente_hash"],
        "rol_requerido": "roles/datacatalog.categoryFineGrainedReader",
    },
    "INTERNO": {
        "descripcion":   "Dato comercial sensible. Uso restringido a ingeniería.",
        "columnas":      ["costo", "margen_porcentaje"],
        "rol_requerido": "roles/datacatalog.categoryFineGrainedReader",
    },
    "PUBLICO": {
        "descripcion":   "Disponible para todos los lectores del dataset.",
        "columnas":      ["fecha_venta", "id_canal", "id_producto",
                         "unidades", "anio", "mes"],
        "rol_requerido": None,
    },
}


def imprimir_clasificacion_columnas() -> None:
    """Imprime la política de clasificación de columnas por nivel de sensibilidad."""
    print("\n─── Clasificación de Columnas por Nivel de Sensibilidad ─────────")
    for nivel, cfg_nivel in CLASIFICACION_COLUMNAS.items():
        print(f"\n  [{nivel}]")
        print(f"    Descripción  : {cfg_nivel['descripcion']}")
        print(f"    Rol requerido: {cfg_nivel['rol_requerido'] or 'Sin restricción'}")
        print(f"    Columnas     : {', '.join(cfg_nivel['columnas'])}")
