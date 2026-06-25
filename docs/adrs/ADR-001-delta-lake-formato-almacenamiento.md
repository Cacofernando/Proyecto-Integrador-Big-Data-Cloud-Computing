# ADR-001: Elección de Delta Lake como Formato de Almacenamiento

**Estado:** Aceptado  
**Fecha:** Mayo 2026  
**Autores:** Ballerini · Torres · Vargas · Vásquez  
**Revisado por:** Prof. Luis Castillo Faune  

---

## Contexto

El pipeline de analítica omnicanal de retail requiere un formato de almacenamiento
para la capa de Data Lake (Bronze, Silver, Gold) sobre Google Cloud Storage (GCS).
El formato elegido debe soportar:

- Transacciones ACID para garantizar consistencia durante escrituras batch concurrentes.
- Lectura eficiente con filtros temporales (consultas frecuentes por mes/año).
- Compatibilidad con Apache Spark 3.x y BigQuery.
- Capacidad de revertir cambios ante errores de transformación (Time Travel).
- Evolución del esquema sin necesidad de reescribir tablas completas.

El dataset principal tiene ~12 millones de registros iniciales, con incremento
diario de ~33.000 registros (estimado). En 3 años de operación, se proyectan
~36 millones de registros en Silver.

---

## Decisión

**Se adopta Delta Lake como formato de almacenamiento para todas las capas
de la arquitectura Medallion (Bronze, Silver, Gold).**

### Configuración aplicada

- Bronze: Delta sin partición (preserva layout original).
- Silver: Delta particionado por `(anio, mes)` para optimizar filtros temporales.
- Gold: Delta sin partición (los data marts son tablas pequeñas, < 10.000 filas).

### Versión utilizada

- `delta-spark==2.4.0` con `pyspark==3.4.1` en Google Colab.
- `delta-core_2.12:2.4.0` como paquete JAR en Dataproc.

---

## Alternativas Consideradas

### Alternativa A: Apache Parquet (sin capa transaccional)

**Descripción:** Formato columnar estándar de facto en el ecosistema Hadoop/Spark.
Ampliamente soportado por todas las herramientas del stack.

**Argumento técnico descartado:**  
Parquet no provee transacciones ACID. En un pipeline batch diario con recargas
parciales, existe riesgo de leer un snapshot incompleto si la escritura falla a
mitad de proceso. Delta Lake resuelve esto con commit log atómico.

Parquet tampoco provee Time Travel: si una transformación Silver introduce un
bug que corrompe datos, hay que volver a correr desde Bronze. Con Delta, basta
hacer `RESTORE TABLE TO VERSION AS OF N`.

**Argumento económico descartado:**  
El costo de recomputación ante errores (tiempo de ingeniero + cómputo) supera
con creces el overhead de almacenamiento del commit log de Delta (~1–2% del
volumen total), que para 50 GB estimados representa menos de USD 0.01/mes.

### Alternativa B: Apache Iceberg

**Descripción:** Formato de tabla abierto con capacidades similares a Delta Lake
(ACID, Time Travel, evolución de esquema). Soportado nativamente por BigQuery.

**Argumento técnico descartado:**  
El ecosistema de Iceberg en GCP es maduro pero la integración con Spark 3.4
requiere configuración más compleja (catálogo Hive o Nessie) que Delta, cuya
integración con PySpark es directa mediante `configure_spark_with_delta_pip`.
Para el alcance de este proyecto académico, Delta ofrece el camino más rápido
a una implementación funcional.

**Argumento económico descartado:**  
La integración de Iceberg con BigQuery como tabla nativa eliminaría el costo
del conector BigQuery Storage API (~USD 5/TB procesado). Sin embargo, dado
que usamos External Tables sobre GCS (lectura directa), el costo es equivalente
en ambas opciones. Iceberg sería preferible en una migración futura a Databricks.

### Alternativa C: CSV plano (sin formato de tabla)

No considerado para Silver ni Gold: sin soporte ACID, lectura completa en cada
consulta (sin predicate pushdown), sin esquema enforced. Solo se mantiene en RAW
como formato de ingesta desde la fuente operacional.

---

## Consecuencias

**Positivas:**
- Transacciones ACID: escrituras seguras incluso ante fallos del job Spark.
- Time Travel disponible: `spark.read.format("delta").option("versionAsOf", 0).load(...)`.
- Compaction automática de archivos small files con `OPTIMIZE`.
- Schema enforcement: detecta cambios en el esquema de la fuente de forma temprana.
- Compatibilidad directa con BigQuery mediante External Tables sobre GCS.

**Negativas / Limitaciones:**
- Overhead de metadatos: directorio `_delta_log/` con JSON de commits.
  Para tablas grandes (Gold), esto es ~100 KB de overhead por versión.
- Dependencia de versión: `delta-spark` debe coincidir con la versión de PySpark.
  Cualquier actualización de Spark requiere verificar compatibilidad con Delta.
- El comando `VACUUM` debe ejecutarse periódicamente (cada 7 días) para limpiar
  versiones antiguas y evitar acumulación de archivos en GCS.

**Deuda técnica identificada:**
- Implementar un job periódico de `OPTIMIZE` y `VACUUM` en el DAG de Airflow
  para mantener el rendimiento de lectura a medida que crecen los datos.
  Prioridad: media. Plazo estimado: 3 meses de operación.

---

## Evidencia Empírica

La decisión de particionar Silver por `(anio, mes)` fue validada con el benchmark
del notebook `Capa_Silver.ipynb`: la consulta de ventas mensuales con filtro
`WHERE anio = 2016 AND mes = 3` leyó solo 1 partición (1/N_MESES del volumen
total) en lugar del dataset completo, reduciendo el tiempo de scan de forma
proporcional al número de particiones saltadas (partition pruning).
