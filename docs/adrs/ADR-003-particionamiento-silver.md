# ADR-003: Estrategia de Particionamiento de la Capa Silver

**Estado:** Aceptado  
**Fecha:** Junio 2026 (ajustado durante implementación Fase 2)  
**Autores:** Ballerini · Torres · Vargas · Vásquez  
**Revisado por:** Prof. Luis Castillo Faune  

---

## Contexto

Durante la implementación de la Fase 2, el equipo debió decidir cómo particionar
físicamente la tabla Silver (`venta_tiendas_delta`) en GCS. El particionamiento
determina el layout de archivos en el Data Lake y tiene impacto directo en:

- **Rendimiento de lectura:** Spark puede saltar particiones que no satisfacen
  el filtro de una consulta (partition pruning), evitando leer datos innecesarios.
- **Tamaño de los archivos:** Particiones muy pequeñas generan el problema de
  "small files" que degrada el rendimiento de GCS y de Spark.
- **Costo:** GCS cobra por bytes leídos desde BigQuery External Tables
  (~USD 5/TB). Reducir el scan reduce el costo.

El dataset Silver contiene **2.249.970 registros** correspondientes a
transacciones de ventas POS de Forus, distribuidos en **99 particiones lógicas**
activas entre 2015 y 2023 (meses no consecutivos, según el volumen real medido
en el notebook `Optimizacion_y_FinOps.ipynb`). Las consultas analíticas más
frecuentes identificadas en el levantamiento de requerimientos son:

1. Ventas del mes anterior (filtro: `anio = X AND mes = Y`).
2. Ventas del último trimestre (filtro: `anio = X AND mes BETWEEN A AND B`).
3. Ventas de una tienda específica en el último año.
4. Top productos del año en curso.

Los filtros 1, 2 y 4 incluyen predicados temporales (anio, mes). El filtro 3
combina tienda y período temporal.

---

## Decisión

**Se particiona la tabla Silver por `(anio, mes)` usando PySpark `.partitionBy("anio", "mes")`.**

Con los 2.249.970 registros reales distribuidos en 99 particiones activas,
cada partición contiene ~22.727 registros en promedio. La partición de mayor
interés en los benchmarks (anio=2016, mes=3) contiene 35.391 registros,
equivalente al ~1,0% del dataset total.

```python
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("anio", "mes") \
    .save(ruta_destino_silver)
```

---

## Alternativas Consideradas

### Alternativa A: Sin partición (tabla plana)

**Descripción:** Escribir toda la tabla Silver como una colección de archivos
Parquet sin estructura de directorios de partición.

**Argumento técnico descartado:**  
Toda consulta con filtro temporal requeriría leer los 2.249.970 registros
completos. La consulta "ventas de marzo 2016" escanea el 100% de los datos
en lugar del ~1,0% (35.391 registros de 2.249.970). El benchmark del notebook
midió un scan completo de 0,24 s con Silver ya en cache; en Dataproc sobre GCS
—donde la red es el cuello de botella— la diferencia se amplifica en proporción
al volumen transferido.

**Argumento económico descartado:**  
Con BigQuery External Tables, el costo de scan es proporcional a los bytes leídos
(~USD 5/TB). Transferir 35.391 registros en lugar de 2.249.970 representa un
ahorro del 99% en costo de lectura por consulta temporal, acumulable en cada
ejecución de los jobs Gold.

### Alternativa B: Partición por `anio` solo

**Descripción:** Una partición por año calendario.

**Descartado:** Con 99 particiones activas distribuidas entre 2015 y 2023, una
partición anual agruparía meses incompletos de distintos años y reduciría la
granularidad del pruning. Las consultas del mes anterior —el caso de uso más
frecuente— seguirían escaneando el año completo (hasta 12× más registros de
los necesarios). La granularidad mensual es el punto óptimo dado el patrón
de acceso.

### Alternativa C: Partición por `(anio, mes, id_canal)`

**Descripción:** Triple partición temporal + canal de venta (POS/ecom/mayorista).

**Descartado:** Con 3 canales × 99 particiones mensuales = hasta 297 particiones
potenciales, los archivos resultantes tendrían ~7.600 registros cada uno
(2.249.970 / 297), generando archivos de tamaño inferior al óptimo (~1 MB).
El overhead de gestionar cientos de particiones pequeñas en Spark y el costo
de GCS listing no justifican la ganancia marginal para consultas que filtran
simultáneamente por canal y período, que representan un subconjunto minoritario
de los patrones de acceso.

### Alternativa D: Partición por `fecha_venta` (granularidad diaria)

**Descripción:** Una partición por día calendario.

**Descartado:** Problema de small files severo: con 2.249.970 registros
distribuidos en ~3.000 días posibles, cada partición tendría ~750 registros
(< 0,1 MB por archivo). El overhead de GCS listing con miles de directorios
degrada significativamente el rendimiento de Spark al abrir el job. La
granularidad mensual produce particiones de tamaño saludable sin este problema.

---

## Consecuencias

**Positivas:**
- Partition pruning efectivo: la consulta sobre anio=2016/mes=3 leyó solo
  35.391 registros (~1,0% del total de 2.249.970), confirmado en el notebook
  `Optimizacion_y_FinOps.ipynb`.
- Evolución natural: al añadir nuevos meses de datos, se crean nuevas
  particiones sin afectar las 99 existentes.
- Compatibilidad con BigQuery partition pruning: BQ reconoce la estructura
  de directorios `anio=X/mes=Y/` y aplica el filtro en GCS directamente,
  reduciendo el costo de scan en ~USD 5/TB proporcional a los datos evitados.

**Negativas / Limitaciones:**
- Las consultas sin predicado temporal (ej. "top productos históricos") escanean
  las 99 particiones. Se mitiga ejecutando esas consultas sobre Silver cacheado
  antes de los jobs Gold.
- En el benchmark con Silver ya en cache, el tiempo de la consulta con pruning
  (0,42 s) fue ligeramente mayor que el scan completo (0,24 s), porque el costo
  fijo de acceder a la partición superó el de leer datos ya materializados en RAM.
  El beneficio real del pruning opera sobre GCS sin cache, no sobre datos en memoria.
- Escribir datos históricos fuera de orden (ej. correcciones de meses anteriores)
  requiere reescribir la partición afectada. Se mitiga con `MERGE INTO` Delta.

**Validación empírica (notebook `Optimizacion_y_FinOps.ipynb`):**

| Escenario | Registros leídos | % del total | Tiempo medido |
|---|---|---|---|
| Scan completo (sin pruning) | 2.249.970 | 100% | 0,24 s |
| Partition pruning (anio=2016, mes=3) | 35.391 | ~1,0% | 0,42 s |

La diferencia de tiempo en favor del scan completo se explica por el contexto
experimental: Silver ya estaba materializado en cache desde el benchmark de
caching. En Dataproc sobre GCS —entorno de producción objetivo— el partition
pruning reduce en un 99% los bytes transferidos desde almacenamiento remoto,
con impacto proporcional en latencia y costo de BigQuery on-demand.
