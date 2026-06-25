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

El dataset Silver contiene ~12 millones de registros con datos entre 2016 y 2023
(aproximadamente 7 años). Las consultas analíticas más frecuentes identificadas
en el levantamiento de requerimientos son:

1. Ventas del mes anterior (filtro: `anio = X AND mes = Y`).
2. Ventas del último trimestre (filtro: `anio = X AND mes BETWEEN A AND B`).
3. Ventas de una tienda específica en el último año.
4. Top productos del año en curso.

Los filtros 1, 2 y 4 incluyen predicados temporales (anio, mes). El filtro 3
combina tienda y período temporal.

---

## Decisión

**Se particiona la tabla Silver por `(anio, mes)` usando PySpark `.partitionBy("anio", "mes")`.**

Con datos históricos de 7 años × 12 meses = máximo 84 particiones físicas.
Cada partición contiene ~142.000 registros en promedio (12M / 84), lo que
produce archivos Parquet de ~15–25 MB por partición (por encima del umbral
mínimo recomendado de 10 MB para evitar small files).

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
Toda consulta con filtro temporal requeriría leer los ~12 millones de registros
completos. La consulta "ventas de marzo 2016" escanea el 100% de los datos
en lugar del ~1.2% (1 partición / 84 total). A medida que el dataset crece,
este impacto se amplifica linealmente.

**Argumento económico descartado:**  
Con BigQuery External Tables, el costo de scan es proporcional a los bytes leídos.
Leer 12 millones vs 142.000 registros implica una diferencia de ~84× en costo
por consulta para los casos de uso más frecuentes.

### Alternativa B: Partición por `anio` solo

**Descripción:** Una partición por año calendario (7 particiones para el dataset actual).

**Descartado:** Las particiones anuales tendrían ~1.7 millones de registros cada una.
Las consultas del mes anterior (caso de uso más frecuente) seguirían escaneando
el año completo (12× más de lo necesario). La granularidad mensual es el punto
óptimo dado el patrón de acceso.

### Alternativa C: Partición por `(anio, mes, id_canal)`

**Descripción:** Triple partición temporal + canal de venta (POS/ecom/mayorista).

**Descartado:** Con 3 canales × 84 meses = 252 particiones, los archivos
resultantes tendrían ~47.000 registros cada uno (~5 MB), rozando el límite
inferior de tamaño óptimo de archivo. Añadir id_canal como partición solo
beneficia consultas que filtran por canal, que representan un subconjunto
minoritario de los patrones de acceso. El overhead de gestionar 252 particiones
en Spark (más etapas de shuffle, más overhead de listing en GCS) no justifica
la ganancia marginal.

### Alternativa D: Partición por `fecha_venta` (granularidad diaria)

**Descripción:** Una partición por día calendario (~2.555 particiones en 7 años).

**Descartado:** Problema de small files severo: ~4.700 registros por partición
(< 1 MB por archivo). El overhead de GCS listing con miles de directorios degrada
significativamente el rendimiento de Spark al abrir el job. Solución common en
producción: usar `OPTIMIZE` periódico de Delta para compactar, pero añade
complejidad operacional sin beneficio real vs la partición mensual.

---

## Consecuencias

**Positivas:**
- Partition pruning efectivo: consultas mensuales leen 1/84 del total.
- Tamaño de archivos saludable: ~15–25 MB por partición (rango óptimo 10–128 MB).
- Evolución natural: al añadir nuevos meses, se crean nuevas particiones
  sin afectar las existentes.
- Compatibilidad con BigQuery partition pruning: BQ reconoce la estructura
  de directorios `anio=X/mes=Y/` y aplica el filtro en GCS directamente.

**Negativas / Limitaciones:**
- Las consultas que no incluyen filtro temporal (ej. "top productos de todos
  los tiempos") escanean todas las particiones, sin beneficio de pruning.
  Estas consultas son menos frecuentes y su costo es aceptable.
- Escribir datos históricos fuera de orden (ej. correcciones de meses anteriores)
  requiere reescribir la partición afectada. Se mitiga con `MERGE INTO` Delta.

**Validación empírica:**  
El notebook `Capa_Silver.ipynb` evidenció que la escritura particionada de
~12 millones de registros tomó 45.7 segundos, generando 84 directorios de
partición con archivos de tamaño dentro del rango óptimo. La consulta de
validación Silver con filtro `anio = 2016 AND mes = 3` completó en < 5 segundos,
consistente con un scan de ~1% del dataset total.
