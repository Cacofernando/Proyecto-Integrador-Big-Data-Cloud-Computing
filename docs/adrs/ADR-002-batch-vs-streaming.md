# ADR-002: Patrón de Procesamiento Batch Incremental Diario

**Estado:** Aceptado  
**Fecha:** Mayo 2026  
**Autores:** Ballerini · Torres · Vargas · Vásquez  
**Revisado por:** Prof. Luis Castillo Faune  

---

## Contexto

El sistema debe procesar datos de ventas de una cadena retail omnicanal con
operaciones en tiendas físicas (POS) y canal e-commerce. El volumen inicial
es de ~12 millones de registros históricos, con un crecimiento diario estimado
de ~33.000 nuevas transacciones (basado en ~285.000 registros/año en el dataset
de e-commerce disponible).

Los stakeholders identificados y sus necesidades de latencia son:

| Stakeholder         | Necesidad                        | Latencia aceptable |
|---------------------|----------------------------------|--------------------|
| Gerencia Comercial  | Dashboard de ventas del día anterior | < 24 horas    |
| Operaciones         | Alertas de quiebre de stock      | < 24 horas         |
| Marketing           | Análisis de campañas semanales   | 24–72 horas        |
| Finanzas            | Cierre contable mensual          | Mes + 2 días       |

Ningún stakeholder requiere visibilidad en tiempo real (subsegundo). El caso
de uso de alertas de fraude en tiempo real (< 200 ms) corresponde al Sistema A
del enunciado; nuestro sistema se enfoca en analítica, no en transacciones.

---

## Decisión

**Se adopta un patrón de procesamiento Batch Incremental Diario.**

El pipeline se ejecuta una vez al día a las 02:00 UTC (23:00 CLT), fuera del
horario de mayor actividad de las tiendas, procesando todos los registros nuevos
del día anterior y actualizando los data marts Gold.

**Modo de carga:** Overwrite completo de Silver y Gold en la implementación actual
(Fase 2). La migración a carga incremental (MERGE/upsert) se documenta como
evolución futura en la sección de Consecuencias.

---

## Alternativas Consideradas

### Alternativa A: Streaming con Apache Kafka + Spark Structured Streaming

**Descripción:** Arquitectura Lambda o Kappa con ingesta en tiempo real desde
los sistemas POS y e-commerce a través de Kafka (o Pub/Sub en GCP), procesamiento
con Structured Streaming y actualización continua de los data marts.

**Argumento técnico descartado:**  
El problema no requiere tiempo real. Introducir streaming añade complejidad
operacional significativa: gestión de offsets Kafka, manejo de late-arriving data,
estado de los microaggregators, y testing de pipelines stateful. El costo de
operación de un clúster Kafka en GCP (mínimo 3 brokers) excede en 10× el costo
del batch, con beneficios nulos para los stakeholders actuales.

Además, los sistemas POS de retail en Chile no suelen exponer un feed de eventos
en tiempo real; la integración más común es mediante extracción nocturna de archivos
CSV desde el sistema ERP (SAP, Retail Pro), lo que hace inviable el streaming
sin refactorizar los sistemas fuente.

**Argumento económico descartado:**  
Un clúster Kafka de 3 nodos n1-standard-2 en GCP cuesta ~USD 300/mes continuo.
El batch ephemeral cuesta USD 9/mes (estimación FinOps: 1.20 USD/h × 0.25h × 30
ejecuciones). El ratio costo/beneficio de streaming es 33× peor para este caso.

### Alternativa B: Arquitectura Lambda (batch + streaming simultáneos)

**Descripción:** Mantener el pipeline batch para datos históricos y añadir una
capa speed (streaming) para datos del día en curso, combinando ambos en la capa
de servicio.

**Descartado:** Mismas razones de costo que Alternativa A, con la complejidad
adicional de mantener dos pipelines que deben producir resultados coherentes.
La deuda técnica de una arquitectura Lambda supera los beneficios en un contexto
donde 24h de latencia es aceptable.

### Alternativa C: Batch Semanal o Mensual

**Descripción:** Ejecutar el pipeline solo una vez por semana o al cierre de mes.

**Descartado:** Insuficiente para el caso de uso de Operaciones (alertas de stock)
y para el seguimiento diario de ventas de la Gerencia Comercial. La diferencia
de costo entre batch diario y semanal es marginal (USD 9/mes vs USD 2/mes),
mientras que el impacto en utilidad del sistema es sustancial.

---

## Consecuencias

**Positivas:**
- Simplicidad de implementación y operación: un DAG de Airflow con 6 tareas.
- Costo predecible y bajo: USD 9–12/mes en cómputo.
- Tolerancia a fallos simple: ante un fallo, se re-ejecuta el DAG completo.
  Delta Lake garantiza consistencia con el overwrite transaccional.
- Sin estado que gestionar entre ejecuciones (hasta implementar incremental).

**Negativas / Limitaciones:**
- Latencia máxima de 24 horas: los datos de ventas del día están disponibles
  a las ~02:30 CLT del día siguiente. Esto es aceptable para los stakeholders
  actuales pero limita casos de uso reactivos intradiarios.
- La carga completa (overwrite) de Silver en cada ejecución es ineficiente
  para el largo plazo: a medida que el dataset histórico crece, el tiempo de
  procesamiento aumenta linealmente.

**Deuda técnica identificada:**
- Migrar Silver de overwrite a carga incremental usando `MERGE INTO` de Delta Lake:
  ```sql
  MERGE INTO silver.venta_tiendas AS target
  USING nuevas_ventas AS source
  ON target.numero_transaccion = source.numero_transaccion
     AND target.id_producto = source.id_producto
  WHEN NOT MATCHED THEN INSERT *
  WHEN MATCHED AND source.venta != target.venta THEN UPDATE SET *;
  ```
  Prioridad: alta. Plazo estimado: cuando el tiempo de procesamiento supere
  30 minutos (proyectado para ~50 millones de registros, ~4 años de operación).

**Evolución hacia Near Real-Time (si el negocio lo requiere):**
- Migrar a arquitectura Kappa: Pub/Sub → Dataflow → Delta Lake.
- Requiere que los sistemas POS expongan un feed de eventos (webhook o CDC).
- Costo estimado incremental: +USD 80–120/mes en Dataflow y Pub/Sub.
