# Plataforma Big Data para Analítica Omnicanal de Ventas Retail

Proyecto Integrador para la asignatura de Big Data & Cloud Computing del Magíster en Data Science, Universidad del Desarrollo.

## 👥 Equipo del Proyecto
* Claudio Ballerini
* Juan José Torres
* Cristian Vargas
* Christian Vásquez
* **Profesor:** Luis Castillo

## 📝 Descripción del Problema
Este proyecto resuelve el desafío de una empresa chilena de retail que opera un ecosistema omnicanal, cuya información comercial se encontraba dispersa en distintas fuentes operacionales. 

El objetivo es diseñar e implementar una plataforma Big Data capaz de integrar, procesar y analizar grandes volúmenes de información (más de 10 millones de registros transaccionales) correspondientes a ventas POS, e-commerce, inventario, clientes y promociones. Esta solución busca apoyar a la Gerencia Comercial, Operaciones y Marketing respondiendo preguntas críticas sobre rotación de productos, quiebres de stock y comportamiento de clientes.

## 🏛️ Arquitectura Propuesta
La solución se basa en una arquitectura **Medallion** implementada íntegramente sobre **Google Cloud Platform (GCP)** con un patrón de procesamiento Batch Incremental diario:

* **Capa Bronze:** Almacenamiento de datos crudos (ventas, inventario, clientes) preservando el formato original y la trazabilidad histórica en Google Cloud Storage usando Delta Lake.
* **Capa Silver:** Limpieza, normalización y deduplicación de datos procesados mediante Apache Spark.
* **Capa Gold:** Data marts analíticos optimizados con KPIs comerciales, demanda agregada y rotación.

## 🛠️ Stack Tecnológico
* **Ingesta:** Python.
* **Orquestación:** Apache Airflow / Cloud Composer.
* **Data Lake:** Google Cloud Storage.
* **Procesamiento:** Apache Spark (PySpark) ejecutado en Google Dataproc.
* **Formato de Almacenamiento:** Delta Lake (proporciona transacciones ACID y Time Travel).
* **Data Warehouse:** BigQuery.
* **Visualización:** Looker Studio.
* **Seguridad y Control:** IAM y GitHub.

## 📂 Estructura del Repositorio
```text
mi-proyecto/
├── data/               <- Datos de prueba o subconjuntos anonimizados (ignorados por git)
├── notebooks/          <- Cuadernos Jupyter con el desarrollo del pipeline
│   ├── 00_EDA_y_Calidad.ipynb      <- Análisis exploratorio y perfilamiento en PySpark
│   ├── Capa_Bronze.ipynb        <- Ingesta de datos crudos a formato Delta
│   ├── Capa_Silver.ipynb        <- Limpieza y transformación distribuida
│   ├── Capa_Gold.ipynb          <- Creación de Data Marts comerciales
│   ├── Gobernabilidadipynb      <- Control de acceso y documentación
├── src/                <- Scripts modulares de Python (en desarrollo para Fase 3)
├── requirements.txt    <- Dependencias del proyecto (pyspark, delta-spark, etc.)
├── .gitignore          <- Archivos a ignorar (datasets pesados, credenciales)
└── README.md           <- Este archivo