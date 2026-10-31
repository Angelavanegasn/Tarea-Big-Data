# Análisis Batch de sensores ambientales con Apache Spark

## Descripción

Este proyecto implementa un procesamiento Batch de lecturas de sensores ambientales utilizando Apache Spark y PySpark. El programa carga un archivo CSV, inspecciona su estructura, identifica valores nulos, elimina registros duplicados e incompletos, valida rangos de temperatura y humedad y genera estadísticas descriptivas y un resumen por sensor.

## Tecnologías

- Python 3
- Apache Spark 3.5.0
- PySpark
- Ubuntu Server 22.04 (entorno de desarrollo)

## Estructura del repositorio

```text
.
├── data/
│   └── sensor_data.csv
├── output/                  # Se crea al ejecutar el programa
├── spark_batch_analysis.py
└── README.md
```

## Requisitos

- Java 11
- Python 3
- Apache Spark 3.5.0
- PySpark compatible con Spark 3.5.0

En el entorno utilizado para desarrollar y ejecutar el proyecto, Spark está instalado en `/opt/spark`.

## Ejecución

1. Clonar el repositorio:

   ```bash
   git clone URL_DE_TU_REPOSITORIO
   cd NOMBRE_DEL_REPOSITORIO
   ```

2. Verificar que Spark esté disponible:

   ```bash
   spark-submit --version
   ```

3. Ejecutar el análisis desde la carpeta raíz:

   ```bash
   spark-submit spark_batch_analysis.py
   ```

   Si `spark-submit` no está en el PATH, se puede ejecutar con la ruta de instalación:

   ```bash
   /opt/spark/bin/spark-submit spark_batch_analysis.py
   ```

## Descripción del procesamiento

1. **Carga:** lectura de `data/sensor_data.csv` con encabezados e inferencia del esquema.
2. **Inspección:** visualización de registros, esquema y cantidad inicial de filas.
3. **Detección de nulos:** conteo de valores nulos en `sensor_id`, `temperatura`, `humedad` y `fecha`.
4. **Limpieza de duplicados:** eliminación de filas completamente repetidas.
5. **Limpieza de incompletos:** eliminación de registros que tengan nulos en las columnas requeridas.
6. **Validación:** conservación de temperaturas entre 0 y 40 °C y humedad entre 0 y 100 %.
7. **Análisis:** cálculo de estadísticas descriptivas y agregaciones por sensor: total de lecturas, temperatura promedio, máxima y mínima, y humedad promedio.
8. **Exportación:** escritura de los datos limpios y del resumen por sensor en la carpeta `output/`, en formato CSV.

## Datos de entrada

El archivo de ejemplo contiene lecturas simuladas de tres sensores (`S001`, `S002` y `S003`). Incluye intencionalmente un registro duplicado, campos nulos y una temperatura fuera del rango definido para demostrar las etapas de limpieza.

Los rangos de temperatura (0 a 40 °C) y humedad (0 a 100 %) son reglas de validación definidas para este ejercicio académico; no representan una calibración universal de sensores.

## Resultados

Al finalizar, se crean dos directorios de resultados:

- `output/datos_limpios/`: registros que superaron las validaciones.
- `output/resumen_por_sensor/`: métricas agrupadas por sensor.

Spark escribe cada resultado como una carpeta que contiene uno o más archivos `part-*.csv` y un archivo `_SUCCESS`.

## Autor

Estudiante: [Escribe aquí tu nombre]
Curso: Big Data
Universidad: UNAD
