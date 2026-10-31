"""
Análisis Batch de datos de sensores ambientales con Apache Spark (PySpark).

Entrada:
    data/sensor_data.csv
Columnas esperadas:
    sensor_id, temperatura, humedad, fecha

Proceso:
    1. Carga el CSV.
    2. Inspecciona el esquema y los datos originales.
    3. Cuenta valores nulos.
    4. Elimina registros duplicados.
    5. Elimina filas con campos requeridos nulos.
    6. Filtra temperaturas fuera del rango 0-40 °C y humedad fuera de 0-100 %.
    7. Calcula estadísticas descriptivas y métricas agrupadas por sensor.

Ejecución desde la carpeta raíz:
    spark-submit spark_batch_analysis.py
"""

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    max as spark_max,
    min as spark_min,
)

BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / "data" / "sensor_data.csv"
OUTPUT_DIR = BASE_DIR / "output"


def main():
    spark = (
        SparkSession.builder
        .appName("AnalisisBatchSensoresAmbientales")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        if not INPUT_FILE.exists():
            raise FileNotFoundError(
                f"No se encontró el archivo de entrada: {INPUT_FILE}"
            )

        print("\n=== 1. CARGA DE DATOS ===")
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(INPUT_FILE))
        )

        df.printSchema()
        df.show(truncate=False)
        total_original = df.count()
        print(f"Registros originales: {total_original}")

        columnas_requeridas = ["sensor_id", "temperatura", "humedad", "fecha"]
        faltantes = [c for c in columnas_requeridas if c not in df.columns]
        if faltantes:
            raise ValueError(
                f"Faltan columnas requeridas en el CSV: {', '.join(faltantes)}"
            )

        print("\n=== 2. VALORES NULOS ===")
        for nombre in columnas_requeridas:
            cantidad = df.filter(col(nombre).isNull()).count()
            print(f"{nombre}: {cantidad} valores nulos")

        print("\n=== 3. ELIMINACIÓN DE DUPLICADOS ===")
        df_sin_duplicados = df.dropDuplicates()
        print(f"Registros después de quitar duplicados: {df_sin_duplicados.count()}")

        print("\n=== 4. ELIMINACIÓN DE REGISTROS INCOMPLETOS ===")
        df_completo = df_sin_duplicados.dropna(subset=columnas_requeridas)
        print(f"Registros completos: {df_completo.count()}")

        print("\n=== 5. VALIDACIÓN DE RANGOS ===")
        df_limpio = df_completo.filter(
            (col("temperatura") >= 0)
            & (col("temperatura") <= 40)
            & (col("humedad") >= 0)
            & (col("humedad") <= 100)
        )
        print(f"Registros válidos después de filtrar rangos: {df_limpio.count()}")

        print("\n=== 6. ESTADÍSTICAS DESCRIPTIVAS ===")
        df_limpio.select("temperatura", "humedad").describe().show()

        print("\n=== 7. RESUMEN POR SENSOR ===")
        resumen = (
            df_limpio.groupBy("sensor_id")
            .agg(
                count("*").alias("total_lecturas"),
                avg("temperatura").alias("temp_promedio"),
                spark_max("temperatura").alias("temp_maxima"),
                spark_min("temperatura").alias("temp_minima"),
                avg("humedad").alias("humedad_promedio"),
            )
            .orderBy("sensor_id")
        )
        resumen.show(truncate=False)

        print("\n=== 8. GUARDADO DE RESULTADOS ===")
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        (
            df_limpio.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(OUTPUT_DIR / "datos_limpios"))
        )
        (
            resumen.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(OUTPUT_DIR / "resumen_por_sensor"))
        )
        print(f"Resultados guardados en: {OUTPUT_DIR}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
