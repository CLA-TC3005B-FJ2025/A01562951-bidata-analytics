from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, count, sum, desc, asc

def crear_spark_session():
    """
    Crea y devuelve una SparkSession.
    """
    spark = SparkSession.builder.appName("Otras operaciones").getOrCreate()
    return spark

def cargar_datos(spark, file_path):
    """
    Carga los datos desde un archivo CSV en un DataFrame de Spark.
    """
    df = spark.read.option("header", "true").csv(file_path)
    print("Datos cargados exitosamente:")
    df.show()
    return df

def realizar_analisis(df):
    """
    Realiza varias operaciones de análisis en los datos.
    """

    # 1. Contar el número total de registros
    print("\nNúmero total de registros:")
    df.count()

    # 2. Calcular la edad media
    print("\nEdad media:")
    df.select(avg(col("edad")).alias("edad_media")).show()

    # 3. Encontrar la edad máxima
    print("\nEdad máxima:")
    df.select(max(col("edad")).alias("edad_max")).show()

    # 4. Encontrar la edad mínima
    print("\nEdad mínima:")
    df.select(min(col("edad")).alias("edad_min")).show()

    # 5. Contar el número de personas por ciudad
    print("\nNúmero de personas por ciudad:")
    df.groupBy("ciudad").count().show()

    # 6. Sumar las edades por ciudad
    print("\nSuma de edades por ciudad:")
    df.groupBy("ciudad").agg(sum(col("edad")).alias("suma_edades")).show()

    # 7. Contar el número de personas mayores de 50
    print("\nNúmero de personas mayores de 50:")
    df.filter(col("edad") > 50).count()

    # 8. Mostrar los registros ordenados por edad en orden ascendente
    print("\nRegistros ordenados por edad (ascendente):")
    df.orderBy(col("edad").asc()).show()

    # 9. Mostrar los registros ordenados por nombre en orden descendente
    print("\nRegistros ordenados por nombre (descendente):")
    df.orderBy(col("nombre").desc()).show()

    # 10. Filtrar y mostrar los registros cuyo nombre comienza con 'J'
    print("\nRegistros cuyo nombre comienza con 'J':")
    df.filter(col("nombre").startswith("J")).show()

def main():
    # Crear la sesión de Spark
    spark = crear_spark_session()

    # Ruta al archivo de datos
    file_path = 'data.csv'

    # Cargar los datos
    datos = cargar_datos(spark, file_path)

    # Realizar análisis
    realizar_analisis(datos)

    # Finalizar la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()
