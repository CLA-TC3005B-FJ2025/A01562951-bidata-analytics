from pyspark.sql import SparkSession

def crear_spark_session():
    """
    Crea y devuelve una SparkSession.
    """
    spark = SparkSession.builder \
        .appName("Practica Spark") \
        .getOrCreate()
    return spark

def cargar_datos(spark, file_path):
    """
    Carga los datos desde un archivo CSV en un DataFrame de Spark.
    """
    df = spark.read.option("header", "true").csv(file_path)
    print("Datos cargados exitosamente:")
    df.show()
    return df

def analizar_datos(df):
    """
    Realiza un análisis simple de los datos y muestra estadísticas descriptivas.
    """
    print("\nEstadísticas descriptivas:")
    df.describe().show()

    print("\nDistribución de edades:")
    df.groupBy("edad").count().show()

def main():
    # Crear la sesión de Spark
    spark = crear_spark_session()

    # Ruta al archivo de datos
    file_path = 'data1_2.csv'

    # Cargar y analizar los datos
    datos = cargar_datos(spark, file_path)
    analizar_datos(datos)

    # Finalizar la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()
