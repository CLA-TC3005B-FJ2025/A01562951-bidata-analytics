from pyspark.sql import SparkSession

def crear_spark_session():
    """
    Crea y devuelve una SparkSession.
    """
    spark = SparkSession.builder \
        .appName("Practica BigData Spark SQL") \
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

def registrar_temp_view(df, view_name):
    """
    Registra el DataFrame como una vista temporal para usar con Spark SQL.
    """
    df.createOrReplaceTempView(view_name)

def ejecutar_consultas(spark):
    """
    Ejecuta varias consultas SQL usando la vista temporal.
    """
    print("\nConsulta: Contar registros por ciudad")
    resultado = spark.sql("SELECT ciudad, COUNT(*) as conteo FROM data_view GROUP BY ciudad")
    resultado.show()

    print("\nConsulta: Edad media por ciudad")
    resultado = spark.sql("SELECT ciudad, AVG(edad) as edad_media FROM data_view GROUP BY ciudad")
    resultado.show()

    print("\nConsulta: Registros donde edad > 30 ordenados por edad")
    resultado = spark.sql("SELECT * FROM data_view WHERE edad > 30 ORDER BY edad DESC")
    resultado.show()

def main():
    # Crear la sesión de Spark
    spark = crear_spark_session()

    # Ruta al archivo de datos
    file_path = 'data1_2.csv'

    # Cargar los datos
    datos = cargar_datos(spark, file_path)

    # Registrar vista temporal
    registrar_temp_view(datos, "data_view")

    # Ejecutar consultas SQL
    ejecutar_consultas(spark)

    # Finalizar la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()
