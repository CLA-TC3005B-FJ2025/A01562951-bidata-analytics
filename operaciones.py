from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def crear_spark_session():
    """
    Crea y devuelve una SparkSession.
    """
    spark = SparkSession.builder \
        .appName("Práctica avanzada") \
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

def filtrar_datos(df):
    """
    Filtra los datos para mostrar solo los registros donde la edad es mayor a 30.
    """
    print("\nDatos filtrados (edad > 30):")
    df_filtrado = df.filter(col("edad") > 30)
    df_filtrado.show()
    return df_filtrado

def agrupar_y_agrupar(df):
    """
    Agrupa los datos por ciudad y realiza una agregación para contar los registros por ciudad.
    """
    print("\nConteo de registros por ciudad:")
    df_agrupado = df.groupBy("ciudad").count()
    df_agrupado.show()
    return df_agrupado

def ordenar_datos(df):
    """
    Ordena los datos por edad en orden descendente.
    """
    print("\nDatos ordenados por edad (descendente):")
    df_ordenado = df.orderBy(col("edad").desc())
    df_ordenado.show()
    return df_ordenado

def main():
    # Crear la sesión de Spark
    spark = crear_spark_session()

    # Ruta al archivo de datos
    file_path = 'data.csv'

    # Cargar los datos
    datos = cargar_datos(spark, file_path)

    # Filtrar datos
    datos_filtrados = filtrar_datos(datos)

    # Agrupar y agregar datos
    datos_agrupados = agrupar_y_agrupar(datos)

    # Ordenar datos
    datos_ordenados = ordenar_datos(datos)

    # Finalizar la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()

