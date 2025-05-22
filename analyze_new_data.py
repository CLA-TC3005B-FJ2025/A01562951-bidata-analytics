from pyspark.sql import SparkSession

def crear_spark_session():
    """
    Crea y devuelve una SparkSession.
    """
    spark = SparkSession.builder.appName("Analisis de Nuevos Datos").getOrCreate()
    return spark

def cargar_datos(spark, file_path):
    """
    Carga los datos desde un archivo CSV en un DataFrame de Spark.
    """
    df = spark.read.option("header", "true").csv(file_path)
    print("Datos cargados exitosamente:")
    df.show()
    return df

def main():
    # Crear la sesión de Spark
    spark = crear_spark_session()

    # Ruta al archivo de datos
    file_path = 'data.csv'

    # Cargar los datos
    df = cargar_datos(spark, file_path)


    # Contar el número total de registros
    total_registros = df.count()
    print(f"Número total de registros: {total_registros}")

    # Contar número de registros por nickname
    def nicknameCounts(df):
        nickname_counts = df.groupBy("nickname").count()
        return nickname_counts

    result_df = nicknameCounts(df)
    result_df.show()

    def commonFood(df):
        food_counts = df.groupBy("favorite_food").count()
        common_food = food_counts.orderBy("count", ascending=False).first()
        return common_food
    
    print(f"La comida más común es {commonFood(df)}")
    
    def luckyNumberPerCountryAverage(df):
        avg_lucky_number = df.groupBy("country").agg({"lucky_number": "avg"})
        return avg_lucky_number
    avg_lucky_number_df = luckyNumberPerCountryAverage(df)
    avg_lucky_number_df.show()

    def mostCommonNickname(df):
        nickname_counts = df.groupBy("nickname").count()
        most_common_nickname = nickname_counts.orderBy("count", ascending=False).first()
        return most_common_nickname
    
    print(f"El nickname más común es {mostCommonNickname(df)}")

    """
    # Realizar más análisis
    # b. Contar el número de registros por país, mostrar los primeros 15
    print("\nNúmero de registros por país:")
    df.groupBy("country").count().show(15)

    # c. Encontrar el valor máximo y mínimo del número de la suerte
    from pyspark.sql.functions import max, min
    max_numero_suerte = df.select(max("lucky")).first()[0]
    min_numero_suerte = df.select(min("numero_de_la_suerte")).first()[0]
    print(f"Máximo número de la suerte: {max_numero_suerte}")
    print(f"Mínimo número de la suerte: {min_numero_suerte}")

    # d. Filtrar y mostrar los registros donde la comida favorita es 'pizza'
    print("\nRegistros donde la comida favorita es 'pizza':")
    df.filter(df.comida_favorita == 'pizza').show()

    # e. Ordenar y mostrar los registros por el número de la suerte en orden descendente
    print("\nRegistros ordenados por número de la suerte (descendente):")
    df.orderBy(df.numero_de_la_suerte.desc()).show()
    """
    # Finalizar la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()