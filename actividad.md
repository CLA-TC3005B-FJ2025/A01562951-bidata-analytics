# Actividad: Creación y Análisis de un Nuevo Conjunto de Datos con PySpark

## Objetivo

El objetivo de esta actividad es que los estudiantes apliquen lo aprendido sobre Spark y PySpark mediante la creación de un nuevo conjunto de datos y su posterior análisis. Los estudiantes generarán un nuevo conjunto de datos y realizarán varias operaciones de análisis utilizando PySpark. 

## Actividad

### 1. Preparación del Entorno

1. Instala las librerías que necesites de python (si es que abriste de nuevo el codespace)

### 2. Crear Datos Nuevos

1. Crea un nuevo archivo llamado `generate_new_data.py`.
2. Escribe un programa en Python para generar un nuevo conjunto de datos con 100,000 registros con las siguientes columnas: nombre, nickname, país, comida favorita y número de la suerte. Utiliza la librería `faker` para generar datos ficticios.
       **TIP:** `faker` tiene una función  `user_name() ` y  `country() `, pero NO una para comida favorita, por lo que tendrás que hacer una función aleatoria para la siguiente lista en python:
   
   ```python
   comidas_favoritas = ["pizza", "pasta", "sushi", "tacos", "hamburguesa", "ensalada", "pollo", "chocolate", "helado", "fruta"]
	```

3. Ejecuta el programa `generate_new_data.py` para generar el archivo `nuevos_datos.csv`.

```bash
python generate_new_data.py
```

### 3. Cargar y Analizar los Datos con PySpark

1. Crea un nuevo archivo llamado `analyze_new_data.py`.

2. En este archivo, escribe el código para crear una SparkSession, cargar los datos desde `nuevos_datos.csv` y realizar algunas operaciones

```python
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
    file_path = 'nuevos_datos.csv'

    # Cargar los datos
    df = cargar_datos(spark, file_path)

    # Contar el número total de registros
    total_registros = df.count()
    print(f"Número total de registros: {total_registros}")

    # Realizar más análisis
    # b. Contar el número de registros por país, mostrar los primeros 15
    print("\nNúmero de registros por país:")
    df.groupBy("pais").count().show(15)

    # c. Encontrar el valor máximo y mínimo del número de la suerte
    from pyspark.sql.functions import max, min
    max_numero_suerte = df.select(max("numero_de_la_suerte")).first()[0]
    min_numero_suerte = df.select(min("numero_de_la_suerte")).first()[0]
    print(f"Máximo número de la suerte: {max_numero_suerte}")
    print(f"Mínimo número de la suerte: {min_numero_suerte}")

    # d. Filtrar y mostrar los registros donde la comida favorita es 'pizza'
    print("\nRegistros donde la comida favorita es 'pizza':")
    df.filter(df.comida_favorita == 'pizza').show()

    # e. Ordenar y mostrar los registros por el número de la suerte en orden descendente
    print("\nRegistros ordenados por número de la suerte (descendente):")
    df.orderBy(df.numero_de_la_suerte.desc()).show()

    # Finalizar la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()
```

3. Ejecuta el script `analyze_new_data.py` y observa los resultados en la terminal.

```bash
python analyze_new_data.py
```
### 4. Modificaciones al código

1. Modifica el archivo `analyze_new_data.py` para incluir tres nuevas consultas:
    - Contar el número de registros por nickname
    - Encontrar la comida favorita más común
    - Calcular el promedio del número de la suerte por país
    - Encontrar el nickname más común

### 5. Preguntas de Reflexión

Después de completar los ejercicios anteriores, genera el archivo `respuestas.md` y responde las siguientes preguntas de reflexión:

1. ¿Cómo te ayuda PySpark a manejar grandes volúmenes de datos?
2. ¿Qué ventajas encontraste al usar PySpark para realizar análisis de datos en comparación con otras herramientas que conoces?
3. ¿Qué otras operaciones de análisis podrías realizar con el nuevo conjunto de datos?

## Entrega

* Una vez que hayas completado la actividad, guarda tus cambios.
* Asegúrate que tus archivos `respuestas.md`, `generate_new_data.py` y `analyze_new_data.py` estén en el repositorio de GitHub. (commit + sync changes)
* Asegúrate de que el archivo `nuevos_datos.csv` también esté incluido en el repositorio.
* Entrega la liga del repositorio

