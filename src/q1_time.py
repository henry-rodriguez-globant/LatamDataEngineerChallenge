from typing import List, Tuple
from datetime import datetime

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
  """
  Obtiene los usuarios con más tweets por día para los 10 días con mayor actividad.
  
  Esta función lee un archivo JSON de tweets, identifica los 10 días con 
  mayor cantidad de tweets y determina el usuario con más tweets para cada 
  uno de esos días. La función está optimizada para el tiempo de procesamiento 
  utilizando Spark para el análisis distribuido de datos.
  
  Args:
    file_path (str): La ruta al archivo JSON que contiene los tweets.
  
  Returns:
    List[Tuple[datetime.date, str]]: Una lista de tuplas donde cada tupla 
    contiene la fecha (datetime.date) y el nombre de usuario (str) 
    del usuario con más tweets para ese día.
  
  Raises:
    FileNotFoundError: Si el archivo especificado en `file_path` no se encuentra.
    Exception: Si ocurre cualquier otro error durante el procesamiento.
  """
  from pyspark.sql import SparkSession
  from pyspark.sql.window import Window
  from pyspark.sql.functions import col, to_date, to_timestamp, row_number, desc, col
  try:
    spark = SparkSession.builder.appName("TweetAnalysis").getOrCreate()
    dfRaw = spark.read.json(file_path)

    # Lectura y preprocesamiento del dataframe
    df = dfRaw.select(
      col("user.username").alias("username"),
      to_date(to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX")).alias("date")
      )
    
    # Cachear df para evitar recalcularlo
    df.cache()
    
    # Top 10 días con más tweets (usando reduceByKey para minimizar shuffles)
    top_dates = (
      df.rdd
        .map(lambda row: (row.date, 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda item: item[1], ascending=False)
        .take(10)
        )
    
    # Broadcast de top_dates para evitar shuffles en el filtro
    top_dates_broadcast = spark.sparkContext.broadcast([date for date, _ in top_dates])

    # Filtrar por top días y calcular el usuario con más tweets
    day_user = (
      df.filter(col("date").isin(top_dates_broadcast.value))
        .groupBy("date", "username")
        .count()
        .withColumn("rn", row_number().over(Window.partitionBy("date").orderBy(desc("count"))))
        .filter(col("rn") == 1)
        .drop("rn")
        .select("date", "username")
        )

    # Unpersist de df
    df.unpersist()
    
    # Se devuelve el resultado como una lista de tuplas.
    return [tuple(row) for row in day_user.collect()]

  except FileNotFoundError:
      print(f"Error: El archivo '{file_path}' no se encuentra.")
      raise
  except Exception as e:
      print(f"Error durante el procesamiento: {e}")
      raise
  finally: 
    spark.stop()
