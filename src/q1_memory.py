from typing import List, Tuple
from datetime import datetime

def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
  """
  Obtiene los usuarios con más tweets por día para los 10 días con mayor actividad.
  
  Esta función lee un archivo JSON de tweets, identifica los 10 días con 
  mayor cantidad de tweets y determina el usuario con más tweets para cada 
  uno de esos días. La función está optimizada para la memoria usada en el  
  procesamiento utilizando Spark para el análisis distribuido de datos.
  
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
  from pyspark.sql.functions import col, to_date, to_timestamp, row_number, desc, col
  from pyspark.sql.window import Window
  from pyspark.sql.utils import AnalysisException

  try:

    spark = SparkSession.builder.appName("TweetAnalysis").getOrCreate()
    dfRaw = spark.read.json(file_path)

    # Se extrae la fecha y el usuario, convirtiendo la fecha al formato correcto.
    df = dfRaw.select(
        col("user.username").alias("username"),
        to_date(to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ssXXX")).alias("date"),
      )

    # Se obtienen los top N días con más tweets.
    top_dates = (
        df.groupBy("date")
        .count()
        .orderBy(desc("count"))
        .select("date")
        .limit(10)
        .rdd.flatMap(lambda x: x)
        .collect()
      )

    # Se define la ventana para obtener el usuario con más tweets por día.
    window_spec = Window.partitionBy("date").orderBy(desc("count"))

    # Se filtra por los top N días, se agrupa por fecha y usuario, se cuenta
    # y se selecciona el usuario con más tweets (rn = 1).
    day_user = (
      df.filter(col("date").isin(top_dates))
      .groupBy("date", "username")
      .count()
      .select(col("date"), col("username"), row_number().over(window_spec).alias("rn"))
      .filter(col("rn") == 1)
      .drop("rn")
    )

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
