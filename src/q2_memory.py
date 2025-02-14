from typing import List, Tuple

def q2_memory(file_path: str) -> List[Tuple[str, int]]:
  """
  Procesa un archivo JSON de tweets, extrae emojis y retorna los 10 mas usados.

  Args:
      file_path (str): La ruta al archivo JSON.

  Returns:
      List[Tuple[str, int]]: Una lista de tuplas (emoji, conteo) representando
            los 10 emojis mas usados.
  """
  from pyspark.sql import SparkSession
  from pyspark.sql.functions import udf, explode, col
  from pyspark.sql.types import ArrayType, StringType
  import emoji
  
  try:
      spark = SparkSession.builder.appName("TweetAnalysis").getOrCreate()
      dfRaw = spark.read.json(file_path)

      # Define una UDF para extraer los emojis del texto
      def extract_emojis(text):
          if text is not None:
              return [char for char in text if emoji.is_emoji(char)]
          return []

      extract_emojis_udf = udf(extract_emojis, ArrayType(StringType()))

      # Aplica UDF para extraer emojis y explode para transformarlos en columna
      emoji_counts = (
          dfRaw
          .select(explode(extract_emojis_udf(col("content"))).alias("emoji"))
          .groupBy("emoji")
          .count()
          .orderBy(col("count").desc())
      )

      # Obtiene los top 10 y lo convierte a una lista de tuplas
      top_10_emojis = [tuple(row) for row in emoji_counts.limit(10).collect()]

      return top_10_emojis

  except FileNotFoundError:
      print(f"Error: El archivo '{file_path}' no se encuentra.")
      raise
  except Exception as e:
      print(f"Error durante el procesamiento: {e}")
      raise
  finally:
    spark.stop()
