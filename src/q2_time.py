from typing import List, Tuple

def q2_time(file_path: str) -> List[Tuple[str, int]]:
  """
  Procesa un archivo JSON de tweets, extrae emojis y retorna los 10 mas usados.

  Args:
      file_path (str): La ruta al archivo JSON.

  Returns:
      List[Tuple[str, int]]: Una lista de tuplas (emoji, conteo) representando
            los 10 emojis mas usados.
  """
  import ast
  import shutil
  import apache_beam as beam
  import emoji
  import json
  
  try:
      folder_path = "q2/"
      with beam.Pipeline() as pipeline:
          # 1. Leer los tweets desde el archivo JSON
          tweets = (
              pipeline
              | "LeerTweets" >> beam.io.ReadFromText(file_path)  
              | "ParsearJSON" >> beam.Map(lambda line: json.loads(line))
          )

          # 2. Extraer los emojis de cada tweet
          emojis = (
              tweets
              | "ExtraerEmojis" >> beam.FlatMap(lambda tweet: [char for char in tweet.get('content', '') if emoji.is_emoji(char)])
          )

          # 3. Contar la frecuencia de cada emoji
          emoji_counts = (
              emojis
              | "ContarEmojis" >> beam.combiners.Count.PerElement()  
              | "FormatearSalida" >> beam.Map(lambda element: element) 
          )
          
          # 4. Obtener los 10 emojis más frecuentes
          top_10_emojis = (
              emoji_counts
              | "OrdenarEmojis" >> beam.transforms.combiners.Top.Of(10, key=lambda element: element[1])
          )

          # 5. Escribir los resultados a un archivo de texto
          top_10_emojis | 'EscribirResultados' >> beam.io.WriteToText(f'{folder_path}output.txt', num_shards=1, shard_name_template="")

      # 6. Leer el archivo de texto y evaluarlo literalmente    
      with open(f'{folder_path}output.txt', 'r') as f:
            text_field_content = f.read()
      list_of_tuples = ast.literal_eval(text_field_content)

      # 7. Eliminar el archivo
      shutil.rmtree(folder_path)
      return list_of_tuples

  except (IOError, json.JSONDecodeError) as e:
      print(f"Error procesando el archivo: {e}")
      return []
