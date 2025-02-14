from typing import List, Tuple

def q3_memory(file_path: str) -> List[Tuple[str, int]]:
  """
  Encuentra los 10 usuarios más mencionados en un archivo JSON de tweets usando Apache Beam.

  Args:
    file_path: La ruta al archivo JSON.

  Returns:
    Una lista de tuplas, donde cada tupla contiene el nombre de usuario y el número de menciones.
  """
  import apache_beam as beam
  import ast
  import shutil
  import json

  try:

    folder_path = "q3/"

    with beam.Pipeline() as pipeline:

      # Lee y analiza los tweets, extrae las menciones de usuarios, toma las 10 y escribe el resultado
      (
        pipeline
        | 'ReadTweets' >> beam.io.ReadFromText(file_pattern=file_path)
        | 'ParseTweets' >> beam.Map(lambda line: json.loads(line))
        | 'FilterTweets' >> beam.Filter(lambda tweet: tweet.get('mentionedUsers') is not None)
        | 'ExtractMentions' >> beam.FlatMap(lambda tweet: [user['username'] for user in tweet.get('mentionedUsers')])
        | 'CountMentions' >> beam.combiners.Count.PerElement()
        | 'SortMentions' >> beam.combiners.Top.Of(10, key=lambda x: x[1])
        | 'WriteResults' >> beam.io.WriteToText(f'{folder_path}output.txt', num_shards=1, shard_name_template="")
      )

    # Leer el archivo de texto y evaluarlo literalmente
    with open(f'{folder_path}output.txt', 'r') as f:
        text_field_content = f.read()
    list_of_tuples = ast.literal_eval(text_field_content)

    # Eliminar el archivo
    shutil.rmtree(folder_path)

    return list_of_tuples
        
  except (IOError, json.JSONDecodeError) as e:
      print(f"Error procesando el archivo: {e}")
      return []
