from typing import List, Tuple

def q3_time(file_path: str) -> List[Tuple[str, int]]:
  """
  Encuentra los 10 usuarios más mencionados en un archivo JSON de tweets.

  Args:
      file_path: La ruta al archivo JSON.

  Returns:
      Una lista de tuplas, donde cada tupla contiene el nombre de usuario y el número de menciones.
  """
  import json
  from collections import Counter
  user_mentions = Counter()
  try:

    # Abrir el archivo
    with open(file_path, 'r', encoding='utf-8') as file:
      
      # Recorer linea por linea
      for line in file:

        try:
          #Cargar la linea como Json
          tweet = json.loads(line)

          #Verifica si hay mentionedUsers en el tweet
          #Verifica si es una lista, es decir que no sea null
          if 'mentionedUsers' in tweet and isinstance(tweet['mentionedUsers'], list):
            
            #Recorre los usuarios mencionados en cada Tweet
            for user in tweet['mentionedUsers']:

              #Verifica si el usuario tiene un nombre de usuario
              if 'username' in user:
                  
                  #Lo agrega al contador
                  user_mentions[user['username']] += 1                    
        except json.JSONDecodeError as e:
            print(f"Linea de Json invalida, pasa a la siguiente: {e}")
  except FileNotFoundError:
    print(f"Archivo no encontrado {file_path}")
    raise
  except Exception as e:
    print(f"Error inesperado: {e}")
    raise

  #Retorna los 1o usuarios mas mencionados
  return user_mentions.most_common(10)
