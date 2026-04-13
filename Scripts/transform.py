import os
import json
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def extract_id(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logger.info("Extrayendo el id de la data")
        df["id"] = df["url"].str.extract(r'(\d+)').astype('Int64')
    except Exception as e:
        logger.error(f"Error al extraer el id de la data: {e}")
        return None
    return df



def clean_people(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logger.info("Limpiando la data de people_raw")
        df['height'] = pd.to_numeric(df['height'], errors='coerce')
        df['mass'] = pd.to_numeric(df['mass'], errors='coerce')
        df['homeworld'] = df['homeworld'].apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x))
        df['homeworld_id'] = df['homeworld'].str.extract(r'(\d+)').astype('Int64')
        df['species_id'] = df['species'].apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x))
        df['species_id'] = df['species_id'].str.extract(r'(\d+)').astype('Int64')
        df['films_id'] = df['films'].apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x))
        df['films_id'] = df['films_id'].str.extract(r'(\d+)').astype('Int64')
        df["id"] = df["url"].str.extract(r'(\d+)').astype('Int64')
        df = df.replace("unknown", None)
        df = df.replace("n/a", None)
        df = df.drop(columns=["url", "species", "films"])
        df.drop_duplicates(subset=['id'])
    except Exception as e:
        logger.error(f"Error al limpiar la data de people_raw: {e}")
        return None
    return df

def clean_planets(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logger.info("Limpiando la data de planets_raw")
        df['rotation_period'] = pd.to_numeric(df['rotation_period'], errors='coerce')
        df['orbital_period'] = pd.to_numeric(df['orbital_period'], errors='coerce')
        df['diameter'] = pd.to_numeric(df['diameter'], errors='coerce')
        df['surface_water'] = pd.to_numeric(df['surface_water'], errors='coerce')
        df['population'] = pd.to_numeric(df['population'], errors='coerce')
        df['residents'] = df['residents'].apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x))
        df['residents_id'] = df['residents'].str.extract(r'(\d+)').astype('Int64')
        df['films_id'] = df['films'].apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x))
        df['films_id'] = df['films_id'].str.extract(r'(\d+)').astype('Int64')
        df["id"] = df["url"].str.extract(r'(\d+)').astype('Int64')
        df = df.replace("unknown", None)
        df = df.replace("n/a", None)
        df = df.drop(columns=["url", "residents", "films"])
        df.drop_duplicates(subset=['id'])
    except Exception as e:
        logger.error(f"Error al limpiar la data de planets_raw: {e}")
        return None
    return df

def transformData(jsonName: str) -> pd.DataFrame:
    try:
        if jsonName == "people_raw":
            columns = ["name", "height", "mass", "homeworld", "films", "species", "url"]
        elif jsonName == "planets_raw":
            columns = ["name", "rotation_period", "orbital_period", "diameter", "climate", "gravity", "terrain", "surface_water", "population", "residents", "films", "created", "edited", "url"]
        else:
            logger.warning(f"No se encontró el archivo {jsonName}.json")
            return None
            
        print(jsonName)
        logger.info(f"Inicio de transformación de {jsonName}")
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        ruta = os.path.join(base_dir, "Data", "bronze", jsonName + ".json")
        with open(ruta,'r',encoding='utf-8') as f:
            data = json.load(f)
        #Se lee el json y se recorre para poder usar la data ya que llego un forma [[{},....,{}]]    
        flat_data = [lista for sublista in data  for lista in sublista]
        df = pd.DataFrame(flat_data)

        df = df[columns]

        if jsonName == "people_raw":
            df = clean_people(df)
        elif jsonName == "planets_raw":
            df = clean_planets(df)
        else:
            logger.warning(f"No se encontró el archivo {jsonName}.json")
            return None

        print(df.head())
        print(df.info())
        logger.info(f"Transformación de {jsonName} completada exitosamente")
        return df  
    except FileNotFoundError:
        logger.error(f"El archivo no se encontró: {ruta}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"El archivo {jsonName}.json contiene JSON inválido. Error: {e}")
        return None
    except ValueError as e:
        logger.error(f"Error al crear el DataFrame con los datos de {jsonName}. Error: {e}")
        return None
    except Exception as e:
        logger.error(f"Error crítico o desconocido al transformar la data de {jsonName}: {e}")
        return None