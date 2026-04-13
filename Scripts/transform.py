import os
import json
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def clean_url_list(url_string) -> list:
    try:
        logger.info(f"Limpiando la data de url_list: {url_string}")
        if isinstance(url_string, list):
            return [int(url.rstrip("/").split("/")[-1]) for url in url_string if url]
        elif isinstance(url_string, str) and url_string != "":
            return [int(url_string.rstrip("/").split("/")[-1])]
        return []
    except Exception as e:
        logger.error(f"Error al limpiar la data de url_list: {e}")
        return []

def clean_people(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logger.info("Limpiando la data de people_raw")
        df['height'] = pd.to_numeric(df['height'], errors='coerce')
        df['mass'] = pd.to_numeric(df['mass'], errors='coerce')
        df["id"] = df["url"].str.extract(r'(\d+)').astype('Int64')
        
        df['homeworld_id'] = df['homeworld'].apply(clean_url_list)
        df_homeworld_bridge = df[['id', 'homeworld_id']].explode('homeworld_id')
        df_homeworld_bridge = df_homeworld_bridge.dropna().astype('Int64')
        df['species_id'] = df['species'].apply(clean_url_list)
        df_species_bridge = df[['id', 'species_id']].explode('species_id')
        df_species_bridge = df_species_bridge.dropna().astype('Int64')

        df['films_id'] = df['films'].apply(clean_url_list)
        df_films_bridge = df[['id', 'films_id']].explode('films_id')
        df_films_bridge = df_films_bridge.dropna().astype('Int64')

        df = df.replace("unknown", None)
        df = df.replace("n/a", None)
        df = df.drop(columns=["url", "species", "films", "homeworld"])
        df = df.drop(columns=["homeworld_id", "species_id", "films_id"])
        
        df = df.drop_duplicates(subset=['id'])
    except Exception as e:
        logger.error(f"Error al limpiar la data de people_raw: {e}")
        return None
    return df, df_homeworld_bridge, df_species_bridge, df_films_bridge

def clean_planets(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logger.info("Limpiando la data de planets_raw")
        df['rotation_period'] = pd.to_numeric(df['rotation_period'], errors='coerce')
        df['orbital_period'] = pd.to_numeric(df['orbital_period'], errors='coerce')
        df['diameter'] = pd.to_numeric(df['diameter'], errors='coerce')
        df['surface_water'] = pd.to_numeric(df['surface_water'], errors='coerce')
        df['population'] = pd.to_numeric(df['population'], errors='coerce')
        
        df["id"] = df["url"].str.extract(r'(\d+)').astype('Int64')

        df['residents_id'] = df['residents'].apply(clean_url_list)
        df_residents_bridge = df[['id', 'residents_id']].explode('residents_id')
        df_residents_bridge = df_residents_bridge.dropna().astype('Int64')

        df['films_id'] = df['films'].apply(clean_url_list)
        df_films_bridge = df[['id', 'films_id']].explode('films_id')
        df_films_bridge = df_films_bridge.dropna().astype('Int64')
        
        df = df.replace("unknown", None)
        df = df.replace("n/a", None)
        df = df.drop(columns=["url", "residents", "films"])
        df = df.drop(columns=["residents_id", "films_id"])
        
        df = df.drop_duplicates(subset=['id'])
    except Exception as e:
        logger.error(f"Error al limpiar la data de planets_raw: {e}")
        return None
    return df, df_residents_bridge, df_films_bridge

def transformData(jsonName: str) -> pd.DataFrame:
    try:
        if jsonName == "people_raw":
            columns = ["name", "height", "mass", "homeworld", "films", "species", "url"]
        elif jsonName == "planets_raw":
            columns = ["name", "rotation_period", "orbital_period", "diameter", "climate", "gravity", "terrain", "surface_water", "population", "residents", "films", "url"]
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
            df, df_homeworld_bridge, df_species_bridge, df_films_bridge = clean_people(df)
            logger.info(f"Transformación de {jsonName} completada exitosamente")
            return [df, df_homeworld_bridge, df_species_bridge, df_films_bridge]

        elif jsonName == "planets_raw":
            df, df_residents_bridge, df_films_bridge = clean_planets(df)
            logger.info(f"Transformación de {jsonName} completada exitosamente")
            return [df, df_residents_bridge, df_films_bridge]

        else:
            logger.warning(f"No se encontró el archivo {jsonName}.json")
            return None  

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