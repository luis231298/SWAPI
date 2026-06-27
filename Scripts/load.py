from Scripts import extract
import logging
import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

def load_data_to_silver(df_dic: dict):
    try:
        logger.info("Iniciando la fase de carga (Load) a la capa Silver...")
        DATABASE_URL = "postgresql://etl_user:etl123@localhost:5432/star_wars_db"
        engine = create_engine(DATABASE_URL)

        orden_carga = [
            "dim_planets",          
            "dim_people",           
            "fact_people_films",    
            "fact_people_species",  
            "fact_planet_residents" 
        ]

        for tabla in orden_carga:
           if tabla in df_dic and df_dic[tabla] is not None:
                df = df_dic[tabla]
            
                logger.info(f"Cargando {len(df)} filas en silver.{tabla}...")

                df.to_sql(
                    name = tabla,
                    con = engine,
                    if_exists = 'append',
                    index = False,
                    schema = 'silver'
                )

                logger.info(f"Tabla {tabla} cargada exitosamente a la capa silver.")
        
        logger.info("¡Fase de carga completada con éxito en la base de datos!")
        return True
    except Exception as e:
        logger.error(f"Error al cargar los datos a la capa silver: {e}")
        return None


