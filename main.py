from Scripts import load
import logging
import Scripts.extract as extract
import Scripts.transform as transform

logging.basicConfig(
        level=logging.INFO,
        format = '%(asctime)s - %(levelname)s - %(message)s',
        filename = 'app.log',
        filemode = 'w',
        encoding = 'utf-8'
)

logger = logging.getLogger(__name__)

def main():
    
    logger.info("Extracción de la info desde la API")
    dataPeople = extract.getData("https://swapi.py4e.com/api/people/")
    dataPlanets = extract.getData("https://swapi.py4e.com/api/planets/")
    #dataFilms = extract.getData("https://swapi.py4e.com/api/films/")

    logger.info("Guardando datos crudos en Capa Bronze")
    extract.saveToBronze(dataPeople, "people_raw")
    extract.saveToBronze(dataPlanets, "planets_raw")
    logger.info("Transformando datos")
    
    df_people, df_homeworld_bridge, df_species_bridge, df_people_films = transform.transformData("people_raw")
    df_planets, df_residents_bridge, df_planets_films = transform.transformData("planets_raw")

    diccionario_para_carga = {
        "dim_planets": df_planets,
        "dim_people": df_people,
        "fact_people_films": df_people_films,
        "fact_people_species": df_species_bridge,
        "fact_planet_residents": df_residents_bridge
    }

    
    load.load_data_to_silver(diccionario_para_carga)



if __name__ == "__main__":
    main()