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
    #dataPeople = extract.getData("https://swapi.dev/api/people/")
    #dataPlanets = extract.getData("https://swapi.dev/api/planets/")
    #dataFilms = extract.getData("https://swapi.dev/api/films/")

    logger.info("Guardando datos crudos en Capa Bronze")
    #extract.saveToBronze(dataPeople, "people_raw")
    #extract.saveToBronze(dataPlanets, "planets_raw")
    logger.info("Transformando datos")
    transform.transformData("people_raw")
    transform.transformData("planets_raw")
    #print(dataPeople)
    #print(dataPlanets)
    #print(type(dataPeople))
    #print(dataFilms)

if __name__ == "__main__":
    main()