import extract
import logging

logging.basicConfig(
        level=logging.INFO,
        format = '%(asctime)s - %(levelname)s - %(message)s',
        filename = 'app.log',
        filemode = 'w'
)

logger = logging.getLogger(__name__)

def main():
    
    logger.info("Extracción de la info desde la API")
    dataPeople = extract.getData("https://swapi.dev/api/people/")
    dataPlanets = extract.getData("https://swapi.dev/api/planets/")
    #dataFilms = extract.getData("https://swapi.dev/api/films/")

    print(dataPeople)
    print(dataPlanets)
    #print(dataFilms)

if __name__ == "__main__":
    main()