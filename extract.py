import requests
import logging

logger = logging.getLogger(__name__)

def getData(url):
    logger.info(f"Inicio de extracción de {url}")
    try:
        all_data = []
        current_url = url
        while current_url:
            response = requests.get(current_url)
            if response.status_code == 200:
                data = response.json()
                all_data.append(data["results"])
                current_url = data["next"]
            else:
                print("Error al obtener la data")
                return None
        return all_data
    except requests.exceptions.HTTPError as e:
        logger.error(f"Error HTTP: {e.response.status_code} - {url}")
        return None
    except requests.exceptions.RequestException as e:
        logger.exception(f"Fallo crítico en la solicitud a {url}")
        return None 

if __name__ == "__main__":
    main()