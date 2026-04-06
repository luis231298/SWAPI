import os
import json
import logging
import requests

logger = logging.getLogger(__name__)

def saveToBronze(data, filename):
    try:
        logger.info(f"Inicio de guardado de {filename}")
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        ruta = os.path.join(base_dir, "Data", "bronze", filename + ".json")
        os.makedirs(os.path.dirname(ruta), exist_ok=True)
        with open(ruta, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return ruta
    except Exception as e:
        logger.error(f"Error al guardar la data: {e}")
        return None

def getData(url):
    logger.info(f"Inicio de extracción de {url}")
    try:
        all_data = []
        current_url = url
        while current_url:
            logger.info(f"Inicio de página {current_url}")
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