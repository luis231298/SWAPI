import requests

def getData(url, all_data):
    try:
        response = requests.get(url)
        print("Status Code: ", response.status_code)
        if response.status_code == 200:
            data = response.json()
            if data["next"]:
                all_data.append(data)
                getData(data["next"], all_data)  
            return all_data
        else:
            return None
    except requests.exceptions.RequestException as e:
        print("Status Code: ", response.status_code)
        print(f"Error al obtener la data: {e}")
        return None

def main():
    all_data = []
    #Obtención de la data de la API. Rubro: People
    data = getData("https://swapi.dev/api/people/", all_data)


    if data:
        print(data)
    else:
        print("Error al obtener la data")

if __name__ == "__main__":
    main()