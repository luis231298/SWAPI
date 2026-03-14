import requests

url = "https://swapi.dev/api/people/"
response = requests.get(url)
data = response.json()

print(data)