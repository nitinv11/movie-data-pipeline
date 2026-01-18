import requests

r = requests.get(
    "https://www.omdbapi.com/",
    params={"t": "Casino", "apikey": "cc313385"}
)

print(r.json())
