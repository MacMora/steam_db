import requests

url = "https://ipinfo.io/190.60.194.114/json"

try:
    response = requests.get(url)
    data = response.json()
    print(data)
except:
    print("Hubo un Error")