import requests
import json
import os

BASE_URL = "http://maestria.optimusec.com/back/public"

def login():
    token = False
    _url = BASE_URL + '/admin/auth/login'
    
    # Cargar credenciales desde secrets.json
    try:
        with open(os.path.join(os.path.dirname(__file__), 'secrets.json')) as f:
            secrets = json.load(f)
        login = secrets['login']
        password = secrets['password']
    except (FileNotFoundError, KeyError) as e:
        print(f"Error al cargar las credenciales desde secrets.json: {e}")
        return None

    payload = {
        'login': login,
        'password': password
    }

    response = requests.post(_url, data=payload)
    if response.status_code == 200:
        try:
            token = response.json()['data']['token']
        except (KeyError, TypeError, requests.exceptions.JSONDecodeError) as e:
            print(f"Error al extraer el token de la respuesta: {e}")
            print("Texto de la respuesta:", response.text)
    else:
        print(f"La solicitud falló con el código de estado: {response.status_code}")
        print("Respuesta:", response.text)

    return token
