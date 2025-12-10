import requests
import json
import os
import json
import os
try:
    from google.colab import userdata
except ImportError:
    userdata = None

BASE_URL = "http://maestria.optimusec.com/back/public"

def get_secret(key):
    # Buscar en archivo secrets.json (Entorno Local)
    if os.path.exists('../src/secrets.json'):
        with open('../src/secrets.json', 'r') as f:
            data = json.load(f)
            return data.get(key)

    # Buscar en Google Colab Secrets (Entorno Nube)
    if userdata:
        try:
            return userdata.get(key)
        except:
            pass

def login():
    token = False
    _url = BASE_URL + '/admin/auth/login'
    
    # Cargar credenciales
    _API_KEY = get_secret('API_KEY')

    payload = {
        'login': 'maestria',
        'password': _API_KEY
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
