import requests

from . import api_connection


def set_client_cluster(payload):
    _url_post = api_connection.BASE_URL + '/sales/maestria/create'
    token = api_connection.login()

    # Autorizacion
    headers = {
        'Authorization': f'Bearer {token}',
        "Content-Type": "application/json"
    }

    # Respuesta
    try:
        response = requests.post(_url_post, data=payload, headers=headers)

        # Verifica el código de la respuesta
        response.raise_for_status()

        # Imprime la respuesta de la API
        print(f"Código de estado: {response.status_code}")
        print("Cuerpo de la respuesta (JSON):")
        print(response.json())

    except requests.exceptions.HTTPError as http_err:
        print(f"Error HTTP: {http_err}")
        print(f"Cuerpo de la respuesta de error: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"Ocurrió un error al conectar: {req_err}")

    return True
