import time
from . import api_connection
import requests


def get_ventas():
    _url_get = api_connection.BASE_URL+'/sales/maestria/ventasedr'
    data = []
    token = api_connection.login()

    if token:
        for i in [2020, 2021, 2022, 2023, 2024, 2025]:
            start_time = time.time()
            # Parametros
            params = {
                "finicio": "%s-01-01" % i,
                "ffin": "%s-12-31" % i
            }

            # Autorizacion
            headers = {
                'Authorization': f'Bearer {token}'
            }

            # Peticion GET
            try:
                response_get = requests.get(_url_get, headers=headers, params=params, timeout=60) # Timeout de 60 segundos

                # Respuesta
                if response_get.status_code == 200:
                    try:
                        data_response = response_get.json()['data']
                        data.extend(data_response)
                        end_time = time.time()
                        print(f'Datos para año {i}: {len(data_response)} registros. Tomó {end_time - start_time:.2f} segundos.')
                    except requests.exceptions.JSONDecodeError:
                        print("No se pudo decodificar la respuesta como JSON.")
                        print("Texto de la respuesta:", response_get.text)
                else:
                    print(f"***** ERROR: La solicitud GET falló con el código de estado: {response_get.status_code} *****")
                    print("Respuesta:", response_get.text)
            except requests.exceptions.Timeout:
                print(f"***** TIMEOUT: La solicitud para el año {i} excedió el tiempo de espera de 60 segundos. *****")
            except requests.exceptions.RequestException as e:
                print(f"***** ERROR de red: Ocurrió un error en la solicitud para el año {i}: {e} *****")


    return data
