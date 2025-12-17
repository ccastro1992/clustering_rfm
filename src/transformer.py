import pandas as pd
import requests
import json
import time
from . import api_connection


def convert_data_type(df):
    # Convertir fechas  y limpiar automáticamente
    df['fecha_vuelo'] = pd.to_datetime(df['fecha_vuelo'], errors='coerce')
    # Conversion
    df["mes"] = df['fecha_vuelo'].dt.month
    df["año"] = df['fecha_vuelo'].dt.year
    df["semana"] = df['fecha_vuelo'].dt.isocalendar().week
    df['estado_orden'] = df['estado_orden'].astype(int)
    df['cliente_id'] = df['cliente_id'].astype(int)
    df['total'] = df['total'].astype(float)

    # Texto
    cols_texto = [
        'cliente', 'pais', 'estado', 'ciudad',
        'vendedor', 'producto', 'agencia', 'origen_cliente'
    ]

    for col in cols_texto:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    print(df.head())

    return df

def clean_data(df):
    vars_secundarias = ['usuario_id', 'vendedor', 'agencia', 'origen_cliente']
    for col in vars_secundarias:
        if col in df.columns:
            df[col] = df[col].fillna("DESCONOCIDO")

    # Filtrar transacciones que se hayan completado (estado = 3)
    df_filtered_0 = df[df['estado_orden'].isin([3])].copy()
    print(f"Registros originales: {len(df)}, Registros después de filtrar estado = 3: {len(df_filtered_0)}")

    # Filtrar transacciones con cliente_id = 5816
    df_filtered_1 = df_filtered_0[~df_filtered_0['cliente_id'].isin([5816])].copy()
    print(
        f"Registros originales: {len(df_filtered_0)}, Registros después de filtrar al cliente 5816: {len(df_filtered_1)}")

    # Filtrar transacciones que mayor a 0 (total > 0)
    df_filtered = df_filtered_1[df_filtered_1['total'] > 0].copy()
    print(f"Registros originales: {len(df_filtered_1)}, Registros después de filtrar total > 0: {len(df_filtered)}")

    # Eliminamos valores duplicados
    df_filtered = df_filtered.drop_duplicates().reset_index(drop=True)

    # Guardamos la data limpia
    save_clean_data(payload=df_filtered.copy())

    return df_filtered


def save_clean_data(payload, batch_size=2500):
    _url_post = api_connection.BASE_URL + '/sales/maestria/saveclean'
    token = api_connection.login()

    if not token:
        print("Error: No se pudo obtener el token de autenticación. Abortando guardado.")
        return False

    # Convertir la columna de fecha a string ANTES del bucle
    payload['fecha_vuelo'] = payload['fecha_vuelo'].dt.strftime('%Y-%m-%d %H:%M:%S')
    payload = payload.rename(columns={'año': 'anio'})
    payload.drop('producto', axis=1, inplace=True)
    payload.drop('cliente', axis=1, inplace=True)

    # Autorizacion
    headers = {
        'Authorization': f'Bearer {token}',
        "Content-Type": "application/json"
    }

    total_records = len(payload)
    if total_records == 0:
        print("No hay registros para guardar.")
        return True

    num_batches = (total_records // batch_size) + (1 if total_records % batch_size > 0 else 0)
    print(f"Iniciando guardado de {total_records} registros en {num_batches} lotes de tamaño {batch_size}.")

    for i in range(0, total_records, batch_size):
        batch_num = (i // batch_size) + 1
        print(f"--- Procesando lote {batch_num}/{num_batches} ---")

        batch_df = payload.iloc[i:i + batch_size]

        try:
            final_json = batch_df.to_dict(orient="records")
            data_to_send = {
                'data': final_json
            }

            json_data = json.dumps(data_to_send)
            start_time = time.time()
            response = requests.post(_url_post, data=json_data, headers=headers, timeout=60)
            response.raise_for_status()

            end_time = time.time()
            print(f"Lote {batch_num} guardado exitosamente en {end_time - start_time:.2f} segundos. Status: {response.status_code}")

        except requests.exceptions.HTTPError as http_err:
            print(f"Error HTTP en lote {batch_num}: {http_err}")
            if http_err.response is not None:
                print(f"Cuerpo de la respuesta de error: {http_err.response.text}")
        except requests.exceptions.Timeout:
            print(f"Timeout en lote {batch_num}. El servidor no respondió en 60 segundos.")
        except requests.exceptions.RequestException as req_err:
            print(f"Ocurrió un error de red en lote {batch_num}: {req_err}")
        except Exception as e:
            print(f"Ocurrió un error inesperado al procesar el lote {batch_num}: {e}")

    print("--- Proceso de guardado finalizado ---")
    return True
