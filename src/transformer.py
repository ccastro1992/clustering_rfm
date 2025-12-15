import pandas as pd


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

    return df_filtered
