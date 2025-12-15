import pandas as pd


# def convert_data_type(df):
#     # Convertir fechas  y limpiar automáticamente
#     df['fecha_vuelo'] = pd.to_datetime(df['fecha_vuelo'], errors='coerce')
#     # Conversion
#     df["mes"] = df['fecha_vuelo'].dt.month
#     df["año"] = df['fecha_vuelo'].dt.year
#     df["semana"] = df['fecha_vuelo'].dt.isocalendar().week
#     df['estado_orden'] = df['estado_orden'].astype(int)
#     df['cliente_id'] = df['cliente_id'].astype(int)
#     df['total'] = df['total'].astype(float)
#     print(df.head())
#
#     return df

# def clean_data(df):
#     # Filtrar transacciones que se hayan completado (estado = 3)
#     df_filtered_0 = df[df['estado_orden'].isin([3])].copy()
#     print(f"Registros originales: {len(df)}, Registros después de filtrar estado = 3: {len(df_filtered_0)}")
#
#     # Filtrar transacciones con cliente_id = 5816
#     df_filtered_1 = df_filtered_0[~df_filtered_0['cliente_id'].isin([5816])].copy()
#     print(
#         f"Registros originales: {len(df_filtered_0)}, Registros después de filtrar al cliente 5816: {len(df_filtered_1)}")
#
#     # Filtrar transacciones que mayor a 0 (total > 0)
#     df_filtered = df_filtered_1[df_filtered_1['total'] > 0].copy()
#     print(f"Registros originales: {len(df_filtered_1)}, Registros después de filtrar total > 0: {len(df_filtered)}")
#
#     # Eliminamos valores duplicados
#     df_filtered = df_filtered.drop_duplicates().reset_index(drop=True)
#
#     return df_filtered


def clean_data(data):
    df = pd.DataFrame(data)
    print("Shape bruto:", df.shape)

    # Fecha
    df['fecha_vuelo'] = pd.to_datetime(df['fecha_vuelo'], errors='coerce')
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

    # Nulos
    vars_criticas = ['cliente_id', 'producto_id', 'tallos', 'total', 'fecha_vuelo']
    vars_criticas = [c for c in vars_criticas if c in df.columns]
    df = df.dropna(subset=vars_criticas)

    vars_secundarias = ['usuario_id', 'vendedor', 'agencia', 'origen_cliente']
    for col in vars_secundarias:
        if col in df.columns:
            df[col] = df[col].fillna("DESCONOCIDO")

    # Numéricos
    cols_num = ['tallos', 'precio_unitario', 'total', 'largo']
    for c in cols_num:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    df = df.dropna(subset=['tallos', 'precio_unitario', 'total'])

    # estado_orden = 3 (si existe)
    if 'estado_orden' in df.columns:
        df_estado3 = df[df['estado_orden'] == 3]
        if len(df_estado3) > 0:
            df = df_estado3
            print("✔ Filtrado estado_orden = 3:", df.shape)

    # Eliminar descuentos / créditos / muestras
    categorias_excluir = [
        'DESCUENTOS VENTAS',
        'CREDITOS EN VENTAS',
        'CRÉDITOS EN VENTAS',
        'MUESTRA'
    ]

    df['producto_upper'] = df['producto'].astype(str).str.upper()

    df_clean = df[
        ~df['producto_upper'].isin(categorias_excluir) &
        (df['tallos'] >= 0) &
        (df['precio_unitario'] >= 0) &
        (df['total'] >= 0)
    ].copy()

    print("✔ Dataset limpio:", df_clean.shape)

    return df_clean