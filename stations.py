import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import os
import re
def extraer_ubigeo_desde_html(link):
    try:
        response = requests.get(link, timeout=20)
        soup = BeautifulSoup(response.text, 'html.parser')
        texto = soup.get_text(separator=' ', strip=True)

        # Regex robusto para detectar los campos
        match = re.search(r"Departamento\s*:\s*([A-ZÁÉÍÓÚÑ ]+?)\s+Provincia\s*:\s*([A-ZÁÉÍÓÚÑ ]+?)\s+Distrito\s*:\s*([A-ZÁÉÍÓÚÑ ]+)",
                          texto, flags=re.IGNORECASE)
        
        if match:
            departamento = match.group(1).strip().title()
            provincia = match.group(2).strip().title()
            distrito = match.group(3).strip().title().replace("Latitud", "", 1).strip().title()
            return {"departamento": departamento, "provincia": provincia, "distrito": distrito}
        else:
            return {"departamento": None, "provincia": None, "distrito": None}
        
    except Exception as e:
        print(f"Error al extraer ubigeo desde {link}: {e}")
        return {"departamento": None, "provincia": None, "distrito": None}
def stations():
    link = "https://www.senamhi.gob.pe/mapas/mapa-estaciones-2/"
    response = requests.get(link)
    stn_senamhi = BeautifulSoup(response.text, 'html.parser')

    stn_senamhi2 = re.split(r'nom', str(stn_senamhi))[1:]
    stn = []
    cat = []
    lat = []
    lon = []
    ico = []
    cod = []
    cod_old = []
    estado = []
    data_stn = []
    #print("omg",stn_senamhi2[:len(stn_senamhi2)-1])
    stn_senamhi2 = stn_senamhi2[:len(stn_senamhi2)-1]
# estado:
    for i in range(len(stn_senamhi2)):
        x = stn_senamhi2[i].replace('\"', '').replace(': ', ":").replace(',\n', "").replace('\\}\\{', "")
        
        data_estaciones = x.split(",")
        #print("debug",data_estaciones,len(data_estaciones))
        stn.append(data_estaciones[0].replace(":", ""))
        cat.append(data_estaciones[1].replace("cate:", ""))
        lat.append(data_estaciones[2].replace("lat:", ""))
        lon.append(data_estaciones[3].replace("lon:", ""))
        ico.append(data_estaciones[4].replace(" ico:", ""))
        
        cod.append(data_estaciones[5].replace(" cod:", "") if data_estaciones[5][:5] == " cod:" else None)
        
        cod_old.append(data_estaciones[6].replace("cod_old:", "") if data_estaciones[6][:8] == "cod_old:" else None)
        #[':SAN BORJA', ' cate:EMA', ' lat:-12.10859', ' lon:-77.00769', ' ico:M', ' cod:112193', ' estado:AUTOMATICA}{']
        estado_value = data_estaciones[7] if len(data_estaciones) > 7 else data_estaciones[6]
        estado.append( estado_value.replace("}{","").replace(" estado:", "") if estado_value[:8] == " estado:" else None)

        data_stn.append(pd.DataFrame({
            'estacion': stn[-1],
            'categoria': cat[-1],
            'lat': lat[-1],
            'lon': lon[-1],
            'ico': ico[-1],
            'cod': cod[-1],
            'cod_old': cod_old[-1],
            'estado': estado[-1]
        }, index=[0]))

    df_stns = pd.concat(data_stn, ignore_index=True)
    return df_stns

def senamhiws_ger(x, stations, from_date=None, to_date=None):
    # print("CODIGO",x)
    if not x or not all(isinstance(code, str) for code in x):
        print("Codigo no definido")
        return None

    cod_stn = []
    df_history_senamhi = []

    if from_date is None and to_date is None:
        from_date = datetime(2016, 1, 1)
        to_date = datetime(2023, 12, 31)
    elif from_date is None and to_date is not None:
        from_date = datetime(2016, 1, 1)
    elif from_date is not None and to_date is None:
        to_date = datetime(2023, 12, 31)

    for code in x:
        cod_stn.append(code)
        # idx_cod = stations.index[stations['cod'] == code]
        # df_idx_stn = stations.loc[idx_cod].reset_index(drop=True)

        df_idx_stn = stations[stations['cod'] == code].reset_index(drop=True)


        ts_date = pd.date_range(from_date, to_date, freq='MS')
        tsw_date = ts_date.strftime('%Y%m')

        for j, date in enumerate(ts_date):
            if pd.isna(df_idx_stn['cod'][0]):
                link = f"https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones={df_idx_stn['cod'].iloc[0]}&CBOFiltro={tsw_date[j]}&t_e={df_idx_stn['ico'].iloc[0]}&estado={df_idx_stn['estado'].iloc[0]}&cod_old={df_idx_stn['cod_old'].iloc[0]}"
            else:
                link = f"https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones={df_idx_stn['cod'].iloc[0]}&CBOFiltro={tsw_date[j]}&t_e={df_idx_stn['ico'].iloc[0]}&estado={df_idx_stn['estado'].iloc[0]}"
            
            try:    
                    #print(link)
                    data_stn_senamhi = pd.read_html(link)
            # Tu código para manejar los datos después de leerlos correctamente
            except ValueError as e:
                    print(f"Error al leer HTML desde el enlace: {e}")
                    print(f"El enlace que falló es: {link}")

            data_df_history_senamhi = data_stn_senamhi[1]
            data_df_history_senamhi.columns = data_df_history_senamhi.iloc[0]
            try:
                data_df_history_senamhi = data_df_history_senamhi[2:]
                
                # Verificar que aún tiene datos después de eliminar las 2 primeras filas
                if data_df_history_senamhi.empty:
                    print(f"No hay datos después de eliminar cabeceras en {link}")
                    continue
                    
            except IndexError:
                print(f"No hay suficientes filas para procesar en {link}")
                continue
            except Exception as e:
                print(f"Error al procesar datos en {link}: {e}")
                continue
            df_history_senamhi.append(data_df_history_senamhi)

            output_filename = f"{cod_stn[-1]}_{df_idx_stn['estacion'].iloc[0]}.xlsx"
            data_df_history_senamhi.to_excel(output_filename, index=False)

    return df_history_senamhi



def senamhiws_info(x, stations, from_date=None, to_date=None, departamento=None):
    """Versión corregida que maneja correctamente la concatenación"""
    if not x or not all(isinstance(code, str) for code in x):
        print("Codigo no definido")
        return None
    
    # Lista para almacenar TODOS los DataFrames (uno por mes/estación)
    df_history_senamhi = []
     
    if from_date is None and to_date is None:
        from_date = datetime(2016, 1, 1)
        to_date = datetime(2023, 12, 31)
    elif from_date is None and to_date is not None:
        from_date = datetime(2016, 1, 1)
    elif from_date is not None and to_date is None:
        to_date = datetime(2023, 12, 31)

    for code in x:
        # SOLUCIÓN 1: Filtrar directamente sin usar .index para evitar duplicados
        df_idx_stn = stations[stations['cod'] == code].reset_index(drop=True)
        
        if df_idx_stn.empty:
            print(f"No se encontró estación con código: {code}")
            continue
            
        # Si hay duplicados en stations, tomar solo el primero
        if len(df_idx_stn) > 1:
            df_idx_stn = df_idx_stn.iloc[[0]]

        ts_date = pd.date_range(from_date, to_date, freq='MS')
        tsw_date = ts_date.strftime('%Y%m')

        for j, date in enumerate(ts_date):
            if pd.isna(df_idx_stn['cod'].iloc[0]):
                link = f"https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones={df_idx_stn['cod_old'].iloc[0]}&CBOFiltro={tsw_date[j]}&t_e={df_idx_stn['ico'].iloc[0]}&estado={df_idx_stn['estado'].iloc[0]}&cod_old={df_idx_stn['cod_old'].iloc[0]}"
            else:
                link = f"https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones={df_idx_stn['cod'].iloc[0]}&CBOFiltro={tsw_date[j]}&t_e={df_idx_stn['ico'].iloc[0]}&estado={df_idx_stn['estado'].iloc[0]}"
            
            try:    
                data_stn_senamhi = pd.read_html(link)
            except ValueError as e:
                print(f"Error al leer HTML desde el enlace: {e}")
                continue
                
            if len(data_stn_senamhi) < 2:
                print(f"El enlace no contiene suficientes tablas: {link}")
                continue
                
            df_table = data_stn_senamhi[1]
            
            # Validación básica de estructura
            if df_table.shape[0] < 2:
                print(f"Tabla vacía o incompleta en {link}")
                continue

            # Validar contenido específico en la segunda fila
            second_row = df_table.iloc[1].astype(str).str.strip().tolist()

            # Define los valores obligatorios
            valores_esperados = ["AÑO / MES / DÍA", "MAX", "MIN", "HUMEDAD RELATIVA (%)", "TOTAL"]

            # Verifica si todos los esperados están presentes en la segunda fila
            if not all(valor in second_row for valor in valores_esperados):
                print(f"La segunda fila no contiene los valores esperados, se ignora: {link}")
                continue

            # Procesar datos
            data_df_history_senamhi = df_table.copy()
            data_df_history_senamhi.columns = data_df_history_senamhi.iloc[0]
            data_df_history_senamhi = data_df_history_senamhi[1:].copy()

            # SOLUCIÓN 2: Verificar que tiene datos después del procesamiento
            if data_df_history_senamhi.empty:
                print(f"No hay datos después del procesamiento en {link}")
                continue

            # Agregar metadatos de la estación
            data_df_history_senamhi["estacion"] = df_idx_stn['estacion'].iloc[0]
            data_df_history_senamhi["codigo"] = df_idx_stn['cod'].iloc[0]  # Agregar código para identificar
            data_df_history_senamhi["lat"] = df_idx_stn['lat'].iloc[0]
            data_df_history_senamhi["lon"] = df_idx_stn['lon'].iloc[0]
            data_df_history_senamhi["ico"] = df_idx_stn['ico'].iloc[0]
            data_df_history_senamhi["fecha_consulta"] = tsw_date[j]  # Agregar mes consultado
            
            # Filtrar solo estaciones meteorológicas
            data_df_history_senamhi = data_df_history_senamhi[data_df_history_senamhi["ico"] == 'M']
            
            if data_df_history_senamhi.empty:
                continue

            # Extraer información de ubicación
            ubigeo_info = extraer_ubigeo_desde_html(link)
            data_df_history_senamhi["departamento"] = ubigeo_info["departamento"]
            data_df_history_senamhi["provincia"] = ubigeo_info["provincia"]
            data_df_history_senamhi["distrito"] = ubigeo_info["distrito"]
            #recien agregado
            data_df_history_senamhi = data_df_history_senamhi.drop(index=1)

            # SOLUCIÓN 3: Agregar CADA mes como un DataFrame separado
            if len(data_df_history_senamhi) > 0:
                df_history_senamhi.append(data_df_history_senamhi)
                print(f"Procesado: {code} - {df_idx_stn['estacion'].iloc[0]} - {tsw_date[j]} - {len(data_df_history_senamhi)} registros")
            
    # SOLUCIÓN 4: Concatenar TODO al final
    if df_history_senamhi:
        df_final = pd.concat(df_history_senamhi, ignore_index=True)
        
        # Información del resultado
        print(f"\n=== RESUMEN ===")
        print(f"Total de registros: {len(df_final)}")
        print(f"Estaciones únicas: {df_final['estacion'].nunique()}")
        print(f"Códigos únicos: {df_final['codigo'].nunique()}")
        print(f"Departamentos: {df_final['departamento'].nunique()}")
        
        # Guardar
        df_final.to_excel("senamhi_data_completo.xlsx", index=False)
        print(f"Datos guardados en senamhi_data_completo.xlsx")
        
        return df_final
    else:
        print("No se encontraron datos válidos para procesar")
        return pd.DataFrame()

def departamentes_distriluz(fec_ini, fec_fin):
    """Función principal para concatenar datos de múltiples estaciones/meses"""
    # Obtener estaciones
    stations_data = stations()
    
    # Limpiar duplicados en el DataFrame de estaciones
    stations_data = stations_data.drop_duplicates(subset=['cod'], keep='first')
    
    print(f'Estaciones encontradas: {len(stations_data)}')
    
    # Filtrar códigos válidos
    lista_cod = stations_data["cod"].dropna().unique().tolist()
    print(f'Códigos válidos para procesar: {len(lista_cod)}')

    try:
        resultados = senamhiws_info(lista_cod, stations_data, fec_ini, fec_fin)
        return resultados
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

# Para ejecutar:
# python main.py '2018-01-01' '2018-01-31'





