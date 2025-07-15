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
        idx_cod = stations.index[stations['cod'] == code]
        df_idx_stn = stations.loc[idx_cod].reset_index(drop=True)

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
            data_df_history_senamhi = data_df_history_senamhi[1:]

            df_history_senamhi.append(data_df_history_senamhi)

            output_filename = f"{cod_stn[-1]}_{df_idx_stn['estacion'].iloc[0]}.xlsx"
            data_df_history_senamhi.to_excel(output_filename, index=False)

    return df_history_senamhi

def senamhiws_info(x, stations, from_date=None, to_date=None,departamento =None):
    # print("CODIGO",x)
    if not x or not all(isinstance(code, str) for code in x):
        print("Codigo no definido")
        return None
    df_depa = pd.read_excel("departamentos_estaciones.xlsx")    
    print("departamentos")
    print(df_depa.head())
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
        idx_cod = stations.index[stations['cod'] == code]

        df_idx_stn = stations.loc[idx_cod].reset_index(drop=True)
        #print("df_idx_stn")
        #print(df_idx_stn.head())
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
            df_table = data_stn_senamhi[1]

            # Validar contenido específico en la segunda fila
            second_row = df_table.iloc[1].astype(str).str.strip().tolist()

            # Define los valores obligatorios
            valores_esperados = ["AÑO / MES / DÍA", "MAX", "MIN", "HUMEDAD RELATIVA (%)", "TOTAL"]

            # Verifica si todos los esperados están presentes en la segunda fila
            if not all(valor in second_row for valor in valores_esperados):
                print(f"La segunda fila no contiene los valores esperados, se ignora: {link}")
                continue

            # Continúa procesamiento si pasa la validación
            data_df_history_senamhi = df_table
            data_df_history_senamhi.columns = data_df_history_senamhi.iloc[0]
            data_df_history_senamhi = data_df_history_senamhi[1:].copy()



            data_df_history_senamhi = data_stn_senamhi[1]
            data_df_history_senamhi.columns = data_df_history_senamhi.iloc[0]
            data_df_history_senamhi = data_df_history_senamhi[1:]

            df_history_senamhi.append(data_df_history_senamhi)
            data_df_history_senamhi["estacion"] =df_idx_stn['estacion'].iloc[0]
            data_df_history_senamhi["lat"] = df_idx_stn['lat'].iloc[0]
            data_df_history_senamhi["lon"] = df_idx_stn['lon'].iloc[0]
            data_df_history_senamhi["ico"] = df_idx_stn['ico'].iloc[0]
            
            data_df_history_senamhi = data_df_history_senamhi[data_df_history_senamhi["ico"] == 'M']

            ubigeo_info = extraer_ubigeo_desde_html(link)
            data_df_history_senamhi["departamento"] = ubigeo_info["departamento"]
            data_df_history_senamhi["provincia"] = ubigeo_info["provincia"]
            data_df_history_senamhi["distrito"] = ubigeo_info["distrito"]
            
            #data_df_history_senamhi  = data_df_history_senamhi.merge(df_depa, left_on='estacion', right_on='ESTACION', how='left')
            #print("---¬\n" ,data_df_history_senamhi.head())
            # df_history_senamhi ["ico"] =ico
            #output_filename = f"{departamento}/{cod_stn[-1]}_{df_idx_stn['estacion'].iloc[0]}.xlsx"
            output_filename = f"{cod_stn[-1]}_{df_idx_stn['estacion'].iloc[0]}.xlsx"
            

            try:
                    fila_3 = data_df_history_senamhi.iloc[3]
                    print("Fila 3 (iloc):", fila_3)
                    data_df_history_senamhi.to_excel(output_filename, index=False)
            except IndexError:
                     print("Fila 3 no encontrada con iloc")
            
            
    
    return df_history_senamhi
def departamentes_distriluz(fec_ini,fec_fin):
    # Uso del código
    stations_data = stations()
    print('xxxxxxxxx')
    #print(stations_data.head())
    lista_cod= stations_data["cod"].unique().tolist()

    try   :
            #resultados = senamhiws_ger(codigos, stations_data,'2024-01-01','2024-02-16')
            #'2024-01-01' '2024-01-30'
            resultados =  senamhiws_info(lista_cod, stations_data,fec_ini,fec_fin)


            #print(resultados)
    except Exception as e:
             print(f"An error occurred: {e}")
