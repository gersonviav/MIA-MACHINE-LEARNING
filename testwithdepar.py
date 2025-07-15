import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re

def extraer_ubigeo_desde_html(link):
    try:
        response = requests.get(link, timeout=20)
        soup = BeautifulSoup(response.text, 'html.parser')
        texto = soup.get_text(separator=' ', strip=True)

        match = re.search(
            r"Departamento\s*:\s*([A-ZÁÉÍÓÚÑ ]+?)\s+Provincia\s*:\s*([A-ZÁÉÍÓÚÑ ]+?)\s+Distrito\s*:\s*([A-ZÁÉÍÓÚÑ ]+)",
            texto, flags=re.IGNORECASE
        )
        if match:
            return {
                "departamento": match.group(1).title().strip(),
                "provincia": match.group(2).title().strip(),
                "distrito": match.group(3).title().strip()
            }
        return {"departamento": None, "provincia": None, "distrito": None}
    except Exception as e:
        print(f"[ERROR UBIGEO] {link}: {e}")
        return {"departamento": None, "provincia": None, "distrito": None}


def senamhiws_info(codigos, stations, from_date=None, to_date=None):
    if not codigos or not all(isinstance(code, str) for code in codigos):
        print("Código no definido o inválido.")
        return None

    if from_date is None:
        from_date = datetime(2016, 1, 1)
    if to_date is None:
        to_date = datetime(2023, 12, 31)

    df_history_senamhi = []

    for code in codigos:
        idx_cod = stations.index[stations['cod'] == code]
        if idx_cod.empty:
            print(f"[SKIP] Código no encontrado en stations: {code}")
            continue

        df_idx_stn = stations.loc[idx_cod].reset_index(drop=True)
        estacion = df_idx_stn['estacion'].iloc[0]
        ico = df_idx_stn['ico'].iloc[0]
        estado_val = df_idx_stn['estado'].iloc[0]

        ts_date = pd.date_range(from_date, to_date, freq='MS')
        tsw_date = ts_date.strftime('%Y%m')

        for j, date in enumerate(ts_date):
            link = (
                f"https://www.senamhi.gob.pe/mapas/mapa-estaciones-2/export.php"
                f"?estaciones={code}&CBOFiltro={tsw_date[j]}&t_e={ico}&estado={estado_val}"
            )

            try:
                tablas = pd.read_html(link)
            except Exception as e:
                print(f"[ERROR HTML] {code} - {tsw_date[j]}: {e}")
                continue

            if len(tablas) < 2:
                print(f"[SKIP] No hay tabla válida en {link}")
                continue

            df_table = tablas[1]

            second_row = df_table.iloc[1].astype(str).str.strip().tolist()
            valores_esperados = ["AÑO / MES / DÍA", "MAX", "MIN", "HUMEDAD RELATIVA (%)", "TOTAL"]
            if not all(valor in second_row for valor in valores_esperados):
                print(f"[SKIP] Formato inesperado en {link}")
                continue

            df_table.columns = df_table.iloc[0]
            df_table = df_table[1:].copy()

            # Agregar datos meta
            df_table["estacion"] = estacion
            df_table["lat"] = df_idx_stn['lat'].iloc[0]
            df_table["lon"] = df_idx_stn['lon'].iloc[0]
            df_table["ico"] = ico

            # Scraping de ubigeo
            ubigeo_info = extraer_ubigeo_desde_html(link)
            df_table["departamento"] = ubigeo_info["departamento"]
            df_table["provincia"] = ubigeo_info["provincia"]
            df_table["distrito"] = ubigeo_info["distrito"]

            # Filtrar por estaciones meteorológicas
            if df_table["ico"].iloc[0] != 'M':
                continue

            # Guardar y registrar
            filename = f"{code}_{estacion}.xlsx"
            try:
                df_table.to_excel(filename, index=False)
            except Exception as e:
                print(f"[ERROR SAVE] {filename}: {e}")

            df_history_senamhi.append(df_table)

    return df_history_senamhi
