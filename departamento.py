import re
import requests
from bs4 import BeautifulSoup

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
            distrito = match.group(3).strip().title()
            return {"departamento": departamento, "provincia": provincia, "distrito": distrito}
        else:
            return {"departamento": None, "provincia": None, "distrito": None}
        
    except Exception as e:
        print(f"Error al extraer ubigeo desde {link}: {e}")
        return {"departamento": None, "provincia": None, "distrito": None}
link = "https://www.senamhi.gob.pe/mapas/mapa-estaciones-2/export.php?estaciones=112193&CBOFiltro=202302&t_e=M&estado=AUTOMATICA"

print(extraer_ubigeo_desde_html(link))