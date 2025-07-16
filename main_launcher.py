from multiprocessing import Pool
from datetime import datetime, timedelta
import os

def run_script(params):
    fecha_inicio, fecha_fin = params
    cmd = f'python main.py "{fecha_inicio}" "{fecha_fin}"'
    print(f"Ejecutando: {cmd}")
    os.system(cmd)

def generar_periodos(fecha_inicio, fecha_fin, modo='mensual'):
    periodos = []
    actual = fecha_inicio

    while actual <= fecha_fin:
        if modo == 'mensual':
            inicio = actual
            fin = (inicio.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        elif modo == 'trimestral':
            inicio = actual
            mes_siguiente = (inicio.month - 1 + 3) % 12 + 1
            año = inicio.year + ((inicio.month - 1 + 3) // 12)
            fin = datetime(año, mes_siguiente, 1) - timedelta(days=1)
        else:
            raise ValueError("Modo debe ser 'mensual' o 'trimestral'")
        
        if fin > fecha_fin:
            fin = fecha_fin

        periodos.append((inicio.strftime('%Y-%m-%d'), fin.strftime('%Y-%m-%d')))
        actual = fin + timedelta(days=1)

    return periodos

if __name__ == '__main__':
    fecha_inicio = datetime(2019, 1, 1)
    fecha_fin = datetime(2019, 4, 30)
    modo = 'mensual'  # 'mensual' o 'trimestral'
    max_procesos = 4  # ← ejecutará 4 procesos a la vez

    periodos = generar_periodos(fecha_inicio, fecha_fin, modo=modo)

    with Pool(processes=max_procesos) as pool:
        pool.map(run_script, periodos)
