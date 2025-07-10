from stations import departamentes_distriluz

from reaad_files import readfile_piura,readfile_ancash,readfile_moquegua,readfile_junin,readfile_ayacucho,readfile_cajamarca,readfile_huancavelica,readfile_huanuco,readfile_lambayeque
import sys
from datetime import datetime, timedelta
import os

# # Define la ruta del archivo de  registro
# log_file_path = 'logs.txt'

# # Redirige la salida estándar al archivo de registro
# sys.stdout = open(log_file_path,'w')
                  
if len(sys.argv) == 3:
    param1 = sys.argv[1]
    param2 = sys.argv[2]
else:
    # Obtener la fecha actual
    current_date = datetime.now()

    # Calcular el primer día del mes actual
    first_day_current_month = datetime(current_date.year, current_date.month, 1)

    # Calcular el último día del mes pasado
    last_day_last_month = first_day_current_month - timedelta(days=1)

    # Formatear las fechas como cadenas 'YYYY-MM-DD'
    param1 = last_day_last_month.replace(day=1).strftime('%Y-%m-%d')
    param2 = last_day_last_month.strftime('%Y-%m-%d')

departamentes_distriluz(param1, param2)
# Obtener la ruta actual antes de cambiar
#directorio_anterior = os.getcwd()
#readfile_piura(param1,param2)
# #Volver al directorio anterior
# os.chdir(directorio_anterior)

# readfile_ancash(param1,param2)

# os.chdir(directorio_anterior)
# readfile_moquegua(param1,param2)
# os.chdir(directorio_anterior)

# #47E8568A_SANTA ANA ,
# readfile_junin(param1,param2)
# os.chdir(directorio_anterior)
# #47290068_HUAC-HUAS
# readfile_ayacucho(param1,param2)
# os.chdir(directorio_anterior)
# readfile_cajamarca(param1,param2)
# os.chdir(directorio_anterior)
# readfile_huancavelica(param1,param2)
# #4728F216_CORDOVA,4729131E_SANTIAGO DE CHOCORVOS,4729131E_SANTIAGO DE CHOCORVOS
# os.chdir(directorio_anterior)
#readfile_huanuco()
#readfile_lambayeque()




