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




