import os
import pandas as pd
from insert_sql import connect_to_sql
import numpy as np  # Importa numpy para usar np.nan

def replace_sd_with_null(df, columns):
    for col in columns:
        df[col] = df[col].replace('S/D', np.nan)
def  readfile_piura(fec_ini,fec_fin):
    # Ingresa la ruta de tu directorio aquí
    
    directorio_especifico = 'piura'
    print("Directorio actual antes de cambiar:", os.getcwd())

    # Cambiar el directorio actual al directorio específico
    os.chdir(directorio_especifico)
    # Listar todos los archivos en el directorio específico
    archivos_en_directorio = os.listdir()
    extension = ".xlsx"

    archivos_con_extension = [archivo for archivo in archivos_en_directorio if archivo.endswith(extension)]
    print("archivos extraidos ",archivos_con_extension)
    print(f"Archivos con extensión {extension} en el directorio :")
    # Create an empty list to store data frames
    combined_dfs = []  # Use a list to store DataFrames
    for archivo in archivos_con_extension:
        #2024_0801
        #print(archivo)
        if any(substring in archivo for substring in ['472761E6_HUARMACA', '472606FA_AYABACA', '472F6540_CHULUCANAS', '472F7636_MORROPON', '472FD6CE_LANCONES', '472FF022_SAPILLICA']):
            print("archivo enviado a insertar",archivo)
            df = pd.read_excel(archivo)
            #print(df.head())
            df.rename(columns={
                'AÑO / MES / DÍA': 'ano_mes_dia',
                'TEMPERATURA (°C)': 'temperatura',
                'PRECIPITACIÓN (mm/hora)': 'precipitacion',
                'HUMEDAD (%)': 'humedad_porcentual',
                'DIRECCION DEL VIENTO (°)': 'direccion_viento',
                'VELOCIDAD DEL VIENTO (m/s)': 'velocidad_viento'
                # Agrega más columnas según sea necesario
            }, inplace=True)
            columns = ['temperatura','precipitacion','humedad_porcentual','direccion_viento','velocidad_viento']
            replace_sd_with_null(df,columns)
            connect_to_sql(df,directorio_especifico,fec_ini,fec_fin)
            print("archivo inserto ok",archivo)


#readfile()
def  readfile_ancash(fec_ini,fec_fin):
    # Ingresa la ruta de tu directorio aquí
    

    directorio_especifico = 'ancash'

    # Cambiar el directorio actual al directorio específico
    os.chdir(directorio_especifico)
    # Listar todos los archivos en el directorio específico
    archivos_en_directorio = os.listdir()
    extension = ".xlsx"

    archivos_con_extension = [archivo for archivo in archivos_en_directorio if archivo.endswith(extension)]
    print("archivos extraidos ",archivos_con_extension)
    print(f"Archivos con extensión {extension} en el directorio :")
    for archivo in archivos_con_extension:
        #2024_0801
        print("archivo enviado a insertar",archivo)
        if any(substring in archivo for substring in ['47259496_RECUAY']):
            print(archivo)
            df = pd.read_excel(archivo)
            #print(df.head())
            df.rename(columns={
                'AÑO / MES / DÍA': 'ano_mes_dia',
                'TEMPERATURA (°C)': 'temperatura',
                'PRECIPITACIÓN (mm/hora)': 'precipitacion',
                'HUMEDAD (%)': 'humedad_porcentual',
                'DIRECCION DEL VIENTO (°)': 'direccion_viento',
                'VELOCIDAD DEL VIENTO (m/s)': 'velocidad_viento'
                # Agrega más columnas según sea necesario
            }, inplace=True)
            columns = ['temperatura','precipitacion','humedad_porcentual','direccion_viento','velocidad_viento']
            replace_sd_with_null(df,columns)
            connect_to_sql(df,directorio_especifico,fec_ini,fec_fin)
            print("archivo inserto ok",archivo)

def  readfile_moquegua(fec_ini,fec_fin):
    # Ingresa la ruta de tu directorio aquí
    

    directorio_especifico = 'moquegua'

    # Cambiar el directorio actual al directorio específico
    os.chdir(directorio_especifico)
    # Listar todos los archivos en el directorio específico
    archivos_en_directorio = os.listdir()
    extension = ".xlsx"

    archivos_con_extension = [archivo for archivo in archivos_en_directorio if archivo.endswith(extension)]
    print("archivos extraidos ",archivos_con_extension)
    print(f"Archivos con extensión {extension} en el directorio :")
    for archivo in archivos_con_extension:
        #2024_0801
        #print(archivo)
        if any(substring in archivo for substring in ['4723F1BE_MOQUEGUA']):
            print("archivo enviado a insertar",archivo)
            df = pd.read_excel(archivo)
            #print(df.head())
            df.rename(columns={
                'AÑO / MES / DÍA': 'ano_mes_dia',
                'TEMPERATURA (°C)': 'temperatura',
                'PRECIPITACIÓN (mm/hora)': 'precipitacion',
                'HUMEDAD (%)': 'humedad_porcentual',
                'DIRECCION DEL VIENTO (°)': 'direccion_viento',
                'VELOCIDAD DEL VIENTO (m/s)': 'velocidad_viento'
                # Agrega más columnas según sea necesario
            }, inplace=True)
            columns = ['temperatura','precipitacion','humedad_porcentual','direccion_viento','velocidad_viento']
            replace_sd_with_null(df,columns)
            connect_to_sql(df,directorio_especifico,fec_ini,fec_fin)
            print("archivo inserto ok",archivo)
def  readfile_junin(fec_ini,fec_fin):
    # Ingresa la ruta de tu directorio aquí
    

    directorio_especifico = 'junin'

    # Cambiar el directorio actual al directorio específico
    os.chdir(directorio_especifico)
    # Listar todos los archivos en el directorio específico
    archivos_en_directorio = os.listdir()
    extension = ".xlsx"

    archivos_con_extension = [archivo for archivo in archivos_en_directorio if archivo.endswith(extension)]
    print("archivos extraidos ",archivos_con_extension)
    print(f"Archivos con extensión {extension} en el directorio :")
    for archivo in archivos_con_extension:
        #2024_0801
        #print(archivo)
        if any(substring in archivo for substring in ['47E8568A_SANTA ANA']):
            print("archivo enviado a insertar",archivo)
            df = pd.read_excel(archivo)
            #print(df.head())
            df.rename(columns={
                'AÑO / MES / DÍA': 'ano_mes_dia',
                'TEMPERATURA (°C)': 'temperatura',
                'PRECIPITACIÓN (mm/hora)': 'precipitacion',
                'HUMEDAD (%)': 'humedad_porcentual',
                'DIRECCION DEL VIENTO (°)': 'direccion_viento',
                'VELOCIDAD DEL VIENTO (m/s)': 'velocidad_viento'
                # Agrega más columnas según sea necesario
            }, inplace=True)
            columns = ['temperatura','precipitacion','humedad_porcentual','direccion_viento','velocidad_viento']
            replace_sd_with_null(df,columns)
            connect_to_sql(df,directorio_especifico,fec_ini,fec_fin)
            print("archivo inserto ok",archivo)


def  readfile_ayacucho(fec_ini,fec_fin):
    # Ingresa la ruta de tu directorio aquí
    

    directorio_especifico = 'ayacucho'

    # Cambiar el directorio actual al directorio específico
    os.chdir(directorio_especifico)
    # Listar todos los archivos en el directorio específico
    archivos_en_directorio = os.listdir()
    extension = ".xlsx"

    archivos_con_extension = [archivo for archivo in archivos_en_directorio if archivo.endswith(extension)]
    print("archivos extraidos  ",archivos_con_extension)
    print(f"Archivos con extensión {extension} en el directorio :")
    for archivo in archivos_con_extension:
        #2024_0801

        if any(substring in archivo for substring in ['47290068_HUAC-HUAS']):
            print("archivo enviado a insertar",archivo)

            df = pd.read_excel(archivo)
            #print(df.head())
            df.rename(columns={
                'AÑO / MES / DÍA': 'ano_mes_dia',
                'TEMPERATURA (°C)': 'temperatura',
                'PRECIPITACIÓN (mm/hora)': 'precipitacion',
                'HUMEDAD (%)': 'humedad_porcentual',
                'DIRECCION DEL VIENTO (°)': 'direccion_viento',
                'VELOCIDAD DEL VIENTO (m/s)': 'velocidad_viento'
                # Agrega más columnas según sea necesario
            }, inplace=True)
            columns = ['temperatura','precipitacion','humedad_porcentual','direccion_viento','velocidad_viento']
            replace_sd_with_null(df,columns)
            connect_to_sql(df,directorio_especifico,fec_ini,fec_fin)
            print("archivo inserto ok",archivo)
#readfile_ayacucho
            
def  readfile_cajamarca(fec_ini,fec_fin):
    # Ingresa la ruta de tu directorio aquí
    

    directorio_especifico = 'cajamarca'

    # Cambiar el directorio actual al directorio específico
    os.chdir(directorio_especifico)
    # Listar todos los archivos en el directorio específico
    archivos_en_directorio = os.listdir()
    extension = ".xlsx"

    archivos_con_extension = [archivo for archivo in archivos_en_directorio if archivo.endswith(extension)]
    print("archivos extraidos ",archivos_con_extension)
    print(f"Archivos con extensión {extension} en el directorio :")
    for archivo in archivos_con_extension:
        #2024_0801
        #print(archivo)
        if any(substring in archivo for substring in ['47E3055E_CHANCAY BAÑOS','4726A602_CUTERVO','4727F484_CHUGUR','4729F0EC_CAJABAMBA']):
            print("archivo enviado a insertar",archivo)
            df = pd.read_excel(archivo)
            #print(df.head())
            df.rename(columns={
                'AÑO / MES / DÍA': 'ano_mes_dia',
                'TEMPERATURA (°C)': 'temperatura',
                'PRECIPITACIÓN (mm/hora)': 'precipitacion',
                'HUMEDAD (%)': 'humedad_porcentual',
                'DIRECCION DEL VIENTO (°)': 'direccion_viento',
                'VELOCIDAD DEL VIENTO (m/s)': 'velocidad_viento'
                # Agrega más columnas según sea necesario
            }, inplace=True)
            columns = ['temperatura','precipitacion','humedad_porcentual','direccion_viento','velocidad_viento']
            replace_sd_with_null(df,columns)
            connect_to_sql(df,directorio_especifico,fec_ini,fec_fin)
            print("archivo inserto ok",archivo)

def  readfile_huancavelica(fec_ini,fec_fin):
    # Ingresa la ruta de tu directorio aquí
    

    directorio_especifico = 'huancavelica'

    # Cambiar el directorio actual al directorio específico
    os.chdir(directorio_especifico)
    # Listar todos los archivos en el directorio específico
    archivos_en_directorio = os.listdir()
    extension = ".xlsx"

    archivos_con_extension = [archivo for archivo in archivos_en_directorio if archivo.endswith(extension)]
    print("archivos extraidos ",archivos_con_extension)
    print(f"Archivos con extensión {extension} en el directorio :")
    for archivo in archivos_con_extension:
        #2024_0801
        #print(archivo)
        if any(substring in archivo for substring in ['4728F216_CORDOVA','4729131E_SANTIAGO DE CHOCORVOS','4729131E_SANTIAGO DE CHOCORVOS']):
            print("archivo enviado a insertar",archivo)
            df = pd.read_excel(archivo)
            #print(df.head())
            df.rename(columns={
                'AÑO / MES / DÍA': 'ano_mes_dia',
                'TEMPERATURA (°C)': 'temperatura',
                'PRECIPITACIÓN (mm/hora)': 'precipitacion',
                'HUMEDAD (%)': 'humedad_porcentual',
                'DIRECCION DEL VIENTO (°)': 'direccion_viento',
                'VELOCIDAD DEL VIENTO (m/s)': 'velocidad_viento'
                # Agrega más columnas según sea necesario
            }, inplace=True)
            columns = ['temperatura','precipitacion','humedad_porcentual','direccion_viento','velocidad_viento']
            replace_sd_with_null(df,columns)
            connect_to_sql(df,directorio_especifico,fec_ini,fec_fin)
            print("archivo inserto ok",archivo)
def  readfile_huanuco(fec_ini,fec_fin):
    # Ingresa la ruta de tu directorio aquí
    

    directorio_especifico = 'huanuco'

    # Cambiar el directorio actual al directorio específico
    os.chdir(directorio_especifico)
    # Listar todos los archivos en el directorio específico
    archivos_en_directorio = os.listdir()
    extension = ".xlsx"

    archivos_con_extension = [archivo for archivo in archivos_en_directorio if archivo.endswith(extension)]
    print("archivos extraidos ",archivos_con_extension)
    print(f"Archivos con extensión {extension} en el directorio :")
    for archivo in archivos_con_extension:
        #2024_0801
        #print(archivo)
        if any(substring in archivo for substring in ['47270400_TINGO MARIA']):
            print("archivo enviado a insertar",archivo)
            df = pd.read_excel(archivo)
            #print(df.head())
            
            df.rename(columns={
                'AÑO / MES / DÍA': 'ano_mes_dia',
                'TEMPERATURA (°C)': 'temperatura',
                'PRECIPITACIÓN (mm/hora)': 'precipitacion',
                'HUMEDAD (%)': 'humedad_porcentual',
                'DIRECCION DEL VIENTO (°)': 'direccion_viento',
                'VELOCIDAD DEL VIENTO (m/s)': 'velocidad_viento'
                # Agrega más columnas según sea necesario
            }, inplace=True)

           
            columns = ['temperatura','precipitacion','humedad_porcentual','direccion_viento','velocidad_viento']
            replace_sd_with_null(df,columns)
            connect_to_sql(df,directorio_especifico,fec_ini,fec_fin)
            print("archivo inserto ok",archivo)
def  readfile_lambayeque(fec_ini,fec_fin):
    # Ingresa la ruta de tu directorio aquí
    

    directorio_especifico = 'lambayeque'

    # Cambiar el directorio actual al directorio específico
    os.chdir(directorio_especifico)
    # Listar todos los archivos en el directorio específico
    archivos_en_directorio = os.listdir()
    extension = ".xlsx"

    archivos_con_extension = [archivo for archivo in archivos_en_directorio if archivo.endswith(extension)]
    print("archivos extraidos ",archivos_con_extension)
    print(f"Archivos con extensión {extension} en el directorio :")
    for archivo in archivos_con_extension:
        #2024_0801
        #print(archivo)
        if any(substring in archivo for substring in ['200801_PUCHACA']):
            print("archivo enviado a insertar",archivo)
            df = pd.read_excel(archivo)
            #print(df.head())
            df.rename(columns={
                'AÑO / MES / DÍA': 'ano_mes_dia',
                'TEMPERATURA (°C)': 'temperatura',
                'PRECIPITACIÓN (mm/hora)': 'precipitacion',
                'HUMEDAD (%)': 'humedad_porcentual',
                'DIRECCION DEL VIENTO (°)': 'direccion_viento',
                'VELOCIDAD DEL VIENTO (m/s)': 'velocidad_viento'
                # Agrega más columnas según sea necesario
            }, inplace=True)
            columns = ['temperatura','precipitacion','humedad_porcentual','direccion_viento','velocidad_viento']
            replace_sd_with_null(df,columns)
            connect_to_sql(df,directorio_especifico,fec_ini,fec_fin)
            print("archivo inserto ok",archivo) 