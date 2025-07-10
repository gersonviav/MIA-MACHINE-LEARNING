import pandas as pd
from sqlalchemy import create_engine,types,text
import urllib.parse
from  datetime import datetime
import os
from dotenv import load_dotenv
import getpass

current_user = getpass.getuser()

load_dotenv()

def connect_to_sql(df,dep,fec_ini,fec_fin):
    # Configuración de la conexión a SQL Server con autenticación de Windows
    # Reemplaza 'mi_servidor', 'mi_base_de_datos' y 'mi_tabla' con tus propios valores
    server =os.getenv("DB_SERVER")
    database = os.getenv("DB_DATABASE")
    schema = os.getenv("DB_SCHEMA")
    trusted_connection = 'yes'  # Indica el uso de autenticación de Windows

    # Cadena de conexión a SQL Server con el nombre del controlador ODBC
    connection_string = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';Trusted_Connection='+trusted_connection)}"

    # Inicializar la variable de conexión fuera del bloque try
    connection = None

    # Conectar y enviar el DataFrame a SQL Server
    try:
        # Conectar a SQL Server con use_setinputsizes desactivado
        #df['ano_mes_dia'] = pd.to_datetime(df['ano_mes_dia']).dt.date

        engine = create_engine(connection_string, connect_args={'use_setinputsizes': False})
        connection = engine.connect()

        # Construye la consulta SQL dinámicamente usando join
        consulta_sql = f"DELETE FROM {schema}.ANALITICA_InfoSenamhi_{dep} WHERE ano_mes_dia BETWEEN '{fec_ini}' AND '{fec_fin}' "
        print(consulta_sql)
        connection.execute(text(consulta_sql))
        connection.commit()

        if dep == 'piura':
            code = '20'
        if dep == 'ancash':
            code = '02'
        if dep == 'ayacucho':
            code = '05'
        if dep == 'cajamarca':
            code = '06'
        if dep == 'huancavelica':
            code = '09'
        if dep == 'junin':
            code = '12'
        if dep == 'moquegua':
            code = '18'
        

        df['id'] = df['ano_mes_dia'].str.replace('/', '').str[-4:] +  df['HORA'].str.replace(':', '').str[0:2] + code
        
        #print(df['id'])

        # for  i in df['id'] :
        #      print(i)
        # Convertir la nueva columna 'id' en un valor entero
        df['id'] = df['id'].apply(lambda x: int(x))

        # O, si solo necesitas la fecha sin la información de tiempo
        df['ano_mes_dia'] = pd.to_datetime(df['ano_mes_dia']).dt.date
        
      
        df ["osLastApp"] = "Etl python senamhi"
        df["osFirstApp"] = "Etl python senamhi"
        print("zzz",df.head())
        # column_types = {
        #         'ano_mes_dia': types.Date,
        #          'hora' :'NVARCHAR(255)',
        #         'temperatura': 'FLOAT',
        #         'precipitacion': 'FLOAT',
        #         'humedad_porcentual': 'FLOAT',
        #         'direccion_viento': 'FLOAT',
        #         'velocidad_viento': 'FLOAT',
        #         'estacion' : 'NVARCHAR(255)',
        #         'lat' : 'FLOAT',
        #         'lon' : 'FLAOT',
        #         'ico' : 'NVARCHAR(255)',
        #         'departamento' : 'NVARCHAR(255)'
        #         }
        #column_types = {column['name']: column['type'] for column in columns}
        # Enviar el DataFrame a la tabla en SQL Server (reemplaza 'mi_tabla' con el nombre de tu tabla)
        df.to_sql(f'ANALITICA_InfoSenamhi_{dep}', con=connection, if_exists='append', index=False, schema=f'{schema}',dtype={'id': types.INTEGER()})
        # Set the primary key constraint after writing the DataFrame to the database

        print("DataFrame enviado correctamente a SQL Server.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Cerrar la conexión si está definida
        if connection:
            connection.close()

# Llama a la función con tu DataFrame como argumento
# Por ejemplo: connect_to_sql(tu_dataframe)
