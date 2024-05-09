import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime

import schedule
import time

# Ruta a la carpeta "Origen"
ruta_origen = './Origen/'

# Ruta a la carpeta "Respaldo"
ruta_respaldo = './Respaldo/'

def origenArespaldo():
    # Recorrer las carpetas y archivos dentro de "Origen"
    for root, dirs, files in os.walk(ruta_origen):
        for name in files:
            # Verificar si el archivo es un archivo CSV
            if name.endswith('.csv'):
                # Obtener el nombre de la carpeta actual
                nombre_carpeta = os.path.basename(root)
                
                # Construir la ruta completa al archivo CSV
                ruta_csv = os.path.join(root, name)
                
                # Leer el archivo CSV
                datos = pd.read_csv(ruta_csv, sep=';')
                
                # Agregar una nueva columna llamada "Local" con el nombre de la carpeta
                datos['Local'] = nombre_carpeta
                
                # Construir el nombre del nuevo archivo CSV
                nombre_archivo = os.path.splitext(name)[0]
                nombre_nuevo_archivo = f'{nombre_archivo}_{nombre_carpeta}_{datetime.now().strftime("%Y%m%d")}.csv'
                
                # Construir la ruta para guardar el nuevo archivo CSV en la carpeta "Respaldo"
                ruta_nuevo_csv = os.path.join(ruta_respaldo, nombre_nuevo_archivo)
                
                # Validar si el archivo ya existe en la carpeta "Respaldo"
                if os.path.exists(ruta_nuevo_csv):
                    # Leer el archivo existente en "Respaldo"
                    datos_existente = pd.read_csv(ruta_nuevo_csv, sep=';')
                    
                    # Verificar si los nombres de archivo coinciden y si los datos son iguales (ignorando la columna "Local")
                    if nombre_archivo == nombre_archivo.split('_')[0] and datos.drop(columns='Local').equals(datos_existente.drop(columns='Local')):
                        print(f'El archivo {ruta_csv} ya existe en Respaldo y su contenido es similar. Se omite la creación del nuevo archivo.')
                        continue
                
                # Guardar el nuevo archivo CSV con la columna adicional
                datos.to_csv(ruta_nuevo_csv, sep=';', index=False)
                
                print(f'Se ha creado el archivo {ruta_nuevo_csv} con éxito.')
            
            
# Función para conectar a la base de datos MySQL
def conectar_mysql():
    try:
        conexion = mysql.connector.connect(
            host='localhost',
            database='examen_integracion_p1',
            user='root',
            password='gabely1234'
        )
        if conexion.is_connected():
            print('Conexión establecida.')
            return conexion
    except Error as e:
        print(f'Error al conectar a MySQL: {e}')
        
        
def respaldoAMySQL():
    # Recorrer los archivos en la carpeta "Respaldo"
    for root, dirs, files in os.walk(ruta_respaldo):
        for name in files:
            # Verificar si el archivo es un archivo CSV
            if name.endswith('.csv'):
                # Construir la ruta completa al archivo CSV
                ruta_csv = os.path.join(root, name)
                
                # Leer el archivo CSV
                datos = pd.read_csv(ruta_csv, sep=';')
                
                # Conectar a la base de datos MySQL
                conexion = conectar_mysql()
                
                # Insertar los datos en la tabla "ventas_consolidadas"
                if conexion:
                    cursor = conexion.cursor()
                    for index, row in datos.iterrows():
                        # Verificar si ya existe un registro con los mismos valores en la base de datos
                        sql_verificar = "SELECT * FROM ventas_consolidadas WHERE IdTransaccion = %s AND Fecha = %s AND IdCategoria = %s AND IdProducto = %s AND IdLocal = %s"
                        valores_verificar = (row['IdTransaccion'], datetime.strptime(row['Fecha'], '%m/%d/%Y').strftime('%Y-%m-%d'), row['IdCategoria'], row['IdProducto'], row['Local'])
                        cursor.execute(sql_verificar, valores_verificar)
                        registros_existente = cursor.fetchall()
                        if not registros_existente:
                            # Convertir la cadena de fecha al formato adecuado para MySQL
                            fecha_mysql = datetime.strptime(row['Fecha'], '%m/%d/%Y').strftime('%Y-%m-%d')
                            sql_insertar = "INSERT INTO ventas_consolidadas (IdTransaccion, IdLocal, Fecha, IdCategoria, IdProducto, Cantidad, PrecioUnitario, TotalVenta) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                            valores_insertar = (row['IdTransaccion'], row['Local'], fecha_mysql, row['IdCategoria'], row['IdProducto'], row['Cantidad'], row['PrecioUnitario'], row['TotalVenta'])
                            cursor.execute(sql_insertar, valores_insertar)
                        else:
                            print(f'Se omitió la inserción del registro con IdTransaccion={row["IdTransaccion"]}, Fecha={row["Fecha"]}, IdCategoria={row["IdCategoria"]}, IdProducto={row["IdProducto"]} y Local={row["Local"]} debido a que ya existe en la base de datos.')
                    conexion.commit()
                    print(f'Datos insertados en la tabla ventas_consolidadas desde el archivo {ruta_csv}.')
                    cursor.close()
                    conexion.close()
                    
# Programar las tareas para ejecutar una vez al día
schedule.every().day.at("20:45").do(origenArespaldo)
schedule.every().day.at("20:45").do(respaldoAMySQL)

# Ejecutar las tareas programadas
while True:
    schedule.run_pending()
    time.sleep(1)