import pandas as pd
import os
import glob
import pyodbc
import re
import numpy as np

# Configuración de la conexión
conn = pyodbc.connect('''DRIVER={SQL Server}; Server=10.2.12.4; 
                        UID=sa; PWD=C4d4qu3S.2024; DataBase=PIXELPOINT_DOMINO_BI''')

# Lista de columnas
prod_columns = ['PRODNUM', 'DESCRIPT', 'StoreNum', 'REPORTNO', 'PRICEA', 'PRICEB', 'PRICEC', 'TAX1', 'TAX2', 'TAX3', 
    'TAX4', 'TAX5', 'ISACTIVE', 'PRODTYPE', 'USEITEMCAT', 'QUESTION1', 'QUESTION2', 'QUESTION3', 'QUESTION4', 
    'QUESTION5', 'REFCODE', 'PRICEMODE', 'AccountCode', 'MemPoints', 'PRICED', 'PRICEE', 'PRICEF', 'PRICEG', 
    'PRICEH', 'PRICEI', 'PRICEJ', 'PLink']  # Tu lista de columnas aquí

# Preparar la consulta SQL
placeholders = ', '.join(['?'] * len(prod_columns))
column_names = ', '.join(prod_columns)
query = f"INSERT INTO DBO.PRODUCT ({column_names}) VALUES ({placeholders})"
dwh_prod_columns = pd.read_sql("select top 1 * from [dbo].[Product]", conn)

product = pd.DataFrame()

# Procesar archivos
path = os.getcwd()
files = glob.glob(os.path.join(path, "datos/product*.csv"))
for filename in files:
    number = re.search(r'_(\d+)\.csv', filename)
    extracted_number = int(number.group(1))
    
    for chunk in pd.read_csv(filename, sep=';', decimal='.' ,chunksize=10000, encoding='latin1'):
        chunk['StoreNum'] = extracted_number
        chunk = chunk[prod_columns]
        chunk.replace([np.inf, -np.inf, np.nan], 0, inplace=True)
        chunk = chunk.astype(dwh_prod_columns.dtypes.to_dict())   
        chunk['DESCRIPT'] = chunk['DESCRIPT'].str.slice(0, 30)
        product = pd.concat([product, chunk])
        
        
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM DBO.PRODUCT")  # Considerar la lógica de borrado
        cursor.executemany(query, product.values.tolist())
        conn.commit()
        print(f"Datos insertados con éxito desde {filename}")
    except pyodbc.Error as e:
        conn.rollback()
        print(f"Error al insertar datos desde {filename}: {e}")
    finally:
        cursor.close()

conn.close()
