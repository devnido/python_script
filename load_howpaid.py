	
import pandas as pd
import os
import glob
import pyodbc
import numpy as np
import re
import sys

path = os.getcwd() 
files = glob.glob(os.path.join(path, "datos/howpaid*.csv"))

conn = pyodbc.connect('''DRIVER={SQL Server}; Server=10.2.12.4; 
                        UID=sa; PWD=C4d4qu3S.2024; DataBase=PIXELPOINT_DOMINO_BI''')

dwh_howpaid = pd.DataFrame()

dwh_howpaid_columns = pd.read_sql("select top 1 * from [dbo].[HowPaid]", conn)

howpaid = pd.DataFrame()

posicion_final_store_num = 2

for filename in files:
    # Busca el número de la tienda en el nombre del archivo
    number = re.search(r'_(\d+)\.csv', filename)
    extracted_number = int(number.group(1))
    howpaid = pd.DataFrame()
    dwh_howpaid = pd.read_sql(f"SELECT HOWPAIDLINK, STORENUM FROM PIXELPOINT_DOMINO_BI.dbo.Howpaid where storenum = {extracted_number} and transdate >= DATEADD(DAY, -105, GETDATE()) ", conn)
    
    if extracted_number == 37:
        for chunk in pd.read_csv(filename, sep=';', decimal=',' ,chunksize=10000, encoding='latin1'):
            df = chunk.iloc[:,:36]
            df['StoreNum'] = extracted_number
            cols = list(df.columns)
            cols.insert(2, cols.pop(cols.index('StoreNum')))
            df = df[cols]
            df.replace([np.inf, -np.inf, np.nan], 0, inplace=True)
            df = df.astype(dwh_howpaid_columns.dtypes.to_dict())   
            df['OPENDATE'] = df['OPENDATE'].dt.floor('T')
            df['OPENDATE'] = pd.to_datetime(df['OPENDATE'])
            df = df.iloc[:,:26]
            howpaid = pd.concat([howpaid, df])           
            missing_rows_howpaid = howpaid[~howpaid[['HowPaidLink', 'StoreNum']].apply(tuple, axis=1).isin(dwh_howpaid[['HOWPAIDLINK', 'STORENUM']].apply(tuple, axis=1))]


        cursor = conn.cursor()


        missing_rows_howpaid.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        missing_rows_howpaid = missing_rows_howpaid.fillna(0)




        missing_rows_howpaid['AUTHCODE'] = missing_rows_howpaid['AUTHCODE'].apply(lambda x: x[:15] if isinstance(x, str) else x)
        missing_rows_howpaid['PayReason'] = missing_rows_howpaid['PayReason'].apply(lambda x: x[:20] if isinstance(x, str) else x)


        values = missing_rows_howpaid.values.tolist()

        query = "INSERT INTO HOWPAID VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"



        total_registros = len(values)
        print(f"Total de registros a insertar: {total_registros}")

        registros_insertados = 0
        porcentaje_anterior = 0

        for value in values:
            try:
                cursor.execute(query, value)
                registros_insertados += 1
                porcentaje_actual = (registros_insertados / total_registros) * 100

                # Verificar si el porcentaje actual ha aumentado en al menos un 10% con respecto al porcentaje anterior
                if porcentaje_actual - porcentaje_anterior >= 10:
                    print(f"Progreso: {int(porcentaje_actual)}%")
                    porcentaje_anterior = porcentaje_actual

            except pyodbc.IntegrityError:
                print(f"Error de integridad al intentar insertar el registro: {value}. Registro omitido.")
                continue
            except pyodbc.Error as e:
                print(f"Otro error al intentar insertar el registro: {value}. Error: {e}")
                continue

        conn.commit()
    else:
        for chunk in pd.read_csv(filename, sep=';', decimal='.' ,chunksize=10000, encoding='latin1'):
            df = chunk.iloc[:,:36]
            df['StoreNum'] = extracted_number
            cols = list(df.columns)
            # por que en la posicion 2 ?
            cols.insert(2, cols.pop(cols.index('StoreNum')))
            # reordenar las columnas
            df = df[cols]
            
            df.replace([np.inf, -np.inf, np.nan], 0, inplace=True)

            df = df.astype(dwh_howpaid_columns.dtypes.to_dict())   
            df['OPENDATE'] = df['OPENDATE'].dt.floor('T')
            df['OPENDATE'] = pd.to_datetime(df['OPENDATE'])
            df = df.iloc[:,:26]
            howpaid = pd.concat([howpaid, df]) 

        missing_rows_howpaid = howpaid[~howpaid[['HowPaidLink', 'StoreNum']].apply(tuple, axis=1).isin(dwh_howpaid[['HOWPAIDLINK', 'STORENUM']].apply(tuple, axis=1))]


        cursor = conn.cursor()


        missing_rows_howpaid.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        missing_rows_howpaid = missing_rows_howpaid.fillna(0)




        missing_rows_howpaid['AUTHCODE'] = missing_rows_howpaid['AUTHCODE'].apply(lambda x: x[:15] if isinstance(x, str) else x)
        missing_rows_howpaid['PayReason'] = missing_rows_howpaid['PayReason'].apply(lambda x: x[:20] if isinstance(x, str) else x)


        values = missing_rows_howpaid.values.tolist()

        query = "INSERT INTO HOWPAID VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"



        total_registros = len(values)
        print(f"Total de registros a insertar: {total_registros}")

        registros_insertados = 0
        porcentaje_anterior = 0

        for value in values:
            try:
                cursor.execute(query, value)
                registros_insertados += 1
                porcentaje_actual = (registros_insertados / total_registros) * 100

                # Verificar si el porcentaje actual ha aumentado en al menos un 10% con respecto al porcentaje anterior
                if porcentaje_actual - porcentaje_anterior >= 10:
                    print(f"Progreso: {int(porcentaje_actual)}%")
                    porcentaje_anterior = porcentaje_actual

            except pyodbc.IntegrityError:
                print(f"Error de integridad al intentar insertar el registro: {value}. Registro omitido.")
                continue
            except pyodbc.Error as e:
                print(f"Otro error al intentar insertar el registro: {value}. Error: {e}")
                continue

        conn.commit()

conn.close()