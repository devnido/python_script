
import pandas as pd
import os
import glob
import pyodbc
import numpy as np
import re
import sys

path = os.getcwd() 
files = glob.glob(os.path.join(path, "datos/posdetail*.csv"))

conn = pyodbc.connect('''DRIVER={SQL Server}; Server=10.2.12.4; 
                        UID=sa; PWD=C4d4qu3S.2024; DataBase=PIXELPOINT_DOMINO_BI''')
 
dwh_posdetail = pd.DataFrame()


dwh_posdetail_columns = pd.read_sql("select top 1 * from [dbo].[posdetail]", conn)




for filename in files:
    posdetail = pd.DataFrame()
    number = re.search(r'_(\d+)\.csv', filename)
    extracted_number = int(number.group(1))
    posdetail = pd.DataFrame()
    dwh_posdetail = pd.read_sql(f"SELECT UNIQUEID, STORENUM FROM PIXELPOINT_DOMINO_BI.dbo.posdetail WHERE storenum = {extracted_number} and timeord >= DATEADD(DAY, -105, GETDATE())", conn)
    print(extracted_number)
    if extracted_number == 56:
        for chunk in pd.read_csv(filename, sep=';', decimal=',' ,chunksize=40000, encoding='latin1'):
                df = chunk.iloc[:,:36]
                cols = list(df.columns)
                cols.insert(2, cols.pop(cols.index('StoreNum')))
                df['StoreNum'] = extracted_number
                df = df[cols]
                df.replace([np.inf, -np.inf, np.nan], 0, inplace=True)
                df = df.astype(dwh_posdetail_columns.dtypes.to_dict())   
                df['OpenDate'] = df['OpenDate'].dt.floor('T')
                df['OpenDate'] = pd.to_datetime(df['OpenDate'])
                posdetail = pd.concat([posdetail, df])


        missing_rows_posdetail = posdetail[~posdetail[['UNIQUEID', 'StoreNum']].apply(tuple, axis=1).isin(dwh_posdetail[['UNIQUEID', 'STORENUM']].apply(tuple, axis=1))]

        mask = missing_rows_posdetail['OpenDate'] <= '01-01-1900'


        missing_rows_posdetail.loc[mask, 'OpenDate'] = missing_rows_posdetail.loc[mask, 'TIMEORD'].dt.floor('D')



        cursor = conn.cursor()


        missing_rows_posdetail.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        missing_rows_posdetail = missing_rows_posdetail.fillna(0)

        max_length = 35


        missing_rows_posdetail['LineDes'] = missing_rows_posdetail['LineDes'].apply(lambda x: x[:max_length] if isinstance(x, str) else x)

        values = missing_rows_posdetail.values.tolist()

        query = "INSERT INTO posdetail  VALUES (?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"


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
        for chunk in pd.read_csv(filename, sep=';', decimal='.' ,chunksize=40000, encoding='latin1'):
                df = chunk.iloc[:,:36]
                cols = list(df.columns)
                cols.insert(2, cols.pop(cols.index('StoreNum')))
                df['StoreNum'] = extracted_number
                df = df[cols]
                df.replace([np.inf, -np.inf, np.nan], 0, inplace=True)
                df = df.astype(dwh_posdetail_columns.dtypes.to_dict())   
                df['OpenDate'] = df['OpenDate'].dt.floor('T')
                df['OpenDate'] = pd.to_datetime(df['OpenDate'])
                posdetail = pd.concat([posdetail, df])
        missing_rows_posdetail = posdetail[~posdetail[['UNIQUEID', 'StoreNum']].apply(tuple, axis=1).isin(dwh_posdetail[['UNIQUEID', 'STORENUM']].apply(tuple, axis=1))]

        mask = missing_rows_posdetail['OpenDate'] <= '01-01-1900'


        missing_rows_posdetail.loc[mask, 'OpenDate'] = missing_rows_posdetail.loc[mask, 'TIMEORD'].dt.floor('D')



        cursor = conn.cursor()


        missing_rows_posdetail.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
        missing_rows_posdetail = missing_rows_posdetail.fillna(0)

        max_length = 35


        missing_rows_posdetail['LineDes'] = missing_rows_posdetail['LineDes'].apply(lambda x: x[:max_length] if isinstance(x, str) else x)

        values = missing_rows_posdetail.values.tolist()

        query = "INSERT INTO posdetail  VALUES (?,?,?,?,?,? ,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"


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