import pandas as pd
import os
import glob
import pyodbc
import numpy as np
import re
import sys

path = os.getcwd() 
files = glob.glob(os.path.join(path, "datos/posheader*.csv"))

conn = pyodbc.connect('''DRIVER={SQL Server}; Server=10.2.12.4; 
                        UID=sa; PWD=C4d4qu3S.2024; DataBase=PIXELPOINT_DOMINO_BI''')

dwh_posheader_columns = pd.read_sql("select top 1 * from [dbo].[posheader]", conn)
dwh_posheader_columns = dwh_posheader_columns.iloc[:,:50]


for filename in files:
    number = re.search(r'_(\d+)\.csv', filename)
    extracted_number = int(number.group(1))
    posheader = pd.DataFrame()
    print(filename)
    dwh_posheader = pd.read_sql(f"SELECT TRANSACT, STORENUM FROM PIXELPOINT_DOMINO_BI.dbo.posheader where storenum = {extracted_number} and timestart >= DATEADD(DAY, -105, GETDATE())", conn)
    if extracted_number == 56:
        
        for chunk in pd.read_csv(filename, sep=';', decimal=',' ,chunksize=10000, encoding='latin1'):
                df = chunk.iloc[:,:50]
                cols = list(df.columns)
                cols.insert(2, cols.pop(cols.index('StoreNum')))
                df['StoreNum'] = extracted_number
                df = df[cols]
                df.replace([np.inf, -np.inf, np.nan], 0, inplace=True)
                mask_invalid_timeend = ~df['TIMEEND'].astype(str).str.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
                df.loc[mask_invalid_timeend, 'TIMEEND'] = df.loc[mask_invalid_timeend, 'TIMESTART']
                df = df.astype(dwh_posheader_columns.dtypes.to_dict())
                df['OPENDATE'] = df['OPENDATE'].dt.floor('T')
                df['OPENDATE'] = pd.to_datetime(df['OPENDATE'])            
                posheader = pd.concat([posheader, df])

        missing_rows_posheader = posheader[~posheader[['TRANSACT', 'StoreNum']].apply(tuple, axis=1).isin(dwh_posheader[['TRANSACT', 'STORENUM']].apply(tuple, axis=1))]

        # Crear máscara para filas con 'OPENDATE' <= '01-01-1900'
        mask = missing_rows_posheader['OPENDATE'] <= '01-01-1900'

        # Reemplazar valores en 'OPENDATE' con el valor de 'TIMESTART' seteado a las 00:00:00
        missing_rows_posheader.loc[mask, 'OPENDATE'] = missing_rows_posheader.loc[mask, 'TIMESTART'].dt.floor('D')

        # Crear máscara para filas con 'OPENDATE' <= '01-01-1900'
        mask = missing_rows_posheader['ScheduleDate'] <= '01-01-1900'

        # Reemplazar valores en 'OPENDATE' con el valor de 'TIMESTART' seteado a las 00:00:00
        missing_rows_posheader.loc[mask, 'ScheduleDate'] = missing_rows_posheader.loc[mask, 'TIMESTART'].dt.floor('D')


        cursor = conn.cursor()



        values = missing_rows_posheader.values.tolist()

        query = "INSERT  INTO POSHEADER (TRANSACT, TABLENUM, StoreNum, TIMESTART, TIMEEND, NUMCUST, TAX1, TAX2, TAX3, TAX4, TAX5, TAX1ABLE, TAX2ABLE,TAX3ABLE, TAX4ABLE, TAX5ABLE, NETTOTAL, WHOSTART, WHOCLOSE, ISSPLIT, SALETYPEINDEX, EXP, WAITINGAUTH, STATNUM, STATUS, FINALTOTAL, PUNCHINDEX, Gratuity, OPENDATE, MemCode,TotalPoints, PointsApplied, UpdateStatus, ISDelivery,ScheduleDate, Tax1Exempt, Tax2Exempt, Tax3Exempt, Tax4Exempt,Tax5Exempt, MEMRATE, MealTime, IsInternet, RevCenter,PunchIdxStart, StatNumStart, SecNum, GratAmount, ShipTo,EnforcedGrat) VALUES (?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"


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
                df = chunk.iloc[:,:50]
                cols = list(df.columns)
                cols.insert(2, cols.pop(cols.index('StoreNum')))
                df['StoreNum'] = extracted_number
                df = df[cols]
                df.replace([np.inf, -np.inf, np.nan], 0, inplace=True)
                mask_invalid_timeend = ~df['TIMEEND'].astype(str).str.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
                df.loc[mask_invalid_timeend, 'TIMEEND'] = df.loc[mask_invalid_timeend, 'TIMESTART']
                df = df.astype(dwh_posheader_columns.dtypes.to_dict())
                df['OPENDATE'] = df['OPENDATE'].dt.floor('T')
                df['OPENDATE'] = pd.to_datetime(df['OPENDATE'])            
                posheader = pd.concat([posheader, df])
        missing_rows_posheader = posheader[~posheader[['TRANSACT', 'StoreNum']].apply(tuple, axis=1).isin(dwh_posheader[['TRANSACT', 'STORENUM']].apply(tuple, axis=1))]

        # Crear máscara para filas con 'OPENDATE' <= '01-01-1900'
        mask = missing_rows_posheader['OPENDATE'] <= '01-01-1900'

        # Reemplazar valores en 'OPENDATE' con el valor de 'TIMESTART' seteado a las 00:00:00
        missing_rows_posheader.loc[mask, 'OPENDATE'] = missing_rows_posheader.loc[mask, 'TIMESTART'].dt.floor('D')

        # Crear máscara para filas con 'OPENDATE' <= '01-01-1900'
        mask = missing_rows_posheader['ScheduleDate'] <= '01-01-1900'

        # Reemplazar valores en 'OPENDATE' con el valor de 'TIMESTART' seteado a las 00:00:00
        missing_rows_posheader.loc[mask, 'ScheduleDate'] = missing_rows_posheader.loc[mask, 'TIMESTART'].dt.floor('D')


        cursor = conn.cursor()



        values = missing_rows_posheader.values.tolist()

        query = "INSERT  INTO POSHEADER (TRANSACT, TABLENUM, StoreNum, TIMESTART, TIMEEND, NUMCUST, TAX1, TAX2, TAX3, TAX4, TAX5, TAX1ABLE, TAX2ABLE,TAX3ABLE, TAX4ABLE, TAX5ABLE, NETTOTAL, WHOSTART, WHOCLOSE, ISSPLIT, SALETYPEINDEX, EXP, WAITINGAUTH, STATNUM, STATUS, FINALTOTAL, PUNCHINDEX, Gratuity, OPENDATE, MemCode,TotalPoints, PointsApplied, UpdateStatus, ISDelivery,ScheduleDate, Tax1Exempt, Tax2Exempt, Tax3Exempt, Tax4Exempt,Tax5Exempt, MEMRATE, MealTime, IsInternet, RevCenter,PunchIdxStart, StatNumStart, SecNum, GratAmount, ShipTo,EnforcedGrat) VALUES (?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"


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
    print(f'local {extracted_number} finalizado')


#Confirmar los cambios en la base de datos

conn.close()
print("Filas insertadas")