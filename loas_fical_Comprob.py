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
fiscal_columns = ['ComprobNum', 'StatNum', 'IsActive','Transact', 'ComprobType', 'OpenDate', 'FiscalDate', 'PrintDate',
                   'CancelDate', 'FolioNum', 'Printed', 'PrinterNum', 'PrinterSerie', 'PrintCount', 'WHOSTART', 'WHOCLOSE',
                    'RevCenter', 'SaleType','Gratuity', 'TAX1', 'TAX2', 'TAX3', 'TAX4', 'TAX5', 'TAX1ABLE', 'TAX2ABLE',
                      'TAX3ABLE', 'TAX4ABLE', 'TAX5ABLE', 'NETTOTAL', 'FINALTOTAL', 'TABLENUM', 'NUMCUST', 'TotalPoints',
                        'PointsApplied', 'ActualNum', 'MemCode', 'MemNum', 'FiscalNum', 'Descript', 'RefComprobNum',
                         'FormatNum', 'SubType', 'stamp', 'CountNum', 'CASO', 'PrintCustomPay', 'RefFiscalDate', 
                         'StoreNum', 'DataType']  
# Tu lista de columnas aquí

# Preparar la consulta SQL
placeholders = ', '.join(['?'] * len(fiscal_columns))
column_names = ', '.join(fiscal_columns)
query = f"INSERT INTO DBO.fiscal_Comprob ({column_names}) VALUES ({placeholders})"
dwh_fiscal_columns = pd.read_sql("select top 1 * from [dbo].[fiscal_Comprob]", conn)
dwh_fiscal = pd.DataFrame()

# Procesar archivos
path = os.getcwd()
files = glob.glob(os.path.join(path, "datos/fiscal_comprob*.csv"))

for filename in files:
      # Busca el número de la tienda en el nombre del archivo
  number = re.search(r'_(\d+)\.csv', filename)
  extracted_number = int(number.group(1))
  fiscal = pd.DataFrame()

  dwh_fiscal = pd.read_sql(f"SELECT ComprobNum, StoreNum, Transact FROM PIXELPOINT_DOMINO_BI.dbo.fiscal_Comprob where StoreNum = {extracted_number} and FiscalDate >= DATEADD(DAY, -105, GETDATE()) ", conn)
  
  for chunk in pd.read_csv(filename, sep=';', decimal='.' ,chunksize=10000, encoding='latin1'):
      df = chunk.iloc[:,:56]
      df['StoreNum'] = extracted_number
      cols = list(df.columns)
      # por que cambia la columna 2  ?
      cols.insert(2, cols.pop(cols.index('StoreNum')))
      df = df[cols]
      df.replace([np.inf, -np.inf, np.nan], 0, inplace=True)
      df = df.astype(dwh_fiscal_columns.dtypes.to_dict())   
      df['OpenDate'] = df['OpenDate'].dt.floor('T')
      df['OpenDate'] = pd.to_datetime(df['OpenDate'])
      # por que selecciona columnas 26 ?
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