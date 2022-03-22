from httplib2 import GoogleLoginAuthentication
import mysql.connector
import os
from mysql.connector import errorcode
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
import numpy as np
#VALIDAÇÃO DE CREDENCIAIS E ESCOPO
SERVICE_ACCOUNT_FILE = 'keys.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

#CREDENCIAIS
creds = None
creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)


#PLANILHA ALVO      
SAMPLE_SPREADSHEET_ID = '12rvxZNR9xgHsHaAWv4tuemgiKmble6NDWg5Rp1DeVQI'

#SERVICE CALL
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()
sql_cmv = "SELECT YEAR(i.dt)`Ano`, MONTH(i.dt)`Mes`, IF(p.classe IS NULL or p.classe='','Sem Classe',descricao_classe(p.classe))`Classe`,i.cd_prod`Cd Prod`,p.nm`Nome`,i.vl_custo`Vl Custo`,SUM(i.qtd)`Qtd`,i.unid`Unid`,IFNULL(SUM(i.qtd)*i.vl_custo,0)`CMV` FROM item_ficha i LEFT JOIN produto p ON p.cd=i.cd_prod WHERE i.comp IS NULL AND YEAR(i.dt) >=2021 GROUP BY YEAR(i.dt), MONTH(i.dt), i.cd_prod,i.vl_unit,i.nm_prod ORDER BY p.classe"
writer = pd.ExcelWriter('cmv.xlsx', engine='xlsxwriter')

def cls(planilha):
        sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                        range=planilha, body={}).execute()
                                       
def ins(dataframe, planilha):
        request =  sheet.values().append(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                 range=planilha, valueInputOption="USER_ENTERED", body=dict(majorDimension = 'ROWS', 
                                 values = dataframe.T.reset_index().T.values.tolist())).execute()

print("PLANILHAS VAZIAS")

#CONECTA VILA MOEMA
try:
    conn = mysql.connector.connect(host='26.54.76.44', database='pub', user='root', password='#food#')
    print("Conexão Estabelecida")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Acesso Negado")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Banco de dados não existe")    
    else:
        print(err)
else:
    db_info=conn.get_server_info()
    print("Conectado ao sevidor MySQL versão ", db_info)
    cursor = conn.cursor()
    cursor.execute(sql_cmv)
    cmv = cursor.fetchall()
    df = pd.DataFrame(cmv, columns=cursor.column_names)
    print(df.head(10))
    df.to_excel(writer, sheet_name="cmv_moema", index=False ) 
    df.replace(np.nan, '', inplace=True) 
    cls("cmv_moema")
    ins(df, "cmv_moema!A1")
    cursor.close()
    conn.close()
    print("Conexão ao MySQL foi encerrada")

#CONECTA RINCAO
try:
    conn = mysql.connector.connect(host='26.232.34.52', database='pub', user='root', password='#food#')
    print("Conexão Estabelecida")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Acesso Negado")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Banco de dados não existe")    
    else:
        print(err)
else:
    db_info=conn.get_server_info()
    print("Conectado ao sevidor MySQL versão ", db_info)
    cursor = conn.cursor()
    cursor.execute(sql_cmv)
    cmv = cursor.fetchall()
    df2 = pd.DataFrame(cmv, columns=cursor.column_names)
    print(df2.head(10))
    df2.to_excel(writer, sheet_name="cmv_rincao", index=False )   
    df2.replace(np.nan, '', inplace=True) 
    cls("cmv_rincao")
    ins(df2, "cmv_rincao") 
    cursor.close()
    conn.close()
    print("Conexão ao MySQL foi encerrada")

#CONECTA LAGUNA
try:
    conn = mysql.connector.connect(host='26.146.9.96', database='pub', user='root', password='#food#')
    print("Conexão Estabelecida")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Acesso Negado")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Banco de dados não existe")    
    else:
        print(err)
else:
    db_info=conn.get_server_info()
    print("Conectado ao sevidor MySQL versão ", db_info)
    cursor = conn.cursor()
    cursor.execute(sql_cmv)
    cmv = cursor.fetchall()
    df3 = pd.DataFrame(cmv, columns=cursor.column_names)
    print(df3.head(10))
    df3.to_excel(writer, sheet_name="cmv_laguna", index=False )
    df3.replace(np.nan, '', inplace=True)
    cls("cmv_laguna") 
    ins(df3, "cmv_laguna")     
    cursor.close()
    conn.close()
    print("Conexão ao MySQL foi encerrada")

#CONECTA FAROL
try:
    conn = mysql.connector.connect(host='26.110.175.12', database='pub', user='root', password='#food#')
    print("Conexão Estabelecida")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Acesso Negado")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Banco de dados não existe")    
    else:
        print(err)
else:
    db_info=conn.get_server_info()
    print("Conectado ao sevidor MySQL versão ", db_info)
    cursor = conn.cursor()
    cursor.execute(sql_cmv)
    cmv = cursor.fetchall()
    df4 = pd.DataFrame(cmv, columns=cursor.column_names)
    print(df4.head(10))
    df4.to_excel(writer, sheet_name="cmv_farol", index=False )   
    df4.replace(np.nan, '', inplace=True) 
    cls("cmv_farol")
    ins(df4, "cmv_farol")  
    cursor.close()
    conn.close()
    print("Conexão ao MySQL foi encerrada")


writer.save()
