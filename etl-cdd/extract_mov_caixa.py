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
SAMPLE_SPREADSHEET_ID = '1lACQkex8-477JMvLPpEnILBhhF2L8PMSV2WP7KhL7_g'

#SERVICE CALL
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()
sql_mov_caixa = "SELECT YEAR(dt)`ANO`, MONTH(dt)`MÊS`, DAY(dt)`DIA`, HOUR(dt)`HORA`, SUM(IF(forma='FIADO' AND (cd_cli NOT IN (639, 745, 781, 866, 236)),vl,0))`Crediário`, SUM(IF(operacao='RECEBIMENTO',vl,0))`Recebimento`,SUM(IF(forma='CARTAO',vl,0))`Cartão`, SUM(IF(operacao='VENDA' AND (forma='DINHEIRO' OR forma='TROCO'), vl, 0))`Dinheiro`, SUM(IF(operacao='SANGRIA',vl,0))*-1 `Sangria` FROM mov_caixa WHERE YEAR(dt) >= 2021 GROUP BY YEAR(dt), MONTH(dt), DAY(dt), HOUR(dt) ORDER BY dt"
writer = pd.ExcelWriter('mov_caixa.xlsx', engine='xlsxwriter')

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
    cursor.execute(sql_mov_caixa)
    mov_caixa = cursor.fetchall()
    df = pd.DataFrame(mov_caixa, columns=cursor.column_names)
    print(df.head(10))
    df.to_excel(writer, sheet_name="mov_caixa_moema", index=False ) 
    df.replace(np.nan, '', inplace=True) 
    cls("mov_caixa_moema")
    ins(df, "mov_caixa_moema!A1")
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
    cursor.execute(sql_mov_caixa)
    mov_caixa = cursor.fetchall()
    df2 = pd.DataFrame(mov_caixa, columns=cursor.column_names)
    print(df2.head(10))
    df2.to_excel(writer, sheet_name="mov_caixa_rincao", index=False )   
    df2.replace(np.nan, '', inplace=True) 
    cls("mov_caixa_rincao")
    ins(df2, "mov_caixa_rincao") 
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
    cursor.execute(sql_mov_caixa)
    mov_caixa = cursor.fetchall()
    df3 = pd.DataFrame(mov_caixa, columns=cursor.column_names)
    print(df3.head(10))
    df3.to_excel(writer, sheet_name="mov_caixa_laguna", index=False )
    df3.replace(np.nan, '', inplace=True)
    cls("mov_caixa_laguna") 
    ins(df3, "mov_caixa_laguna")     
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
    cursor.execute(sql_mov_caixa)
    mov_caixa = cursor.fetchall()
    df4 = pd.DataFrame(mov_caixa, columns=cursor.column_names)
    print(df4.head(10))
    df4.to_excel(writer, sheet_name="mov_caixa_farol", index=False )   
    df4.replace(np.nan, '', inplace=True) 
    cls("mov_caixa_farol")
    ins(df4, "mov_caixa_farol")  
    cursor.close()
    conn.close()
    print("Conexão ao MySQL foi encerrada")


writer.save()