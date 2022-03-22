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
SAMPLE_SPREADSHEET_ID = '1DkE1JJIOYrUSdcu-nUDiDuudrV-LKV_irvdvZa13jx0'

#SERVICE CALL
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()
sql_contas_a_pagar = "SELECT YEAR(p.dt_vencimento)`ANO`, MONTH(p.dt_vencimento)`MES`,DAY(p.dt_vencimento)`DIA`,f.nm_razao`Fornecedor`,c.historico`Descrição`,ROUND(p.vl,2)`Valor`, c.plano`Plano de Contas`, IF(p.dt_pgto IS NOT NULL, 'SIM', 'NÃO')`Pago?`,p.num_doc`Parcela atual`,c.parcelas`Parcelas` FROM conta_pagar c JOIN pessoa f ON f.cd=c.cd_pes LEFT JOIN parcela p ON p.cd_conta_pagar=c.cd WHERE YEAR(p.dt_vencimento) >= 2021 ORDER BY p.dt_vencimento, f.nm_razao"
writer = pd.ExcelWriter('contas.xlsx', engine='xlsxwriter')

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
    cursor.execute(sql_contas_a_pagar)
    contas = cursor.fetchall()
    df = pd.DataFrame(contas, columns=cursor.column_names)
    print(df.head(10))
    df.to_excel(writer, sheet_name="contas_moema", index=False ) 
    df.replace(np.nan, '', inplace=True) 
    cls("contas_moema")
    ins(df, "contas_moema!A1")
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
    cursor.execute(sql_contas_a_pagar)
    contas = cursor.fetchall()
    df2 = pd.DataFrame(contas, columns=cursor.column_names)
    print(df2.head(10))
    df2.to_excel(writer, sheet_name="contas_rincao", index=False )   
    df2.replace(np.nan, '', inplace=True) 
    cls("contas_rincao")
    ins(df2, "contas_rincao") 
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
    cursor.execute(sql_contas_a_pagar)
    contas = cursor.fetchall()
    df3 = pd.DataFrame(contas, columns=cursor.column_names)
    print(df3.head(10))
    df3.to_excel(writer, sheet_name="contas_laguna", index=False )
    df3.replace(np.nan, '', inplace=True)
    cls("contas_laguna") 
    ins(df3, "contas_laguna")     
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
    cursor.execute(sql_contas_a_pagar)
    contas = cursor.fetchall()
    df4 = pd.DataFrame(contas, columns=cursor.column_names)
    print(df4.head(10))
    df4.to_excel(writer, sheet_name="contas_farol", index=False )   
    df4.replace(np.nan, '', inplace=True) 
    cls("contas_farol")
    ins(df4, "contas_farol")  
    cursor.close()
    conn.close()
    print("Conexão ao MySQL foi encerrada")


writer.save()