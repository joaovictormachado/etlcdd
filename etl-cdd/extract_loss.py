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
SAMPLE_SPREADSHEET_ID = '1PoKmTXrD6uJ4FNf95aVtnfj_Y7MruRURNKSZcb1Irys'

#SERVICE CALL
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()
sql_loss = "SELECT YEAR(m.dt)`Ano`,MONTH(m.dt)`Mes`, DAY(m.dt)`Dia`, m.cd_prod`Cd Prod`,p.nm`Nome`,p.preco_custo`Vl Custo`, m.cd`Cd Mov`,u.login`Usuário`,m.obs`Observação`,m.qtd`Qtd`,((p.preco_custo)*(m.qtd))`Custo Total` FROM mov_prod m JOIN produto p ON p.cd=m.cd_prod LEFT JOIN usuario u ON u.cd=m.cd_usu WHERE m.tipo='SAIDA' AND YEAR(m.dt) >=2021 AND p.baixa<>'N' ORDER BY m.dt"
writer = pd.ExcelWriter('loss.xlsx', engine='xlsxwriter')

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
    cursor.execute(sql_loss)
    loss = cursor.fetchall()
    df = pd.DataFrame(loss, columns=cursor.column_names)
    print(df.head(10))
    df.to_excel(writer, sheet_name="loss_moema", index=False ) 
    df.replace(np.nan, '', inplace=True) 
    cls("loss_moema")
    ins(df, "loss_moema!A1")
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
    cursor.execute(sql_loss)
    loss = cursor.fetchall()
    df2 = pd.DataFrame(loss, columns=cursor.column_names)
    print(df2.head(10))
    df2.to_excel(writer, sheet_name="loss_rincao", index=False )   
    df2.replace(np.nan, '', inplace=True) 
    cls("loss_rincao")
    ins(df2, "loss_rincao") 
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
    cursor.execute(sql_loss)
    loss = cursor.fetchall()
    df3 = pd.DataFrame(loss, columns=cursor.column_names)
    print(df3.head(10))
    df3.to_excel(writer, sheet_name="loss_laguna", index=False )
    df3.replace(np.nan, '', inplace=True)
    cls("loss_laguna") 
    ins(df3, "loss_laguna")     
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
    cursor.execute(sql_loss)
    loss = cursor.fetchall()
    df4 = pd.DataFrame(loss, columns=cursor.column_names)
    print(df4.head(10))
    df4.to_excel(writer, sheet_name="loss_farol", index=False )   
    df4.replace(np.nan, '', inplace=True) 
    cls("loss_farol")
    ins(df4, "loss_farol")  
    cursor.close()
    conn.close()
    print("Conexão ao MySQL foi encerrada")


writer.save()