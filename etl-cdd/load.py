from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

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

aoa = [["1/1/2020", 450], ["4/4/2020",300]]

def cls():
        sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                        range="mov_caixa_moema", body={}).execute()
        sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                        range="mov_caixa_farol", body={}).execute()
        sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                        range="mov_caixa_rincao", body={}).execute()
        sheet.values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                        range="mov_caixa_laguna", body={}).execute()                                
cls()                                                                          

def ins(dataframe):
        request =  sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                 range="mov_caixa_moema", valueInputOption="USER_ENTERED", body={"values":dataframe}).execute()

print(request)


