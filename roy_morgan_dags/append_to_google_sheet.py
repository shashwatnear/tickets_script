import boto3
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Path to the service account key JSON file
SERVICE_ACCOUNT_FILE = '/home/rishitha/testing_spreadsheet/gsheets-dagmonitor-f4a71bba18c4.json'

# Scopes required for the Google Sheets API
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Authenticate with the service account credentials
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Build the Sheets API service
service = build('sheets', 'v4', credentials=creds)

def append_to_google_sheet(values):
    spreadsheet_id = '1iAMxnVRrrNqYgHVi2aEpA_sEQNsCnoxSZbJJsz28J-U'  # Your Google Sheet ID
    sheet_name = 'daily_count'  # Specify the sheet name
    
    # Calculate the next empty row for appending
    try:
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id, range=sheet_name).execute()
        num_rows = len(result.get('values', []))
        next_row = num_rows + 1
        start_row = f'{sheet_name}!A{next_row}'
    except HttpError as e:
        start_row = f'{sheet_name}!A1'  # If the sheet is empty, start from the first row

    # Call the Sheets API to append the data
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=start_row,
        valueInputOption='RAW',
        body={'values': values},
        insertDataOption='INSERT_ROWS',
        includeValuesInResponse=True,
        responseValueRenderOption='FORMATTED_VALUE',
        responseDateTimeRenderOption='FORMATTED_STRING',
    ).execute()

    print("Data transferred successfully to Google Sheet.")