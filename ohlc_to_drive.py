#Importing necessary modules
from NorenRestApiPy.NorenApi import NorenApi
from threading import Timer
import pandas as pd
import time
from datetime import datetime, date
import concurrent.futures
import pyotp
import requests
from tqdm import tqdm
import sys
import zipfile
import os
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2 import service_account
import warnings
warnings.filterwarnings('ignore')


# def fetch_options(start_time, end_time):
#Logging
# usercred = pd.read_excel(rf'C:\My Data\Python Work\Shoonya Api\\Login_Cred(Himanshu).xlsx')

USER = os.getenv("SHOONYA_USER")
PWD = os.getenv("SHOONYA_PWD")
VC = os.getenv("SHOONYA_VC")
APP_KEY = os.getenv("SHOONYA_APP_KEY")
IMEI = os.getenv("SHOONYA_IMEI")
QR_SECRET = os.getenv("SHOONYA_QR")
FACTOR2 = pyotp.TOTP(QR_SECRET).now()


#Defining Class, Modules And Login
class ShoonyaApiPy(NorenApi):
    def __init__(self):
        NorenApi.__init__(self, host='https://api.shoonya.com/NorenWClientTP/', websocket='wss://api.shoonya.com/NorenWSTP/')
        global api
        api = self

import logging
logging.basicConfig(level=logging.DEBUG)
api = ShoonyaApiPy()
ret = api.login(userid=USER, password=PWD, twoFA=FACTOR2, vendor_code=VC, api_secret=APP_KEY, imei=IMEI)

print("✅ Successfully logged in to Shoonya API")

def get_time (time_string):
    data = time.strptime(time_string, "%Y-%m-%d %H:%M:%S")
    return int(time.mktime(data))


# Suppress debug logs
import logging
logging.getLogger("NorenRestApiPy").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


#Modify Dates
# start_time = datetime.strftime((datetime.strptime(str(date.today()), "%Y-%m-%d")), "%Y%m%d")
# end_time = datetime.strftime((datetime.strptime(str(date.today()), "%Y-%m-%d")), "%Y%m%d")

start_time = 20250207
end_time = 20250207

st = get_time(f'{str(start_time)[:4]}-{str(start_time)[4:6]}-{str(start_time)[6:]} 01:00:00')
et = get_time(f'{str(end_time)[:4]}-{str(end_time)[4:6]}-{str(end_time)[6:]} 23:59:00')


#Extracting Spot
ret = api.get_time_price_series(exchange='NSE', token='26000', starttime=st, endtime=et, interval=1)
tokens = [{
    'NSE_SPOT':'26000'
}]
spot_df = pd.DataFrame.from_dict(ret)
spot_df.sort_values(by='time', inplace=True)
spot_df.reset_index(drop=True, inplace=True)
spot_df.rename(columns={
    'time': 'Datetime', 
    'into':'Open', 
    'inth':'High', 
    'intl':'Low', 
    'intc':'Close', 
    'v':'Volume', 
    'oi':'OI'}, inplace=True)
spot_df['Ticker'] = 'SPOT'
spot_df['Date'] = spot_df['Datetime'].apply (lambda x: x.split(' ')[0])
spot_df['Time'] = spot_df['Datetime'].apply (lambda x: x.split(' ')[-1])
spot_df['Days'] = spot_df['Date'].apply (lambda x: datetime.strptime(x, "%d-%m-%Y").strftime('%A'))
spot_df['Name'] = 'NIFTY'
spot_df['Type'] = 'INDEX'
spot_df['Strike_Price'] = None
spot_df['Expiry_Date'] = None
spot_df = spot_df[['Ticker', 'Date', 'Time', 'Days', 'Name', 'Datetime', 'Type', 'Strike_Price', 'Expiry_Date', 'Open', 'High', 'Low', 'Close', 'OI', 'Volume']]

print ('------------------')
print ('------------------')
print ('------------------')
print ('------------------')
print (f"***********Working For Date {str(start_time)[6:]}-{str(start_time)[4:6]}-{str(start_time)[:4]}***********")
print (f'✅Spot Downloaded..!!')
print (f'Size = {len(spot_df)}Rows')
print ('------------------')

#Downloading Master Symbols
url = "https://api.shoonya.com/NFO_symbols.txt.zip"
directory = rf"C:\My Data\Python Work\Historical Data"

os.makedirs(directory, exist_ok=True)

zip_filepath = os.path.join(directory, "file.zip")
with open(zip_filepath, "wb") as f:
    f.write(requests.get(url).content)

with zipfile.ZipFile(zip_filepath, "r") as zip_ref:
    zip_ref.extractall(directory)

file = pd.read_csv(os.path.join(directory, "NFO_symbols.txt"))

os.remove(zip_filepath)
os.remove(os.path.join(directory, "NFO_symbols.txt"))

file = file[file['Symbol'] == 'NIFTY']
file['Expiry'] = file['Expiry'].apply (lambda x: datetime.strptime(x, "%d-%b-%Y"))

#Extracting Options Downloadable Dataframe
fut_df = file[file['Instrument'] == 'FUTIDX']
fut_df.sort_values(by='Expiry', inplace=True)
fut_df["Ticker"] = [f"NIFTY-{i*'I'}" for i in range(1, len(fut_df)+1)]
current_month = fut_df['Expiry'].min()
mid = (((spot_df['High'].astype(float).max() + spot_df['Low'].astype(float).min()) / 2) // 50) * 50
maxx = mid+(50*40)
minn = mid-(50*40)

opt_df = file[((file['Expiry'] <= current_month) | (file['Expiry'].isin(fut_df[fut_df['Expiry'] != fut_df['Expiry'].min()]['Expiry']))) & (file['Expiry'] >= (datetime.strptime(str(date.today()), "%Y-%m-%d"))) & (file['StrikePrice'] >= minn) & (file['StrikePrice'] <= maxx)]
opt_df.reset_index(drop=True, inplace=True) 


#Downloading OPTIONS Data
options_df = pd.DataFrame()

progress_bar = tqdm(total=len(opt_df), desc="Downloading Options Data", dynamic_ncols=True, position=0, leave=True)
for idx, rows in opt_df.iterrows():
    sys.stdout.write(f"\rProcessing Index: {idx}/{len(opt_df)} | Token: {rows['Token']} | Symbol: {rows['TradingSymbol']}  ")
    sys.stdout.flush()
    
    ret = api.get_time_price_series(exchange=rows['Exchange'], token=str(rows['Token']), starttime=st, endtime=et, interval=1)
    
    if ret and len(ret) == 375:
        tempdf = pd.DataFrame.from_dict(ret)

        tempdf['Ticker'] = rows['TradingSymbol']
        tempdf['Date'] = tempdf['time'].apply(lambda x: x.split(' ')[0])
        tempdf['Time'] = tempdf['time'].apply(lambda x: x.split(' ')[-1])
        tempdf['Days'] = tempdf['Date'].apply(lambda x: datetime.strptime(x, "%d-%m-%Y").strftime('%A'))
        tempdf['Name'] = 'NIFTY'
        tempdf.rename(columns={'time': 'Datetime'}, inplace=True)

        if tempdf['Ticker'].iloc[0][-6:-5] == 'P':
            tempdf['Type'] = 'PE'
            tempdf['Strike_Price'] = tempdf['Ticker'].iloc[0][-5:]
            tempdf['Expiry_Date'] = rows['Expiry']

        elif tempdf['Ticker'].iloc[0][-6:-5] == 'C':
            tempdf['Type'] = 'CE'
            tempdf['Strike_Price'] = tempdf['Ticker'].iloc[0][-5:]
            tempdf['Expiry_Date'] = rows['Expiry']

        tempdf.rename(columns={'into': 'Open', 'inth': 'High', 'intl': 'Low', 'intc': 'Close', 'oi': 'OI', 'v': 'Volume'}, inplace=True)
        tempdf = tempdf[['Ticker', 'Date', 'Time', 'Days', 'Name', 'Datetime', 'Type', 'Strike_Price', 'Expiry_Date', 'Open', 'High', 'Low', 'Close', 'OI', 'Volume']]
        tempdf['Time'] = tempdf['Time'].apply(lambda x: datetime.strptime(x, "%H:%M:%S").time())

        tempdf.sort_values(by='Time', inplace=True)
        options_df = pd.concat([options_df, tempdf])

    progress_bar.update(1)

progress_bar.close()
print("\n✅ Options Data Downloaded..!!")
print ('------------------')


#Downloading Futures Data
futures_df = pd.DataFrame()

for idx, rows in fut_df.iterrows():
    ret = api.get_time_price_series(exchange=rows['Exchange'], token=str(rows['Token']), starttime=st, endtime=et, interval=1)
    tempdf = pd.DataFrame.from_dict(ret)

    if ret and len(ret) == 375:
        tempdf = pd.DataFrame.from_dict(ret)

        tempdf['Ticker'] = rows['Ticker']
        tempdf['Date'] = tempdf['time'].apply (lambda x: x.split(' ')[0])
        tempdf['Time'] = tempdf['time'].apply (lambda x: x.split(' ')[-1])
        tempdf['Days'] = tempdf['Date'].apply (lambda x: datetime.strptime(x, "%d-%m-%Y").strftime('%A'))
        tempdf['Name'] = 'NIFTY'
        tempdf.rename(columns={'time' : 'Datetime'}, inplace=True)

        tempdf['Type'] = 'FUT'
        tempdf['Strike_Price'] = None
        tempdf['Expiry_Date'] = rows['Expiry']

        tempdf.rename(columns={'into':'Open', 'inth':'High', 'intl':'Low', 'intc':'Close', 'oi':'OI', 'v':'Volume'}, inplace=True)
        tempdf = tempdf[['Ticker', 'Date', 'Time', 'Days', 'Name','Datetime', 'Type', 'Strike_Price', 'Expiry_Date', 'Open', 'High', 'Low', 'Close', 'OI', 'Volume']]
        tempdf['Time'] = tempdf['Time'].apply (lambda x: datetime.strptime(x, "%H:%M:%S").time())

        tempdf.sort_values(by='Time', inplace=True)

        futures_df = pd.concat([futures_df, tempdf])

print ('✅Futures Data Downloaded..!!')
print ('------------------')

if (len(spot_df) > 0) & (len(options_df) > 0) & (len(futures_df) > 0):
    print ('All Data Seems Correctly Downloaded.')
else:
    print ('Check Data Manually (Error Found)..!')

final_df = pd.concat([spot_df, options_df, futures_df])

final_df['Time'] = final_df['Time'].astype(str)
final_df['Datetime'] = final_df['Datetime'].astype(str)
final_df["Time"] = (final_df["Time"].str.slice(0, 6) + "59").astype(str)
final_df["Datetime"] = (final_df["Datetime"].str.slice(0, 17) + "59").astype(str)


file_name = datetime.strftime(datetime.strptime(str(start_time), '%Y%m%d'), '%d%m%Y')
# final_df.to_csv(rf'C:\My Data\Python Work\Historical Data\Nifty Cleaned (RAW)\2025\\NIFTY_{file_name}.csv', index=False)


# 🔹 Load Google Service Account Credentials
SERVICE_ACCOUNT_FILE = "service_account.json"  # Path to your JSON key file
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build("drive", "v3", credentials=creds)

# 🔹 Your Google Drive Folder ID
FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")  # Replace with your actual Folder ID

def upload_dataframe_to_drive(df, file_name):
    """Uploads a Pandas DataFrame as a CSV directly to Google Drive without saving locally"""
    
    # Convert DataFrame to CSV in memory (no local file)
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Metadata for Google Drive file
    file_metadata = {
        "name": file_name,  # Name in Google Drive
        "parents": [FOLDER_ID],  # Upload inside specific Google Drive folder
        "mimeType": "text/csv"
    }

    # Upload CSV file from memory
    media = MediaIoBaseUpload(csv_buffer, mimetype="text/csv", resumable=True)
    file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()

    print(f"✅ File uploaded to Google Drive: {file_name}")

upload_dataframe_to_drive(final_df, f"{file_name}.csv")

print ('------------------')
print ('✅Final Data Merged And Stored In Specified Locations..!')

    # return final_df
