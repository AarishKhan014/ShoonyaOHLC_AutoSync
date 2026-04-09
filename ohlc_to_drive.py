from NorenApi import NorenApi
from playwright.sync_api import sync_playwright
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
import urllib.parse
import os
import io
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2 import service_account


# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)
# Ya phir purana tarika (Python 3.7+)
os.environ['PYTHONUNBUFFERED'] = '1'

current_ip = requests.get("https://api.ipify.org").text

USER = os.getenv("SHOONYA_USER")
PWD = os.getenv("SHOONYA_PWD")
QR_SECRET_OTP = os.getenv("SHOONYA_QR") #QR CODE
SECRET_CODE = os.getenv("SHOONYA_APP_KEY") #API/APP KEY

LOGIN_URL = f"https://trade.shoonya.com/OAuthlogin/investor-entry-level/login?api_key={USER}_U&route_to={USER}"
LOGIN_LINK_FOR_IP_UPDATE = "https://trade.shoonya.com/#/"

if not all([USER, PWD, QR_SECRET_OTP]):
    raise ValueError("Missing secrets! Please set SHOONYA_USER, SHOONYA_PASSWORD, SHOONYA_TOTP_SECRET in GitHub Secrets.")

current_ip = requests.get("https://api.ipify.org").text.strip()
print(f"Current Public IP: {current_ip}")
sys.stdout.flush()
print("Updating IP on Shoonya Account...")
sys.stdout.flush()

def ip_updater(max_retries=10):
    old_ip = ""
    attempt = 1

    while attempt <= max_retries:
        print(f"\n🔄 Attempt {attempt}/{max_retries} starting...", flush=True)

        with sync_playwright() as p:
            # Launch browser
            browser = p.chromium.launch(headless=True)
            
            # Create context with clipboard permissions
            context = browser.new_context(
                permissions=["clipboard-read", "clipboard-write"]
            )
            
            # Grant permission for Shoonya
            context.grant_permissions(
                ["clipboard-read", "clipboard-write"],
                origin=LOGIN_LINK_FOR_IP_UPDATE
            )

            page = context.new_page()

            try:
                print("Navigating to login page...", flush=True)
                page.goto(LOGIN_LINK_FOR_IP_UPDATE, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(8000)

                # Login
                print("Entering credentials...", flush=True)
                page.keyboard.type(USER)
                page.keyboard.press("Tab")
                page.keyboard.type(PWD)
                page.keyboard.press("Tab")

                totp = pyotp.TOTP(QR_SECRET_OTP).now()
                print(f"Generated TOTP: {totp}", flush=True)
                page.keyboard.type(totp)
                page.keyboard.press("Tab")
                page.keyboard.press("Enter")

                page.wait_for_timeout(15000)

                # Handle Accept button
                for frame in page.frames:
                    if frame.locator('button:has-text("Accept")').count() > 0:
                        frame.locator('button:has-text("Accept")').first.click()
                        print("Clicked Accept button", flush=True)
                        break

                page.wait_for_timeout(6000)

                # Navigate to API Settings
                print("Navigating to Profile → API Settings...", flush=True)
                for _ in range(4):
                    page.keyboard.press("Tab")
                page.keyboard.press("Enter")   # Cancel auth if any
                page.wait_for_timeout(4000)

                for _ in range(10):
                    page.keyboard.press("Tab")
                page.keyboard.press("Enter")   # Go to Profile
                page.wait_for_timeout(4000)

                for _ in range(22):
                    page.keyboard.press("Tab")
                page.keyboard.press("Enter")   # Go to API Settings
                page.wait_for_timeout(8000)    # Important wait

                # ================== Read Old IP ==================
                print("Reading previously whitelisted (Old) IP...", flush=True)
                
                for _ in range(4):
                    page.keyboard.press("Tab")

                page.wait_for_timeout(2000)

                page.keyboard.press("Control+A")
                page.wait_for_timeout(1000)
                page.keyboard.press("Control+C")
                page.wait_for_timeout(1500)

                # Read from clipboard
                old_ip = page.evaluate("() => navigator.clipboard.readText()").strip()

                if not old_ip:
                    print(f"⚠️ Old IP is empty on attempt {attempt}", flush=True)
                    old_ip = ""
                else:
                    print(f"✅ Old IP found: {old_ip}", flush=True)

                    # ================== Update New IP ==================
                    print(f"Updating IP from '{old_ip}' → '{current_ip}'", flush=True)
                    page.keyboard.press("Control+A")
                    page.keyboard.type(current_ip)
                    page.wait_for_timeout(2000)

                    for _ in range(3):
                        page.keyboard.press("Tab")
                    page.keyboard.press("Enter")   # Save

                    page.wait_for_timeout(5000)
                    print("✅ IP Updated Successfully!", flush=True)
                    break   # Success - no more retries

            except Exception as e:
                print(f"❌ Error on attempt {attempt}: {e}", flush=True)
                old_ip = ""

            finally:
                # Clean up browser
                try:
                    context.close()
                    browser.close()
                except:
                    pass

        # If we got a valid old_ip, no need to retry
        if old_ip:
            break

        # Retry logic
        if attempt < max_retries:
            print(f"⏳ Waiting 10 seconds before next retry...", flush=True)
            time.sleep(10)
        
        attempt += 1

    # Final Summary
    if not old_ip:
        print("\n❌ Failed to read Old IP even after maximum retries!", flush=True)
        old_ip = "Failed after retries"
    else:
        print("\n=== FINAL SUMMARY ===", flush=True)
        print(f"Old IP (before update) : {old_ip}", flush=True)
        print(f"New IP (updated)       : {current_ip}", flush=True)

    return old_ip


ip_updater(max_retries=10)


print ("Now Generating Authentication Code.!")
sys.stdout.flush()

time.sleep(2)

def get_auth_code():
    # Generate current OTP dynamically
    totp = pyotp.TOTP(QR_SECRET_OTP).now()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # headless=False so you can watch the browser
        page = browser.new_page()
        page.goto(LOGIN_URL)

        # Fill login form
        page.fill('#lgnusrid', USER)
        page.fill('#lgnpwd', PWD)
        page.fill('#lgnotp', totp)

        # Click login button
        page.click('.lgnBtnClss')

        # Wait for redirect to URL containing "auth_code"
        page.wait_for_url("**code**", timeout=15000)  # 15 seconds max

        # Get the final URL
        final_url = page.url

        # Parse the URL and extract the "code" parameter
        parsed = urllib.parse.urlparse(final_url)
        query_params = urllib.parse.parse_qs(parsed.query)
        auth_code = query_params.get('code', [None])[0]

        browser.close()
        return auth_code

# if __name__ == "__main__":
code = get_auth_code()
print("✅ Authentication code Generated:", code)
sys.stdout.flush()



cred = {
    'client_id': f'{USER}_U',
    'Secret_Code': SECRET_CODE,
    'UID': USER,
    'oauth_url': f'https://api.shoonya.com/NorenWClient/authenticate/{USER}_U'
}



class NorenApiPy(NorenApi):
    def __init__(self):
        super().__init__(host='https://api.shoonya.com/NorenWClientAPI/', websocket='wss://api.shoonya.com/NorenWS/')

api = NorenApiPy()


ret = api.getAccessToken(code, cred['Secret_Code'], cred['client_id'], cred['UID'])
print (ret)
if ret is not None:
    acc_tok, usrid, ref_tok, actid = ret
    print(f"""\nAccess token is : {acc_tok} \nRefresh token is : {ref_tok} \nUser ID token is : {usrid} \nAccount ID is : {actid} \n""")
    # Update values
    cred['Access_token'] = acc_tok
    cred['Account_ID'] = actid
    print (acc_tok, actid)
else:
    print("Failed to retrieve access token.")

# print(cred)
injected_headers = api.injectOAuthHeader(cred['Access_token'],cred['UID'],cred['Account_ID'])

print("✅ Successfully logged in to Shoonya API")


def get_time (time_string):
    data = time.strptime(time_string, "%Y-%m-%d %H:%M:%S")
    return int(time.mktime(data))


# Suppress debug logs
import logging
logging.getLogger("NorenRestApiPy").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


#Modify Dates
start_time = datetime.strftime((datetime.strptime(str(date.today()), "%Y-%m-%d")), "%Y%m%d")
end_time = datetime.strftime((datetime.strptime(str(date.today()), "%Y-%m-%d")), "%Y%m%d")

# start_time = 20250207
# end_time = 20250207

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
file = pd.read_csv("https://api.shoonya.com/NFO_symbols.txt.zip", compression='zip', engine='python', delimiter=',')
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


file_name = f"NIFTY_{datetime.strftime(datetime.strptime(str(start_time), '%Y%m%d'), '%d%m%Y')}"
final_df.to_csv(rf'{file_name}.csv', index=False)


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
