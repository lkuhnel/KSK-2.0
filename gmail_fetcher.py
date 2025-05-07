import os
import pickle
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import base64
import email
import re
from datetime import datetime, date

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def parse_date(date_str):
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str.strip()

def authenticate_gmail():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return build('gmail', 'v1', credentials=creds)

def fetch_requests_from_gmail():
    service = authenticate_gmail()
    results = service.users().messages().list(userId='me', labelIds=['INBOX'], maxResults=20).execute()
    messages = results.get('messages', [])

    pto_requests = []
    non_pto_requests = []

    for msg in messages:
        msg_data = service.users().messages().get(userId='me', id=msg['id'], format='raw').execute()
        msg_str = base64.urlsafe_b64decode(msg_data['raw'].encode('ASCII'))
        mime_msg = email.message_from_bytes(msg_str)

        resident_name = mime_msg['subject'] if mime_msg['subject'] else ""
        body = ""
        if mime_msg.is_multipart():
            for part in mime_msg.walk():
                if part.get_content_type() == 'text/plain':
                    payload = part.get_payload(decode=True)
                    try:
                        body = payload.decode('utf-8')
                    except UnicodeDecodeError:
                        body = payload.decode('latin1', errors='ignore')
                    break
        else:
            payload = mime_msg.get_payload(decode=True)
            try:
                body = payload.decode('utf-8')
            except UnicodeDecodeError:
                body = payload.decode('latin1', errors='ignore')

        # --- Split body into blocks using two or more newlines as delimiters ---
        blocks = re.split(r'\n\s*\n', body)
        for block in blocks:
            block = block.strip()
            if block.lower().startswith('pto:'):
                pto_text = block[len('pto:'):].strip()
                for match in re.finditer(r'Start Date:\s*([^\n]+)\s*End Date:\s*([^\n]+)', pto_text, re.IGNORECASE):
                    start_date, end_date = match.groups()
                    request = {
                        "Resident": resident_name,
                        "Start_Date": parse_date(start_date),
                        "End_Date": parse_date(end_date),
                        "Reason": "PTO"
                    }
                    if request["Resident"] and request["Start_Date"] and request["End_Date"]:
                        pto_requests.append(request)
            elif block.lower().startswith('non-call:'):
                noncall_text = block[len('non-call:'):].strip()
                for match in re.finditer(r'Start Date:\s*([^\n]+)\s*End Date:\s*([^\n]+)', noncall_text, re.IGNORECASE):
                    start_date, end_date = match.groups()
                    request = {
                        "Resident": resident_name,
                        "Start_Date": parse_date(start_date),
                        "End_Date": parse_date(end_date),
                        "Reason": "Non-call"
                    }
                    if request["Resident"] and request["Start_Date"] and request["End_Date"]:
                        non_pto_requests.append(request)

    return pto_requests, non_pto_requests 

def ensure_date(val, fallback):
    if isinstance(val, date):
        return val
    try:
        return datetime.strptime(val, "%Y-%m-%d").date()
    except Exception:
        try:
            return datetime.strptime(val, "%m/%d/%y").date()
        except Exception:
            try:
                return datetime.strptime(val, "%m/%d/%Y").date()
            except Exception:
                return fallback 