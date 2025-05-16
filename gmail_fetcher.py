import os
import pickle
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import base64
import email
import re
from datetime import datetime, date, timedelta

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def parse_date(date_str):
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str.strip()

def parse_flexible_date_range(text, year_hint=None):
    """
    Parse date ranges like 'Sept 30-Oct 4', 'July 27-28', '8/6/25-9/6/25', etc.
    Returns (start_date, end_date) as YYYY-MM-DD strings, or None if not found.
    """
    # Support both full month names and abbreviations
    month_map = {
        'january': 1, 'jan': 1,
        'february': 2, 'feb': 2,
        'march': 3, 'mar': 3,
        'april': 4, 'apr': 4,
        'may': 5,
        'june': 6, 'jun': 6,
        'july': 7, 'jul': 7,
        'august': 8, 'aug': 8,
        'september': 9, 'sep': 9, 'sept': 9,
        'october': 10, 'oct': 10,
        'november': 11, 'nov': 11,
        'december': 12, 'dec': 12
    }
    # 1. Month Day - Month Day (e.g. Sept 30-Oct 4)
    m = re.search(r'([A-Za-z]+)\s*(\d{1,2})\s*[-–]\s*([A-Za-z]+)?\s*(\d{1,2})', text)
    if m:
        m1, d1, m2, d2 = m.groups()
        m1 = m1.lower()
        m2 = m2.lower() if m2 else m1
        year = year_hint or datetime.now().year
        try:
            start = datetime(year, month_map[m1], int(d1))
            end = datetime(year, month_map[m2], int(d2))
            if end < start:
                end = end.replace(year=year+1)
            return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')
        except Exception:
            pass
    # 2. Month Day-Day (e.g. July 27-28)
    m = re.search(r'([A-Za-z]+)\s*(\d{1,2})[-–](\d{1,2})', text)
    if m:
        m1, d1, d2 = m.groups()
        m1 = m1.lower()
        year = year_hint or datetime.now().year
        try:
            start = datetime(year, month_map[m1], int(d1))
            end = datetime(year, month_map[m1], int(d2))
            if end < start:
                end = end.replace(year=year+1)
            return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')
        except Exception:
            pass
    # 3. Numeric MM/DD/YY - MM/DD/YY or MM/DD - MM/DD
    m = re.search(r'(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?\s*[-–]\s*(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?', text)
    if m:
        m1, d1, y1, m2, d2, y2 = m.groups()
        y1 = int(y1) + 2000 if y1 and len(y1) == 2 else int(y1) if y1 else (year_hint or datetime.now().year)
        y2 = int(y2) + 2000 if y2 and len(y2) == 2 else int(y2) if y2 else y1
        try:
            start = datetime(y1, int(m1), int(d1))
            end = datetime(y2, int(m2), int(d2))
            if end < start:
                end = end.replace(year=y1+1)
            return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')
        except Exception:
            pass
    # 4. Single date (e.g. Sept 30)
    m = re.search(r'([A-Za-z]+)\s*(\d{1,2})', text)
    if m:
        m1, d1 = m.groups()
        m1 = m1.lower()
        year = year_hint or datetime.now().year
        try:
            dt = datetime(year, month_map[m1], int(d1))
            return dt.strftime('%Y-%m-%d'), dt.strftime('%Y-%m-%d')
        except Exception:
            pass
    # 5. Numeric MM/DD/YY
    m = re.search(r'(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?', text)
    if m:
        m1, d1, y1 = m.groups()
        y1 = int(y1) + 2000 if y1 and len(y1) == 2 else int(y1) if y1 else (year_hint or datetime.now().year)
        try:
            dt = datetime(y1, int(m1), int(d1))
            return dt.strftime('%Y-%m-%d'), dt.strftime('%Y-%m-%d')
        except Exception:
            pass
    return None, None

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

    # Get all valid resident names (lowercase) from the app if available
    try:
        import streamlit as st
        block_choice = st.session_state.get('block_choice', None)
        if block_choice and 'residents_data_by_block' in st.session_state:
            valid_residents = {res['Name'].strip().lower(): res['Name'] for res in st.session_state.residents_data_by_block[block_choice]}
        else:
            valid_residents = {}
    except Exception:
        valid_residents = {}

    for msg in messages:
        msg_data = service.users().messages().get(userId='me', id=msg['id'], format='raw').execute()
        msg_str = base64.urlsafe_b64decode(msg_data['raw'].encode('ASCII'))
        mime_msg = email.message_from_bytes(msg_str)

        # Extract resident name: prefer From, then signature, then subject
        resident_name = None
        name_method = None
        from_header = mime_msg.get('From', "")
        m = re.match(r'"?([^<"]+)"?\s*<', from_header)
        if m:
            candidate = m.group(1).strip()
            if candidate.lower() in valid_residents:
                resident_name = valid_residents[candidate.lower()]
                name_method = 'from_header (matched)'
            else:
                resident_name = candidate
                name_method = 'from_header (unmatched)'
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

        # If still not matched, try signature (last non-empty line)
        if (not resident_name or resident_name.strip() == "") and body:
            lines = [line.strip() for line in body.splitlines() if line.strip()]
            if lines:
                candidate = lines[-1]
                if candidate.lower() in valid_residents:
                    resident_name = valid_residents[candidate.lower()]
                    name_method = 'signature (matched)'
                else:
                    resident_name = candidate
                    name_method = 'signature (unmatched)'
        # If still not matched, use subject
        if (not resident_name or resident_name.strip() == ""):
            subject = mime_msg['subject'] if mime_msg['subject'] else ""
            if subject.lower() in valid_residents:
                resident_name = valid_residents[subject.lower()]
                name_method = 'subject (matched)'
            else:
                resident_name = subject
                name_method = 'subject (unmatched)'

        print("\n=== EMAIL DEBUG ===")
        print(f"Subject: {mime_msg['subject']}")
        print(f"From: {from_header}")
        print(f"Resident Name: {resident_name} (method: {name_method})")
        print("Raw Body:")
        print(body)

        # --- Improved parsing logic ---
        lines = [line.strip() for line in body.splitlines() if line.strip()]
        # Ignore all lines after a signature/thanks
        filtered_lines = []
        for line in lines:
            if re.search(r'thank(s| you)|best|sincerely|regards|cheers', line, re.IGNORECASE):
                break
            filtered_lines.append(line)
        print("Filtered Lines:")
        for l in filtered_lines:
            print(f"  {l}")
        current_section = None
        year_hint = datetime.now().year
        i = 0
        while i < len(filtered_lines):
            line = filtered_lines[i]
            lcline = line.lower()
            # Flexible section header detection
            if 'pto' in lcline and 'non-pto' not in lcline and ':' in lcline:
                current_section = 'pto'
                print(f"Section header detected: PTO -> {line}")
                # Try to parse next 2 lines as PTO date ranges
                for j in range(1, 3):
                    if i + j < len(filtered_lines):
                        next_line = filtered_lines[i + j]
                        next_lcline = next_line.lower()
                        # Stop if next line is another section header
                        if re.match(r'^\s*(pto:|non-pto|non-call|weekend of)', next_lcline):
                            break
                        # Remove parentheticals
                        line_no_paren = re.sub(r'\(.*?\)', '', next_line).strip()
                        start, end = parse_flexible_date_range(line_no_paren, year_hint)
                        print(f"PTO section: parse next line: {line_no_paren} -> {start} to {end}")
                        if start and end:
                            req = {
                                "Resident": resident_name,
                                "Start_Date": start,
                                "End_Date": end,
                                "Reason": "PTO"
                            }
                            pto_requests.append(req)
                i += 1
                continue
            if any(x in lcline for x in ['non-pto', 'non-call', 'soft constraint', 'weekend']) and ':' in lcline:
                current_section = 'non_pto'
                print(f"Section header detected: NON-PTO -> {line}")
                # Try to parse next 2 lines as Non-PTO date ranges
                for j in range(1, 3):
                    if i + j < len(filtered_lines):
                        next_line = filtered_lines[i + j]
                        next_lcline = next_line.lower()
                        if re.match(r'^\s*(pto:|non-pto|non-call|weekend of)', next_lcline):
                            break
                        line_no_paren = re.sub(r'\(.*?\)', '', next_line).strip()
                        start, end = parse_flexible_date_range(line_no_paren, year_hint)
                        print(f"Non-PTO section: parse next line: {line_no_paren} -> {start} to {end}")
                        if start and end:
                            req = {
                                "Resident": resident_name,
                                "Start_Date": start,
                                "End_Date": end,
                                "Reason": "Non-call"
                            }
                            non_pto_requests.append(req)
                i += 1
                continue
            # Only extract from lines that start with allowed keywords
            if re.match(r'^\s*(pto:|non-pto|non-call|weekend of)', lcline):
                # Remove parentheticals
                line_no_paren = re.sub(r'\(.*?\)', '', line).strip()
                # Weekend of ...
                m = re.match(r'weekend of (.+)', line_no_paren, re.IGNORECASE)
                if m:
                    date_text = m.group(1)
                    start, end = parse_flexible_date_range(date_text, year_hint)
                    print(f"Weekend of detected: {line_no_paren} -> {start} to {end}")
                    if start and end:
                        non_pto_requests.append({
                            "Resident": resident_name,
                            "Start_Date": start,
                            "End_Date": end,
                            "Reason": "Non-call"
                        })
                    i += 1
                    continue
                # PTO or Non-PTO date range in line
                if current_section in ['pto', 'non_pto']:
                    start, end = parse_flexible_date_range(line_no_paren, year_hint)
                    print(f"Date range parse in section {current_section}: {line_no_paren} -> {start} to {end}")
                    if start and end:
                        req = {
                            "Resident": resident_name,
                            "Start_Date": start,
                            "End_Date": end,
                            "Reason": "PTO" if current_section == 'pto' else "Non-call"
                        }
                        if current_section == 'pto':
                            pto_requests.append(req)
                        else:
                            non_pto_requests.append(req)
                i += 1
                continue
            i += 1
        print(f"Final PTO requests: {pto_requests}")
        print(f"Final Non-PTO requests: {non_pto_requests}")
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
