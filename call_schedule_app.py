# call_schedule_app.py

import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime as dt_type, date as date_type, timedelta
import json
import os
from scheduling_engine import run_scheduling_engine
from run_formatter import format_schedule
from openpyxl import Workbook
import io
from gmail_fetcher import fetch_requests_from_gmail, ensure_date
import traceback

# Initialize session state variables
if 'pto_requests' not in st.session_state:
    st.session_state.pto_requests = {}
if 'previous_call_counts' not in st.session_state:
    st.session_state.previous_call_counts = None
if 'schedule_df' not in st.session_state:
    st.session_state.schedule_df = None
if 'residents' not in st.session_state:
    st.session_state.residents = None
if 'block_start_date' not in st.session_state:
    st.session_state.block_start_date = None
if 'block_end_date' not in st.session_state:
    st.session_state.block_end_date = None
if 'block_number' not in st.session_state:
    st.session_state.block_number = None

# Initialize all session state variables
if 'residents_data' not in st.session_state:
    st.session_state.residents_data = []
if 'holiday_assignments_list' not in st.session_state:
    st.session_state.holiday_assignments_list = []
if 'soft_constraints' not in st.session_state:
    st.session_state.soft_constraints = []
if 'resident_count' not in st.session_state:
    st.session_state.resident_count = 1
if 'holiday_count' not in st.session_state:
    st.session_state.holiday_count = 1
if 'pto_count' not in st.session_state:
    st.session_state.pto_count = 1
if 'soft_constraint_count' not in st.session_state:
    st.session_state.soft_constraint_count = 1
if 'current_academic_year' not in st.session_state:
    st.session_state.current_academic_year = None
if 'loaded_residents' not in st.session_state:
    st.session_state.loaded_residents = []
if 'disable_holidays' not in st.session_state:
    st.session_state.disable_holidays = False
if 'disable_pto' not in st.session_state:
    st.session_state.disable_pto = False
if 'disable_soft_constraints' not in st.session_state:
    st.session_state.disable_soft_constraints = False
if 'removed_residents' not in st.session_state:
    st.session_state.removed_residents = set()
if 'removed_holidays' not in st.session_state:
    st.session_state.removed_holidays = set()
if 'removed_pto' not in st.session_state:
    st.session_state.removed_pto = set()
if 'removed_soft_constraints' not in st.session_state:
    st.session_state.removed_soft_constraints = set()
if 'block_dates' not in st.session_state:
    st.session_state.block_dates = None

def save_data(academic_year):
    """Save only resident information for an academic year"""
    if not os.path.exists('saved_data'):
        os.makedirs('saved_data')
    
    data = {
        'residents': st.session_state.residents_data
    }
    
    filename = f'saved_data/academic_year_{academic_year.replace("-", "_")}.json'
    with open(filename, 'w') as f:
        json.dump(data, f)
    return filename

def load_data(academic_year):
    """Load only resident information for an academic year"""
    filename = f'saved_data/academic_year_{academic_year.replace("-", "_")}.json'
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            data = json.load(f)
        return data.get('residents', [])
    return []

st.set_page_config(page_title="Kall Scheduler Kuhnel (KSK)", layout="wide")

st.markdown("""
<h2 style='text-align: center;'>Kall Scheduler Kuhnel (KSK)</h2>
""", unsafe_allow_html=True)

# Academic Year Selection
current_year = dt_type.now().year
year_options = [f"{year}-{year+1}" for year in range(2025, current_year+10)]
academic_year = st.selectbox("Select Academic Year:", year_options)
start_year = int(academic_year.split('-')[0])

# Initialize or update block dates based on academic year
if st.session_state.block_dates is None or st.session_state.current_academic_year != academic_year:
    st.session_state.block_dates = {
        "Block 1": {
            "start": f"{start_year}-07-01",
            "end": f"{start_year}-10-31"
        },
        "Block 2": {
            "start": f"{start_year}-11-01",
            "end": f"{start_year+1}-02-28"
        },
        "Block 3": {
            "start": f"{start_year+1}-03-01",
            "end": f"{start_year+1}-06-30"
        }
    }
    st.session_state.current_academic_year = academic_year
    # Reset holiday assignments when academic year changes
    st.session_state.holiday_assignments_list = []
    st.session_state.holiday_count = 1
    # Reset PTO requests when academic year changes
    st.session_state.pto_requests = {}
    st.session_state.pto_count = 1
    st.session_state.removed_pto = set()
    # Reset soft constraints when academic year changes
    st.session_state.soft_constraints = []
    st.session_state.soft_constraint_count = 1
    st.session_state.removed_soft_constraints = set()

# Load saved data when academic year changes
if st.session_state.current_academic_year != academic_year:
    loaded_residents = load_data(academic_year)
    if loaded_residents:
        st.session_state.loaded_residents = loaded_residents
        st.session_state.resident_count = len(loaded_residents)
    st.session_state.current_academic_year = academic_year

# Block selector with date ranges
block_info = {
    "Block 1": {
        "date_range": f"July - October {start_year}",
        "default_start": f"{start_year}-07-01",
        "default_end": f"{start_year}-10-31",
        "requires_previous": False
    },
    "Block 2": {
        "date_range": f"November {start_year} - February {start_year+1}",
        "default_start": f"{start_year}-11-01",
        "default_end": f"{start_year+1}-02-28",
        "requires_previous": True
    },
    "Block 3": {
        "date_range": f"March - June {start_year+1}",
        "default_start": f"{start_year+1}-03-01",
        "default_end": f"{start_year+1}-06-30",
        "requires_previous": True
    }
}

# Move PGY-4 cap input to the right of block selection
col_block_radio, col_block_cap = st.columns([3, 1])
with col_block_radio:
    block_choice = st.radio("Select block to generate:", list(block_info.keys()), horizontal=True)
with col_block_cap:
    pgy4_cap = st.number_input(
        "Desired PGY-4 call cap for this block:",
        min_value=1,
        max_value=100,
        value=st.session_state.get('pgy4_cap', 2),
        step=1,
        key='pgy4_cap'
    )

# Display block information and date selection
st.info(f"📅 Selected: {block_choice}")

# Add date selection
col1, col2 = st.columns(2)
with col1:
    block_start = st.date_input(
        "Block Start Date",
        value=dt_type.strptime(st.session_state.block_dates[block_choice]["start"], "%Y-%m-%d").date(),
        min_value=dt_type(start_year, 7, 1).date(),
        max_value=dt_type(start_year+1, 6, 30).date(),
        key=f"block_start_{block_choice}"
    )
with col2:
    block_end = st.date_input(
        "Block End Date",
        value=dt_type.strptime(st.session_state.block_dates[block_choice]["end"], "%Y-%m-%d").date(),
        min_value=dt_type(start_year, 7, 1).date(),
        max_value=dt_type(start_year+1, 6, 30).date(),
        key=f"block_end_{block_choice}"
    )

# Convert to datetime objects for comparison
block_start_dt = dt_type.combine(block_start, dt_type.min.time())
block_end_dt = dt_type.combine(block_end, dt_type.min.time())

# Validate dates
if block_end_dt <= block_start_dt:
    st.error("End date must be after start date")
elif block_choice == "Block 1" and block_start != dt_type.strptime(f"{start_year}-07-01", "%Y-%m-%d").date():
    st.error("Block 1 must start on July 1st")
elif block_choice == "Block 3" and block_end != dt_type.strptime(f"{start_year+1}-06-30", "%Y-%m-%d").date():
    st.error("Block 3 must end on June 30th")

# Store the selected dates in session state
st.session_state.block_dates[block_choice] = {
    "start": block_start.strftime("%Y-%m-%d"),
    "end": block_end.strftime("%Y-%m-%d")
}

# --- Tab state management ---
tab_labels = ["Residents", "Holiday Assignments", "Hard Constraints", "Soft Constraints", "Previous Block", "Previous Call Counts"]
if "active_tab" not in st.session_state:
    st.session_state.active_tab = 0
tabs = st.tabs(tab_labels)
active_tab = st.session_state.active_tab

with tabs[0]:
    st.subheader("Resident Information")
    
    def add_resident():
        st.session_state.resident_count += 1

    def remove_resident(index):
        st.session_state.removed_residents.add(index)
        st.rerun()

    # Temporary list to collect new resident data
    new_residents = []

    # Use st.data_editor for batch resident editing (Streamlit 1.22+)
    if st.session_state.residents_data:
        df = pd.DataFrame(st.session_state.residents_data)
    else:
        df = pd.DataFrame(columns=["Name", "PGY", "Transition_Date"])

    # Convert Transition_Date to datetime.date for compatibility with DateColumn
    if "Transition_Date" in df.columns:
        df["Transition_Date"] = pd.to_datetime(df["Transition_Date"], errors="coerce").dt.date

    # Remove the card-style markdown and just show a simple heading and the editable table
    st.markdown("### Resident List")
    edited_df = st.data_editor(
        df,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "Name": st.column_config.TextColumn(
                "Resident Name",
                width="large"
            ),
            "PGY": st.column_config.SelectboxColumn(
                "PGY Level",
                options=[1, 2, 3, 4],
                required=True,
                width="small"
            ),
            "Transition_Date": st.column_config.DateColumn(
                "Transition Date",
                format="YYYY-MM-DD",
                min_value=dt_type(start_year, 7, 1),
                max_value=dt_type(start_year+1, 6, 30),
                width="medium"
            ),
        }
    )
    st.session_state.residents_data = edited_df.to_dict('records')
    st.session_state.resident_count = len(st.session_state.residents_data)

    st.button("Add Another Resident", on_click=add_resident)

    # Save Residents button
    if st.button("Save Residents"):
        if st.session_state.residents_data:
            # Export residents data as CSV
            residents_df = pd.DataFrame(st.session_state.residents_data)
            csv = residents_df.to_csv(index=False)
            st.download_button(
                label="Download Residents CSV",
                data=csv,
                file_name="residents.csv",
                mime="text/csv"
            )
            st.success(f"Residents data exported as CSV for {academic_year}")
        else:
            st.warning("No resident data to save")

    # --- CSV upload: ensure all rows are loaded ---
    uploaded_file = st.file_uploader("Upload Residents CSV", type=["csv"], key="residents_csv_upload")
    if uploaded_file is not None and not st.session_state.get("residents_csv_uploaded", False):
        try:
            residents_df = pd.read_csv(uploaded_file, dtype=str).fillna("")
            st.session_state.residents_data = residents_df.to_dict('records')
            st.session_state.resident_count = len(st.session_state.residents_data)
            st.session_state["residents_csv_uploaded"] = True  # Mark as processed
            st.success("Residents data uploaded successfully!")
            st.rerun()
        except Exception as e:
            st.error(f"Error uploading file: {str(e)}")
    elif uploaded_file is None and st.session_state.get("residents_csv_uploaded", False):
        st.session_state["residents_csv_uploaded"] = False  # Reset for next upload

with tabs[1]:
    st.subheader("Holiday Call Assignments")
    
    def add_holiday():
        st.session_state.holiday_count += 1

    def remove_holiday(index):
        st.session_state.removed_holidays.add(index)
        st.rerun()

    # Add disable holidays checkbox
    st.session_state.disable_holidays = st.checkbox("Disable Holiday Assignments", value=st.session_state.disable_holidays)

    if not st.session_state.disable_holidays:
        # Initialize holiday_assignments_list if not already initialized
        if 'holiday_assignments_list' not in st.session_state:
            st.session_state.holiday_assignments_list = []
        
        # Filter PGY-2 residents for holiday assignments
        pgy2_residents = [res["Name"] for res in st.session_state.residents_data if int(res["PGY"]) == 2]
        
        # Temporary list to collect new holiday assignments
        new_holiday_assignments = []
        
        for i in range(st.session_state.holiday_count):
            if i in st.session_state.removed_holidays:
                continue
                
            st.markdown(f"### Holiday #{i+1}")
            col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 1, 0.5])
            
            with col1:
                # Get existing holiday name if available
                default_name = st.session_state.holiday_assignments_list[i].get('Name', "") if i < len(st.session_state.holiday_assignments_list) else ""
                holiday_name = st.text_input("Holiday Name", value=default_name, key=f"holiday_name_{i}")
            with col2:
                # Get existing holiday date if available, otherwise use July 4th of the current academic year
                if i < len(st.session_state.holiday_assignments_list):
                    default_date = dt_type.strptime(st.session_state.holiday_assignments_list[i]['Date'], "%Y-%m-%d")
                else:
                    default_date = dt_type(start_year, 7, 4)
                holiday_date = st.date_input(
                    "Date",
                    value=default_date,
                    min_value=dt_type(start_year, 7, 1),
                    max_value=dt_type(start_year+1, 6, 30),
                    key=f"holiday_date_{i}"
                )
            with col3:
                # Get existing call assignment if available
                default_call = st.session_state.holiday_assignments_list[i].get('Call', "") if i < len(st.session_state.holiday_assignments_list) else ""
                call = st.selectbox(
                    "Call Assignment",
                    [""] + pgy2_residents if pgy2_residents else [""],
                    index=0 if default_call == "" else pgy2_residents.index(default_call) + 1 if default_call in pgy2_residents else 0,
                    key=f"holiday_call_{i}"
                )
            with col4:
                # Get existing backup assignment if available
                default_backup = st.session_state.holiday_assignments_list[i].get('Backup', "") if i < len(st.session_state.holiday_assignments_list) else ""
                backup = st.selectbox(
                    "Backup Assignment",
                    [""] + pgy2_residents if pgy2_residents else [""],
                    index=0 if default_backup == "" else pgy2_residents.index(default_backup) + 1 if default_backup in pgy2_residents else 0,
                    key=f"holiday_backup_{i}"
                )
            with col5:
                st.button("❌", key=f"remove_hol_{i}", on_click=remove_holiday, args=(i,))
            
            if holiday_name and call != "" and backup != "":  # Only add if all fields are filled
                new_holiday_assignments.append({
                    "Name": holiday_name,
                    "Date": holiday_date.strftime("%Y-%m-%d"),
                    "Call": call,
                    "Backup": backup
                })
        
        # Update holiday_assignments_list with new data
        st.session_state.holiday_assignments_list = new_holiday_assignments

        col1, col2 = st.columns([6, 1])
        with col1:
            st.button("Add Another Holiday", on_click=add_holiday)
        with col2:
            if st.button("Clear Holidays"):
                st.session_state.holiday_count = 1
                st.session_state.holiday_assignments_list = []
                st.session_state.removed_holidays = set()
                st.rerun()

with tabs[2]:
    st.subheader("Hard Constraints")
    st.info("Use this section to specify PTO requests and rotation blocks where residents cannot be assigned call. These are high-priority constraints that will be strictly enforced in the schedule.")

    # --- Move the fetch button to the top of the tab ---
    if st.button("Fetch Requests from Gmail"):
        pto_requests, _ = fetch_requests_from_gmail()  # Only use PTO requests here
        # Only keep requests with Reason == 'PTO'
        pto_only = [req for req in pto_requests if req.get("Reason", "").lower() == "pto"]
        print('DEBUG PTO ONLY:', pto_only)
        # Normalize resident names in the app for matching
        valid_residents = {res["Name"].strip().lower(): res["Name"] for res in st.session_state.residents_data}
        grouped_pto = {}
        for req in pto_only:
            req_name = req["Resident"].strip().lower()
            if req_name in valid_residents:
                canonical_name = valid_residents[req_name]
                if canonical_name not in grouped_pto:
                    grouped_pto[canonical_name] = []
                grouped_pto[canonical_name].append({
                    "Start_Date": req["Start_Date"],
                    "End_Date": req["End_Date"]
                })
        st.session_state.pto_requests = grouped_pto
        msg = f"Fetched PTO requests for {len(grouped_pto)} residents from Gmail."
        st.success(msg)

    def add_pto_for_resident(resident):
        if not isinstance(st.session_state.pto_requests, dict):
            st.session_state.pto_requests = {}
        st.session_state.pto_requests.setdefault(resident, []).append({
            "Start_Date": "",
            "End_Date": ""
        })
        st.session_state.active_tab = 2  # Stay on Hard Constraints tab
        st.rerun()

    def remove_pto_for_resident(resident, idx):
        if resident in st.session_state.pto_requests and 0 <= idx < len(st.session_state.pto_requests[resident]):
            st.session_state.pto_requests[resident].pop(idx)
            if not st.session_state.pto_requests[resident]:
                del st.session_state.pto_requests[resident]
        st.session_state.active_tab = 2  # Stay on Hard Constraints tab
        st.rerun()

    st.session_state.disable_pto = st.checkbox("Disable PTO Requests", value=st.session_state.disable_pto)

    if not st.session_state.disable_pto:
        if 'pto_requests' not in st.session_state:
            st.session_state.pto_requests = {}
        for resident in [res["Name"] for res in st.session_state.residents_data]:
            # Robustly handle both dict and list types for PTO requests
            if isinstance(st.session_state.pto_requests, dict):
                requests = st.session_state.pto_requests.get(resident, [])
            elif isinstance(st.session_state.pto_requests, list):
                requests = [req for req in st.session_state.pto_requests if req.get('Resident') == resident]
            else:
                requests = []
            st.markdown(f"### {resident} Hard Constraints")
            for idx, req in enumerate(requests):
                col1, col2, col3 = st.columns([1, 1, 0.2])
                min_date = dt_type(start_year, 7, 1).date()
                max_date = dt_type(start_year+1, 6, 30).date()
                raw_start = req.get('Start_Date', min_date)
                raw_end = req.get('End_Date', min_date)
                # Convert to date objects before comparison
                raw_start = ensure_date(raw_start, min_date)
                raw_end = ensure_date(raw_end, min_date)
                # Now safe to compare since both are date objects
                safe_start = raw_start if min_date <= raw_start <= max_date else min_date
                safe_end = raw_end if min_date <= raw_end <= max_date else min_date
                with col1:
                    start_date = st.date_input(
                        f"Start Date for {resident} #{idx+1}",
                        value=safe_start,
                        min_value=min_date,
                        max_value=max_date,
                        key=f"pto_start_{resident}_{idx}"
                    )
                with col2:
                    end_date = st.date_input(
                        f"End Date for {resident} #{idx+1}",
                        value=safe_end,
                        min_value=min_date,
                        max_value=max_date,
                        key=f"pto_end_{resident}_{idx}"
                    )
                with col3:
                    st.button("❌", key=f"remove_pto_{resident}_{idx}", on_click=remove_pto_for_resident, args=(resident, idx))
                # Update the request in session state
                if isinstance(st.session_state.pto_requests, dict):
                    st.session_state.pto_requests[resident][idx] = {
                        "Start_Date": start_date.strftime("%Y-%m-%d"),
                        "End_Date": end_date.strftime("%Y-%m-%d")
                    }
            st.button(f"Add Another PTO Request for {resident}", on_click=add_pto_for_resident, args=(resident,))

with tabs[3]:
    st.subheader("Soft Constraints")
    st.info("Use this section to specify requests that are preferred but not strictly required.")

    # --- Add a fetch button for Non-PTO requests only ---
    if st.button("Fetch Non-PTO Requests from Gmail"):
        _, non_pto_requests = fetch_requests_from_gmail()  # Only use Non-PTO requests here
        print('DEBUG NON-PTO (button):', non_pto_requests)
        # Only keep requests with Reason == 'Non-call' or 'Non-PTO'
        non_pto_only = [req for req in non_pto_requests if req.get("Reason", "").lower() in ["non-call", "non-pto"]]
        valid_residents = {res["Name"].strip().lower(): res["Name"] for res in st.session_state.residents_data}
        grouped_soft = {}
        skipped_non_pto_names = []
        for req in non_pto_only:
            req_name = req["Resident"].strip().lower()
            if req_name in valid_residents:
                canonical_name = valid_residents[req_name]
                grouped_soft.setdefault(canonical_name, []).append({
                    "Start_Date": req["Start_Date"],
                    "End_Date": req["End_Date"]
                })
            else:
                skipped_non_pto_names.append(req["Resident"])
        st.session_state.soft_constraints = grouped_soft
        msg = f"Fetched Non-PTO requests for {len(grouped_soft)} residents from Gmail."
        if skipped_non_pto_names:
            msg += f" Skipped Non-PTO requests for: {', '.join(skipped_non_pto_names)}."
        st.success(msg)

    def add_soft_constraint_for_resident(resident):
        st.session_state.soft_constraints.setdefault(resident, []).append({
            "Start_Date": "",
            "End_Date": ""
        })

    def remove_soft_constraint_for_resident(resident, idx):
        if resident in st.session_state.soft_constraints and 0 <= idx < len(st.session_state.soft_constraints[resident]):
            st.session_state.soft_constraints[resident].pop(idx)
            if not st.session_state.soft_constraints[resident]:
                del st.session_state.soft_constraints[resident]
        st.rerun()

    st.session_state.disable_soft_constraints = st.checkbox("Disable Soft Constraints", value=st.session_state.disable_soft_constraints)

    if not st.session_state.disable_soft_constraints:
        if 'soft_constraints' not in st.session_state or not isinstance(st.session_state.soft_constraints, dict):
            st.session_state.soft_constraints = {}
        for resident in [res["Name"] for res in st.session_state.residents_data]:
            requests = st.session_state.soft_constraints.get(resident, [])
            st.markdown(f"### {resident} Soft Constraints")
            for idx, req in enumerate(requests):
                col1, col2, col3 = st.columns([1, 1, 0.2])
                min_date = dt_type(start_year, 7, 1).date()
                max_date = dt_type(start_year+1, 6, 30).date()
                raw_start = req.get('Start_Date', min_date)
                raw_end = req.get('End_Date', min_date)
                raw_start = ensure_date(raw_start, min_date)
                raw_end = ensure_date(raw_end, min_date)
                safe_start = raw_start if min_date <= raw_start <= max_date else min_date
                safe_end = raw_end if min_date <= raw_end <= max_date else min_date
                with col1:
                    start_date = st.date_input(
                        f"Start Date for {resident} #{idx+1}",
                        value=safe_start,
                        min_value=min_date,
                        max_value=max_date,
                        key=f"soft_start_{resident}_{idx}"
                    )
                with col2:
                    end_date = st.date_input(
                        f"End Date for {resident} #{idx+1}",
                        value=safe_end,
                        min_value=min_date,
                        max_value=max_date,
                        key=f"soft_end_{resident}_{idx}"
                    )
                with col3:
                    st.button("❌", key=f"remove_soft_{resident}_{idx}", on_click=remove_soft_constraint_for_resident, args=(resident, idx))
                # Update the request in session state
                st.session_state.soft_constraints[resident][idx] = {
                    "Start_Date": start_date.strftime("%Y-%m-%d"),
                    "End_Date": end_date.strftime("%Y-%m-%d")
                }
            st.button(f"Add Another Soft Constraint for {resident}", on_click=add_soft_constraint_for_resident, args=(resident,))

with tabs[4]:
    st.subheader("Previous Block End Assignments")
    
    if not block_info[block_choice]['requires_previous']:
        st.info("No previous block assignments needed for Block 1.")
    else:
        st.info("Please enter the call assignments for the last 4 days of the previous block to maintain spacing rules.")
        
        # Calculate the dates (last 4 days of previous block)
        prev_block_end = block_start_dt - timedelta(days=1)
        dates = [(prev_block_end - timedelta(days=i)) for i in range(3, -1, -1)]
        
        # Initialize previous assignments in session state if not exists
        if 'previous_assignments' not in st.session_state:
            st.session_state.previous_assignments = []
        
        # Create a list to store new assignments
        new_previous_assignments = []
        
        # Create input fields for each date
        for idx, date in enumerate(dates):
            date_str = date.strftime("%Y-%m-%d")
            st.markdown(f"**{date_str}**")
            col1, col2 = st.columns(2)
            with col1:
                default_call = st.session_state.previous_assignments[idx]['Call'] if idx < len(st.session_state.previous_assignments) else ""
                call = st.selectbox(
                    "Call Resident",
                    [""] + [res["Name"] for res in st.session_state.residents_data],
                    index=0 if default_call == "" else [res["Name"] for res in st.session_state.residents_data].index(default_call) + 1 if default_call in [res["Name"] for res in st.session_state.residents_data] else 0,
                    key=f"prev_call_{date_str}"
                )
            with col2:
                default_backup = st.session_state.previous_assignments[idx]['Backup'] if idx < len(st.session_state.previous_assignments) else ""
                backup = st.selectbox(
                    "Backup Resident",
                    [""] + [res["Name"] for res in st.session_state.residents_data],
                    index=0 if default_backup == "" else [res["Name"] for res in st.session_state.residents_data].index(default_backup) + 1 if default_backup in [res["Name"] for res in st.session_state.residents_data] else 0,
                    key=f"prev_backup_{date_str}"
                )
            
            if call and backup:
                new_previous_assignments.append({
                    "Date": date_str,
                    "Call": call,
                    "Backup": backup
                })
            else:
                new_previous_assignments.append({
                    "Date": date_str,
                    "Call": call,
                    "Backup": backup
                })
        
        # Update session state
        st.session_state.previous_assignments = new_previous_assignments

with tabs[5]:
    st.subheader("Previous Call Counts")
    
    if block_choice == "Block 1":
        st.info("No previous call counts needed for Block 1.")
    else:
        st.info("Please upload the call statistics CSV file(s) from the previous block(s). For Block 2, upload Block 1's statistics. For Block 3, upload both Block 1 and Block 2's statistics.")
        
        # Initialize previous call counts in session state if not exists
        if 'previous_call_counts' not in st.session_state:
            st.session_state.previous_call_counts = {}
        
        # File upload section (CSV only)
        uploaded_files = st.file_uploader(
            "Upload previous block call statistics (CSV)",
            type=['csv'],
            accept_multiple_files=True if block_choice == "Block 3" else False
        )
        
        if uploaded_files:
            try:
                all_counts = {}
                # Always treat uploaded_files as a list for uniform processing
                files = uploaded_files if isinstance(uploaded_files, list) else [uploaded_files]
                for file in files:
                    # Handle both file-like and bytes objects
                    if hasattr(file, 'seek'):
                        file.seek(0)
                        df = pd.read_csv(file)
                    else:
                        # file is bytes, decode to string and use StringIO
                        s = file.decode('utf-8')
                        df = pd.read_csv(io.StringIO(s))
                    for _, row in df.iterrows():
                        resident = row['Resident']
                        if resident not in all_counts:
                            all_counts[resident] = {
                                'Weekday': 0,
                                'Fridays': 0,
                                'Saturday': 0,
                                'Sunday': 0,
                                'Total': 0
                            }
                        all_counts[resident]['Weekday'] += int(row.get('Weekday', 0))
                        all_counts[resident]['Fridays'] += int(row.get('Fridays', 0))
                        all_counts[resident]['Saturday'] += int(row.get('Saturday', 0))
                        all_counts[resident]['Sunday'] += int(row.get('Sunday', 0))
                        all_counts[resident]['Total'] += int(row.get('Total', 0))
                st.session_state.previous_call_counts = all_counts
                st.success("Successfully processed previous call counts!")
            except Exception as e:
                st.error(f"Error processing files: {str(e)}")
                st.error("Please make sure you're uploading the correct call statistics CSV files.")
        else:
            st.warning("Please upload the call statistics CSV file(s) from the previous block(s).")

# Generate button
if st.button("Generate Schedule"):
    # Clear previous results
    st.session_state.pop('last_schedule_df', None)
    st.session_state.pop('last_stats', None)
    st.session_state.pop('last_excel_file', None)
    st.session_state.pop('last_block_choice', None)
    st.session_state.pop('last_block_name', None)
    st.session_state.pop('last_success', None)

    if not st.session_state.residents_data:
        st.error("Please enter at least one resident.")
    elif block_info[block_choice]['requires_previous'] and len(st.session_state.previous_assignments) < 4:
        st.error("Please enter all previous block assignments to maintain proper spacing rules.")
    elif block_end_dt <= block_start_dt:
        st.error("End date must be after start date")
    else:
        with st.spinner("Generating schedule..."):
            try:
                # Transform the form data into the format expected by the engine
                residents_df = pd.DataFrame([{
                    'Resident': res['Name'],
                    'PGY': res['PGY'],
                    'Transition Date': res['Transition_Date'],
                    'Transition PGY': min(int(res['PGY']) + 1, 4) if res['Transition_Date'] else None
                } for res in st.session_state.residents_data])
                
                # Validate transition dates are within the block
                for _, row in residents_df.iterrows():
                    if pd.notna(row['Transition Date']):
                        trans_val = row['Transition Date']
                        if isinstance(trans_val, (dt_type, date_type)):
                            trans_date = trans_val
                        else:
                            trans_date = dt_type.strptime(str(trans_val), "%Y-%m-%d")
                        # Ensure block_start_dt and block_end_dt are datetime
                        bsd = block_start_dt
                        bed = block_end_dt
                        if isinstance(bsd, date_type) and not isinstance(bsd, dt_type):
                            bsd = dt_type.combine(bsd, dt_type.min.time())
                        if isinstance(bed, date_type) and not isinstance(bed, dt_type):
                            bed = dt_type.combine(bed, dt_type.min.time())
                        # Ensure trans_date is also datetime
                        if isinstance(trans_date, date_type) and not isinstance(trans_date, dt_type):
                            trans_date = dt_type.combine(trans_date, dt_type.min.time())
                        if not (bsd <= trans_date <= bed):
                            st.warning(f"Transition date for {row['Resident']} ({row['Transition Date']}) is outside the selected block period.")
                
                # Create previous assignments DataFrame if needed
                prev_df = None
                if block_info[block_choice]['requires_previous'] and st.session_state.previous_assignments:
                    prev_df = pd.DataFrame(st.session_state.previous_assignments)
                
                # Transform holiday assignments into the format expected by the engine
                holidays_df = pd.DataFrame([{
                    'Date': holiday['Date'],
                    'Call': holiday['Call'],
                    'Backup': holiday['Backup']
                } for holiday in st.session_state.holiday_assignments_list]) if not st.session_state.disable_holidays and st.session_state.holiday_assignments_list else pd.DataFrame(columns=['Date', 'Call', 'Backup'])
                
                # Transform PTO requests into the format expected by the engine
                pto_df = pd.DataFrame([{
                    'Resident': pto['Resident'],
                    'Start Date': pto['Start_Date'],
                    'End Date': pto['End_Date']
                } for pto in st.session_state.pto_requests]) if not st.session_state.disable_pto and st.session_state.pto_requests else pd.DataFrame(columns=['Resident', 'Start Date', 'End Date'])
                
                # Run the scheduling engine with selected dates and pgy4_cap
                schedule_df = run_scheduling_engine(
                    prev_df,
                    residents_df,
                    pto_df,
                    holidays_df,
                    block_start_dt,
                    block_end_dt,
                    pgy4_cap=pgy4_cap,
                    previous_call_counts=st.session_state.previous_call_counts if block_choice != "Block 1" else None
                )
                
                # Calculate call statistics
                # st.subheader("Call Distribution Statistics")
                
                # Create DataFrames for each PGY level
                pgy_stats = {1: [], 2: [], 3: [], 4: []}
                
                # For all uses of datetime.strptime on schedule/transition dates, robustly handle both string and datetime.date/datetime.datetime
                # --- For intern_calls and resident_calls date columns ---
                def safe_parse_date(x):
                    if isinstance(x, (dt_type, date_type)):
                        return x
                    try:
                        return dt_type.strptime(str(x), "%Y-%m-%d")
                    except (ValueError, TypeError):
                        return None

                # Determine the final PGY for each resident for the block
                final_pgy_by_resident = {}
                for _, row in residents_df.iterrows():
                    name = row['Resident']
                    pgy = int(row['PGY'])
                    if pd.notna(row['Transition Date']):
                        trans_val = row['Transition Date']
                        if isinstance(trans_val, (dt_type, date_type)):
                            trans_date = trans_val
                        else:
                            trans_date = dt_type.strptime(str(trans_val), "%Y-%m-%d")
                        bed = block_end_dt
                        # Ensure both are datetime.datetime
                        if isinstance(trans_date, date_type) and not isinstance(trans_date, dt_type):
                            trans_date = dt_type.combine(trans_date, dt_type.min.time())
                        if isinstance(bed, date_type) and not isinstance(bed, dt_type):
                            bed = dt_type.combine(bed, dt_type.min.time())
                        if trans_date <= bed:
                            final_pgy_by_resident[name] = int(row['Transition PGY'])
                        else:
                            final_pgy_by_resident[name] = pgy
                    else:
                        final_pgy_by_resident[name] = pgy

                for _, resident_row in residents_df.iterrows():
                    resident = resident_row['Resident']
                    pgy = int(resident_row['PGY'])
                    display_pgy = final_pgy_by_resident[resident]
                    
                    # For PGY-1, count intern assignments
                    if display_pgy == 1:
                        intern_calls = schedule_df[schedule_df['Intern'] == resident]
                        weekday_calls = len(intern_calls[intern_calls['Date'].apply(lambda x: safe_parse_date(x).weekday() not in [5, 6])])
                        saturday_calls = len(intern_calls[intern_calls['Date'].apply(lambda x: safe_parse_date(x).weekday() == 5)])
                        total_calls = len(intern_calls)
                        
                        stats = {
                            'Resident': resident,
                            'Weekday': weekday_calls,
                            'Saturday': saturday_calls,
                            'Total': total_calls
                        }
                        pgy_stats[display_pgy].append(stats)
                    else:
                        # For other PGY levels, count primary call assignments
                        resident_calls = schedule_df[schedule_df['Call'] == resident]
                        
                        # Calculate day type breakdowns
                        weekday_calls = len(resident_calls[resident_calls['Date'].apply(lambda x: safe_parse_date(x).weekday() in [0,1,2,3])])
                        friday_calls = len(resident_calls[resident_calls['Date'].apply(lambda x: safe_parse_date(x).weekday() == 4)])
                        saturday_calls = len(resident_calls[resident_calls['Date'].apply(lambda x: safe_parse_date(x).weekday() == 5)])
                        sunday_calls = len(resident_calls[resident_calls['Date'].apply(lambda x: safe_parse_date(x).weekday() == 6)])
                        total_calls = len(resident_calls)
                        
                        stats = {
                            'Resident': resident
                        }
                        
                        # Add appropriate columns based on display PGY level
                        if display_pgy == 2:
                            stats.update({
                                'Weekday': weekday_calls,
                                'Fridays': friday_calls,
                                'Sunday': sunday_calls
                            })
                        elif display_pgy == 3:
                            stats.update({
                                'Weekday': weekday_calls,
                                'Saturday': saturday_calls
                            })
                        elif display_pgy == 4:
                            stats.update({
                                'Total': total_calls
                            })
                        
                        pgy_stats[display_pgy].append(stats)
                
                # Display PGY-1 stats
                # st.markdown("### PGY-1")
                # if pgy_stats[1]:
                #     pgy1_df = pd.DataFrame(pgy_stats[1])
                #     st.dataframe(pgy1_df.set_index('Resident'), use_container_width=True)
                # else:
                #     st.info("No PGY-1 residents")
                
                # Display PGY-2 stats
                # st.markdown("### PGY-2")
                # if pgy_stats[2]:
                #     pgy2_df = pd.DataFrame(pgy_stats[2])
                #     st.dataframe(pgy2_df.set_index('Resident'), use_container_width=True)
                # else:
                #     st.info("No PGY-2 residents")
                
                # Display PGY-3 stats
                # st.markdown("### PGY-3")
                # if pgy_stats[3]:
                #     pgy3_df = pd.DataFrame(pgy_stats[3])
                #     st.dataframe(pgy3_df.set_index('Resident'), use_container_width=True)
                # else:
                #     st.info("No PGY-3 residents")
                
                # Display PGY-4 stats
                # st.markdown("### PGY-4")
                # if pgy_stats[4]:
                #     pgy4_df = pd.DataFrame(pgy_stats[4])
                #     st.dataframe(pgy4_df.set_index('Resident'), use_container_width=True)
                # else:
                #     st.info("No PGY-4 residents")
                
                # Format the schedule
                wb = format_schedule(schedule_df, schedule_df, schedule_df)  # Using same schedule for all blocks
                
                # Save to BytesIO
                excel_file = BytesIO()
                wb.save(excel_file)
                excel_file.seek(0)
                
                # Store in session state
                st.session_state['last_schedule_df'] = schedule_df
                st.session_state['last_stats'] = pgy_stats
                st.session_state['last_excel_file'] = excel_file.getvalue()
                st.session_state['last_block_choice'] = block_choice
                st.session_state['last_block_name'] = block_choice.lower().replace(' ', '_')
                st.session_state['last_success'] = True
            except Exception as e:
                st.session_state['last_success'] = False
                st.error(f"An error occurred: {str(e)}")
                print("=== FULL TRACEBACK ===")
                traceback.print_exc()

# --- Always display if present in session state ---
if st.session_state.get('last_success'):
    schedule_df = st.session_state.get('last_schedule_df')
    pgy_stats = st.session_state.get('last_stats')
    excel_file_bytes = st.session_state.get('last_excel_file')
    block_choice = st.session_state.get('last_block_choice', block_choice)
    block_name = st.session_state.get('last_block_name', block_choice.lower().replace(' ', '_'))

    st.subheader("Call Distribution Statistics")
    # Display PGY-1 stats
    st.markdown("### PGY-1")
    if pgy_stats[1]:
        pgy1_df = pd.DataFrame(pgy_stats[1])
        if 'Total' in pgy1_df.columns:
            pgy1_df = pgy1_df.drop(columns=['Total'])
        st.dataframe(pgy1_df.set_index('Resident'), use_container_width=True)
    else:
        st.info("No PGY-1 residents")
    # Display PGY-2 stats
    st.markdown("### PGY-2")
    if pgy_stats[2]:
        pgy2_df = pd.DataFrame(pgy_stats[2])
        st.dataframe(pgy2_df.set_index('Resident'), use_container_width=True)
    else:
        st.info("No PGY-2 residents")
    # Display PGY-3 stats
    st.markdown("### PGY-3")
    if pgy_stats[3]:
        pgy3_df = pd.DataFrame(pgy_stats[3])
        st.dataframe(pgy3_df.set_index('Resident'), use_container_width=True)
    else:
        st.info("No PGY-3 residents")
    # Display PGY-4 stats
    st.markdown("### PGY-4")
    if pgy_stats[4]:
        pgy4_df = pd.DataFrame(pgy_stats[4])
        st.dataframe(pgy4_df.set_index('Resident'), use_container_width=True)
    else:
        st.info("No PGY-4 residents")

    # --- Running Total Section ---
    if block_choice in ["Block 2", "Block 3"] and st.session_state.get('previous_call_counts'):
        st.markdown("## Running Total (All Blocks)")
        previous_counts = st.session_state['previous_call_counts']
        # Build running total for each resident
        running_totals = {1: [], 2: [], 3: [], 4: []}
        for pgy in [1, 2, 3, 4]:
            for stats in pgy_stats[pgy]:
                resident = stats.get("Resident", "")
                prev = previous_counts.get(resident, {})
                # Always sum all columns, and compute Total as sum of all day types
                weekday = stats.get('Weekday', 0) + int(prev.get('Weekday', 0) or 0)
                fridays = stats.get('Fridays', 0) + int(prev.get('Fridays', 0) or 0)
                saturday = stats.get('Saturday', 0) + int(prev.get('Saturday', 0) or 0)
                sunday = stats.get('Sunday', 0) + int(prev.get('Sunday', 0) or 0)
                # For PGY-4, only Total is relevant, but show all columns for consistency
                if pgy == 4:
                    total = stats.get('Total', 0) + int(prev.get('Total', 0) or 0)
                else:
                    total = weekday + fridays + saturday + sunday
                total_stats = {
                    'Resident': resident,
                    'Weekday': weekday,
                    'Fridays': fridays,
                    'Saturday': saturday,
                    'Sunday': sunday,
                    'Total': total
                }
                running_totals[pgy].append(total_stats)
        # Display running totals by PGY with only relevant columns
        for pgy in [1, 2, 3, 4]:
            st.markdown(f"### PGY-{pgy} Running Total")
            if running_totals[pgy]:
                df = pd.DataFrame(running_totals[pgy])
                if pgy == 1:
                    display_cols = ['Weekday', 'Saturday', 'Total']
                elif pgy == 2:
                    display_cols = ['Weekday', 'Fridays', 'Sunday', 'Total']
                elif pgy == 3:
                    display_cols = ['Weekday', 'Saturday', 'Total']
                elif pgy == 4:
                    display_cols = ['Total']
                st.dataframe(df.set_index('Resident')[display_cols], use_container_width=True)
            else:
                st.info(f"No PGY-{pgy} residents")

    # Create CSV file for statistics (KSK_DATA)
    ksk_columns = ["Resident", "PGY", "Weekday", "Fridays", "Saturday", "Sunday", "Total"]
    ksk_data = []
    for pgy in [1, 2, 3, 4]:
        for stats in pgy_stats[pgy]:
            row = {
                "Resident": stats.get("Resident", ""),
                "PGY": pgy,
                "Weekday": stats.get("Weekday", 0),
                "Fridays": stats.get("Fridays", 0),
                "Saturday": stats.get("Saturday", 0),
                "Sunday": stats.get("Sunday", 0),
                "Total": stats.get("Total", 0)
            }
            ksk_data.append(row)
    ksk_df = pd.DataFrame(ksk_data, columns=ksk_columns)
    csv_buffer = io.StringIO()
    ksk_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Create two columns for download buttons
    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            label="Download Schedule",
            data=excel_file_bytes,
            file_name=f"call_schedule_{block_name}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    with col2:
        st.download_button(
            label="Download Call Statistics (CSV)",
            data=csv_buffer.getvalue(),
            file_name=f"call_statistics_{block_name}.csv",
            mime="text/csv"
        )
    st.success("Schedule generated successfully!") 