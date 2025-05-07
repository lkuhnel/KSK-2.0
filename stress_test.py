import pandas as pd
from datetime import datetime, timedelta
from engine_optimization_testing import CallScheduler, run_scheduling_engine
import random
import time
import itertools
from collections import defaultdict

def create_test_data():
    # Create test residents with actual numbers
    residents_info = {
        1: [f"Intern{i}" for i in range(1, 7)],  # 6 PGY-1
        2: [f"Resident2_{i}" for i in range(1, 7)],  # 6 PGY-2
        3: [f"Resident3_{i}" for i in range(1, 7)],  # 6 PGY-3
        4: [f"Resident4_{i}" for i in range(1, 5)]  # 4 PGY-4
    }
    
    # Create test date range (4 months - one block)
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 4, 30)
    
    # Create test holidays
    holidays = {
        "2024-01-01": ("Resident4_1", "Resident4_2"),  # New Year's Day
        "2024-01-15": ("Resident3_1", "Resident3_2"),  # MLK Day
        "2024-02-19": ("Resident3_2", "Resident3_3"),  # Presidents Day
    }
    
    return residents_info, start_date, end_date, holidays

def generate_pto_requests(residents_info, start_date, end_date):
    pto_requests = {}
    non_call_requests = {}
    
    # Calculate block duration in days
    block_days = (end_date - start_date).days
    
    # Generate PTO requests (4 weeks per resident)
    for pgy, residents in residents_info.items():
        # Split residents into groups to avoid too many being off at once
        group_size = len(residents) // 3  # Only allow ~1/3 of residents of each level to be off at once
        for i, resident in enumerate(residents):
            # Determine which third of the block this resident should primarily request
            block_third = (i // group_size) * (block_days // 3)
            
            # Generate 2-3 PTO periods (totaling ~4 weeks)
            pto_dates = []
            total_pto_days = 0
            attempts = 0
            
            while total_pto_days < 28 and attempts < 10:  # Try to get close to 4 weeks (28 days)
                # Random start date within the assigned third of the block, with some overlap
                earliest_start = block_third - 10
                latest_start = block_third + (block_days // 3) + 10
                period_start = start_date + timedelta(days=random.randint(max(0, earliest_start), min(block_days-7, latest_start)))
                
                # Random duration (5-10 days)
                duration = random.randint(5, 10)
                period_end = period_start + timedelta(days=duration)
                
                if period_end <= end_date:
                    # Check if this period overlaps with too many other residents of same PGY
                    overlapping = 0
                    for other_resident, other_dates in pto_requests.items():
                        if other_resident in residents:  # Same PGY level
                            for other_start, other_end in other_dates:
                                other_start_date = datetime.strptime(other_start, "%Y-%m-%d")
                                other_end_date = datetime.strptime(other_end, "%Y-%m-%d")
                                if (period_start <= other_end_date and period_end >= other_start_date):
                                    overlapping += 1
                    
                    if overlapping < group_size:  # Allow the request if not too many overlapping
                        pto_dates.extend([
                            (period_start.strftime("%Y-%m-%d"),
                             period_end.strftime("%Y-%m-%d"))
                        ])
                        total_pto_days += duration
                
                attempts += 1
            
            if pto_dates:
                pto_requests[resident] = pto_dates
    
    # Generate non-call requests (2 weeks per resident)
    for pgy, residents in residents_info.items():
        # Split residents into groups to avoid too many being off at once
        group_size = len(residents) // 3  # Only allow ~1/3 of residents of each level to be off at once
        for i, resident in enumerate(residents):
            # Determine which third of the block this resident should primarily request
            block_third = ((i // group_size + 1) % 3) * (block_days // 3)  # Offset from PTO third
            
            # Generate 1-2 non-call periods (totaling ~2 weeks)
            non_call_dates = []
            total_non_call_days = 0
            attempts = 0
            
            while total_non_call_days < 14 and attempts < 10:  # Try to get close to 2 weeks (14 days)
                # Random start date within the assigned third of the block, with some overlap
                earliest_start = block_third - 10
                latest_start = block_third + (block_days // 3) + 10
                period_start = start_date + timedelta(days=random.randint(max(0, earliest_start), min(block_days-7, latest_start)))
                
                # Random duration (3-7 days)
                duration = random.randint(3, 7)
                period_end = period_start + timedelta(days=duration)
                
                if period_end <= end_date:
                    # Check if this period overlaps with too many other residents of same PGY
                    overlapping = 0
                    for other_resident, other_dates in non_call_requests.items():
                        if other_resident in residents:  # Same PGY level
                            for other_start, other_end in other_dates:
                                other_start_date = datetime.strptime(other_start, "%Y-%m-%d")
                                other_end_date = datetime.strptime(other_end, "%Y-%m-%d")
                                if (period_start <= other_end_date and period_end >= other_start_date):
                                    overlapping += 1
                    
                    # Also check PTO overlap for same resident
                    has_pto_overlap = False
                    if resident in pto_requests:
                        for pto_start, pto_end in pto_requests[resident]:
                            pto_start_date = datetime.strptime(pto_start, "%Y-%m-%d")
                            pto_end_date = datetime.strptime(pto_end, "%Y-%m-%d")
                            if (period_start <= pto_end_date and period_end >= pto_start_date):
                                has_pto_overlap = True
                                break
                    
                    if overlapping < group_size and not has_pto_overlap:  # Allow the request if not too many overlapping
                        non_call_dates.extend([
                            (period_start.strftime("%Y-%m-%d"),
                             period_end.strftime("%Y-%m-%d"))
                        ])
                        total_non_call_days += duration
                
                attempts += 1
            
            if non_call_dates:
                non_call_requests[resident] = non_call_dates
    
    return pto_requests, non_call_requests

def test_heavy_pto():
    print("\n=== Testing Heavy PTO Scenario ===")
    residents_info, start_date, end_date, holidays = create_test_data()
    pto_requests, non_call_requests = generate_pto_requests(residents_info, start_date, end_date)
    
    # Convert PTO requests to DataFrame format
    pto_df_rows = []
    for resident, dates in pto_requests.items():
        for start_date_str, end_date_str in dates:
            pto_df_rows.append({
                "Resident": resident,
                "Start Date": start_date_str,
                "End Date": end_date_str,
                "Type": "PTO"
            })
    
    # Convert non-call requests to DataFrame format
    non_call_df_rows = []
    for resident, dates in non_call_requests.items():
        for start_date_str, end_date_str in dates:
            non_call_df_rows.append({
                "Resident": resident,
                "Start Date": start_date_str,
                "End Date": end_date_str,
                "Type": "Non-Call"
            })
    
    # Combine PTO and non-call requests
    all_requests_df = pd.DataFrame(pto_df_rows + non_call_df_rows)
    
    # Create residents DataFrame with PGY information
    residents_df = pd.DataFrame([
        {"Resident": resident, "PGY": pgy, "Transition Date": None, "Transition PGY": None}
        for pgy, residents in residents_info.items()
        for resident in residents
    ])
    
    try:
        schedule = run_scheduling_engine(
            prev_df=None,
            res_df=residents_df,
            pto_df=all_requests_df,
            hol_df=pd.DataFrame([{"Date": k, "Call": v[0], "Backup": v[1]} 
                               for k, v in holidays.items()]),
            start_date=start_date,
            end_date=end_date,
            pgy4_cap=5
        )
        print("Heavy PTO test passed!")
        return schedule, all_requests_df
    except Exception as e:
        print(f"Heavy PTO test failed: {str(e)}")
        return None, None

def analyze_requests(requests_df):
    if requests_df is None:
        return
    
    print("\n=== Request Analysis ===")
    
    # Total requests by type
    request_counts = requests_df.groupby('Type').size()
    print("\nTotal Requests by Type:")
    print(request_counts)
    
    # Requests per resident
    resident_counts = requests_df.groupby(['Resident', 'Type']).size().unstack(fill_value=0)
    print("\nRequests per Resident:")
    print(resident_counts)
    
    # Total days requested per resident
    requests_df['Start Date'] = pd.to_datetime(requests_df['Start Date'])
    requests_df['End Date'] = pd.to_datetime(requests_df['End Date'])
    requests_df['Duration'] = (requests_df['End Date'] - requests_df['Start Date']).dt.days + 1  # Add 1 to include both start and end dates
    
    days_per_resident = requests_df.groupby(['Resident', 'Type'])['Duration'].sum().unstack(fill_value=0)
    print("\nTotal Days Requested per Resident:")
    print(days_per_resident)
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"Average PTO days per resident: {days_per_resident['PTO'].mean():.1f}")
    print(f"Average Non-Call days per resident: {days_per_resident['Non-Call'].mean():.1f}")
    print(f"Maximum PTO days: {days_per_resident['PTO'].max()}")
    print(f"Maximum Non-Call days: {days_per_resident['Non-Call'].max()}")

def analyze_schedule(schedule):
    if schedule is None:
        return
    
    print("\n=== Schedule Analysis ===")
    
    # Convert schedule to DataFrame if it's not already
    if not isinstance(schedule, pd.DataFrame):
        schedule = pd.DataFrame(schedule)
    
    # Basic statistics
    print(f"Total days scheduled: {len(schedule)}")
    
    # Call distribution
    call_counts = schedule['Call'].value_counts()
    print("\nCall Distribution:")
    print(call_counts)
    print(f"\nAverage calls per resident: {call_counts.mean():.1f}")
    print(f"Max calls for any resident: {call_counts.max()}")
    
    # Backup distribution
    backup_counts = schedule['Backup'].value_counts()
    print("\nBackup Distribution:")
    print(backup_counts)
    print(f"\nAverage backups per resident: {backup_counts.mean():.1f}")
    print(f"Max backups for any resident: {backup_counts.max()}")
    
    # Intern distribution
    if 'Intern' in schedule.columns:
        intern_counts = schedule['Intern'].value_counts()
        print("\nIntern Distribution:")
        print(intern_counts)
        print(f"\nAverage intern assignments per intern: {intern_counts.mean():.1f}")
        print(f"Max intern assignments: {intern_counts.max()}")
    
    # Weekend vs Weekday distribution
    schedule['Date'] = pd.to_datetime(schedule['Date'])
    schedule['DayOfWeek'] = schedule['Date'].dt.dayofweek
    weekend_calls = schedule[schedule['DayOfWeek'].isin([5, 6])]['Call'].value_counts()
    weekday_calls = schedule[~schedule['DayOfWeek'].isin([5, 6])]['Call'].value_counts()
    
    print("\nWeekend Call Distribution:")
    print(weekend_calls)
    print(f"\nAverage weekend calls per resident: {weekend_calls.mean():.1f}")
    print(f"Max weekend calls for any resident: {weekend_calls.max()}")
    
    print("\nWeekday Call Distribution:")
    print(weekday_calls)
    print(f"\nAverage weekday calls per resident: {weekday_calls.mean():.1f}")
    print(f"Max weekday calls for any resident: {weekday_calls.max()}")

def validate_schedule(schedule_df, residents_info, holidays, start_date, end_date):
    """
    Validate the generated schedule for:
    - Spacing violations (call/call, call/backup, backup/call: 4 days; backup/backup: 3 days)
    - Double-booking (no resident in more than one role per day)
    - PGY match (call/backup must match PGY requirements)
    Returns (True, None) if valid, (False, reason) if not.
    """
    schedule_df = pd.DataFrame(schedule_df, columns=["Date", "Call", "Backup", "Intern"])
    schedule_df["Date"] = pd.to_datetime(schedule_df["Date"])
    schedule_df = schedule_df.sort_values("Date")
    # 1. Spacing violations
    assignments = defaultdict(list)  # resident -> list of (date, role)
    for _, row in schedule_df.iterrows():
        current_date = row["Date"]
        for role in ["Call", "Backup"]:
            resident = row[role]
            if pd.isna(resident):
                continue
            assignments[resident].append((current_date, role))
    for resident, events in assignments.items():
        events = sorted(events)
        for i in range(len(events)):
            date1, role1 = events[i]
            for j in range(i+1, len(events)):
                date2, role2 = events[j]
                gap = (date2 - date1).days
                if gap == 0:
                    continue  # same day, will be caught by double-booking
                if role1 == "Backup" and role2 == "Backup":
                    if gap < 3:
                        return False, f"Spacing violation: {resident} assigned as Backup on {date1.date()} and {date2.date()} (<3 days apart)"
                else:
                    if gap < 4:
                        return False, f"Spacing violation: {resident} assigned as {role1} on {date1.date()} and {role2} on {date2.date()} (<4 days apart)"
    # 2. Double-booking
    for _, row in schedule_df.iterrows():
        roles = [row["Call"], row["Backup"], row["Intern"]]
        roles = [r for r in roles if pd.notna(r)]
        if len(set(roles)) < len(roles):
            return False, f"Double-booking: {roles} on {row['Date'].date()}"
    # 3. PGY match (call/backup)
    for _, row in schedule_df.iterrows():
        current_date = row["Date"]
        for role in ["Call", "Backup"]:
            resident = row[role]
            if pd.isna(resident):
                continue
            # Find PGY
            pgy = None
            for lvl, names in residents_info.items():
                if resident in names:
                    pgy = lvl
                    break
            if pgy is None:
                return False, f"Resident {resident} not found in residents_info"
            # Call PGY rules
            dow = current_date.weekday()
            if role == "Call":
                if pgy == 1:
                    return False, f"PGY-1 assigned to call on {current_date.date()}"
                elif pgy == 2 and dow not in [1,2,4,6]:
                    return False, f"PGY-2 assigned to call on invalid day {current_date.date()}"
                elif pgy == 3 and dow not in [0,2,3,5]:
                    return False, f"PGY-3 assigned to call on invalid day {current_date.date()}"
                elif pgy == 4 and dow != 3:
                    return False, f"PGY-4 assigned to call on invalid day {current_date.date()}"
            if role == "Backup":
                if pgy < 2:
                    return False, f"PGY-1 assigned to backup on {current_date.date()}"
    return True, None

def test_minimum_residents():
    """Grid search: Find minimum number of residents needed at each PGY level (all combinations)"""
    print("\n=== Grid Search: Minimum Resident Requirements ===")
    print("Testing with no PTO/non-call requests over 4 months")
    print("Testing period: January 1, 2024 - April 30, 2024")
    print("Time limit: 10 minutes (will stop if exceeded)")
    
    start_time = time.time()
    time_limit = 600  # 10 minutes in seconds
    
    # Test period: January to April 2024
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 4, 30)
    
    # Holidays
    holidays = [
        datetime(2024, 1, 1),   # New Year's Day
        datetime(2024, 1, 15),  # MLK Day
        datetime(2024, 2, 19),  # Presidents Day
    ]
    
    # Resident name templates
    intern_names = [f"Intern{i}" for i in range(1, 7)]
    res2_names = [f"Res2-{i}" for i in range(1, 7)]
    res3_names = [f"Res3-{i}" for i in range(1, 7)]
    res4_names = [f"Res4-{i}" for i in range(1, 5)]
    
    # Ranges for grid search
    pgy1_range = range(1, 7)
    pgy2_range = range(1, 7)
    pgy3_range = range(1, 7)
    pgy4_range = range(1, 5)
    
    print("\nGrid search ranges:")
    print(f"PGY-1: {pgy1_range.start}-{pgy1_range.stop-1}")
    print(f"PGY-2: {pgy2_range.start}-{pgy2_range.stop-1}")
    print(f"PGY-3: {pgy3_range.start}-{pgy3_range.stop-1}")
    print(f"PGY-4: {pgy4_range.start}-{pgy4_range.stop-1}")
    
    successful_configs = []
    failed_configs = []
    total_configs = 0
    
    for p1, p2, p3, p4 in itertools.product(pgy1_range, pgy2_range, pgy3_range, pgy4_range):
        elapsed_time = time.time() - start_time
        if elapsed_time > time_limit:
            print(f"\nTime limit of {time_limit/60} minutes reached!")
            break
        total_configs += 1
        print(f"\n{'='*50}")
        print(f"Testing: PGY-1={p1}, PGY-2={p2}, PGY-3={p3}, PGY-4={p4}  (Config {total_configs})")
        residents = {
            1: intern_names[:p1],
            2: res2_names[:p2],
            3: res3_names[:p3],
            4: res4_names[:p4],
        }
        try:
            pto_requests = pd.DataFrame(columns=['Resident', 'Start Date', 'End Date'])
            scheduler = CallScheduler(
                residents_info=residents,
                fixed_assignments={},
                holidays=holidays,
                pto_requests=pto_requests
            )
            scheduler.schedule_range(start_date, end_date)
            # Validate schedule
            valid, reason = validate_schedule(scheduler.assignments, residents, holidays, start_date, end_date)
            if valid:
                print("SUCCESS!")
                successful_configs.append((p1, p2, p3, p4))
            else:
                print(f"FAILED: {reason}")
                failed_configs.append((p1, p2, p3, p4, reason))
        except Exception as e:
            print(f"FAILED: {str(e)}")
            failed_configs.append((p1, p2, p3, p4, str(e)))
    
    print("\n" + "="*50)
    print(f"Grid search complete. Total configs tested: {total_configs}")
    print(f"Successful configs: {len(successful_configs)}")
    print(f"Failed configs: {len(failed_configs)}")
    
    if successful_configs:
        print("\nLowest working resident counts (by PGY):")
        # Find minimums for each PGY level among successful configs
        min_p1 = min(x[0] for x in successful_configs)
        min_p2 = min(x[1] for x in successful_configs)
        min_p3 = min(x[2] for x in successful_configs)
        min_p4 = min(x[3] for x in successful_configs)
        print(f"PGY-1: {min_p1}")
        print(f"PGY-2: {min_p2}")
        print(f"PGY-3: {min_p3}")
        print(f"PGY-4: {min_p4}")
        print("\nAll successful combinations:")
        for combo in successful_configs:
            print(f"PGY-1={combo[0]}, PGY-2={combo[1]}, PGY-3={combo[2]}, PGY-4={combo[3]}")
    else:
        print("No successful configurations found.")

def test_single_configuration():
    print("\n=== Single Test: PGY-1=6, PGY-2=6, PGY-3=6, PGY-4=4 ===")
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 4, 30)
    holidays = [
        datetime(2024, 1, 1),   # New Year's Day
        datetime(2024, 1, 15),  # MLK Day
        datetime(2024, 2, 19),  # Presidents Day
    ]
    residents = {
        1: [f"Intern{i}" for i in range(1, 7)],
        2: [f"Res2-{i}" for i in range(1, 7)],
        3: [f"Res3-{i}" for i in range(1, 7)],
        4: [f"Res4-{i}" for i in range(1, 5)],
    }
    pto_requests = pd.DataFrame(columns=['Resident', 'Start Date', 'End Date'])
    try:
        scheduler = CallScheduler(
            residents_info=residents,
            fixed_assignments={},
            holidays=holidays,
            pto_requests=pto_requests
        )
        scheduler.schedule_range(start_date, end_date)
        print("Schedule generated. Now validating...")
        valid, reason = validate_schedule(scheduler.assignments, residents, holidays, start_date, end_date)
        if valid:
            print("Validation PASSED: Schedule is valid.")
        else:
            print(f"Validation FAILED: {reason}")
    except Exception as e:
        print(f"Schedule generation failed: {str(e)}")

def main():
    test_minimum_residents()
    # test_single_configuration()

if __name__ == "__main__":
    main() 