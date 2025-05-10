# scheduling_engine.py

# (This file is now updated to match the validated, optimized engine_optimization_testing.py)

import pandas as pd
import random
from datetime import datetime as dt_type, date as date_type, timedelta
import re
import argparse

# --- CallScheduler CLASS ---

class CallScheduler:
    def __init__(self, residents_info, fixed_assignments, holidays, pto_requests=None, transitions=None, pgy4_cap=None, previous_call_counts=None):
        self.residents_info = residents_info
        self.fixed_assignments = fixed_assignments
        self.holidays = holidays
        if isinstance(pto_requests, pd.DataFrame):
            if not pto_requests.empty:
                pto_dict = {}
                for _, row in pto_requests.iterrows():
                    resident = row['Resident']
                    if isinstance(row['Start Date'], (dt_type, date_type)):
                        start = row['Start Date']
                    else:
                        start = dt_type.strptime(str(row['Start Date']), "%Y-%m-%d")
                    if isinstance(row['End Date'], (dt_type, date_type)):
                        end = row['End Date']
                    else:
                        end = dt_type.strptime(str(row['End Date']), "%Y-%m-%d")
                    current = start
                    while current <= end:
                        pto_dict.setdefault(resident, []).append(current.strftime('%Y-%m-%d'))
                        current += timedelta(days=1)
                self.pto_requests = pto_dict
            else:
                self.pto_requests = {}
        elif pto_requests:
            self.pto_requests = pto_requests
        else:
            self.pto_requests = {}
        self.transitions = transitions if transitions else {}
        self.pgy4_cap = pgy4_cap
        
        self.call_log = {}
        self.backup_log = {}
        self.intern_log = {}
        self.assignments = []
        self.assignment_history = []  # Track all assignments for backtracking
        self.tried_combinations = set()  # Track tried combinations to avoid cycles

        # Initialize call counts with previous values if provided
        self.call_counts = {}
        for resident in self.get_all_residents():
            if previous_call_counts and resident in previous_call_counts:
                prev_counts = previous_call_counts[resident]
                self.call_counts[resident] = {
                    "weekday": prev_counts.get("Weekday", 0),
                    "friday": prev_counts.get("Fridays", 0),
                    "sunday": prev_counts.get("Sunday", 0),
                    "saturday": prev_counts.get("Saturday", 0),
                    "total": prev_counts.get("Total", 0),
                    "intern_weekday": 0,
                    "intern_saturday": 0
                }
            else:
                self.call_counts[resident] = {
                    "weekday": 0,
                    "friday": 0,
                    "sunday": 0,
                    "saturday": 0,
                    "total": 0,
                    "intern_weekday": 0,
                    "intern_saturday": 0
                }

        for date_str, (call, backup) in self.fixed_assignments.items():
            date_obj = dt_type.strptime(date_str, "%Y-%m-%d")
            self.call_log.setdefault(call, []).append(date_obj)
            self.backup_log.setdefault(backup, []).append(date_obj)

    def get_all_residents(self):
        return sum(self.residents_info.values(), [])

    def get_resident_pgy(self, resident, current_date):
        if resident in self.transitions:
            transition_date, new_pgy = self.transitions[resident]
            d1 = current_date
            d2 = transition_date
            if isinstance(d1, date_type) and not isinstance(d1, dt_type):
                d1 = dt_type.combine(d1, dt_type.min.time())
            if isinstance(d2, date_type) and not isinstance(d2, dt_type):
                d2 = dt_type.combine(d2, dt_type.min.time())
            if d1 > d2:  # Only return new PGY if date is strictly after transition date
                return new_pgy
        for pgy, residents in self.residents_info.items():
            if resident in residents:
                return pgy
        return None

    def is_pgy_match(self, resident, current_date, role="call"):
        date_str = current_date.strftime("%Y-%m-%d")
        if date_str in self.fixed_assignments:
            # For holidays, only the assigned residents are eligible
            call_fixed, backup_fixed = self.fixed_assignments[date_str]
            if role == "call":
                return resident == call_fixed
            else:  # backup
                return resident == backup_fixed
        
        pgy = self.get_resident_pgy(resident, current_date)
        if pgy is None:
            return False

        # For backup role, allow any PGY-2 or higher
        if role == "backup":
            return pgy >= 2

        # For call role, use specific day requirements
        dow = current_date.weekday()
        if pgy == 1:  # PGY-1 interns
            # Interns are not eligible for primary call
            return False
        elif pgy == 2:
            if dow in [1, 2, 4, 6]:  # Tuesday, Wednesday, Friday, Sunday
                return True
        elif pgy == 3:
            if dow in [0, 2, 3, 5]:  # Monday, Wednesday, Thursday, Saturday
                return True
        elif pgy == 4:
            if dow == 3:  # Thursday
                return True
        return False

    def spacing_okay(self, resident, current_date, role):
        # For call: 4 days from all previous call and backup assignments
        if role == "call":
            for assigned_date in self.call_log.get(resident, []):
                if abs((current_date - assigned_date).days) < 4:
                    return False
            for assigned_date in self.backup_log.get(resident, []):
                if abs((current_date - assigned_date).days) < 4:
                    return False
        # For backup: 4 days from all previous call assignments, 3 days from all previous backup assignments
        elif role == "backup":
            for assigned_date in self.call_log.get(resident, []):
                if abs((current_date - assigned_date).days) < 4:
                    return False
            for assigned_date in self.backup_log.get(resident, []):
                if abs((current_date - assigned_date).days) < 3:
                    return False
        return True

    def pto_okay(self, resident, current_date):
        return current_date.strftime("%Y-%m-%d") not in self.pto_requests.get(resident, [])

    def fairness_score(self, resident, dow):
        counts = self.call_counts[resident]
        score = counts["total"]
        if dow in [0,1,2,3]:
            score += counts["weekday"] * 2
        elif dow == 4:
            score += counts["friday"] * 3
        elif dow == 5:
            score += counts["saturday"] * 2
        elif dow == 6:
            score += counts["sunday"] * 3
        return score

    def eligible_residents(self, current_date, role):
        candidates = []
        date_str = current_date.strftime("%Y-%m-%d")
        for r in self.get_all_residents():
            if not self.is_pgy_match(r, current_date, role):
                continue
            if not self.spacing_okay(r, current_date, role):
                continue
            if not self.pto_okay(r, current_date):
                continue
            # Enforce PGY-4 cap for call role
            if role == "call" and self.pgy4_cap is not None:
                pgy = self.get_resident_pgy(r, current_date)
                if pgy == 4 and self.call_counts[r]["total"] >= self.pgy4_cap:
                    continue
            candidates.append(r)
        return candidates

    def is_intern_eligible(self, intern, current_date, call_resident):
        date_str = current_date.strftime("%Y-%m-%d")
        if date_str in self.fixed_assignments:
            return False
        call_pgy = self.get_resident_pgy(call_resident, current_date)
        if call_pgy not in [3, 4]:
            return False
        if not self.pto_okay(intern, current_date):
            return False
        return True

    def undo_assignment(self, date_str):
        assignment = None
        for i, (d, c, b, i) in enumerate(self.assignments):
            if d == date_str:
                assignment = (d, c, b, i)
                self.assignments.pop(i)
                break
        if assignment:
            date_str, call, backup, intern = assignment
            date_obj = dt_type.strptime(date_str, "%Y-%m-%d")
            if call in self.call_log:
                self.call_log[call].remove(date_obj)
            if backup in self.backup_log:
                self.backup_log[backup].remove(date_obj)
            if intern and intern in self.intern_log:
                self.intern_log[intern].remove(date_obj)
            dow = date_obj.weekday()
            self.call_counts[call]["total"] -= 1
            if dow in [0,1,2,3]:
                self.call_counts[call]["weekday"] -= 1
            elif dow == 4:
                self.call_counts[call]["friday"] -= 1
            elif dow == 5:
                self.call_counts[call]["saturday"] -= 1
            elif dow == 6:
                self.call_counts[call]["sunday"] -= 1
            if intern:
                if dow == 5:
                    self.call_counts[intern]["intern_saturday"] -= 1
                else:
                    self.call_counts[intern]["intern_weekday"] -= 1

    def get_combination_key(self, date_str, call, backup, intern):
        return f"{date_str}:{call}:{backup}:{intern}"

    def assign_day(self, current_date, backtrack=False):
        date_str = current_date.strftime("%Y-%m-%d")
        dow = current_date.weekday()
        if date_str in self.fixed_assignments:
            call_fixed, backup_fixed = self.fixed_assignments[date_str]
            self.assignments.append((date_str, call_fixed, backup_fixed, None))
            self.update_counters(call_fixed, backup_fixed, dow)
            self.call_log.setdefault(call_fixed, []).append(current_date)
            self.backup_log.setdefault(backup_fixed, []).append(current_date)
            return True
        call_candidates = self.eligible_residents(current_date, "call")
        if not call_candidates:
            return False
        call_candidates.sort(key=lambda r: self.fairness_score(r, dow))
        for call_resident in call_candidates:
            call_pgy = self.get_resident_pgy(call_resident, current_date)
            backup_candidates = []
            for r in self.get_all_residents():
                if r == call_resident:
                    continue
                if not self.spacing_okay(r, current_date, "backup"):
                    continue
                if not self.pto_okay(r, current_date):
                    continue
                backup_pgy = self.get_resident_pgy(r, current_date)
                if backup_pgy == call_pgy:
                    backup_candidates.append(r)
            if not backup_candidates:
                continue
            backup_candidates.sort(key=lambda r: self.fairness_score(r, dow))
            for backup_resident in backup_candidates:
                intern_assigned = None
                if call_pgy in [3, 4]:
                    intern_candidates = self.residents_info.get(1, [])
                    if intern_candidates:
                        eligible_interns = [r for r in intern_candidates if self.is_intern_eligible(r, current_date, call_resident)]
                        if eligible_interns:
                            if dow == 5:
                                intern_assigned = min(eligible_interns, 
                                    key=lambda r: (
                                        self.call_counts[r]["intern_saturday"],
                                        self.call_counts[r]["intern_weekday"]
                                    )
                                )
                            else:
                                intern_assigned = min(eligible_interns, 
                                    key=lambda r: (
                                        self.call_counts[r]["intern_weekday"],
                                        self.call_counts[r]["intern_saturday"]
                                    )
                                )
                combination_key = self.get_combination_key(date_str, call_resident, backup_resident, intern_assigned)
                if combination_key in self.tried_combinations:
                    continue
                self.tried_combinations.add(combination_key)
                self.assignments.append((date_str, call_resident, backup_resident, intern_assigned))
                self.update_counters(call_resident, backup_resident, dow)
                self.call_log.setdefault(call_resident, []).append(current_date)
                self.backup_log.setdefault(backup_resident, []).append(current_date)
                if intern_assigned:
                    if dow == 5:
                        self.call_counts[intern_assigned]["intern_saturday"] += 1
                    else:
                        self.call_counts[intern_assigned]["intern_weekday"] += 1
                return True
        return False

    def update_counters(self, call, backup, dow):
        self.call_counts[call]["total"] += 1
        if dow == 4:
            self.call_counts[call]["friday"] += 1
        elif dow == 5:
            self.call_counts[call]["saturday"] += 1
        elif dow == 6:
            self.call_counts[call]["sunday"] += 1
        else:
            self.call_counts[call]["weekday"] += 1

    def schedule_range(self, start_date, end_date):
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            if not self.assign_day(current_date):
                backtrack_date = current_date - timedelta(days=1)
                while backtrack_date >= start_date:
                    self.undo_assignment(backtrack_date.strftime("%Y-%m-%d"))
                    if self.assign_day(backtrack_date, backtrack=True):
                        break
                    backtrack_date -= timedelta(days=1)
                if backtrack_date < start_date:
                    raise Exception(f"No valid schedule possible starting from {date_str}")
            current_date += timedelta(days=1)

    def export_schedule(self):
        df = pd.DataFrame(self.assignments, columns=["Date", "Call", "Backup", "Intern"])
        return df

# --- Wrapper Function to Connect to App ---

def run_scheduling_engine(prev_df, res_df, pto_df, hol_df, start_date=None, end_date=None, pgy4_cap=None, previous_call_counts=None):
    residents_info = {1: [], 2: [], 3: [], 4: []}  # Added PGY-1
    transitions = {}

    # Default dates for Block 1
    if start_date is None:
        start_date = dt_type(2025, 7, 1)
    if end_date is None:
        end_date = dt_type(2025, 10, 31)

    print("\nProcessing residents:")
    for _, row in res_df.iterrows():
        name = row["Resident"]
        pgy = int(row["PGY"])
        residents_info[pgy].append(name)
        print(f"Added {name} as PGY-{pgy}")
        
        # Handle transitions
        if pd.notna(row["Transition Date"]):
            if isinstance(row["Transition Date"], (dt_type, date_type)):
                trans_date = row["Transition Date"]
            else:
                trans_date = dt_type.strptime(str(row["Transition Date"]), "%Y-%m-%d")
            transitions[name] = (trans_date, int(row["Transition PGY"]))
            print(f"  Transition: {name} will become PGY-{row['Transition PGY']} on {row['Transition Date']}")

    print("\nResidents by PGY level:")
    for pgy, residents in residents_info.items():
        print(f"PGY-{pgy}: {residents}")

    # Process PTO requests
    pto_requests = {}
    if not pto_df.empty:
        for _, row in pto_df.iterrows():
            resident = row["Resident"]
            if isinstance(row["Start Date"], (dt_type, date_type)):
                start = row["Start Date"]
            else:
                start = dt_type.strptime(str(row["Start Date"]), "%Y-%m-%d")
            if isinstance(row["End Date"], (dt_type, date_type)):
                end = row["End Date"]
            else:
                end = dt_type.strptime(str(row["End Date"]), "%Y-%m-%d")
            current = start
            while current <= end:
                pto_requests.setdefault(resident, []).append(current.strftime("%Y-%m-%d"))
                current += timedelta(days=1)

    # Process holiday assignments
    fixed_assignments = {}
    if not hol_df.empty:
        print("\nProcessing holiday assignments:")
        for _, row in hol_df.iterrows():
            date_val = row["Date"]
            if isinstance(date_val, (dt_type, date_type)):
                date_str = date_val.strftime("%Y-%m-%d")
            else:
                date_str = str(date_val)
            call = row["Call"]
            backup = row["Backup"]
            fixed_assignments[date_str] = (call, backup)
            print(f"Added holiday assignment for {date_str}: Call={call}, Backup={backup}")

    # Process previous block assignments
    if prev_df is not None and not prev_df.empty:
        for _, row in prev_df.iterrows():
            date_val = row["Date"]
            if isinstance(date_val, (dt_type, date_type)):
                date_str = date_val.strftime("%Y-%m-%d")
            else:
                date_str = str(date_val)
            call = row["Call"]
            backup = row["Backup"]
            fixed_assignments[date_str] = (call, backup)

    # Create scheduler instance with previous call counts if provided
    scheduler = CallScheduler(
        residents_info, 
        fixed_assignments, 
        hol_df, 
        pto_requests, 
        transitions, 
        pgy4_cap=pgy4_cap,
        previous_call_counts=previous_call_counts
    )
    
    # Generate schedule
    scheduler.schedule_range(start_date, end_date)
    
    # Export schedule and add supervisor assignment
    df = scheduler.export_schedule()
    df["Supervisor"] = None

    # Build a lookup for call assignments by date
    call_by_date = {row["Date"]: row["Call"] for _, row in df.iterrows()}
    pgy_by_name = {}
    for _, row in res_df.iterrows():
        name = row["Resident"]
        pgy = int(row["PGY"])
        pgy_by_name[name] = pgy
        if pd.notna(row["Transition Date"]):
            if isinstance(row["Transition Date"], (dt_type, date_type)):
                trans_date = row["Transition Date"]
            else:
                trans_date = dt_type.strptime(str(row["Transition Date"]), "%Y-%m-%d")
            pgy_by_name[(name, trans_date.strftime("%Y-%m-%d"))] = int(row["Transition PGY"])

    # Helper to get PGY for a resident on a given date
    def get_pgy(resident, date):
        # Check for transition
        for _, row in res_df.iterrows():
            if row["Resident"] == resident and pd.notna(row["Transition Date"]):
                if isinstance(row["Transition Date"], (dt_type, date_type)):
                    trans_date = row["Transition Date"]
                else:
                    trans_date = dt_type.strptime(str(row["Transition Date"]), "%Y-%m-%d")
                d1 = date
                d2 = trans_date
                if isinstance(d1, date_type) and not isinstance(d1, dt_type):
                    d1 = dt_type.combine(d1, dt_type.min.time())
                if isinstance(d2, date_type) and not isinstance(d2, dt_type):
                    d2 = dt_type.combine(d2, dt_type.min.time())
                if d1 > d2:
                    return int(row["Transition PGY"])
        return pgy_by_name.get(resident, None)

    # Supervisor assignment tracking
    supervisor_counts = {r: 0 for r in scheduler.get_all_residents() if get_pgy(r, start_date) in [3, 4]}
    last_call_by_resident = {}

    for idx, row in df.iterrows():
        current_date = dt_type.strptime(row["Date"], "%Y-%m-%d")
        call_resident = row["Call"]
        # Skip holidays (already assigned in fixed_assignments)
        if row["Date"] in fixed_assignments:
            continue
        # Skip supervisor assignment if call resident is on a Sunday
        if current_date.weekday() == 6:
            continue
        # Only assign supervisor if call resident is PGY-2 on this date
        if get_pgy(call_resident, current_date) != 2:
            continue
        # Build eligible supervisor list
        eligible_supervisors = []
        for r in supervisor_counts:
            # Not on call the previous day
            prev_date = (current_date - timedelta(days=1)).strftime("%Y-%m-%d")
            if call_by_date.get(prev_date) == r:
                continue
            # Must be PGY-3 or PGY-4 on this date
            if get_pgy(r, current_date) not in [3, 4]:
                continue
            # Check PTO
            if current_date.strftime("%Y-%m-%d") in pto_requests.get(r, []):
                continue
            eligible_supervisors.append(r)
        # Friday rule: if Friday, try to assign Saturday call resident as supervisor
        if current_date.weekday() == 4:  # Friday
            sat_date = (current_date + timedelta(days=1)).strftime("%Y-%m-%d")
            sat_call = call_by_date.get(sat_date)
            if sat_call in eligible_supervisors:
                df.at[idx, "Supervisor"] = sat_call
                supervisor_counts[sat_call] += 1
                continue
        # Otherwise, pick eligible supervisor with fewest assignments
        if eligible_supervisors:
            chosen = min(eligible_supervisors, key=lambda r: supervisor_counts[r])
            df.at[idx, "Supervisor"] = chosen
            supervisor_counts[chosen] += 1
        else:
            # If no one is eligible, leave blank (or could relax rule/log warning)
            df.at[idx, "Supervisor"] = None

    # Check if the current day is Wednesday (0 = Monday, 2 = Wednesday)
    if current_date.weekday() == 2:
        # Adjust call count for PGY-3s on Wednesdays
        for r in df["Call"]:
            pgy = get_pgy(r, current_date)
            if pgy == 3:
                # Add a small penalty (0.5) to the call count for PGY-3s on Wednesdays
                self.call_counts[r]["total"] += 0.5

    # Check if the current day is Thursday (0 = Monday, 3 = Thursday)
    if current_date.weekday() == 3:
        # Adjust call count for PGY-3s on Thursdays
        for r in df["Call"]:
            pgy = get_pgy(r, current_date)
            if pgy == 3:
                # Add a small penalty (0.5) to the call count for PGY-3s on Thursdays
                self.call_counts[r]["total"] += 0.75

    return df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate call schedule')
    parser.add_argument('--start_date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--previous_schedule', type=str, help='Path to previous block schedule CSV')
    parser.add_argument('--output_file', type=str, help='Path to save the generated schedule')
    args = parser.parse_args()

    # Read input files
    res_df = pd.read_csv('resident_list_structured.csv')
    pto_df = pd.read_csv('pto_requests.csv')
    hol_df = pd.read_csv('holiday_schedule.csv')
    
    # Read previous schedule if provided
    prev_df = None
    if args.previous_schedule:
        prev_df = pd.read_csv(args.previous_schedule)

    # Convert dates
    start_date = dt_type.strptime(args.start_date, "%Y-%m-%d")
    end_date = dt_type.strptime(args.end_date, "%Y-%m-%d")

    # Generate schedule
    schedule_df = run_scheduling_engine(prev_df, res_df, pto_df, hol_df, start_date, end_date)
    
    # Save the schedule
    output_file = args.output_file if args.output_file else 'generated_schedule.csv'
    schedule_df.to_csv(output_file, index=False)
    print(f"Schedule generated from {args.start_date} to {args.end_date}")
    if args.previous_schedule:
        print(f"Using previous schedule from: {args.previous_schedule}")
    print(f"Schedule saved to: {output_file}")
