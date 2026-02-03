import pandas as pd
import re
from dateutil import parser
import os

FILE_PATH = "loan_data_70k.xlsx"

def parse_date_safe(date_str):
    """Reflects the exact logic in sync_processors.py"""
    if str(date_str).lower() in ['nan', 'none', 'nat', '']:
        return None
        
    try:
        # Fast path for ISO format
        if isinstance(date_str, str) and "-" in date_str and len(date_str) == 10:
            return date_str
            
        s = str(date_str).strip()
        if not s: return None
            
        # Handle SEP2020 format - insert space
        if re.match(r"^[A-Za-z]{3}\d{4}$", s):
            s = s[:3] + " " + s[3:]
            
        dt = parser.parse(s)
        return dt.strftime("%Y-%m-%d")
    except:
        return None

def verify_data():
    print(f"Loading {FILE_PATH}...")
    try:
        df = pd.read_excel(FILE_PATH)
    except Exception as e:
        print(f"CRITICAL: Failed to read Excel file: {e}")
        return

    print("Normalizing columns...")
    original_cols = df.columns.tolist()
    df.columns = [c.strip().lower().replace(".", "").replace(" ", "_") for c in df.columns]
    
    print(f"Columns found: {df.columns.tolist()}")

    # Define expectations
    target_columns = ["loan_no", "employee_id", "loan_amount", "loan_term", "monthly_installment", 
                      "accumulated_arrears", "outstanding_loan_balance", "loan_issue_date", 
                      "deduction_start_period", "submission_period", "maturity_period"]
    
    date_cols = ["deduction_start_period", "loan_issue_date", "submission_period", "maturity_period"]
    int_cols = ["loan_term"]
    numeric_cols = ["loan_amount", "monthly_installment", "accumulated_arrears", "outstanding_loan_balance"]
    str_cols = ["loan_no", "employee_id"] # Identifiers
    
    issues = []

    # 1. Check for missing critical columns
    missing_cols = [c for c in target_columns if c not in df.columns]
    if missing_cols:
        issues.append(f"MISSING COLUMNS: The following columns are required but missing: {missing_cols}")
        print(f"WARNING: The following columns are missing and will be NULL: {missing_cols}")

    # 2. Simulate Data Transformation
    print("\n--- Simulating Transformations & Type Checks ---")
    
    # Check String Identifiers
    for col in str_cols:
        if col in df.columns:
            print(f"Checking identifier column {col}...")
            # Check for float-like strings in IDs (e.g. "123.0")
            float_ids = []
            for idx, val in df[col].items():
                s_val = str(val)
                if s_val.endswith(".0"):
                     float_ids.append((idx, val))
            
            if float_ids:
                print(f"NOTE: Column '{col}' contains {len(float_ids)} float-looking IDs (e.g., '{float_ids[0][1]}'). These will be ingested as strings (e.g., '12345.0'). If this is unintended, clean the Excel file.")
            else:
                 print(f"OK: Column '{col}' looks clean.")

    # Check Numeric Columns
    for col in numeric_cols:
        if col in df.columns:
             print(f"Checking numeric column {col}...")
             bad_entries = []
             for idx, val in df[col].items():
                 try:
                     # Simulate stripping regex
                     clean_val = str(val).replace(",", "") # simplified
                     float(clean_val)
                 except:
                     if str(val).lower() not in ['nan', 'none', '']:
                        bad_entries.append((idx, val))
             
             if bad_entries:
                 issues.append(f"Column '{col}' has {len(bad_entries)} non-numeric entries. Examples: {bad_entries[:3]}")
             else:
                 print(f"OK: Column '{col}' is valid numeric.")

    # Check Integer Columns
    for col in int_cols:
        if col in df.columns:
            print(f"Checking integer cast for {col}...")
            # Simulate the robust cast: replace non-digits, then float, then int
            try:
                # We do this row by row to find specific bad entries for the user
                failed_indices = []
                for idx, val in df[col].items():
                    try:
                        clean_val = str(val).replace(".0", "") # Simple simulation of the cleaning
                        int(float(clean_val))
                    except:
                        failed_indices.append((idx, val))
                
                if failed_indices:
                    issues.append(f"Column '{col}' has {len(failed_indices)} invalid integer entries. Examples: {failed_indices[:3]}")
                else:
                    print(f"OK: Column '{col}' can be cast to Integer.")
            except Exception as e:
                issues.append(f"Failed to check integer column {col}: {e}")

    for col in date_cols:
        if col in df.columns:
            print(f"Checking date parsing for {col}...")
            failed_indices = []
            parsed_count = 0
            for idx, val in df[col].items():
                parsed = parse_date_safe(val)
                if parsed is None and not (str(val).lower() in ['nan', 'none', 'nat', '']):
                     # If original wasn't empty but result is None, it failed or was unrecognizable
                     # But our parse_safe returns None on failure too.
                     # Let's strictly check if it returns valid string
                     pass
                
                # Double check specific failures
                if parsed:
                    parsed_count += 1
                elif str(val).lower() not in ['nan', 'none', 'nat', '']:
                     failed_indices.append((idx, val))

            if failed_indices:
                issues.append(f"Column '{col}' has {len(failed_indices)} unparseable dates. Examples: {failed_indices[:3]}")
                print(f"WARNING: {len(failed_indices)} rows failed in '{col}'.")
            else:
                print(f"OK: Column '{col}' - {parsed_count} valid dates found.")

    print("\n--- Verification Summary ---")
    if issues:
        print("ISSUES FOUND:")
        for i in issues:
            print(f"- {i}")
        print("\nFix these issues in the Excel file or ensure the code updates (which handle these) are applied.")
    else:
        print("SUCCESS: Data appears compatible with the UPDATED ingestion logic.")

if __name__ == "__main__":
    verify_data()
