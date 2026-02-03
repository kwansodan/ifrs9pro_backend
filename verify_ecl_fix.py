import sys
import os
import pandas as pd
from datetime import date
import logging

# Add app to path
sys.path.append(os.getcwd())

# Mock logger to avoid clutter
logging.basicConfig(level=logging.INFO)

try:
    from app.utils.background_calculations import process_loan_sync
    print("Successfully imported process_loan_sync")
except ImportError as e:
    print(f"Failed to import: {e}")
    sys.exit(1)

def test_process_loan_sync():
    print("Testing process_loan_sync...")
    
    # Minimal loan data that arguably shouldn't crash now
    loan_data = {
        "id": 123,
        "loan_amount": 10000,
        "loan_term": 12,
        "accumulated_arrears": 0,
        # "deduction_start_period": MISSING to test fallback/fix
        "loan_issue_date": "2023-01-01", 
        "monthly_installment": 1000,
        "administrative_fees": 100,
        "ifrs9_stage": "Stage 1",
        "pd_value": 5.0,
        "submission_period": "2023-01-01",
        "maturity_period": "2024-01-01",
    }
    
    selected_dt_str = "2023-06-01"
    
    try:
        # This used to crash with UnboundLocalError: local variable 'start_date' referenced before assignment
        lid, result, error = process_loan_sync(loan_data, selected_dt_str)
        
        if error:
            print(f"Got expected error (or handled error): {error}")
        else:
            print("Success! Result calculated:")
            print(f"Final ECL: {result.get('final_ecl')}")
            print(f"EIR: {result.get('eir')}")
            
    except Exception as e:
        print(f"CRASHED: {e}")
        # traceback
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_process_loan_sync()
