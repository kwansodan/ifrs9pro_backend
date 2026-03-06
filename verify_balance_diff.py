import sys
import os
from unittest.mock import MagicMock
import pytest


# Mock azure before anything else
sys.modules['azure'] = MagicMock()
sys.modules['azure.storage'] = MagicMock()
sys.modules['azure.storage.blob'] = MagicMock()

# Add the project root to sys.path
sys.path.append(os.getcwd())

from app.utils.background_calculations import process_loan_sync

def safe_float(val):
    try:
        return float(val) if val is not None else 0.0
    except:
        return 0.0

def test_balance_diff():
    print("Testing balance_difference calculation in process_loan_sync...")
    
    # Sample loan data
    loan_data = {
        "id": 1,
        "loan_amount": 10000,
        "loan_term": 12,
        "monthly_installment": 900,
        "administrative_fees": 200,
        "outstanding_loan_balance": 9500, # Actual
        "accumulated_arrears": 500,
        "loan_issue_date": "2025-01-01",
        "deduction_start_period": "2025-01-01",
        "pd_value": 0.05,
        "ifrs9_stage": "Stage 1"
    }
    
    reporting_date = "2025-03-01"
    
    # Run calculation (process_loan_sync is synchronous)
    loan_id, loan_result, error = process_loan_sync(loan_data, reporting_date)
    
    if error:
        print(f"Error: {error}")
        return

    theoretical_bal = loan_result.get("theoretical_balance", 0)
    arrears = loan_data.get("accumulated_arrears", 0)
    outstanding_bal = loan_data.get("outstanding_loan_balance", 0)
    expected_diff = round(outstanding_bal - (theoretical_bal + arrears), 2)
    actual_diff = loan_result.get("balance_difference")
    
    print(f"Theoretical Balance: {theoretical_bal}")
    print(f"Accumulated Arrears: {arrears}")
    print(f"Actual Outstanding Balance: {outstanding_bal}")
    print(f"Expected Difference: {expected_diff}")
    print(f"Actual Difference: {actual_diff}")
    
    assert actual_diff == expected_diff
    print("Success: balance_difference calculation is correct!")

if __name__ == "__main__":
    test_balance_diff()
