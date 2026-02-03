
from app.database import SessionLocal
from app.models import Loan
from sqlalchemy import func
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def scan_portfolio_1():
    db = SessionLocal()
    try:
        count = db.query(Loan).filter(Loan.portfolio_id == 1).count()
        print(f"Loans in Portfolio 1: {count}")

        print("\n--- Searching for missing dates in Portfolio 1 ---")
        problem_loans = db.query(Loan).filter(
            Loan.portfolio_id == 1,
            Loan.deduction_start_period == None,
            Loan.loan_issue_date == None
        ).limit(10).all()
        
        if problem_loans:
            print(f"Found {len(problem_loans)} problematic loans in Portfolio 1:")
            for loan in problem_loans:
                 print(f"ID: {loan.id}, Issue: {loan.loan_issue_date}, Start: {loan.deduction_start_period}, Created: {loan.created_at}")
        else:
             print("No problematic loans found in Portfolio 1.")
             
             # Show a few normal ones
             print("First 5 loans in Portfolio 1:")
             loans = db.query(Loan).filter(Loan.portfolio_id == 1).limit(5).all()
             for loan in loans:
                 print(f"ID: {loan.id}, Issue: {loan.loan_issue_date}, Start: {loan.deduction_start_period}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    scan_portfolio_1()
