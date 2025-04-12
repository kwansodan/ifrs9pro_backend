from collections import defaultdict
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from app.models import Client, Loan, QualityIssue, Portfolio


def find_duplicate_customer_ids(db: Session, portfolio_id: int) -> List[Dict]:
    """
    Find clients with duplicate employee IDs in the portfolio.
    Returns a list of groups of clients with the same employee ID.
    """
    # Get all clients in the portfolio
    clients = db.query(Client).filter(Client.portfolio_id == portfolio_id).all()

    # Group clients by employee_id
    employee_id_groups = defaultdict(list)
    for client in clients:
        if client.employee_id:  # Skip empty employee IDs
            employee_id_groups[client.employee_id].append(
                {
                    "id": client.id,
                    "employee_id": client.employee_id,
                    "name": f"{client.last_name} {client.other_names}",
                    "phone_number": client.phone_number,
                }
            )

    duplicates = [group for employee_id, group in employee_id_groups.items() if len(group) > 1]

    return duplicates


def find_duplicate_addresses(db: Session, portfolio_id: int) -> List[Dict]:
    """
    Find clients with duplicate addresses in the portfolio.
    Returns a list of groups of clients with the same address.
    """
    # Get all clients in the portfolio
    clients = db.query(Client).filter(Client.portfolio_id == portfolio_id).all()

    # Group clients by address (case-insensitive)
    address_groups = defaultdict(list)
    for client in clients:
        # Create a clean address for comparison
        address = (client.residential_address or "").lower().strip()
        if address:  # Skip empty addresses
            address_groups[address].append(
                {
                    "id": client.id,
                    "employee_id": client.employee_id,
                    "name": f"{client.last_name} {client.other_names}",
                    "address": client.residential_address,
                }
            )

    # Filter only groups with more than one client
    duplicates = [group for address, group in address_groups.items() if len(group) > 1]

    return duplicates


def find_duplicate_dob(db: Session, portfolio_id: int) -> List[Dict]:
    """
    Find clients with duplicate dates of birth in the portfolio.
    Returns a list of groups of clients with the same date of birth.
    """
    # Get all clients in the portfolio
    clients = db.query(Client).filter(Client.portfolio_id == portfolio_id).all()

    # Group clients by date of birth
    dob_groups = defaultdict(list)
    for client in clients:
        dob = client.date_of_birth
        
        if dob:  # Skip clients with no DOB
            dob_key = dob.isoformat()
            dob_groups[dob_key].append(
                {
                    "id": client.id,
                    "employee_id": client.employee_id,
                    "name": f"{client.last_name} {client.other_names}",
                    "date_of_birth": dob.isoformat(),
                }
            )

    # Filter only groups with more than one client
    duplicates = [group for dob, group in dob_groups.items() if len(group) > 1]

    return duplicates


def find_duplicate_loan_ids(db: Session, portfolio_id: int) -> List[Dict]:
    """
    Find loans with duplicate loan numbers in the portfolio.
    Returns a list of groups of loans with the same loan_no.
    """
    # Get all loans in the portfolio
    loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()

    # Group loans by loan_no
    loan_no_groups = defaultdict(list)
    for loan in loans:
        if loan.loan_no:  # Skip empty loan numbers
            loan_no_groups[loan.loan_no].append(
                {
                    "id": loan.id,
                    "loan_no": loan.loan_no,
                    "employee_id": loan.employee_id,
                    "loan_amount": float(loan.loan_amount) if loan.loan_amount else None,
                    "loan_issue_date": loan.loan_issue_date.isoformat() if loan.loan_issue_date else None,
                }
            )

    duplicates = [group for loan_no, group in loan_no_groups.items() if len(group) > 1]

    return duplicates


def find_unmatched_employee_ids(db: Session, portfolio_id: int) -> List[Dict]:
    """
    Find customers who cannot be matched to employee IDs in the loan details.
    """
    # Get all clients in the portfolio
    clients = db.query(Client).filter(Client.portfolio_id == portfolio_id).all()
    
    # Get all employee IDs from loans in the portfolio
    loan_employee_ids = set(
        row[0] for row in 
        db.query(Loan.employee_id)
        .filter(Loan.portfolio_id == portfolio_id)
        .distinct()
        .all()
        if row[0]  # Skip None values
    )
    
    # Find clients whose employee_id does not exist in loans
    unmatched_clients = []
    for client in clients:
        if client.employee_id and client.employee_id not in loan_employee_ids:
            unmatched_clients.append({
                "id": client.id,
                "employee_id": client.employee_id,
                "name": f"{client.last_name} {client.other_names}",
                "phone_number": client.phone_number,
            })
    
    return unmatched_clients


def find_loan_customer_mismatches(db: Session, portfolio_id: int) -> List[Dict]:
    """
    Find loan details that don't match customer data.
    """
    # Get all loans in the portfolio
    loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
    
    # Get all employee IDs from clients in the portfolio
    client_employee_ids = set(
        row[0] for row in 
        db.query(Client.employee_id)
        .filter(Client.portfolio_id == portfolio_id)
        .distinct()
        .all()
        if row[0]  # Skip None values
    )
    
    # Find loans whose employee_id does not exist in clients
    unmatched_loans = []
    for loan in loans:
        if loan.employee_id and loan.employee_id not in client_employee_ids:
            unmatched_loans.append({
                "id": loan.id,
                "loan_no": loan.loan_no,
                "employee_id": loan.employee_id,
                "loan_amount": float(loan.loan_amount) if loan.loan_amount else None,
            })
    
    return unmatched_loans


def find_missing_dob(db: Session, portfolio_id: int) -> List[Dict]:
    """
    Find customers with missing date of birth.
    """
    # Get all clients in the portfolio with missing DOB
    clients_with_missing_dob = (
        db.query(Client)
        .filter(Client.portfolio_id == portfolio_id, Client.date_of_birth.is_(None))
        .all()
    )
    
    missing_dob_clients = []
    for client in clients_with_missing_dob:
        missing_dob_clients.append({
            "id": client.id,
            "employee_id": client.employee_id,
            "name": f"{client.last_name} {client.other_names}",
            "phone_number": client.phone_number,
        })
    
    return missing_dob_clients


def create_quality_issues_if_needed(db: Session, portfolio_id: int) -> Dict[str, int]:
    """
    Retrieve existing quality issues from the database without creating new ones.
    Returns count of issues by type.
    
    Optimized for large portfolios to prevent database timeouts.
    """
    # Get the portfolio
    portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
    if not portfolio:
        raise ValueError(f"Portfolio with ID {portfolio_id} not found")

    # Initialize issue counts dictionary with default values
    issue_counts = {
        "duplicate_customer_ids": 0,
        "duplicate_addresses": 0,
        "duplicate_dob": 0,
        "duplicate_loan_ids": 0,
        "unmatched_employee_ids": 0,
        "loan_customer_mismatches": 0,
        "missing_dob": 0,
        "missing_addresses": 0,
        "missing_loan_numbers": 0,
        "missing_loan_dates": 0,
        "missing_loan_terms": 0,
        "missing_interest_rates": 0,
        "missing_loan_amounts": 0,
    }

    # Get existing quality issues for this portfolio
    existing_issues = (
        db.query(QualityIssue)
        .filter(QualityIssue.portfolio_id == portfolio_id)
        .all()
    )
    
    # Count issues by type
    for issue in existing_issues:
        issue_type = issue.issue_type
        
        # Map issue types to our count dictionary
        if issue_type == "duplicate_customer_id":
            issue_counts["duplicate_customer_ids"] += 1
        elif issue_type == "duplicate_address":
            issue_counts["duplicate_addresses"] += 1
        elif issue_type == "duplicate_dob":
            issue_counts["duplicate_dob"] += 1
        elif issue_type == "duplicate_loan_id":
            issue_counts["duplicate_loan_ids"] += 1
        elif issue_type == "unmatched_employee_ids":
            issue_counts["unmatched_employee_ids"] += 1
        elif issue_type == "loan_customer_mismatches":
            issue_counts["loan_customer_mismatches"] += 1
        elif issue_type == "missing_dob":
            issue_counts["missing_dob"] += 1
        elif issue_type == "missing_addresses":
            issue_counts["missing_addresses"] += 1
        elif issue_type == "missing_loan_numbers":
            issue_counts["missing_loan_numbers"] += 1
        elif issue_type == "missing_loan_dates":
            issue_counts["missing_loan_dates"] += 1
        elif issue_type == "missing_loan_terms":
            issue_counts["missing_loan_terms"] += 1
        elif issue_type == "missing_interest_rates":
            issue_counts["missing_interest_rates"] += 1
        elif issue_type == "missing_loan_amounts":
            issue_counts["missing_loan_amounts"] += 1
    
    # Calculate summary counts
    issue_counts["total_issues"] = len(existing_issues)
    issue_counts["high_severity_issues"] = sum(1 for issue in existing_issues if issue.severity == "high")
    issue_counts["open_issues"] = sum(1 for issue in existing_issues if issue.status == "open")
    
    return issue_counts
