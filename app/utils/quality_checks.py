from collections import defaultdict
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from app.models import Client, Loan, QualityIssue, Portfolio
import time


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


def find_duplicate_dobs(db: Session, portfolio_id: int) -> List[Dict]:
    """
    Find clients with duplicate dates of birth in a portfolio.
    
    Args:
        db: Database session
        portfolio_id: Portfolio ID to check
        
    Returns:
        List of groups of clients with the same date of birth
    """
    # Get all clients in the portfolio
    clients = (
        db.query(Client)
        .filter(Client.portfolio_id == portfolio_id)
        .filter(Client.date_of_birth.isnot(None))  # Only check clients with DOB
        .all()
    )
    
    # Group clients by DOB
    dob_groups = {}
    for client in clients:
        if client.date_of_birth:
            dob_str = client.date_of_birth.isoformat() if hasattr(client.date_of_birth, 'isoformat') else str(client.date_of_birth)
            if dob_str not in dob_groups:
                dob_groups[dob_str] = []
            
            dob_groups[dob_str].append({
                "id": client.id,
                "employee_id": client.employee_id,
                "date_of_birth": dob_str,
                "name": f"{client.last_name} {client.other_names}" if client.last_name and client.other_names else "Unknown"
            })
    
    # Filter groups with more than one client
    duplicate_groups = [group for dob, group in dob_groups.items() if len(group) > 1]
    
    return duplicate_groups


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


def find_clients_without_matching_loans(db: Session, portfolio_id: int) -> List[Dict]:
    """
    Find clients who cannot be matched to loans in the portfolio.
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


def find_loans_without_matching_clients(db: Session, portfolio_id: int) -> List[Dict]:
    """
    Find loan details that don't match customer data (loans without matching clients).
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
        "clients_without_matching_loans": 0,
        "loans_without_matching_clients": 0,
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
        elif issue_type == "client_without_matching_loan":
            issue_counts["clients_without_matching_loans"] += 1
        elif issue_type == "loan_without_matching_client":
            issue_counts["loans_without_matching_clients"] += 1
        elif issue_type == "missing_dob":
            issue_counts["missing_dob"] += 1
        elif issue_type == "missing_address":
            issue_counts["missing_addresses"] += 1
        elif issue_type == "missing_loan_number":
            issue_counts["missing_loan_numbers"] += 1
        elif issue_type == "missing_loan_date":
            issue_counts["missing_loan_dates"] += 1
        elif issue_type == "missing_loan_term":
            issue_counts["missing_loan_terms"] += 1
        elif issue_type == "missing_interest_rate":
            issue_counts["missing_interest_rates"] += 1
        elif issue_type == "missing_loan_amount":
            issue_counts["missing_loan_amounts"] += 1
        # Handle legacy issue types for backward compatibility
        elif issue_type == "unmatched_employee_id":
            issue_counts["clients_without_matching_loans"] += 1
        elif issue_type == "loan_customer_mismatch":
            issue_counts["loans_without_matching_clients"] += 1

    # Calculate totals
    total_issues = sum(issue_counts.values())
    high_severity_issues = (
        issue_counts["duplicate_customer_ids"]
        + issue_counts["duplicate_loan_ids"]
        + issue_counts["clients_without_matching_loans"]
        + issue_counts["loans_without_matching_clients"]
        + issue_counts["missing_loan_amounts"]
    )

    # Add total counts
    issue_counts["total_issues"] = total_issues
    issue_counts["high_severity_issues"] = high_severity_issues
    
    # Count open issues
    open_issues = sum(1 for issue in existing_issues if issue.status == "open")
    issue_counts["open_issues"] = open_issues

    return issue_counts


def create_and_save_quality_issues(db: Session, portfolio_id: int, task_id: str = None) -> Dict[str, Any]:
    """
    Create quality issues for a portfolio and save them to the database.
    Optimized for large datasets with batch processing and progress reporting.
    
    Args:
        db: Database session
        portfolio_id: ID of the portfolio to check
        task_id: Optional task ID for progress reporting
        
    Returns:
        Dictionary with counts of issues by type
    """
    from app.utils.background_tasks import get_task_manager
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info(f"Starting quality issue creation for portfolio {portfolio_id}")
    
    # Get the portfolio
    portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
    if not portfolio:
        raise ValueError(f"Portfolio with ID {portfolio_id} not found")

    # Initialize issue counts dictionary
    issue_counts = {
        "duplicate_customer_ids": 0,
        "duplicate_addresses": 0,
        "duplicate_dob": 0,
        "duplicate_loan_ids": 0,
        "clients_without_matching_loans": 0,
        "loans_without_matching_clients": 0,
        "missing_dob": 0,
        "missing_addresses": 0,
        "missing_loan_numbers": 0,
        "missing_loan_dates": 0,
        "missing_loan_terms": 0,
        "missing_interest_rates": 0,
        "missing_loan_amounts": 0,
    }
    
    # First, clear existing quality issues for this portfolio
    if task_id:
        get_task_manager().update_task(
            task_id,
            status_message="Clearing existing quality issues"
        )
        time.sleep(0.1)
    
    try:
        deleted_count = db.query(QualityIssue).filter(
            QualityIssue.portfolio_id == portfolio_id
        ).delete()
        db.commit()
        logger.info(f"Deleted {deleted_count} existing quality issues for portfolio {portfolio_id}")
    except Exception as e:
        db.rollback()
        logger.error(f"Error deleting existing quality issues: {str(e)}")
    
    # Update progress if task_id provided
    if task_id:
        get_task_manager().update_task(
            task_id,
            status_message="Checking for duplicate customer IDs"
        )
        time.sleep(0.1)
    
    # 1. Check for duplicate customer IDs
    try:
        duplicate_customers = find_duplicate_customer_ids(db, portfolio_id)
        for group in duplicate_customers:
            for client_info in group:
                issue = QualityIssue(
                    portfolio_id=portfolio_id,
                    issue_type="duplicate_customer_id",
                    severity="high",
                    status="open",
                    description=f"Duplicate employee ID: {client_info['employee_id']}",
                    affected_records=[{
                        "entity_type": "client",
                        "entity_id": client_info["id"],
                        "employee_id": client_info["employee_id"],
                        "duplicate_count": len(group)
                    }]
                )
                db.add(issue)
            issue_counts["duplicate_customer_ids"] += len(group)
        
        # Commit in batches to avoid memory issues
        db.commit()
        logger.info(f"Created {issue_counts['duplicate_customer_ids']} duplicate customer ID issues")
    except Exception as e:
        db.rollback()
        logger.error(f"Error checking duplicate customer IDs: {str(e)}")
    
    # Update progress if task_id provided
    if task_id:
        get_task_manager().update_task(
            task_id,
            status_message="Checking for duplicate addresses"
        )
        time.sleep(0.1)
    
    # 2. Check for duplicate addresses
    try:
        duplicate_addresses = find_duplicate_addresses(db, portfolio_id)
        for group in duplicate_addresses:
            for client_info in group:
                issue = QualityIssue(
                    portfolio_id=portfolio_id,
                    issue_type="duplicate_address",
                    severity="medium",
                    status="open",
                    description=f"Duplicate address: {client_info.get('address', 'Unknown')}",
                    affected_records=[{
                        "entity_type": "client",
                        "entity_id": client_info["id"],
                        "address": client_info.get("address", "Unknown"),
                        "duplicate_count": len(group)
                    }]
                )
                db.add(issue)
            issue_counts["duplicate_addresses"] += len(group)
        
        # Commit in batches
        db.commit()
        logger.info(f"Created {issue_counts['duplicate_addresses']} duplicate address issues")
    except Exception as e:
        db.rollback()
        logger.error(f"Error checking duplicate addresses: {str(e)}")
    
    # Update progress if task_id provided
    if task_id:
        get_task_manager().update_task(
            task_id,
            status_message="Checking for duplicate DOBs"
        )
        time.sleep(0.1)
    
    # 3. Check for duplicate DOBs
    try:
        duplicate_dobs = find_duplicate_dobs(db, portfolio_id)
        for group in duplicate_dobs:
            for client_info in group:
                issue = QualityIssue(
                    portfolio_id=portfolio_id,
                    issue_type="duplicate_dob",
                    severity="medium",
                    status="open",
                    description=f"Duplicate date of birth: {client_info.get('date_of_birth', 'Unknown')}",
                    affected_records=[{
                        "entity_type": "client",
                        "entity_id": client_info["id"],
                        "date_of_birth": client_info.get("date_of_birth", "Unknown"),
                        "duplicate_count": len(group)
                    }]
                )
                db.add(issue)
            issue_counts["duplicate_dob"] += len(group)
        
        # Commit in batches
        db.commit()
        logger.info(f"Created {issue_counts['duplicate_dob']} duplicate DOB issues")
    except Exception as e:
        db.rollback()
        logger.error(f"Error checking duplicate DOBs: {str(e)}")
    
    # Update progress if task_id provided
    if task_id:
        get_task_manager().update_task(
            task_id,
            status_message="Checking for duplicate loan IDs"
        )
        time.sleep(0.1)
    
    # 4. Check for duplicate loan IDs
    try:
        duplicate_loans = find_duplicate_loan_ids(db, portfolio_id)
        for group in duplicate_loans:
            for loan_info in group:
                issue = QualityIssue(
                    portfolio_id=portfolio_id,
                    issue_type="duplicate_loan_id",
                    severity="high",
                    status="open",
                    description=f"Duplicate loan ID: {loan_info['loan_no']}",
                    affected_records=[{
                        "entity_type": "loan",
                        "entity_id": loan_info["id"],
                        "loan_no": loan_info["loan_no"],
                        "duplicate_count": len(group)
                    }]
                )
                db.add(issue)
            issue_counts["duplicate_loan_ids"] += len(group)
        
        # Commit in batches
        db.commit()
        logger.info(f"Created {issue_counts['duplicate_loan_ids']} duplicate loan ID issues")
    except Exception as e:
        db.rollback()
        logger.error(f"Error checking duplicate loan IDs: {str(e)}")
    
    # Update progress if task_id provided
    if task_id:
        get_task_manager().update_task(
            task_id,
            status_message="Checking for clients without matching loans"
        )
        time.sleep(0.1)
    
    # 5. Check for clients without matching loans
    try:
        unmatched_clients = find_clients_without_matching_loans(db, portfolio_id)
        for client_info in unmatched_clients:
            issue = QualityIssue(
                portfolio_id=portfolio_id,
                issue_type="client_without_matching_loan",
                severity="high",
                status="open",
                description=f"Client has no matching loan with employee ID: {client_info['employee_id']}",
                affected_records=[{
                    "entity_type": "client",
                    "entity_id": client_info["id"],
                    "employee_id": client_info["employee_id"],
                    "name": client_info.get("name", "Unknown")
                }]
            )
            db.add(issue)
        
        issue_counts["clients_without_matching_loans"] = len(unmatched_clients)
        
        # Commit in batches
        db.commit()
        logger.info(f"Created {issue_counts['clients_without_matching_loans']} client without matching loan issues")
    except Exception as e:
        db.rollback()
        logger.error(f"Error checking clients without matching loans: {str(e)}")
    
    # Update progress if task_id provided
    if task_id:
        get_task_manager().update_task(
            task_id,
            status_message="Checking for loans without matching clients"
        )
        time.sleep(0.1)
    
    # 5b. Check for loans without matching clients
    try:
        unmatched_loans = find_loans_without_matching_clients(db, portfolio_id)
        for loan_info in unmatched_loans:
            issue = QualityIssue(
                portfolio_id=portfolio_id,
                issue_type="loan_without_matching_client",
                severity="high",
                status="open",
                description=f"Loan has no matching client with employee ID: {loan_info['employee_id']}",
                affected_records=[{
                    "entity_type": "loan",
                    "entity_id": loan_info["id"],
                    "employee_id": loan_info["employee_id"],
                    "loan_amount": loan_info.get("loan_amount", 0)
                }]
            )
            db.add(issue)
        
        issue_counts["loans_without_matching_clients"] = len(unmatched_loans)
        
        # Commit in batches
        db.commit()
        logger.info(f"Created {issue_counts['loans_without_matching_clients']} loan without matching client issues")
    except Exception as e:
        db.rollback()
        logger.error(f"Error checking loans without matching clients: {str(e)}")
    
    # Update progress if task_id provided
    if task_id:
        get_task_manager().update_task(
            task_id,
            status_message="Checking for missing data"
        )
        time.sleep(0.1)
    
    # 6. Check for missing DOB
    try:
        missing_dob_clients = find_missing_dob(db, portfolio_id)
        for client_info in missing_dob_clients:
            issue = QualityIssue(
                portfolio_id=portfolio_id,
                issue_type="missing_dob",
                severity="medium",
                status="open",
                description=f"Client has no date of birth",
                affected_records=[{
                    "entity_type": "client",
                    "entity_id": client_info["id"],
                    "employee_id": client_info["employee_id"],
                    "name": client_info["name"]
                }]
            )
            db.add(issue)
        
        issue_counts["missing_dob"] = len(missing_dob_clients)
        
        # Commit in batches
        db.commit()
        logger.info(f"Created {issue_counts['missing_dob']} missing DOB issues")
    except Exception as e:
        db.rollback()
        logger.error(f"Error checking missing DOB: {str(e)}")
    
    # Calculate totals
    total_issues = sum(issue_counts.values())
    high_severity_issues = (
        issue_counts["duplicate_customer_ids"]
        + issue_counts["duplicate_loan_ids"]
        + issue_counts["clients_without_matching_loans"]
        + issue_counts["loans_without_matching_clients"]
        + issue_counts["missing_loan_amounts"]
    )
    
    # Add total counts
    issue_counts["total_issues"] = total_issues
    issue_counts["high_severity_issues"] = high_severity_issues
    issue_counts["open_issues"] = total_issues  # All new issues are open by default
    
    logger.info(f"Completed quality issue creation for portfolio {portfolio_id}: {total_issues} total issues")
    
    return issue_counts
