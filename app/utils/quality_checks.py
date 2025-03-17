from collections import defaultdict
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from app.models import Client, Loan, QualityIssue, Portfolio


def find_duplicate_names(db: Session, portfolio_id: int) -> List[Dict]:
    """
    Find clients with duplicate names in the portfolio.
    Returns a list of groups of clients with the same name.
    """
    # Get all clients in the portfolio
    clients = db.query(Client).filter(Client.portfolio_id == portfolio_id).all()
    
    # Group clients by full name (case-insensitive)
    name_groups = defaultdict(list)
    for client in clients:
        full_name = f"{client.last_name} {client.other_names}".lower().strip()
        if full_name:  # Skip empty names
            name_groups[full_name].append({
                "id": client.id,
                "employee_id": client.employee_id,
                "name": f"{client.last_name} {client.other_names}",
                "phone_number": client.phone_number
            })
    
    duplicates = [group for name, group in name_groups.items() if len(group) > 1]
    
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
            address_groups[address].append({
                "id": client.id,
                "employee_id": client.employee_id,
                "name": f"{client.last_name} {client.other_names}",
                "address": client.residential_address
            })
    
    # Filter only groups with more than one client
    duplicates = [group for address, group in address_groups.items() if len(group) > 1]
    
    return duplicates


def find_missing_repayment_data(db: Session, portfolio_id: int) -> List[Dict]:
    """
    Find loans with missing historical repayment data.
    This could be loans where repayment data is expected but not found.
    """
    # Get all loans in the portfolio
    loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
    
    missing_data = []
    for loan in loans:
        # Check if loan has expected repayment data
        # This is just an example condition - customize based on your data model
        if (loan.loan_issue_date and loan.maturity_period and 
            (loan.principal_paid is None or loan.interest_paid is None)):
            
            # Get client info if available
            client = db.query(Client).filter(Client.employee_id == loan.employee_id).first()
            client_name = f"{client.last_name} {client.other_names}" if client else "Unknown"
            
            missing_data.append({
                "id": loan.id,
                "loan_no": loan.loan_no,
                "employee_id": loan.employee_id,
                "client_name": client_name,
                "loan_amount": loan.loan_amount,
                "missing_fields": ["principal_paid", "interest_paid"]  # Example fields
            })
    
    return missing_data


def create_quality_issues_if_needed(db: Session, portfolio_id: int) -> Dict[str, int]:
    """
    Check for quality issues and create or update QualityIssue records as needed.
    Returns count of issues by type.
    """
    # Get the portfolio
    portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
    if not portfolio:
        raise ValueError(f"Portfolio with ID {portfolio_id} not found")
    
    issue_counts = {
        "duplicate_names": 0,
        "duplicate_addresses": 0,
        "missing_repayment_data": 0,
        "total_issues": 0,
        "high_severity_issues": 0,
        "open_issues": 0
    }
    
    # Find duplicate names
    duplicate_names = find_duplicate_names(db, portfolio_id)
    if duplicate_names:
        for group in duplicate_names:
            # Check if this issue already exists
            existing_issue = db.query(QualityIssue).filter(
                QualityIssue.portfolio_id == portfolio_id,
                QualityIssue.issue_type == "duplicate_name",
                QualityIssue.status != "resolved"
            ).first()
            
            if existing_issue:
                # Update existing issue
                existing_issue.affected_records = group
                existing_issue.description = f"Found {len(group)} clients with duplicate names"
                db.flush()
            else:
                # Create new issue
                if len(group) < 10:
                    severity = "low"
                elif len(group) < 20:
                    severity = "medium"
                else:
                    severity = "high"
                
                new_issue = QualityIssue(
                    portfolio_id=portfolio_id,
                    issue_type="duplicate_name",
                    description=f"Found {len(group)} clients with duplicate names",
                    affected_records=group,
                    severity=severity,
                    status="open"
                )
                db.add(new_issue)
                db.flush()
                
                if severity == "high":
                    issue_counts["high_severity_issues"] += 1
                    
                issue_counts["open_issues"] += 1
                
        issue_counts["duplicate_names"] = len(duplicate_names)
        issue_counts["total_issues"] += len(duplicate_names)
    
    # Find duplicate addresses
    duplicate_addresses = find_duplicate_addresses(db, portfolio_id)
    if duplicate_addresses:
        for group in duplicate_addresses:
            # Check if this issue already exists
            existing_issue = db.query(QualityIssue).filter(
                QualityIssue.portfolio_id == portfolio_id,
                QualityIssue.issue_type == "duplicate_address",
                QualityIssue.status != "resolved"
            ).first()
            
            if existing_issue:
                # Update existing issue
                existing_issue.affected_records = group
                existing_issue.description = f"Found {len(group)} clients with duplicate addresses"
                db.flush()
            else:
                # Create new issue
                if len(group) < 10:
                    severity = "low"
                elif len(group) < 20:
                    severity = "medium"
                else:
                    severity = "high"
                
                new_issue = QualityIssue(
                    portfolio_id=portfolio_id,
                    issue_type="duplicate_address",
                    description=f"Found {len(group)} clients with duplicate addresses",
                    affected_records=group,
                    severity=severity,
                    status="open"
                )
                db.add(new_issue)
                db.flush()
                
                issue_counts["open_issues"] += 1
                
        issue_counts["duplicate_addresses"] = len(duplicate_addresses)
        issue_counts["total_issues"] += len(duplicate_addresses)
    
    # Find missing repayment data
    missing_data = find_missing_repayment_data(db, portfolio_id)
    if missing_data:
        # Group missing data by type to avoid creating too many issues
        missing_issue = db.query(QualityIssue).filter(
            QualityIssue.portfolio_id == portfolio_id,
            QualityIssue.issue_type == "missing_repayment_data",
            QualityIssue.status != "resolved"
        ).first()
        
        if missing_issue:
            # Update existing issue
            missing_issue.affected_records = missing_data
            missing_issue.description = f"Found {len(missing_data)} loans with missing repayment data"
            db.flush()
        else:
            # Create new issue
            if len(missing_data) < 10:
                severity = "low"
            elif len(missing_data) < 20:
                severity = "medium"
            else:
                severity = "high"
                
            new_issue = QualityIssue(
                portfolio_id=portfolio_id,
                issue_type="missing_repayment_data",
                description=f"Found {len(missing_data)} loans with missing repayment data",
                affected_records=missing_data,
                severity=severity,
                status="open"
            )
            db.add(new_issue)
            db.flush()
            
            if severity == "high":
                issue_counts["high_severity_issues"] += 1
                
            issue_counts["open_issues"] += 1
            
        issue_counts["missing_repayment_data"] = len(missing_data)
        issue_counts["total_issues"] += 1  # Count this as one issue type
    
    # Calculate counts for existing issues
    existing_issues = db.query(QualityIssue).filter(
        QualityIssue.portfolio_id == portfolio_id
    ).all()
    
    for issue in existing_issues:
        if issue.status == "open":
            issue_counts["open_issues"] += 1
        if issue.severity == "high":
            issue_counts["high_severity_issues"] += 1
    
    # Commit changes to the database
    db.commit()
    
    return issue_counts
