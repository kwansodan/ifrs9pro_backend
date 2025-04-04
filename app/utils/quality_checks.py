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


def find_duplicate_addresses_dob(db: Session, portfolio_id: int) -> List[Dict]:
    """
    Find clients with duplicate addresses AND date of birth in the portfolio.
    Returns a list of groups of clients with the same address and DOB.
    """
    # Get all clients in the portfolio
    clients = db.query(Client).filter(Client.portfolio_id == portfolio_id).all()

    # Group clients by address and date of birth (case-insensitive for address)
    address_dob_groups = defaultdict(list)
    for client in clients:
        # Create a clean address for comparison
        address = (client.residential_address or "").lower().strip()
        dob = client.date_of_birth
        
        if address and dob:  # Skip if either is missing
            # Create a combined key for address + DOB
            key = f"{address}|{dob.isoformat() if dob else 'no-dob'}"
            
            address_dob_groups[key].append(
                {
                    "id": client.id,
                    "employee_id": client.employee_id,
                    "name": f"{client.last_name} {client.other_names}",
                    "address": client.residential_address,
                    "date_of_birth": dob.isoformat() if dob else None,
                }
            )

    # Filter only groups with more than one client
    duplicates = [group for key, group in address_dob_groups.items() if len(group) > 1]

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
    Check for quality issues and create or update QualityIssue records as needed.
    Returns count of issues by type.
    """
    # Get the portfolio
    portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
    if not portfolio:
        raise ValueError(f"Portfolio with ID {portfolio_id} not found")

    issue_counts = {
        "duplicate_customer_ids": 0,
        "duplicate_addresses_dob": 0,
        "duplicate_loan_ids": 0,
        "unmatched_employee_ids": 0,
        "loan_customer_mismatches": 0,
        "missing_dob": 0,
        "total_issues": 0,
        "high_severity_issues": 0,
        "open_issues": 0,
    }

    # Track existing issues to avoid double counting
    existing_open_issues = (
        db.query(QualityIssue)
        .filter(QualityIssue.portfolio_id == portfolio_id, QualityIssue.status == "open")
        .all()
    )
    
    for issue in existing_open_issues:
        issue_counts["open_issues"] += 1
        if issue.severity == "high":
            issue_counts["high_severity_issues"] += 1

    # 1. Find duplicate customer IDs
    duplicate_customer_ids = find_duplicate_customer_ids(db, portfolio_id)
    if duplicate_customer_ids:
        for group in duplicate_customer_ids:
            # Check if this issue already exists
            existing_issue = (
                db.query(QualityIssue)
                .filter(
                    QualityIssue.portfolio_id == portfolio_id,
                    QualityIssue.issue_type == "duplicate_customer_id",
                    QualityIssue.status != "resolved",
                )
                .first()
            )

            if existing_issue:
                # Update existing issue
                existing_issue.affected_records = group
                existing_issue.description = (
                    f"Found {len(group)} clients with duplicate employee IDs"
                )
                db.flush()
            else:
                # Create new issue
                if len(group) < 5:
                    severity = "low"
                elif len(group) < 10:
                    severity = "medium"
                else:
                    severity = "high"

                new_issue = QualityIssue(
                    portfolio_id=portfolio_id,
                    issue_type="duplicate_customer_id",
                    description=f"Found {len(group)} clients with duplicate employee IDs",
                    affected_records=group,
                    severity=severity,
                    status="open",
                )
                db.add(new_issue)
                db.flush()

                if severity == "high":
                    issue_counts["high_severity_issues"] += 1

                issue_counts["open_issues"] += 1

        issue_counts["duplicate_customer_ids"] = len(duplicate_customer_ids)
        issue_counts["total_issues"] += len(duplicate_customer_ids)

    # 2. Find duplicate addresses and DOB
    duplicate_addresses_dob = find_duplicate_addresses_dob(db, portfolio_id)
    if duplicate_addresses_dob:
        for group in duplicate_addresses_dob:
            # Check if this issue already exists
            existing_issue = (
                db.query(QualityIssue)
                .filter(
                    QualityIssue.portfolio_id == portfolio_id,
                    QualityIssue.issue_type == "duplicate_address_dob",
                    QualityIssue.status != "resolved",
                )
                .first()
            )

            if existing_issue:
                # Update existing issue
                existing_issue.affected_records = group
                existing_issue.description = (
                    f"Found {len(group)} clients with identical address and date of birth"
                )
                db.flush()
            else:
                # Create new issue
                if len(group) < 5:
                    severity = "low"
                elif len(group) < 10:
                    severity = "medium"
                else:
                    severity = "high"

                new_issue = QualityIssue(
                    portfolio_id=portfolio_id,
                    issue_type="duplicate_address_dob",
                    description=f"Found {len(group)} clients with identical address and date of birth",
                    affected_records=group,
                    severity=severity,
                    status="open",
                )
                db.add(new_issue)
                db.flush()

                if severity == "high":
                    issue_counts["high_severity_issues"] += 1

                issue_counts["open_issues"] += 1

        issue_counts["duplicate_addresses_dob"] = len(duplicate_addresses_dob)
        issue_counts["total_issues"] += len(duplicate_addresses_dob)

    # 3. Find duplicate loan IDs
    duplicate_loan_ids = find_duplicate_loan_ids(db, portfolio_id)
    if duplicate_loan_ids:
        for group in duplicate_loan_ids:
            # Check if this issue already exists
            existing_issue = (
                db.query(QualityIssue)
                .filter(
                    QualityIssue.portfolio_id == portfolio_id,
                    QualityIssue.issue_type == "duplicate_loan_id",
                    QualityIssue.status != "resolved",
                )
                .first()
            )

            if existing_issue:
                # Update existing issue
                existing_issue.affected_records = group
                existing_issue.description = (
                    f"Found {len(group)} loans with duplicate loan numbers"
                )
                db.flush()
            else:
                # Always high severity for duplicate loan IDs
                severity = "high"

                new_issue = QualityIssue(
                    portfolio_id=portfolio_id,
                    issue_type="duplicate_loan_id",
                    description=f"Found {len(group)} loans with duplicate loan numbers",
                    affected_records=group,
                    severity=severity,
                    status="open",
                )
                db.add(new_issue)
                db.flush()

                issue_counts["high_severity_issues"] += 1
                issue_counts["open_issues"] += 1

        issue_counts["duplicate_loan_ids"] = len(duplicate_loan_ids)
        issue_counts["total_issues"] += len(duplicate_loan_ids)

    # 4. Find unmatched employee IDs
    unmatched_employee_ids = find_unmatched_employee_ids(db, portfolio_id)
    if unmatched_employee_ids:
        # Group unmatched data by type to avoid creating too many issues
        unmatched_issue = (
            db.query(QualityIssue)
            .filter(
                QualityIssue.portfolio_id == portfolio_id,
                QualityIssue.issue_type == "unmatched_employee_ids",
                QualityIssue.status != "resolved",
            )
            .first()
        )

        if unmatched_issue:
            # Update existing issue
            unmatched_issue.affected_records = unmatched_employee_ids
            unmatched_issue.description = (
                f"Found {len(unmatched_employee_ids)} customers without matching loans"
            )
            db.flush()
        else:
            # Determine severity based on number of issues
            if len(unmatched_employee_ids) < 10:
                severity = "low"
            elif len(unmatched_employee_ids) < 30:
                severity = "medium"
            else:
                severity = "high"

            new_issue = QualityIssue(
                portfolio_id=portfolio_id,
                issue_type="unmatched_employee_ids",
                description=f"Found {len(unmatched_employee_ids)} customers without matching loans",
                affected_records=unmatched_employee_ids,
                severity=severity,
                status="open",
            )
            db.add(new_issue)
            db.flush()

            if severity == "high":
                issue_counts["high_severity_issues"] += 1

            issue_counts["open_issues"] += 1

        issue_counts["unmatched_employee_ids"] = len(unmatched_employee_ids)
        issue_counts["total_issues"] += 1  # Count as one issue type

    # 5. Find loan customer mismatches
    loan_customer_mismatches = find_loan_customer_mismatches(db, portfolio_id)
    if loan_customer_mismatches:
        # Group mismatches by type to avoid creating too many issues
        mismatch_issue = (
            db.query(QualityIssue)
            .filter(
                QualityIssue.portfolio_id == portfolio_id,
                QualityIssue.issue_type == "loan_customer_mismatches",
                QualityIssue.status != "resolved",
            )
            .first()
        )

        if mismatch_issue:
            # Update existing issue
            mismatch_issue.affected_records = loan_customer_mismatches
            mismatch_issue.description = (
                f"Found {len(loan_customer_mismatches)} loans without matching customers"
            )
            db.flush()
        else:
            # Determine severity based on number of issues
            if len(loan_customer_mismatches) < 10:
                severity = "low"
            elif len(loan_customer_mismatches) < 30:
                severity = "medium"
            else:
                severity = "high"

            new_issue = QualityIssue(
                portfolio_id=portfolio_id,
                issue_type="loan_customer_mismatches",
                description=f"Found {len(loan_customer_mismatches)} loans without matching customers",
                affected_records=loan_customer_mismatches,
                severity=severity,
                status="open",
            )
            db.add(new_issue)
            db.flush()

            if severity == "high":
                issue_counts["high_severity_issues"] += 1

            issue_counts["open_issues"] += 1

        issue_counts["loan_customer_mismatches"] = len(loan_customer_mismatches)
        issue_counts["total_issues"] += 1  # Count as one issue type

    # 6. Find missing DOB
    missing_dob = find_missing_dob(db, portfolio_id)
    if missing_dob:
        # Group missing DOB by type to avoid creating too many issues
        missing_dob_issue = (
            db.query(QualityIssue)
            .filter(
                QualityIssue.portfolio_id == portfolio_id,
                QualityIssue.issue_type == "missing_dob",
                QualityIssue.status != "resolved",
            )
            .first()
        )

        if missing_dob_issue:
            # Update existing issue
            missing_dob_issue.affected_records = missing_dob
            missing_dob_issue.description = (
                f"Found {len(missing_dob)} customers with missing date of birth"
            )
            db.flush()
        else:
            # Determine severity based on number of issues
            if len(missing_dob) < 10:
                severity = "low"
            elif len(missing_dob) < 30:
                severity = "medium"
            else:
                severity = "high"

            new_issue = QualityIssue(
                portfolio_id=portfolio_id,
                issue_type="missing_dob",
                description=f"Found {len(missing_dob)} customers with missing date of birth",
                affected_records=missing_dob,
                severity=severity,
                status="open",
            )
            db.add(new_issue)
            db.flush()

            if severity == "high":
                issue_counts["high_severity_issues"] += 1

            issue_counts["open_issues"] += 1

        issue_counts["missing_dob"] = len(missing_dob)
        issue_counts["total_issues"] += 1  # Count as one issue type

    # Commit changes to the database
    db.commit()

    return issue_counts
