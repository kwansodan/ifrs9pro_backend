from collections import defaultdict
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from app.models import Client, Loan, QualityIssue, Portfolio
import time


from sqlalchemy import text, func

def find_duplicate_customer_ids(db: Session, portfolio_id: int) -> List[List[Dict]]:
    """Optimized using SQL aggregation."""
    sql = text("""
        SELECT employee_id, array_agg(id) as ids, array_agg(last_name || ' ' || other_names) as names, array_agg(phone_number) as phones
        FROM clients
        WHERE portfolio_id = :pid AND employee_id IS NOT NULL AND employee_id != ''
        GROUP BY employee_id
        HAVING COUNT(*) > 1
    """)
    result = db.execute(sql, {"pid": portfolio_id}).fetchall()
    
    duplicates = []
    for row in result:
        group = []
        for i in range(len(row.ids)):
            group.append({
                "id": row.ids[i],
                "employee_id": row.employee_id,
                "name": row.names[i],
                "phone_number": row.phones[i]
            })
        duplicates.append(group)
    return duplicates


def find_duplicate_addresses(db: Session, portfolio_id: int) -> List[List[Dict]]:
    """Optimized using SQL aggregation."""
    sql = text("""
        SELECT LOWER(TRIM(residential_address)) as clean_address, array_agg(id) as ids, array_agg(employee_id) as employee_ids, array_agg(last_name || ' ' || other_names) as names, array_agg(residential_address) as raw_addresses
        FROM clients
        WHERE portfolio_id = :pid AND residential_address IS NOT NULL AND residential_address != ''
        GROUP BY LOWER(TRIM(residential_address))
        HAVING COUNT(*) > 1
    """)
    result = db.execute(sql, {"pid": portfolio_id}).fetchall()
    
    duplicates = []
    for row in result:
        group = []
        for i in range(len(row.ids)):
            group.append({
                "id": row.ids[i],
                "employee_id": row.employee_ids[i],
                "name": row.names[i],
                "address": row.raw_addresses[i]
            })
        duplicates.append(group)
    return duplicates


def find_duplicate_dobs(db: Session, portfolio_id: int) -> List[List[Dict]]:
    """Optimized using SQL aggregation."""
    sql = text("""
        SELECT date_of_birth, array_agg(id) as ids, array_agg(employee_id) as employee_ids, array_agg(last_name || ' ' || other_names) as names
        FROM clients
        WHERE portfolio_id = :pid AND date_of_birth IS NOT NULL
        GROUP BY date_of_birth
        HAVING COUNT(*) > 1
    """)
    result = db.execute(sql, {"pid": portfolio_id}).fetchall()
    
    duplicates = []
    for row in result:
        group = []
        for i in range(len(row.ids)):
            group.append({
                "id": row.ids[i],
                "employee_id": row.employee_ids[i],
                "date_of_birth": row.date_of_birth.isoformat() if hasattr(row.date_of_birth, 'isoformat') else str(row.date_of_birth),
                "name": row.names[i] or "Unknown"
            })
        duplicates.append(group)
    return duplicates


def find_duplicate_loan_ids(db: Session, portfolio_id: int) -> List[List[Dict]]:
    """Optimized using SQL aggregation."""
    sql = text("""
        SELECT loan_no, array_agg(id) as ids, array_agg(employee_id) as employee_ids, array_agg(loan_amount) as amounts, array_agg(loan_issue_date) as dates
        FROM loans
        WHERE portfolio_id = :pid AND loan_no IS NOT NULL AND loan_no != ''
        GROUP BY loan_no
        HAVING COUNT(*) > 1
    """)
    result = db.execute(sql, {"pid": portfolio_id}).fetchall()
    
    duplicates = []
    for row in result:
        group = []
        for i in range(len(row.ids)):
            group.append({
                "id": row.ids[i],
                "loan_no": row.loan_no,
                "employee_id": row.employee_ids[i],
                "loan_amount": float(row.amounts[i]) if row.amounts[i] else None,
                "loan_issue_date": row.dates[i].isoformat() if row.dates[i] else None,
            })
        duplicates.append(group)
    return duplicates


def find_duplicate_phone_numbers(db: Session, portfolio_id: int) -> List[List[Dict]]:
    """Optimized using SQL aggregation."""
    sql = text("""
        SELECT phone_number, array_agg(id) as ids, array_agg(employee_id) as employee_ids, array_agg(last_name || ' ' || other_names) as names
        FROM clients
        WHERE portfolio_id = :pid AND phone_number IS NOT NULL AND phone_number != ''
        GROUP BY phone_number
        HAVING COUNT(*) > 1
    """)
    result = db.execute(sql, {"pid": portfolio_id}).fetchall()
    
    duplicates = []
    for row in result:
        group = []
        for i in range(len(row.ids)):
            group.append({
                "id": row.ids[i],
                "employee_id": row.employee_ids[i],
                "name": row.names[i],
                "phone_number": row.phone_number
            })
        duplicates.append(group)
    return duplicates


def find_clients_without_matching_loans(db: Session, portfolio_id: int) -> List[Dict]:
    """Optimized using SQL NOT EXISTS."""
    sql = text("""
        SELECT id, employee_id, last_name || ' ' || other_names as name, phone_number
        FROM clients c
        WHERE portfolio_id = :pid 
        AND employee_id IS NOT NULL 
        AND NOT EXISTS (
            SELECT 1 FROM loans l 
            WHERE l.portfolio_id = :pid AND l.employee_id = c.employee_id
        )
    """)
    result = db.execute(sql, {"pid": portfolio_id}).fetchall()
    return [dict(row._mapping) for row in result]


def find_loans_without_matching_clients(db: Session, portfolio_id: int) -> List[Dict]:
    """Optimized using SQL NOT EXISTS."""
    sql = text("""
        SELECT id, loan_no, employee_id, loan_amount
        FROM loans l
        WHERE portfolio_id = :pid 
        AND employee_id IS NOT NULL 
        AND NOT EXISTS (
            SELECT 1 FROM clients c 
            WHERE c.portfolio_id = :pid AND c.employee_id = l.employee_id
        )
    """)
    result = db.execute(sql, {"pid": portfolio_id}).fetchall()
    return [
        {
            "id": row.id,
            "loan_no": row.loan_no,
            "employee_id": row.employee_id,
            "loan_amount": float(row.loan_amount) if row.loan_amount else None
        } for row in result
    ]


def find_missing_dob(db: Session, portfolio_id: int) -> List[Dict]:
    """Optimized using direct filter."""
    result = db.query(Client.id, Client.employee_id, (Client.last_name + " " + Client.other_names).label("name"), Client.phone_number).\
        filter(Client.portfolio_id == portfolio_id, Client.date_of_birth.is_(None)).all()
    return [dict(row._mapping) for row in result]


def create_quality_issues_if_needed(db: Session, portfolio_id: int) -> Dict[str, int]:
    """
    Retrieve existing quality issues from the database utilizing efficient SQL aggregation.
    Returns count of issues by type.
    """
    from sqlalchemy import func
    
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
        "duplicate_phones": 0,
        "clients_without_matching_loans": 0,
        "loans_without_matching_clients": 0,
        "missing_dob": 0,
        "missing_addresses": 0,
        "missing_loan_numbers": 0,
        "missing_loan_dates": 0,
        "missing_loan_terms": 0,
        "missing_interest_rates": 0,
        "missing_loan_amounts": 0,
        "total_issues": 0,
        "high_severity_issues": 0,
        "open_issues": 0
    }

    # Execute optimized aggregation query
    # SELECT issue_type, status, COUNT(id) ... GROUP BY issue_type, status
    results = db.query(
        QualityIssue.issue_type,
        QualityIssue.status,
        func.count(QualityIssue.id)
    ).filter(
        QualityIssue.portfolio_id == portfolio_id
    ).group_by(
        QualityIssue.issue_type,
        QualityIssue.status
    ).all()
    
    # Map DB issue types to dictionary keys
    type_mapping = {
        "duplicate_customer_id": "duplicate_customer_ids",
        "duplicate_address": "duplicate_addresses",
        "duplicate_dob": "duplicate_dob",
        "duplicate_loan_id": "duplicate_loan_ids",
        "duplicate_phone": "duplicate_phones",
        "client_without_matching_loan": "clients_without_matching_loans",
        "loan_without_matching_client": "loans_without_matching_clients",
        "missing_dob": "missing_dob",
        "missing_address": "missing_addresses",
        "missing_loan_number": "missing_loan_numbers",
        "missing_loan_date": "missing_loan_dates",
        "missing_loan_term": "missing_loan_terms",
        "missing_interest_rate": "missing_interest_rates",
        "missing_loan_amount": "missing_loan_amounts",
        # Legacy mappings
        "unmatched_employee_id": "clients_without_matching_loans",
        "loan_customer_mismatch": "loans_without_matching_clients"
    }

    total_issues = 0
    open_issues = 0
    
    for issue_type, status, count in results:
        # Update specific type count
        if issue_type in type_mapping:
            key = type_mapping[issue_type]
            if key in issue_counts:
                issue_counts[key] += count
        
        # valid_statuses for open issues
        if status == "open":
            open_issues += count
            
        total_issues += count

    # Calculate high severity issues from the aggregated counts
    high_severity_issues = (
        issue_counts["duplicate_customer_ids"]
        + issue_counts["duplicate_loan_ids"]
        + issue_counts["clients_without_matching_loans"]
        + issue_counts["loans_without_matching_clients"]
        + issue_counts["missing_loan_amounts"]
    )

    issue_counts["total_issues"] = total_issues
    issue_counts["high_severity_issues"] = high_severity_issues
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
        
    tenant_id = portfolio.tenant_id

    # Initialize issue counts dictionary
    issue_counts = {
        "duplicate_customer_ids": 0,
        "duplicate_addresses": 0,
        "duplicate_dob": 0,
        "duplicate_loan_ids": 0,
        "duplicate_phones": 0,
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
    
def create_and_save_quality_issues(db: Session, portfolio_id: int, task_id: str = None) -> Dict[str, Any]:
    """
    Create quality issues for a portfolio and save them to the database.
    Optimized for large datasets with batch processing.
    """
    from app.utils.background_tasks import get_task_manager
    import logging
    
    logger = logging.getLogger(__name__)
    logger.info(f"Starting optimized quality issue creation for portfolio {portfolio_id}")
    
    portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
    if not portfolio:
        raise ValueError(f"Portfolio with ID {portfolio_id} not found")
    tenant_id = portfolio.tenant_id

    # 1. Clear existing issues
    if task_id:
        get_task_manager().update_task(task_id, status_message="Clearing existing quality issues")
    
    db.query(QualityIssue).filter(QualityIssue.portfolio_id == portfolio_id).delete()
    db.commit()

    issues_to_create = []
    issue_counts = defaultdict(int)

    def add_issue(issue_type, severity, description, affected_records):
        issues_to_create.append(QualityIssue(
            portfolio_id=portfolio_id,
            tenant_id=tenant_id,
            issue_type=issue_type,
            severity=severity,
            status="open",
            description=description,
            affected_records=affected_records
        ))
        issue_counts[issue_type] += 1

    # Define tasks to run
    check_tasks = [
        ("duplicate_customer_id", "high", "Duplicate employee ID: {} (found in {} clients)", find_duplicate_customer_ids),
        ("duplicate_address", "medium", "Duplicate address: {} (found in {} clients)", find_duplicate_addresses),
        ("duplicate_dob", "medium", "Duplicate DOB: {} (found in {} clients)", find_duplicate_dobs),
        ("duplicate_loan_id", "high", "Duplicate loan ID: {} (found in {} loans)", find_duplicate_loan_ids),
        ("duplicate_phone", "medium", "Duplicate phone: {} (found in {} clients)", find_duplicate_phone_numbers),
    ]

    for type_name, severity, desc_templ, func in check_tasks:
        if task_id:
            get_task_manager().update_task(task_id, status_message=f"Checking for {type_name.replace('_', ' ')}s")
        
        try:
            results = func(db, portfolio_id)
            for group in results:
                # Key for the description depends on data structure returned by func
                if type_name == "duplicate_customer_id": key = group[0]['employee_id']
                elif type_name == "duplicate_address": key = group[0]['address']
                elif type_name == "duplicate_dob": key = group[0]['date_of_birth']
                elif type_name == "duplicate_loan_id": key = group[0]['loan_no']
                elif type_name == "duplicate_phone": key = group[0]['phone_number']
                else: key = "Unknown"

                affected = []
                entity_type = "loan" if "loan" in type_name else "client"
                for item in group:
                    rec = {"entity_type": entity_type, "entity_id": item["id"]}
                    if entity_type == "client":
                        rec.update({"employee_id": item.get("employee_id"), "name": item.get("name")})
                    else:
                        rec.update({"loan_no": item.get("loan_no"), "employee_id": item.get("employee_id")})
                    affected.append(rec)
                
                add_issue(type_name, severity, desc_templ.format(key, len(group)), affected)
        except Exception as e:
            logger.error(f"Error in {type_name} check: {e}")

    # Unmatched checks
    if task_id:
        get_task_manager().update_task(task_id, status_message="Checking for unmatched records")
    
    try:
        # Clients without loans
        unmatched_clients = find_clients_without_matching_loans(db, portfolio_id)
        for c in unmatched_clients:
            add_issue("client_without_matching_loan", "high", f"Client has no matching loan: {c['employee_id']}", 
                      [{"entity_type": "client", "entity_id": c['id'], "employee_id": c['employee_id'], "name": c.get('name')}])
        
        # Loans without clients
        unmatched_loans = find_loans_without_matching_clients(db, portfolio_id)
        for l in unmatched_loans:
            add_issue("loan_without_matching_client", "high", f"Loan has no matching client: {l['employee_id']}", 
                      [{"entity_type": "loan", "entity_id": l['id'], "loan_no": l['loan_no'], "employee_id": l['employee_id']}])
            
        # Missing DOB
        missing_dob = find_missing_dob(db, portfolio_id)
        for c in missing_dob:
            add_issue("missing_dob", "medium", "Client has no date of birth", 
                      [{"entity_type": "client", "entity_id": c['id'], "employee_id": c['employee_id'], "name": c.get('name')}])
    except Exception as e:
        logger.error(f"Error in unmatched/missing checks: {e}")

    # 2. Bulk save and commit
    if issues_to_create:
        if task_id:
            get_task_manager().update_task(task_id, status_message=f"Saving {len(issues_to_create)} quality issues")
        db.bulk_save_objects(issues_to_create)
        db.commit()

    # Build response summary
    summary = {
        "duplicate_customer_ids": issue_counts["duplicate_customer_id"],
        "duplicate_addresses": issue_counts["duplicate_address"],
        "duplicate_dob": issue_counts["duplicate_dob"],
        "duplicate_loan_ids": issue_counts["duplicate_loan_id"],
        "duplicate_phones": issue_counts["duplicate_phone"],
        "clients_without_matching_loans": issue_counts["client_without_matching_loan"],
        "loans_without_matching_clients": issue_counts["loan_without_matching_client"],
        "missing_dob": issue_counts["missing_dob"],
        "total_issues": len(issues_to_create),
        "open_issues": len(issues_to_create),
        "high_severity_issues": (
            issue_counts["duplicate_customer_id"] + 
            issue_counts["duplicate_loan_id"] + 
            issue_counts["client_without_matching_loan"] + 
            issue_counts["loan_without_matching_client"]
        )
    }
    logger.info(f"Quality check completed: {summary['total_issues']} issues found.")
    return summary
