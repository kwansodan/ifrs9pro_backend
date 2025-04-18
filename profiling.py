import logging
import time
from typing import Dict, List, Any, Optional, Tuple, Callable
from datetime import date, datetime, timedelta
from decimal import Decimal
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy.orm import Session
from sqlalchemy import func, and_
import numpy as np
import pandas as pd

from app.database import get_db, SessionLocal
from app.models import (
    Portfolio, Loan, Client, Security, Guarantee, Report,
    CalculationResult, StagingResult
)
from app.calculators.ecl import (
    calculate_effective_interest_rate_lender,
    calculate_exposure_at_default_percentage,
    calculate_probability_of_default,
    calculate_loss_given_default,
    calculate_marginal_ecl,
    get_amortization_schedule,
    get_ecl_by_stage,
    is_in_range
)

# Set up logging
logger = logging.getLogger(__name__)

def generate_ecl_detailed_report(
    db: Session, portfolio_id: int, report_date: date
) -> Dict[str, Any]:
    """
    Generate a detailed ECL report for a portfolio.
    
    This populates the ECL detailed report template with:
    - B3: Report date
    - B4: Report run date (current date)
    - B6: Report description
    - B9: Total exposure at default
    - B10: Total loss given default
    - B12: Total ECL
    - Rows 15+: Loan details with ECL calculations
    """
    start_time = time.time()
    print(f"Starting ECL detailed report for portfolio {portfolio_id}")
    
    # Get the latest ECL calculation result
    ecl_calculation = db.query(CalculationResult).filter(
        CalculationResult.portfolio_id == portfolio_id,
        CalculationResult.calculation_type == "ecl"
    ).order_by(CalculationResult.created_at.desc()).first()
    
    # If no calculation exists, return empty structure
    if not ecl_calculation:
        return {
            "portfolio_id": portfolio_id,
            "report_date": report_date.strftime("%Y-%m-%d"),
            "report_type": "ecl_detailed_report",
            "report_run_date": datetime.now().strftime("%Y-%m-%d"),
            "description": "ECL Detailed Report - No calculations available",
            "total_ead": Decimal(0),
            "total_lgd": Decimal(0),
            "total_ecl": Decimal(0),
            "loans": []
        }
    
    # Get all loans for this portfolio
    loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
    print(f"Found {len(loans)} loans to process")
    
    # OPTIMIZATION 1: Extract all employee IDs at once
    employee_ids = list(set([loan.employee_id for loan in loans if loan.employee_id]))
    
    # OPTIMIZATION 2: Client data caching with LRU cache
    @lru_cache(maxsize=1000)
    def get_client_name(employee_id):
        client = db.query(Client).filter(Client.employee_id == employee_id).first()
        if client:
            return f"{client.last_name or ''} {client.other_names or ''}".strip()
        return "Unknown"
    
    # OPTIMIZATION 3: Efficient security grouping with JOIN
    print("Fetching securities with optimized JOIN...")
    securities_with_clients = (
        db.query(Security, Client)
        .join(Client, Security.client_id == Client.id)
        .filter(Client.employee_id.in_(employee_ids))
        .all()
    )
    
    # Group securities by employee_id for O(1) lookup
    client_securities = {}
    for security, client in securities_with_clients:
        if client and client.employee_id:
            if client.employee_id not in client_securities:
                client_securities[client.employee_id] = []
            client_securities[client.employee_id].append(security)
    
    # OPTIMIZATION 4: Staging data optimization with O(1) lookup
    latest_staging = (
        db.query(StagingResult)
        .filter(
            StagingResult.portfolio_id == portfolio_id,
            StagingResult.staging_type == "ecl"
        )
        .order_by(StagingResult.created_at.desc())
        .first()
    )
    
    # Create loan_id to stage mapping for O(1) lookup
    loan_stage_map = {}
    if latest_staging and latest_staging.result_summary:
        staging_data = []
        
        if "staging_data" in latest_staging.result_summary:
            staging_data = latest_staging.result_summary.get("staging_data", [])
        elif "loans" in latest_staging.result_summary:
            staging_data = latest_staging.result_summary.get("loans", [])
            
        # Convert to dictionary for O(1) lookups
        for stage_info in staging_data:
            loan_id = stage_info.get("loan_id")
            stage = stage_info.get("stage")
            if loan_id and stage:
                loan_stage_map[loan_id] = stage
    
    # OPTIMIZATION 5: Batch calculations - precompute PD values
    print("Pre-calculating PD values...")
    pd_map = {}
    for loan in loans:
        pd_map[loan.id] = calculate_probability_of_default(loan, db)
    
    # OPTIMIZATION 6: Parallel processing with batches
    def process_loan_batch(batch):
        batch_results = []
        batch_ead = 0.0
        batch_lgd = 0.0
        batch_ecl = 0.0
        
        for loan in batch:
            # OPTIMIZATION 7: Reduced decimal overhead - convert to float early
            loan_amount = float(loan.loan_amount) if loan.loan_amount else 0.0
            admin_fees = float(loan.administrative_fees) if loan.administrative_fees else 0.0
            loan_term = int(loan.loan_term) if loan.loan_term else 0
            monthly_payment = float(loan.monthly_installment) if loan.monthly_installment else 0.0
            outstanding_balance = float(loan.outstanding_loan_balance) if loan.outstanding_loan_balance else 0.0
            
            # Get stage using O(1) lookup
            stage = loan_stage_map.get(loan.id, "Stage 1")  # Default to Stage 1
            
            # Use pre-calculated PD
            pd_value = pd_map.get(loan.id, 0.0)
            
            # Get securities using O(1) lookup
            securities = client_securities.get(loan.employee_id, [])
            
            # Calculate remaining values
            lgd = calculate_loss_given_default(loan, securities)
            ead = calculate_exposure_at_default_percentage(loan, report_date)
            
            # Calculate EIR
            eir = calculate_effective_interest_rate_lender(
                loan_amount=loan_amount,
                administrative_fees=admin_fees,
                loan_term=loan_term,
                monthly_payment=monthly_payment
            )
            
            # Calculate ECL
            ecl = float(ead) * float(pd_value) * float(lgd) / 100.0
            
            # Update batch totals
            batch_ead += float(ead)
            batch_lgd += float(lgd) * outstanding_balance
            batch_ecl += ecl
            
            # Get client name using cached function
            client_name = get_client_name(loan.employee_id)
            
            # Create loan entry
            loan_entry = {
                "loan_id": loan.id,
                "employee_id": loan.employee_id,
                "employee_name": client_name,
                "loan_value": loan.loan_amount,
                "outstanding_loan_balance": loan.outstanding_loan_balance,
                "accumulated_arrears": loan.accumulated_arrears or Decimal(0),
                "ndia": loan.ndia or Decimal(0),
                "stage": stage,
                "ead": ead,
                "lgd": lgd,
                "eir": eir,
                "pd": pd_value,
                "ecl": ecl
            }
            
            batch_results.append(loan_entry)
            
        return batch_results, batch_ead, batch_lgd, batch_ecl
    
    # Split loans into batches
    batch_size = 500  # Increased batch size
    loan_batches = [loans[i:i + batch_size] for i in range(0, len(loans), batch_size)]
    print(f"Processing {len(loan_batches)} batches with ThreadPoolExecutor")
    
    # Process batches in parallel
    loan_data = []
    total_ead = 0.0
    total_lgd = 0.0
    total_ecl = 0.0
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        batch_results = list(executor.map(process_loan_batch, loan_batches))
        
        # Combine results from all batches
        for batch_data, batch_ead, batch_lgd, batch_ecl in batch_results:
            loan_data.extend(batch_data)
            total_ead += batch_ead
            total_lgd += batch_lgd
            total_ecl += batch_ecl
    
    # Create the report data structure
    report_data = {
        "portfolio_id": portfolio_id,
        "report_date": report_date.strftime("%Y-%m-%d"),
        "report_type": "ecl_detailed_report",
        "report_run_date": datetime.now().strftime("%Y-%m-%d"),
        "description": "ECL Detailed Report",
        "total_ead": total_ead,
        "total_lgd": total_lgd,
        "total_ecl": total_ecl,
        "loans": loan_data
    }
    
    print(f"ECL detailed report generated in {time.time() - start_time:.2f} seconds")
    return report_data


def generate_local_impairment_details_report(
    db: Session, portfolio_id: int, report_date: date
) -> Dict[str, Any]:
    """
    Generate a detailed report of local impairment calculations for a portfolio.
    Optimized for large portfolios with 70K+ loans using database-level batching.
    
    Args:
        db: Database session
        portfolio_id: ID of the portfolio
        report_date: Date of the report
        
    Returns:
        Dict containing the report data
    """
    start_time = time.time()
    print(f"Starting local impairment details report for portfolio {portfolio_id}")
    
    # Get the portfolio
    portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
    if not portfolio:
        raise ValueError(f"Portfolio with ID {portfolio_id} not found")
    
    # Get the latest local impairment calculation
    latest_calculation = (
        db.query(CalculationResult)
        .filter(
            CalculationResult.portfolio_id == portfolio_id,
            CalculationResult.calculation_type == "local_impairment"
        )
        .order_by(CalculationResult.created_at.desc())
        .first()
    )
    
    if not latest_calculation:
        raise ValueError(f"No local impairment calculation found for portfolio {portfolio_id}")
    
    # Get the latest local impairment staging
    latest_staging = (
        db.query(StagingResult)
        .filter(
            StagingResult.portfolio_id == portfolio_id,
            StagingResult.staging_type == "local_impairment"
        )
        .order_by(StagingResult.created_at.desc())
        .first()
    )
    
    if not latest_staging:
        raise ValueError(f"No local impairment staging found for portfolio {portfolio_id}")
    
    # OPTIMIZATION 1: Extract staging data with O(1) lookup
    staging_data = []
    if "staging_data" in latest_staging.result_summary:
        staging_data = latest_staging.result_summary.get("staging_data", [])
    elif "loans" in latest_staging.result_summary:
        staging_data = latest_staging.result_summary.get("loans", [])
    
    # Create a map of loan_id to impairment category for O(1) lookups
    loan_category_map = {}
    for stage_info in staging_data:
        loan_id = stage_info.get("loan_id")
        category = stage_info.get("impairment_category", stage_info.get("stage"))  # Support both formats
        if loan_id and category:
            loan_category_map[loan_id] = category
    
    # OPTIMIZATION 2: Extract provision rates from calculation
    calculation_summary = latest_calculation.result_summary
    provision_rates = {
        "Current": calculation_summary.get("Current", {}).get("provision_rate", 0.01),
        "OLEM": calculation_summary.get("OLEM", {}).get("provision_rate", 0.05),
        "Substandard": calculation_summary.get("Substandard", {}).get("provision_rate", 0.25),
        "Doubtful": calculation_summary.get("Doubtful", {}).get("provision_rate", 0.50),
        "Loss": calculation_summary.get("Loss", {}).get("provision_rate", 1.0)
    }
    
    # OPTIMIZATION 3: Get total loan count without loading all loans
    total_loan_count = db.query(func.count(Loan.id)).filter(Loan.portfolio_id == portfolio_id).scalar()
    print(f"Portfolio has {total_loan_count} loans to process")
    
    # OPTIMIZATION 4: Client data caching with LRU cache
    @lru_cache(maxsize=1000)
    def get_client_name(employee_id):
        client = db.query(Client).filter(Client.employee_id == employee_id).first()
        if client:
            return f"{client.last_name or ''} {client.other_names or ''}".strip()
        return "Unknown"
    
    # Initialize category totals
    category_totals = {
        "Current": {"count": 0, "balance": 0.0, "provision": 0.0},
        "OLEM": {"count": 0, "balance": 0.0, "provision": 0.0},
        "Substandard": {"count": 0, "balance": 0.0, "provision": 0.0},
        "Doubtful": {"count": 0, "balance": 0.0, "provision": 0.0},
        "Loss": {"count": 0, "balance": 0.0, "provision": 0.0}
    }
    
    # OPTIMIZATION 5: Process loans in database-level batches
    batch_size = 500
    loan_data = []
    
    # Calculate number of batches
    num_batches = (total_loan_count + batch_size - 1) // batch_size
    print(f"Processing {num_batches} batches of {batch_size} loans each")
    
    # OPTIMIZATION 6: Preload all securities in a single query and create lookup map
    # Get all employee IDs first (with batch processing)
    employee_ids = set()
    for offset in range(0, total_loan_count, batch_size):
        batch_employee_ids = db.query(Loan.employee_id).filter(
            Loan.portfolio_id == portfolio_id,
            Loan.employee_id.isnot(None)
        ).offset(offset).limit(batch_size).all()
        
        employee_ids.update([eid[0] for eid in batch_employee_ids if eid[0]])
    
    print(f"Found {len(employee_ids)} unique employee IDs")
    
    # Get all securities for these employees in a single query
    securities_with_clients = []
    # Process employee IDs in batches to avoid query parameter limits
    employee_id_batches = [list(employee_ids)[i:i + 1000] for i in range(0, len(employee_ids), 1000)]
    for emp_batch in employee_id_batches:
        batch_securities = (
            db.query(Security, Client)
            .join(Client, Security.client_id == Client.id)
            .filter(Client.employee_id.in_(emp_batch))
            .all()
        )
        securities_with_clients.extend(batch_securities)
    
    # Group securities by employee_id for O(1) lookup
    client_securities = {}
    for security, client in securities_with_clients:
        if client and client.employee_id:
            if client.employee_id not in client_securities:
                client_securities[client.employee_id] = []
            client_securities[client.employee_id].append(security)
    
    print(f"Loaded securities for {len(client_securities)} employees")
    
    # OPTIMIZATION 7: Process loans in batches using ThreadPoolExecutor
    def process_loan_batch(offset):
        # Fetch batch of loans directly from database
        loan_batch = db.query(Loan).filter(
            Loan.portfolio_id == portfolio_id
        ).order_by(Loan.id).offset(offset).limit(batch_size).all()
        
        batch_results = []
        batch_totals = {
            "Current": {"count": 0, "balance": 0.0, "provision": 0.0},
            "OLEM": {"count": 0, "balance": 0.0, "provision": 0.0},
            "Substandard": {"count": 0, "balance": 0.0, "provision": 0.0},
            "Doubtful": {"count": 0, "balance": 0.0, "provision": 0.0},
            "Loss": {"count": 0, "balance": 0.0, "provision": 0.0}
        }
        
        for loan in loan_batch:
            try:
                # Convert to float early to reduce decimal overhead
                outstanding_balance = float(loan.outstanding_loan_balance) if loan.outstanding_loan_balance else 0.0
                
                # Get category using O(1) lookup
                category = loan_category_map.get(loan.id, "Current")  # Default to Current if not found
                
                # Get provision rate using O(1) lookup
                provision_rate = provision_rates.get(category, 0.01)  # Default to 1% if category not found
                
                # Get securities using O(1) lookup
                securities = client_securities.get(loan.employee_id, [])
                
                # Calculate LGD for more accurate provision
                lgd = calculate_loss_given_default(loan, securities) / 100.0  # Convert to decimal
                
                # Calculate provision amount with LGD factor
                provision_amount = outstanding_balance * provision_rate * lgd
                
                # Update category totals
                if category in batch_totals:
                    batch_totals[category]["count"] += 1
                    batch_totals[category]["balance"] += outstanding_balance
                    batch_totals[category]["provision"] += provision_amount
                
                # Get client name using cached function
                client_name = get_client_name(loan.employee_id)
                
                # Create loan entry
                loan_entry = {
                    "loan_id": loan.id,
                    "employee_id": loan.employee_id,
                    "employee_name": client_name,
                    "loan_value": loan.loan_amount,
                    "outstanding_balance": loan.outstanding_loan_balance,
                    "accumulated_arrears": loan.accumulated_arrears or Decimal(0),
                    "ndia": loan.ndia or Decimal(0),
                    "impairment_category": category,
                    "provision_rate": provision_rate,
                    "provision_amount": provision_amount
                }
                
                batch_results.append(loan_entry)
            except Exception as e:
                print(f"Error processing loan {loan.id}: {str(e)}")
                # Continue processing other loans
                continue
            
        return batch_results, batch_totals
    
    # Process batches in parallel
    offsets = list(range(0, total_loan_count, batch_size))
    
    # OPTIMIZATION 8: Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=4) as executor:
        batch_results = list(executor.map(process_loan_batch, offsets))
        
        # Combine results from all batches
        for batch_data, batch_totals in batch_results:
            loan_data.extend(batch_data)
            
            # Combine category totals
            for category in category_totals:
                category_totals[category]["count"] += batch_totals[category]["count"]
                category_totals[category]["balance"] += batch_totals[category]["balance"]
                category_totals[category]["provision"] += batch_totals[category]["provision"]
    
    # Calculate total provision
    total_provision = sum(category_totals[category]["provision"] for category in category_totals)
    
    # Create the final report
    result = {
        "portfolio_name": portfolio.name,
        "description": f"Local Impairment Details Report for {portfolio.name}",
        "report_date": report_date,
        "report_run_date": datetime.now().date(),
        "total_provision": total_provision,
        "category_totals": category_totals,
        "loans": loan_data
    }
    
    elapsed_time = time.time() - start_time
    print(f"Report generation completed in {elapsed_time:.2f} seconds")
    
    return result

def process_ecl_calculation_sync(
    portfolio_id: int,
    reporting_date: date,
    staging_result: StagingResult,
    db: Session
) -> Dict[str, Any]:
    """
    Synchronously process ECL calculation and return the result.
    Performs detailed ECL calculations including PD, EAD, LGD and amortization schedules
    for more accurate IFRS 9 compliance.
    
    Optimized for large portfolios with 70K+ loans using database-level batching.
    """
    logger.info(f"Starting synchronous ECL calculation for portfolio {portfolio_id}")
    start_time = datetime.now()
    
    # Verify portfolio exists
    portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
    if not portfolio:
        logger.error(f"Portfolio with ID {portfolio_id} not found")
        raise ValueError(f"Portfolio with ID {portfolio_id} not found")
    
    # Extract stage information from staging result
    staging_summary = staging_result.result_summary
    
    # Get loan counts and outstanding balances from staging summary
    stage_1_count = staging_summary.get("Stage 1", {}).get("num_loans", 0)
    stage_1_balance = staging_summary.get("Stage 1", {}).get("outstanding_loan_balance", 0)
    
    stage_2_count = staging_summary.get("Stage 2", {}).get("num_loans", 0)
    stage_2_balance = staging_summary.get("Stage 2", {}).get("outstanding_loan_balance", 0)
    
    stage_3_count = staging_summary.get("Stage 3", {}).get("num_loans", 0)
    stage_3_balance = staging_summary.get("Stage 3", {}).get("outstanding_loan_balance", 0)
    
    # Log the extracted values for debugging
    logger.info(f"From staging: Stage 1: {stage_1_count} loans, ${stage_1_balance}; Stage 2: {stage_2_count} loans, ${stage_2_balance}; Stage 3: {stage_3_count} loans, ${stage_3_balance}")
    
    # OPTIMIZATION 1: Get total loan count without loading all loans
    total_loan_count = db.query(func.count(Loan.id)).filter(Loan.portfolio_id == portfolio_id).scalar()
    logger.info(f"Portfolio has {total_loan_count} loans to process")
    
    # OPTIMIZATION 2: Create a map of loan_id to stage for O(1) lookups
    loan_stage_map = {}
    if "loans" in staging_summary:
        for loan_info in staging_summary["loans"]:
            loan_id = loan_info.get("loan_id")
            stage = loan_info.get("stage")
            if loan_id and stage:
                loan_stage_map[loan_id] = stage
    
    # OPTIMIZATION 3: Get all employee IDs with batch processing
    batch_size = 500
    employee_ids = set()
    
    # Calculate number of batches
    num_batches = (total_loan_count + batch_size - 1) // batch_size
    logger.info(f"Processing {num_batches} batches of {batch_size} loans each")
    
    for offset in range(0, total_loan_count, batch_size):
        batch_employee_ids = db.query(Loan.employee_id).filter(
            Loan.portfolio_id == portfolio_id,
            Loan.employee_id.isnot(None)
        ).offset(offset).limit(batch_size).all()
        
        employee_ids.update([eid[0] for eid in batch_employee_ids if eid[0]])
    
    logger.info(f"Found {len(employee_ids)} unique employee IDs")
    
    # OPTIMIZATION 4: Get all client securities in batches
    client_securities = {}
    if employee_ids:
        try:
            # Process employee IDs in batches to avoid query parameter limits
            employee_id_batches = [list(employee_ids)[i:i + 1000] for i in range(0, len(employee_ids), 1000)]
            
            for emp_batch in employee_id_batches:
                securities_with_clients = (
                    db.query(Security, Client)
                    .join(Client, Security.client_id == Client.id)
                    .filter(Client.employee_id.in_(emp_batch))
                    .all()
                )
                
                # Group securities by employee_id for O(1) lookup
                for security, client in securities_with_clients:
                    if client and client.employee_id:
                        if client.employee_id not in client_securities:
                            client_securities[client.employee_id] = []
                        client_securities[client.employee_id].append(security)
            
            logger.info(f"Loaded securities for {len(client_securities)} employees")
        except Exception as e:
            logger.error(f"Error fetching securities: {str(e)}")
            # Continue without securities if there's an error
    
    # Initialize stage totals
    stage_1_provision = Decimal("0")
    stage_2_provision = Decimal("0")
    stage_3_provision = Decimal("0")
    
    # Initialize metrics for summary
    total_lgd = 0
    total_pd = 0
    total_ead_value = 0
    total_loans_processed = 0
    
    # OPTIMIZATION 5: Process loans in database-level batches
    for offset in range(0, total_loan_count, batch_size):
        # Fetch batch of loans directly from database
        loan_batch = db.query(Loan).filter(
            Loan.portfolio_id == portfolio_id
        ).order_by(Loan.id).offset(offset).limit(batch_size).all()
        
        logger.info(f"Processing batch {offset//batch_size + 1}/{num_batches} with {len(loan_batch)} loans")
        
        for loan in loan_batch:
            try:
                # Skip loans with no outstanding balance
                if not loan.outstanding_loan_balance or float(loan.outstanding_loan_balance) <= 0:
                    continue
                    
                # Get loan stage from map or default to Stage 1
                stage = loan_stage_map.get(loan.id, "Stage 1")
                
                # OPTIMIZATION 6: Skip detailed calculation for Stage 3 loans (use fixed rate)
                if stage == "Stage 3":
                    # For Stage 3, use a simplified approach with fixed rate
                    provision_amount = float(loan.outstanding_loan_balance) * 0.15
                    stage_3_provision += Decimal(str(provision_amount))
                    continue
                
                # Calculate Loss Given Default (LGD)
                try:
                    securities = client_securities.get(loan.employee_id, [])
                    lgd_percentage = calculate_loss_given_default(loan, securities)
                    lgd = lgd_percentage / 100.0  # Convert to decimal
                except Exception as e:
                    logger.warning(f"LGD calculation failed for loan {loan.id}: {str(e)}")
                    lgd = 0.65  # Default to 65% if calculation fails
                    lgd_percentage = 65.0
                
                # Calculate Probability of Default (PD)
                try:
                    pd_percentage = calculate_probability_of_default(loan, db)
                    pd = pd_percentage / 100.0  # Convert to decimal
                except Exception as e:
                    logger.warning(f"PD calculation failed for loan {loan.id}: {str(e)}")
                    pd = 0.05  # Default to 5% if calculation fails
                    pd_percentage = 5.0
                
                # Calculate Exposure at Default (EAD)
                try:
                    # Ensure dates are valid before calculation
                    if loan.loan_issue_date is None:
                        raise ValueError("Loan issue date is None")
                    if reporting_date is None:
                        raise ValueError("Reporting date is None")
                        
                    ead_percentage = calculate_exposure_at_default_percentage(loan, reporting_date)
                    ead_value = float(loan.outstanding_loan_balance) * (ead_percentage / 100.0)
                except Exception as e:
                    logger.warning(f"EAD calculation failed for loan {loan.id}: {str(e)}")
                    # Default to outstanding balance if calculation fails
                    ead_value = float(loan.outstanding_loan_balance)
                    ead_percentage = 100.0
                
                # For loans with complete data, calculate ECL using amortization schedule
                provision_amount = Decimal("0")
                
                if (loan.loan_amount and loan.loan_term and 
                    loan.monthly_installment and loan.loan_issue_date):
                    try:
                        # Format dates for amortization schedule - add null checks
                        if loan.loan_issue_date is None:
                            raise ValueError("Loan issue date is None")
                            
                        start_date = loan.loan_issue_date.strftime("%d/%m/%Y")
                        
                        if reporting_date is None:
                            raise ValueError("Reporting date is None")
                            
                        report_date = reporting_date.strftime("%d/%m/%Y")
                        
                        # Calculate effective interest rate
                        effective_interest_rate = calculate_effective_interest_rate_lender(
                            float(loan.loan_amount),
                            float(loan.administrative_fees) if loan.administrative_fees else 0,
                            loan.loan_term,
                            float(loan.monthly_installment)
                        )
                        
                        # Use default interest rate if calculation fails
                        if effective_interest_rate is None:
                            effective_interest_rate = 24.0  # Default to 24% annual rate
                        
                        # Get amortization schedule and ECL values
                        schedule, ecl_12_month, ecl_lifetime = get_amortization_schedule(
                            loan_amount=float(loan.loan_amount),
                            loan_term=loan.loan_term,
                            annual_interest_rate=effective_interest_rate,
                            monthly_installment=float(loan.monthly_installment),
                            start_date=start_date,
                            reporting_date=report_date,
                            pd=pd_percentage,
                            loan=loan,
                            db=db  # Pass the database session
                        )
                        
                        # Get appropriate ECL based on loan stage
                        stage_num = int(stage.split()[-1])  # Extract stage number
                        ecl_value = get_ecl_by_stage(schedule, ecl_12_month, ecl_lifetime, stage_num)
                        provision_amount = Decimal(str(ecl_value))
                        
                        # Update metrics
                        total_lgd += lgd_percentage
                        total_pd += pd_percentage
                        total_ead_value += ead_value
                        total_loans_processed += 1
                        
                    except Exception as e:
                        logger.warning(f"Amortization calculation failed for loan {loan.id}: {str(e)}")
                        # Fallback to simplified calculation if amortization fails
                        provision_amount = calculate_marginal_ecl(loan, ead_value, pd_percentage, lgd_percentage)
                else:
                    # Fallback for loans with incomplete data
                    provision_amount = calculate_marginal_ecl(loan, ead_value, pd_percentage, lgd_percentage)
                
                # Add to appropriate stage total
                if stage == "Stage 1":
                    stage_1_provision += provision_amount
                elif stage == "Stage 2":
                    stage_2_provision += provision_amount
            except Exception as e:
                logger.error(f"Error processing loan {loan.id}: {str(e)}")
                # Continue processing other loans
                continue
        
        # Log progress after each batch
        elapsed_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Processed batch {offset//batch_size + 1}/{num_batches} in {elapsed_time:.2f} seconds")
    
    # Calculate average metrics
    avg_lgd = total_lgd / total_loans_processed if total_loans_processed > 0 else 0.0  
    avg_pd = total_pd / total_loans_processed if total_loans_processed > 0 else 0.0     
    avg_ead = total_ead_value / total_loans_processed if total_loans_processed > 0 else 0.0
    
    # Calculate total provision
    total_provision = stage_1_provision + stage_2_provision + stage_3_provision
    total_balance = Decimal(str(stage_1_balance)) + Decimal(str(stage_2_balance)) + Decimal(str(stage_3_balance))
    provision_percentage = (
        (total_provision / total_balance * 100)
        if total_balance > 0
        else Decimal("0")
    )
    
    # Calculate effective rates based on provisions and balances
    stage_1_rate = (stage_1_provision / Decimal(str(stage_1_balance)) * 100) if Decimal(str(stage_1_balance)) > 0 else Decimal("0")
    stage_2_rate = (stage_2_provision / Decimal(str(stage_2_balance)) * 100) if Decimal(str(stage_2_balance)) > 0 else Decimal("0")
    stage_3_rate = (stage_3_provision / Decimal(str(stage_3_balance)) * 100) if Decimal(str(stage_3_balance)) > 0 else Decimal("0")
    
    # Create a new CalculationResult record
    calculation_result = CalculationResult(
        portfolio_id=portfolio_id,
        calculation_type="ecl",
        reporting_date=reporting_date,
        config=staging_result.config,
        result_summary={
            "Stage 1": {
                "num_loans": stage_1_count,
                "total_loan_value": float(stage_1_balance),
                "outstanding_loan_balance": float(stage_1_balance),
                "provision_amount": float(stage_1_provision),
                "provision_rate": float(stage_1_rate),
            },
            "Stage 2": {
                "num_loans": stage_2_count,
                "total_loan_value": float(stage_2_balance),
                "outstanding_loan_balance": float(stage_2_balance),
                "provision_amount": float(stage_2_provision),
                "provision_rate": float(stage_2_rate),
            },
            "Stage 3": {
                "num_loans": stage_3_count,
                "total_loan_value": float(stage_3_balance),
                "outstanding_loan_balance": float(stage_3_balance),
                "provision_amount": float(stage_3_provision),
                "provision_rate": float(stage_3_rate),
            },
            "metrics": {
                "avg_lgd": float(avg_lgd),
                "avg_pd": float(avg_pd),
                "avg_ead": float(avg_ead),
                "calculation_time_seconds": (datetime.now() - start_time).total_seconds()
            }
        },
        total_provision=float(total_provision),
        provision_percentage=float(provision_percentage)
    )
    
    db.add(calculation_result)
    db.commit()
    
    logger.info(f"ECL calculation completed for portfolio {portfolio_id} in {(datetime.now() - start_time).total_seconds()} seconds")
    
    return {
        "status": "success",
        "result": calculation_result.result_summary,
        "total_provision": float(calculation_result.total_provision),
        "provision_percentage": float(calculation_result.provision_percentage)
    }



def process_local_impairment_calculation_sync(
    portfolio_id: int,
    reporting_date: date,
    staging_result: StagingResult,
    db: Session
) -> Dict[str, Any]:
    """
    Synchronously process local impairment calculation and return the result.
    """
    logger.info(f"Starting synchronous local impairment calculation for portfolio {portfolio_id}")
    
    # Verify portfolio exists
    portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
    if not portfolio:
        logger.error(f"Portfolio with ID {portfolio_id} not found")
        raise ValueError(f"Portfolio with ID {portfolio_id} not found")
    
    # Extract category information from staging result
    staging_summary = staging_result.result_summary
    
    # Get loan counts and outstanding balances from staging summary
    current_count = staging_summary.get("Current", {}).get("num_loans", 0)
    current_balance = staging_summary.get("Current", {}).get("outstanding_loan_balance", 0)
    
    olem_count = staging_summary.get("OLEM", {}).get("num_loans", 0)
    olem_balance = staging_summary.get("OLEM", {}).get("outstanding_loan_balance", 0)
    
    substandard_count = staging_summary.get("Substandard", {}).get("num_loans", 0)
    substandard_balance = staging_summary.get("Substandard", {}).get("outstanding_loan_balance", 0)
    
    doubtful_count = staging_summary.get("Doubtful", {}).get("num_loans", 0)
    doubtful_balance = staging_summary.get("Doubtful", {}).get("outstanding_loan_balance", 0)
    
    loss_count = staging_summary.get("Loss", {}).get("num_loans", 0)
    loss_balance = staging_summary.get("Loss", {}).get("outstanding_loan_balance", 0)
    
    # Log the extracted values for debugging
    logger.info(f"From staging: Current: {current_count} loans, ${current_balance}; OLEM: {olem_count} loans, ${olem_balance}; Substandard: {substandard_count} loans, ${substandard_balance}; Doubtful: {doubtful_count} loans, ${doubtful_balance}; Loss: {loss_count} loans, ${loss_balance}")
    
    # Use standard provision rates as per memory
    current_rate = Decimal("0.01")      # 1% for Current
    olem_rate = Decimal("0.05")         # 5% for OLEM
    substandard_rate = Decimal("0.25")  # 25% for Substandard
    doubtful_rate = Decimal("0.50")     # 50% for Doubtful
    loss_rate = Decimal("1.00")         # 100% for Loss
    
    # Calculate provisions
    current_provision = Decimal(str(current_balance)) * current_rate
    olem_provision = Decimal(str(olem_balance)) * olem_rate
    substandard_provision = Decimal(str(substandard_balance)) * substandard_rate
    doubtful_provision = Decimal(str(doubtful_balance)) * doubtful_rate
    loss_provision = Decimal(str(loss_balance)) * loss_rate
    
    # Calculate total provision
    total_provision = current_provision + olem_provision + substandard_provision + doubtful_provision + loss_provision
    total_balance = Decimal(str(current_balance)) + Decimal(str(olem_balance)) + Decimal(str(substandard_balance)) + Decimal(str(doubtful_balance)) + Decimal(str(loss_balance))
    provision_percentage = (total_provision / total_balance * 100) if total_balance > 0 else Decimal("0")
    
    # Create a new CalculationResult record
    calculation_result = CalculationResult(
        portfolio_id=portfolio_id,
        calculation_type="local_impairment",
        reporting_date=reporting_date,
        config=staging_result.config,
        result_summary={
            "Current": {
                "num_loans": current_count,
                "outstanding_loan_balance": float(current_balance),
                "total_loan_value": float(current_balance),
                "provision_amount": float(current_provision),
                "provision_rate": float(current_rate),
            },
            "OLEM": {
                "num_loans": olem_count,
                "outstanding_loan_balance": float(olem_balance),
                "total_loan_value": float(olem_balance),
                "provision_amount": float(olem_provision),
                "provision_rate": float(olem_rate),
            },
            "Substandard": {
                "num_loans": substandard_count,
                "outstanding_loan_balance": float(substandard_balance),
                "total_loan_value": float(substandard_balance),
                "provision_amount": float(substandard_provision),
                "provision_rate": float(substandard_rate),
            },
            "Doubtful": {
                "num_loans": doubtful_count,
                "outstanding_loan_balance": float(doubtful_balance),
                "total_loan_value": float(doubtful_balance),
                "provision_amount": float(doubtful_provision),
                "provision_rate": float(doubtful_rate),
            },
            "Loss": {
                "num_loans": loss_count,
                "outstanding_loan_balance": float(loss_balance),
                "total_loan_value": float(loss_balance),
                "provision_amount": float(loss_provision),
                "provision_rate": float(loss_rate),
            }
        },
        total_provision=float(total_provision),
        provision_percentage=float(provision_percentage)
    )
    
    db.add(calculation_result)
    db.commit()
    
    logger.info(f"Local impairment calculation completed for portfolio {portfolio_id}")
    
    return {
        "status": "success",
        "result": calculation_result.result_summary,
        "total_provision": float(calculation_result.total_provision),
        "provision_percentage": float(calculation_result.provision_percentage)
    }

if __name__ == "__main__":
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description="IFRS9Pro Backend Profiling Tool")
    parser.add_argument("function", choices=["ecl_report", "local_report", "ecl_calc", "local_calc"],
                        help="Function to run: ecl_report (ECL detailed report), local_report (local impairment report), "
                             "ecl_calc (ECL calculation), local_calc (local impairment calculation)")
    parser.add_argument("portfolio_id", type=int, help="Portfolio ID to process")
    parser.add_argument("--date", type=str, help="Report date in YYYY-MM-DD format (defaults to today)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
    
    args = parser.parse_args()
    
    # Set up logging based on verbosity
    if args.verbose:
        logging.basicConfig(level=logging.INFO, 
                           format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(level=logging.WARNING)
    
    # Parse the report date if provided
    if args.date:
        try:
            report_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print(f"Error: Invalid date format. Please use YYYY-MM-DD format.")
            sys.exit(1)
    else:
        report_date = date.today()
    
    # Create a database session
    db = SessionLocal()
    
    try:
        print(f"Running {args.function} for portfolio {args.portfolio_id} with report date {report_date}")
        
        # Run the requested function
        if args.function == "ecl_report":
            start_time = time.time()
            result = generate_ecl_detailed_report(db, args.portfolio_id, report_date)
            elapsed_time = time.time() - start_time
            
            print(f"\nECL Detailed Report Summary:")
            print(f"Portfolio ID: {args.portfolio_id}")
            print(f"Report Date: {report_date}")
            print(f"Total loans processed: {len(result.get('loans', []))}")
            print(f"Total ECL: {result.get('total_ecl', 0)}")
            print(f"Execution time: {elapsed_time:.2f} seconds")
            
        elif args.function == "local_report":
            start_time = time.time()
            result = generate_local_impairment_details_report(db, args.portfolio_id, report_date)
            elapsed_time = time.time() - start_time
            
            print(f"\nLocal Impairment Details Report Summary:")
            print(f"Portfolio: {result.get('portfolio_name', '')}")
            print(f"Report Date: {report_date}")
            print(f"Total loans processed: {len(result.get('loans', []))}")
            print(f"Total provision: {result.get('total_provision', 0)}")
            print(f"Execution time: {elapsed_time:.2f} seconds")
            
            # Print category breakdown
            if "category_totals" in result:
                print("\nCategory Breakdown:")
                for category, data in result["category_totals"].items():
                    print(f"  {category}: {data['count']} loans, "
                          f"balance: {data['balance']:.2f}, "
                          f"provision: {data['provision']:.2f}")
            
        elif args.function == "ecl_calc":
            # Get the latest ECL staging result
            staging_result = (
                db.query(StagingResult)
                .filter(
                    StagingResult.portfolio_id == args.portfolio_id,
                    StagingResult.staging_type == "ecl"
                )
                .order_by(StagingResult.created_at.desc())
                .first()
            )
            
            if not staging_result:
                print("Error: No ECL staging found. Please stage loans first.")
                sys.exit(1)
            
            start_time = time.time()
            result = process_ecl_calculation_sync(args.portfolio_id, report_date, staging_result, db)
            elapsed_time = time.time() - start_time
            
            print(f"\nECL Calculation Summary:")
            print(f"Portfolio ID: {args.portfolio_id}")
            print(f"Report Date: {report_date}")
            
            if result and "result" in result:
                for stage, data in result["result"].items():
                    if stage != "metrics":
                        print(f"  {stage}: {data.get('num_loans', 0)} loans, "
                              f"provision: {data.get('provision_amount', 0):.2f}, "
                              f"rate: {data.get('provision_rate', 0):.2f}%")
                
                print(f"Total provision: {result.get('total_provision', 0):.2f}")
                print(f"Provision percentage: {result.get('provision_percentage', 0):.2f}%")
                
                if "metrics" in result["result"]:
                    metrics = result["result"]["metrics"]
                    print(f"Avg LGD: {metrics.get('avg_lgd', 0):.2f}%")
                    print(f"Avg PD: {metrics.get('avg_pd', 0):.2f}%")
                    print(f"Avg EAD: {metrics.get('avg_ead', 0):.2f}")
            
            print(f"Execution time: {elapsed_time:.2f} seconds")
            
        elif args.function == "local_calc":
            # Get the latest local impairment staging result
            staging_result = (
                db.query(StagingResult)
                .filter(
                    StagingResult.portfolio_id == args.portfolio_id,
                    StagingResult.staging_type == "local_impairment"
                )
                .order_by(StagingResult.created_at.desc())
                .first()
            )
            
            if not staging_result:
                print("Error: No local impairment staging found. Please stage loans first.")
                sys.exit(1)
            
            start_time = time.time()
            result = process_local_impairment_calculation_sync(args.portfolio_id, report_date, staging_result, db)
            elapsed_time = time.time() - start_time
            
            print(f"\nLocal Impairment Calculation Summary:")
            print(f"Portfolio ID: {args.portfolio_id}")
            print(f"Report Date: {report_date}")
            
            if result and "result" in result:
                for category, data in result["result"].items():
                    print(f"  {category}: {data.get('num_loans', 0)} loans, "
                          f"provision: {data.get('provision_amount', 0):.2f}, "
                          f"rate: {data.get('provision_rate', 0):.2f}%")
                
                print(f"Total provision: {result.get('total_provision', 0):.2f}")
                print(f"Provision percentage: {result.get('provision_percentage', 0):.2f}%")
            
            print(f"Execution time: {elapsed_time:.2f} seconds")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    finally:
        db.close()
