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
    calculate_marginal_ecl,
    is_in_range
)

from app.utils.ecl_calculator import (
    get_amortization_schedule,
    get_ecl_by_stage,
    calculate_loss_given_default
)
from app.utils.staging import parse_days_range



# Set up logging
logger = logging.getLogger(__name__)



# Function to determine stage based on configuration
def get_loan_stage(loan, stage_1_range, stage_2_range, stage_3_range):
    if loan.ndia is None:
        return "Stage 1"  # Default if NDIA is not available
    
    ndia = loan.ndia
    
    # Use is_in_range from app.calculators.ecl
    if is_in_range(ndia, stage_1_range):
        return "Stage 1"
    elif is_in_range(ndia, stage_2_range):
        return "Stage 2"
    elif is_in_range(ndia, stage_3_range):
        return "Stage 3"
    else:
        return "Stage 3"  # Default to Stage 3 if not in any range

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
    # Get all loans for the portfolio
    loans = db.query(Loan).filter(
        Loan.portfolio_id == portfolio_id,
        Loan.outstanding_loan_balance > 0  # Only get loans with balance
    ).all()

    # Get the staging configuration
    latest_staging = (
        db.query(StagingResult)
        .filter(
            StagingResult.portfolio_id == portfolio_id,
            StagingResult.staging_type == "ecl"
        )
        .order_by(StagingResult.created_at.desc())
        .first()
    )

    config = latest_staging.config
    # Parse the day ranges from configuration
    stage_1_range = parse_days_range(config["stage_1"]["days_range"])
    stage_2_range = parse_days_range(config["stage_2"]["days_range"])
    stage_3_range = parse_days_range(config["stage_3"]["days_range"])

    total_ead = Decimal(0.0)
    total_lgd = Decimal(0.0)
    total_ecl = Decimal(0.0)
    loan_data = []

    # OPTIMIZATION 1: Preload securities for all loans at once
    employee_ids = list(set([loan.employee_id for loan in loans if loan.employee_id]))
    securities_with_clients = (
        db.query(Security, Client)
        .join(Client, Security.client_id == Client.id)
        .filter(Client.employee_id.in_(employee_ids))
        .all()
    )

    client_securities = {}
    for security, client in securities_with_clients:
        if client and client.employee_id:
            if client.employee_id not in client_securities:
                client_securities[client.employee_id] = []
            client_securities[client.employee_id].append(security)

    # OPTIMIZATION 2: Create a PD cache
    pd_cache = {}

    # Process each loan
    for loan in loans:
        # Calculate EAD
        ead = calculate_exposure_at_default_percentage(loan, report_date)
        total_ead += ead

        # Calculate LGD
        securities = client_securities.get(loan.employee_id, [])
        lgd = calculate_loss_given_default(loan, securities)
        lgd_amount = Decimal(lgd) * loan.outstanding_loan_balance
        total_lgd += lgd_amount
        
        # Get or calculate PD
        if loan.id in pd_cache:
            pd_value = pd_cache[loan.id]
        else:
            pd_value = calculate_probability_of_default(loan, db)
            pd_cache[loan.id] = pd_value

        # Get loan stage
        stage = get_loan_stage(loan, stage_1_range, stage_2_range, stage_3_range)
        stage_num = int(stage.split()[-1])  # Extract stage number (1, 2, or 3)
        
        # OPTIMIZATION 3: Skip detailed calculation for Stage 3 or incomplete data
        # Use simplified calculation
        ecl_value = float(ead) * float(pd_value) * float(lgd) / 100.0
        # if stage == "Stage 3" or not (loan.loan_amount and loan.loan_term and 
        #                               loan.monthly_installment and loan.loan_issue_date):
        #     # Use simplified calculation
        #     ecl_value = float(ead) * float(pd_value) * float(lgd) / 100.0
        # else:
        #     try:
        #         # Format dates for amortization schedule
        #          start_date = loan.loan_issue_date.strftime("%d/%m/%Y")
        #         report_date_str = report_date.strftime("%d/%m/%Y")
                
        #         # Calculate effective interest rate
        #         effective_interest_rate = calculate_effective_interest_rate_lender(
        #             float(loan.loan_amount),
        #             float(loan.administrative_fees) if loan.administrative_fees else 0,
        #             loan.loan_term,
        #             float(loan.monthly_installment)
        #         )
                
        #         # Get amortization schedule and ECL values
        #         schedule, ecl_12_month, ecl_lifetime = get_amortization_schedule(
        #             loan_amount=float(loan.loan_amount),
        #             loan_term=loan.loan_term,
        #             annual_interest_rate=effective_interest_rate,
        #             monthly_installment=float(loan.monthly_installment),
        #             start_date=start_date,
        #             reporting_date=report_date_str,
        #             pd=pd_value,
        #             loan=loan,
        #             db=db
        #         )
                
        #         # Get ECL value based on stage
        #         ecl_value = get_ecl_by_stage(schedule, ecl_12_month, ecl_lifetime, stage_num)
        #     except Exception as e:
        #         # Fallback to simplified calculation
        #         if "Reporting month not found in schedule" in str(e):
        #             ecl_value = float(ead) * float(pd_value) * float(lgd) / 100.0
        #         else:
        #             ecl_value = float(ead) * float(pd_value) * float(lgd) / 100.0
        
        # Add to total ECL
        total_ecl += Decimal(str(ecl_value))
        
        Store loan data
        loan_data.append({
            "loan_id": loan.id,
            "employee_id": loan.employee_id,
            "stage": stage,
            "ead": ead,
            "lgd": lgd,
            "pd": pd_value,
            "ecl": ecl_value
        })
        
    report_data = {
        "portfolio_id": portfolio_id,
        "report_date": report_date.strftime("%Y-%m-%d"),
        "report_type": "ecl_detailed_report",
        "report_run_date": datetime.now().strftime("%Y-%m-%d"),
        "description": "ECL Detailed Report",
        "total_ead": total_ead,
        "total_lgd": total_lgd,
        "total_ecl": total_ecl,
        "loans": loan_data,
    }
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
    # Implementation will go here
    pass


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
    # Implementation will go here
    pass


def process_local_impairment_calculation_sync(
    portfolio_id: int,
    reporting_date: date,
    staging_result: StagingResult,
    db: Session
) -> Dict[str, Any]:
    """
    Synchronously process local impairment calculation and return the result.
    """
    # Implementation will go here
    pass


if __name__ == "__main__":
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description="IFRS9Pro Backend Rewrites Tool")
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
            print(f"Total EAD: {result.get('total_ead', 0)}")
            print(f"Total LGD: {result.get('total_lgd', 0)}")
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
