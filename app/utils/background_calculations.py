import asyncio
import multiprocessing
import logging
import threading
import random
import pandas as pd
from typing import Optional, Dict, Any
from datetime import date, datetime
from decimal import Decimal
from sqlalchemy.orm import Session
import pickle
from app.database import SessionLocal
from app.models import (
    Portfolio, Loan, Client, Security, StagingResult, CalculationResult
)
from app.utils.background_tasks import get_task_manager, run_background_task
from app.utils.ecl_calculator import (
    calculate_loss_given_default,
    calculate_exposure_at_default_percentage, calculate_marginal_ecl, is_in_range,
    get_amortization_schedule, get_ecl_by_stage, calculate_effective_interest_rate_lender
)
from app.utils.process_email_notifyer import (
    send_calc_ecl_started_email,
    send_calc_ecl_success_email,
    send_calc_ecl_failed_email,
    send_calc_local_impairment_started_email,
    send_calc_local_impairment_success_email,
    send_calc_local_impairment_failed_email,
)
from app.calculators.ecl import calculate_probability_of_default
from app.utils.staging import parse_days_range
from sqlalchemy import func
from dateutil.relativedelta import relativedelta


logger = logging.getLogger(__name__)

import asyncio
from concurrent.futures import ProcessPoolExecutor
from sqlalchemy.orm import Session
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

# --- Correct safe_float ---
def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

# --- Move process_loan to pure sync ---
def process_loan_sync(loan_data, selected_dt_str):
    try:
        selected_dt = pd.to_datetime(selected_dt_str)
        principal = safe_float(loan_data.get('loan_amount', 0))
        term_months = int(loan_data.get('loan_term', 0))
        arrears = safe_float(loan_data.get('accumulated_arrears', 0))
        start_date = pd.to_datetime(loan_data.get('deduction_start_period'))
        monthly_installment = safe_float(loan_data.get('monthly_installment', 0))
        pd_rate=safe_float(loan_data.get('pd_value', 0))
        end_date = start_date + relativedelta(months=term_months)
        employee_id=loan_data.get('employee_id')
        administrative_fees=safe_float(loan_data.get('administrative_fees', 0))
        submission_period=pd.to_datetime(loan_data.get('submission_period'))
        maturity_period=pd.to_datetime(loan_data.get('maturity_period'))
        loan_result = {}


        

        # Handle matured or future loans
        # if end_date <= selected_dt or (start_date > selected_dt and arrears <= 0):
        #     loan_result['eir'] = round(eir, 2) if eir else 0.0
            
        #     for field in ['amortised_bal', 'adjusted_amortised_bal', 'theoretical_balance', 'ecl_lifetime', 'ecl_12']:
        #         loan_result[field] = 0.0 if arrears <= 0 else round(arrears, 2)
        #     return loan_data['id'], loan_result

        # if principal <= 0 or term_months <= 0 or monthly_installment <= 0:
        #     for field in ['ead', 'pd', 'final_ecl', 'ecl_12', 'ecl_lifetime']:
        #         loan_result[field] = 0.0
        #     return loan_data['id'], loan_result
        

        # Calculate EIR and store
        eir = calculate_effective_interest_rate_lender(
            loan_amount=principal,
            administrative_fees=administrative_fees,
            loan_term=term_months,
            monthly_payment=monthly_installment,
            submission_period=submission_period,
            report_date=selected_dt,
            maturity_period=maturity_period,
            
            
        )
        loan_result['eir'] = eir if isinstance(eir, float) else eir


        # Calculation of loss given default
        loan_result['lgd']=1 #given all loans are unsecured


        # Calculate EIR and store
        
        loan_result['pd'] = pd_rate
        pd_monthly = pd_rate / 12 if isinstance(pd_rate, float) else 0.0

        # Determining latest amortised loan balance before report-date
        eir_monthly = safe_float(eir) / 12
        delta_months = max(0, (selected_dt.year - start_date.year) * 12 + (selected_dt.month - start_date.month))
        balance = principal-administrative_fees
        for _ in range(1, delta_months + 1):
            interest = balance * eir_monthly
            balance = balance + interest - monthly_installment
            balance = max(0, balance)  # Cannot be negative
        loan_result['amortised_bal'] = loan_result['theoretical_balance'] = round(balance, 2)
        adjusted_balance = round(balance + arrears,2)
        loan_result['adjusted_amortised_bal'] = round(adjusted_balance, 0)
        loan_result['ead'] = round(adjusted_balance, 2)
        

        # ECL Calculation
        current_balance = adjusted_balance
        discounted_el_schedule = []
        remaining_months = max(0, term_months - delta_months)


        for m in range(1, remaining_months + 1):
            interest = current_balance * eir_monthly
            current_balance = current_balance + interest - monthly_installment
            current_balance = max(0, current_balance)
            expected_loss = current_balance * pd_monthly
            discount_factor = 1 / ((1 + eir_monthly) ** m)
            discounted_el_schedule.append(expected_loss * discount_factor)

        loan_result['ecl_12'] = round(sum(discounted_el_schedule[:12]), 2)
        loan_result['ecl_lifetime'] = round(sum(discounted_el_schedule), 2)
        loan_result['calculation_date'] = selected_dt.to_pydatetime() 

        # Determining final ECL
        if loan_data.get('ifrs9_stage') == "Stage 1":
            loan_result['final_ecl'] = loan_result['ecl_12']
        else:
            loan_result['final_ecl'] = loan_result['ecl_lifetime']

        return loan_data['id'], loan_result

    except Exception as e:
        print(f"Error processing loan ID {loan_data.get('id', 'unknown')}: {e}")
        return loan_data.get('id', None)

# --- Main controller for ECL calculations---
async def process_ecl_calculation_sync(portfolio_id: int, reporting_date: str, db: Session, user_email, first_name):
    try:
        # -------------------------------------------------------
        # 1. Always Send Start Email
        # -------------------------------------------------------
        try:
            await send_calc_ecl_started_email(
                user_email, first_name, portfolio_id,
                cc_emails=["support@service4gh.com"]
            )
        except Exception as e:
            logger.error(f"Failed to send ECL calculation STARTED email: {e}")

        # -------------------------------------------------------
        # 2. MAIN CALCULATION LOGIC (unchanged)
        # -------------------------------------------------------
        selected_dt_str = reporting_date
        batch_size = 500
        max_workers = max(1, multiprocessing.cpu_count() - 1)

        updates = []
        processed_count = 0
        loan_count = 0
        grand_total_ecl = 0

        executor = ProcessPoolExecutor(max_workers=max_workers)

        offset = 0

        first_loan = db.query(Loan).order_by(Loan.id).filter(Loan.portfolio_id == portfolio_id).first()
        if not first_loan:
            return {"error": "No loans found."}
        portfolio_id = first_loan.portfolio_id

        while True:
            loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).order_by(Loan.id).offset(offset).limit(batch_size).all()
            if not loans:
                break
            
            tasks = []

            for loan in loans:
                pd_value = calculate_probability_of_default(
                    employee_id=loan.employee_id,
                    outstanding_loan_balance=loan.outstanding_loan_balance,
                    start_date=loan.deduction_start_period,
                    selected_dt=selected_dt_str,
                    end_date=loan.maturity_period,
                    arrears=loan.accumulated_arrears,
                    db=db
                )

                loan_data = {
                    "id": loan.id,
                    "loan_amount": loan.loan_amount,
                    "loan_term": loan.loan_term,
                    "accumulated_arrears": loan.accumulated_arrears,
                    "deduction_start_period": loan.deduction_start_period,
                    "monthly_installment": loan.monthly_installment,
                    "administrative_fees": loan.administrative_fees,
                    "ifrs9_stage": loan.ifrs9_stage,
                    "pd_value": pd_value,
                    "submission_period": loan.submission_period,
                    "maturity_period": loan.maturity_period,
                }

                task = asyncio.get_running_loop().run_in_executor(
                    executor, process_loan_sync, loan_data, selected_dt_str
                )
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in results:
                if isinstance(res, tuple) and res[1] is not None:
                    loan_id, loan_fields = res
                    db.query(Loan).filter(Loan.id == loan_id).update(loan_fields)
                    loan_count += 1
                    grand_total_ecl += loan_fields.get("final_ecl", 0.0)

            db.commit()
            processed_count += len(loans)
            print(f"Processed and saved {processed_count} loans...")

            offset += batch_size

        # Save summary record
        calculation_result = CalculationResult(
            portfolio_id=portfolio_id,
            calculation_type="ecl",
            total_provision=grand_total_ecl,
            provision_percentage=0.0,
            reporting_date=datetime.now().date(),
            config={},
            result_summary={}
        )
        db.add(calculation_result)
        db.commit()

        # -------------------------------------------------------
        # 3. SEND SUCCESS EMAIL (only if NO errors)
        # -------------------------------------------------------
        try:
            await send_calc_ecl_success_email(
                user_email, first_name, portfolio_id,
                cc_emails=["support@service4gh.com"]
            )
        except Exception as e:
            logger.error(f"Failed to send ECL calculation SUCCESS email: {e}")

        # Final return
        return {
            "calculation_id": calculation_result.id,
            "portfolio_id": calculation_result.portfolio_id,
            "grand_total_ecl": safe_float(grand_total_ecl),
            "provision_percentage": 0.0,
            "loan_count": loan_count
        }

    except Exception as e:
        logger.error(f"Failed to process ECL calculation: {e}")

        # -------------------------------------------------------
        # 4. SEND FAILURE EMAIL (only when exception occurs)
        # -------------------------------------------------------
        try:
            await send_calc_ecl_failed_email(
                user_email, first_name, portfolio_id,
                cc_emails=["support@service4gh.com"]
            )
        except Exception as inner_e:
            logger.error(f"Failed to send ECL calculation FAILED email: {inner_e}")

        return {"error": str(e)}


# --- Move process_loan to pure sync ---
def process_loan_local_sync(loan_data, relevant_prov_rate, selected_dt_str):
    try:
        selected_dt = pd.to_datetime(selected_dt_str)
        ead = Decimal(safe_float(loan_data.get('ead', 0)))
        if not loan_data.get('bog_stage'):
            print(f"Warning: Loan ID {loan_data['id']} has no bog_stage!")

        loan_result = {}
        rate=relevant_prov_rate
        loan_result = {}
        loan_result['bog_prov_rate']=relevant_prov_rate
        loan_result['bog_provision'] = round(float(rate * ead),2)
        return loan_data['id'], loan_result

    except Exception as e:
        print(f"Error processing loan ID {loan_data.get('id', 'unknown')}: {e}")
        return loan_data.get('id', None)


async def process_bog_impairment_calculation_sync(
    portfolio_id: int,
    reporting_date: str,
    db: Session,
    user_email,
    first_name
):
    # -------------------------------------------------------
    # 1. ALWAYS SEND START EMAIL
    # -------------------------------------------------------
    try:
        await send_calc_local_impairment_started_email(
            user_email, first_name, portfolio_id,
            cc_emails=["support@service4gh.com"]
        )
    except Exception as e:
        logger.error(f"Failed to send LOCAL impairment STARTED email: {e}")

    try:
        # -------------------------------------------------------
        # 2. MAIN IMPAIRMENT CALCULATION LOGIC
        # -------------------------------------------------------
        selected_dt_str = reporting_date
        batch_size = 500
        max_workers = max(1, multiprocessing.cpu_count() - 1)

        processed_count = 0
        loan_count = 0
        grand_total_local = 0

        executor = ProcessPoolExecutor(max_workers=max_workers)
        offset = 0

        # Verify portfolio has at least one loan
        first_loan = (
            db.query(Loan)
            .filter(Loan.portfolio_id == portfolio_id)
            .order_by(Loan.id)
            .first()
        )

        if not first_loan:
            return {"error": "No loans found."}

        while True:
            # Fetch BOG staging/impairment rules
            latest_bog_config = (
                db.query(Portfolio.bog_staging_config)
                .filter(Portfolio.id == portfolio_id)
                .scalar()
            )

            if not latest_bog_config:
                return {"error": "No BOG staging rules found. Update BOG staging rules."}

            provision_config = latest_bog_config or {}

            # Fetch loans in batches
            loans = (
                db.query(Loan)
                .filter(Loan.portfolio_id == portfolio_id)
                .order_by(Loan.id)
                .offset(offset)
                .limit(batch_size)
                .all()
            )

            if not loans:
                break

            tasks = []

            for loan in loans:
                stage_key = (loan.bog_stage or "").lower()
                relevant_prov_rate = Decimal(
                    provision_config.get(stage_key, {}).get("rate", 0)
                )

                loan_data = {
                    "id": loan.id,
                    "ead": loan.ead,
                    "bog_stage": loan.bog_stage,
                }

                task = asyncio.get_running_loop().run_in_executor(
                    executor,
                    process_loan_local_sync,
                    loan_data,
                    relevant_prov_rate,
                    selected_dt_str,
                )
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in results:
                if isinstance(res, tuple) and res[1] is not None:
                    loan_id, loan_fields = res
                    db.query(Loan).filter(Loan.id == loan_id).update(loan_fields)
                    loan_count += 1
                    grand_total_local += loan_fields.get("bog_provision", 0.0)

            db.commit()
            processed_count += len(loans)
            print(f"Processed and saved {processed_count} loans...")

            offset += batch_size

        # -------------------------------------------------------
        # 3. SAVE SUMMARY RECORD
        # -------------------------------------------------------
        calculation_result = CalculationResult(
            portfolio_id=portfolio_id,
            calculation_type="local_impairment",
            total_provision=grand_total_local,
            provision_percentage=0.0,
            reporting_date=datetime.now().date(),
            config={},
            result_summary={}
        )
        db.add(calculation_result)
        db.commit()

        # -------------------------------------------------------
        # 4. SEND SUCCESS EMAIL (only if no exception occurred)
        # -------------------------------------------------------
        try:
            await send_calc_local_impairment_success_email(
                user_email, first_name, portfolio_id,
                cc_emails=["support@service4gh.com"]
            )
        except Exception as e:
            logger.error(f"Failed to send LOCAL impairment SUCCESS email: {e}")

        return {
            "calculation_id": calculation_result.id,
            "portfolio_id": calculation_result.portfolio_id,
            "grand_total_local": safe_float(grand_total_local),
            "loan_count": loan_count,
        }

    # -------------------------------------------------------
    # 5. FAILURE CASE — send only ON ERROR
    # -------------------------------------------------------
    except Exception as e:
        logger.error(f"Local impairment calculation FAILED: {e}")

        try:
            await send_calc_local_impairment_failed_email(
                user_email, first_name, portfolio_id,
                cc_emails=["support@service4gh.com"]
            )
        except Exception as inner:
            logger.error(f"Failed to send LOCAL impairment FAILED email: {inner}")

        return {"error": str(e)}


# async def process_local_impairment_calculation(
#     task_id: str,
#     portfolio_id: int,
#     reporting_date: date,
#     db: Session
# ):
#     """
#     Process local impairment calculation in the background with progress reporting.
#     """
#     try:
#         get_task_manager().update_progress(
#             task_id,
#             progress=5,
#             status_message=f"Starting local impairment calculation for portfolio {portfolio_id}"
#         )
#         await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
#         # Verify portfolio exists
#         portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
#         if not portfolio:
#             raise ValueError(f"Portfolio with ID {portfolio_id} not found")
        
#         get_task_manager().update_progress(
#             task_id,
#             progress=10,
#             status_message="Retrieving latest local impairment staging data"
#         )
#         await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
#         # Get the latest local impairment staging result
#         latest_staging = (
#             db.query(StagingResult)
#             .filter(
#                 StagingResult.portfolio_id == portfolio_id,
#                 StagingResult.staging_type == "local_impairment"
#             )
#             .order_by(StagingResult.created_at.desc())
#             .first()
#         )
        
#         if not latest_staging:
#             raise ValueError("No local impairment staging found. Please stage loans first.")
        
#         # Extract config from the staging result
#         config = latest_staging.config
#         if not config:
#             raise ValueError("Invalid staging configuration")
        
#         get_task_manager().update_progress(
#             task_id,
#             progress=20,
#             status_message="Processing loan staging data"
#         )
#         await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
#         # Get the loan staging data from the result_summary
#         staging_data = []
#         if "loans" in latest_staging.result_summary:
#             # New format with detailed loan data
#             staging_data = latest_staging.result_summary["loans"]
#         else:
#             # Without detailed loan data, we need to re-stage based on summary stats
#             logger.warning("No detailed loan staging data in result_summary, reconstructing staging using database query")
            
#             # Recreate basic staging info from loan query using the config
#             try:
#                 current_range = parse_days_range(config["current"]["days_range"])
#                 olem_range = parse_days_range(config["olem"]["days_range"])
#                 substandard_range = parse_days_range(config["substandard"]["days_range"])
#                 doubtful_range = parse_days_range(config["doubtful"]["days_range"])
#                 loss_range = parse_days_range(config["loss"]["days_range"])
#             except (KeyError, ValueError) as e:
#                 logger.error(f"Error parsing day ranges: {str(e)}")
#                 raise ValueError(f"Could not parse staging configuration: {str(e)}")
                
#             # Get the loans
#             loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
            
#             logger.info(f"Found {len(loans)} loans for portfolio {portfolio_id}")
            
#             get_task_manager().update_progress(
#                 task_id,
#                 progress=30,
#                 status_message=f"Re-staging {len(loans)} loans based on configuration"
#             )
#             await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
            
                
#         if not staging_data:
#             # If we still don't have staging data, return an error
#             raise ValueError("No loan staging data found. Please re-run the staging process.")
            
#         get_task_manager().update_progress(
#             task_id,
#             progress=40,
#             status_message="Retrieving loan data"
#         )
#         await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
#         # Get all loans in the portfolio
#         loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
        
#         # Create a map of loan_id to loan object for faster lookup
#         loan_map = {loan.id: loan for loan in loans}

#         # Initialize category tracking
#         current_loans = []
#         olem_loans = []
#         substandard_loans = []
#         doubtful_loans = []
#         loss_loans = []

#         # Calculate totals for each category
#         current_total = 0
#         olem_total = 0
#         substandard_total = 0
#         doubtful_total = 0
#         loss_total = 0

#         # Get provision rates from config
#         try:
#             # Check if we have a provision_config object
#             provision_config = config.get("provision_config", {})
#             if provision_config:
#                 # Get rates from provision_config
#                 current_rate = Decimal(provision_config.get("Current", 0.01))
#                 olem_rate = Decimal(provision_config.get("OLEM", 0.03))
#                 substandard_rate = Decimal(provision_config.get("Substandard", 0.2))
#                 doubtful_rate = Decimal(provision_config.get("Doubtful", 0.5))
#                 loss_rate = Decimal(provision_config.get("Loss", 1.0))
#             else:
#                 # Fall back to old structure
#                 current_rate = Decimal(config.get("current", {}).get("rate", 0.01))
#                 olem_rate = Decimal(config.get("olem", {}).get("rate", 0.03))
#                 substandard_rate = Decimal(config.get("substandard", {}).get("rate", 0.2))
#                 doubtful_rate = Decimal(config.get("doubtful", {}).get("rate", 0.5))
#                 loss_rate = Decimal(config.get("loss", {}).get("rate", 1.0))
            
#             # Log the rates being used
#             logger.info(f"Using provision rates - Current: {current_rate}, OLEM: {olem_rate}, Substandard: {substandard_rate}, Doubtful: {doubtful_rate}, Loss: {loss_rate}")
            
#         except (KeyError, ValueError) as e:
#             logger.error(f"Error parsing provision rates: {str(e)}")
#             raise ValueError(f"Could not parse provision rates from configuration: {str(e)}")

#         # Calculate stage statistics
#         stage_stats = {}
#         for stage in ["Current", "OLEM", "Substandard", "Doubtful", "Loss"]:
#             stage_loans = [loan for loan in staging_data if loan["stage"] == stage]
#             total_balance = sum(loan["outstanding_loan_balance"] for loan in stage_loans)
#             stage_stats[stage] = {
#                 "num_loans": len(stage_loans),
#                 "total_loan_value": round(total_balance, 2),
#             }
            
#         logger.info(f"Local impairment stage statistics for portfolio {portfolio_id}:")
#         logger.info(f"Current: {stage_stats['Current']['num_loans']} loans, balance: {stage_stats['Current']['total_loan_value']}")
#         logger.info(f"OLEM: {stage_stats['OLEM']['num_loans']} loans, balance: {stage_stats['OLEM']['total_loan_value']}")
#         logger.info(f"Substandard: {stage_stats['Substandard']['num_loans']} loans, balance: {stage_stats['Substandard']['total_loan_value']}")
#         logger.info(f"Doubtful: {stage_stats['Doubtful']['num_loans']} loans, balance: {stage_stats['Doubtful']['total_loan_value']}")
#         logger.info(f"Loss: {stage_stats['Loss']['num_loans']} loans, balance: {stage_stats['Loss']['total_loan_value']}")
        
#         get_task_manager().update_progress(
#             task_id,
#             progress=60,
#             status_message=f"Calculating local impairment for {len(staging_data)} loans"
#         )
#         await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
#         # Process loans using staging data
#         total_items = len(staging_data)
#         for i, stage_info in enumerate(staging_data):
#             if i % 50 == 0:  # Update progress every 50 loans (more frequent updates)
#                 progress = 60 + (i / total_items) * 30  # Progress from 60% to 90%
#                 get_task_manager().update_progress(
#                     task_id,
#                     progress=round(progress, 2),  # Round to 2 decimal places
#                     processed_items=i,
#                     total_items=total_items,
#                     status_message=f"Calculating local impairment: Processed {i}/{total_items} loans ({round(i/total_items*100, 1)}%)"
#                 )
#                 await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
                
#             loan_id = stage_info.get("loan_id")
#             stage = stage_info.get("stage")
            
#             if not loan_id or not stage:
#                 logger.warning(f"Missing loan_id or stage in staging data: {stage_info}")
#                 continue
                
#             loan = loan_map.get(loan_id)
#             if not loan or loan.outstanding_loan_balance is None:
#                 logger.warning(f"Loan {loan_id} not found or has no outstanding balance")
#                 continue
                
#             outstanding_loan_balance = loan.outstanding_loan_balance
            
#             # Update category totals based on the assigned stage
#             if stage == "Current":
#                 current_loans.append(loan)
#                 current_total += outstanding_loan_balance
#             elif stage == "OLEM":
#                 olem_loans.append(loan)
#                 olem_total += outstanding_loan_balance
#             elif stage == "Substandard":
#                 substandard_loans.append(loan)
#                 substandard_total += outstanding_loan_balance
#             elif stage == "Doubtful":
#                 doubtful_loans.append(loan)
#                 doubtful_total += outstanding_loan_balance
#             elif stage == "Loss":
#                 loss_loans.append(loan)
#                 loss_total += outstanding_loan_balance
#             else:
#                 # Default to Loss if stage is something unexpected
#                 logger.warning(f"Unexpected stage '{stage}' for loan {loan_id}, treating as Loss")
#                 loss_loans.append(loan)
#                 loss_total += outstanding_loan_balance

#         get_task_manager().update_progress(
#             task_id,
#             progress=90,
#             status_message="Finalizing local impairment calculation results",
#         )
#         await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
#         # Calculate provisions for each category
#         current_provision = current_total * current_rate
#         olem_provision = olem_total * olem_rate
#         substandard_provision = substandard_total * substandard_rate
#         doubtful_provision = doubtful_total * doubtful_rate
#         loss_provision = loss_total * loss_rate

#         logger.info(f"Local impairment provisions for portfolio {portfolio_id}:")
#         logger.info(f"Current: rate={current_rate}, value={current_total}, provision={current_provision}")
#         logger.info(f"OLEM: rate={olem_rate}, value={olem_total}, provision={olem_provision}")
#         logger.info(f"Substandard: rate={substandard_rate}, value={substandard_total}, provision={substandard_provision}")
#         logger.info(f"Doubtful: rate={doubtful_rate}, value={doubtful_total}, provision={doubtful_provision}")
#         logger.info(f"Loss: rate={loss_rate}, value={loss_total}, provision={loss_provision}")
        
#         # Calculate total loan value and provision amount
#         total_loan_value = current_total + olem_total + substandard_total + doubtful_total + loss_total
#         total_provision = current_provision + olem_provision + substandard_provision + doubtful_provision + loss_provision

#        # Create a new CalculationResult record
#         calculation_result = CalculationResult(
#             portfolio_id=portfolio_id,
#             calculation_type="local_impairment",
#             config=config,  # Use the config from staging
#             result_summary={
#                 "Current": {
#                     "num_loans": len(current_loans),
#                     "total_loan_value": safe_float(current_total),
#                     "outstanding_loan_balance": safe_float(current_total),
#                     "provision_amount": safe_float(current_provision),
#                     "provision_rate": safe_float(current_rate),
#                 },
#                 "OLEM": {
#                     "num_loans": len(olem_loans),
#                     "total_loan_value": safe_float(olem_total),
#                     "outstanding_loan_balance": safe_float(olem_total),
#                     "provision_amount": safe_float(olem_provision),
#                     "provision_rate": safe_float(olem_rate),
#                 },
#                 "Substandard": {
#                     "num_loans": len(substandard_loans),
#                     "total_loan_value": safe_float(substandard_total),
#                     "outstanding_loan_balance": safe_float(substandard_total),
#                     "provision_amount": safe_float(substandard_provision),
#                     "provision_rate": safe_float(substandard_rate),
#                 },
#                 "Doubtful": {
#                     "num_loans": len(doubtful_loans),
#                     "total_loan_value": safe_float(doubtful_total),
#                     "outstanding_loan_balance": safe_float(doubtful_total),
#                     "provision_amount": safe_float(doubtful_provision),
#                     "provision_rate": safe_float(doubtful_rate),
#                 },
#                 "Loss": {
#                     "num_loans": len(loss_loans),
#                     "total_loan_value": safe_float(loss_total),
#                     "outstanding_loan_balance": safe_float(loss_total),
#                     "provision_amount": safe_float(loss_provision),
#                     "provision_rate": safe_float(loss_rate),
#                 },
#                 "total_loans": len(staging_data)
#             },
#             total_provision=safe_float(total_provision),
#             provision_percentage=safe_float(provision_percentage),
#             reporting_date=reporting_date
#         )
#         db.add(calculation_result)
#         db.commit()

#         get_task_manager().update_progress(
#             task_id,
#             progress=95,
#             status_message="Saving local impairment calculation results to database"
#         )
#         await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
#         get_task_manager().update_progress(
#             task_id,
#             progress=100,
#             status_message="Local impairment calculation completed successfully"
#         )
#         await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
#         # Return the calculation result ID
#         return {
#             "calculation_id": calculation_result.id,
#             "portfolio_id": portfolio_id,
#             "total_provision": safe_float(total_provision),
#             "provision_percentage": safe_float(provision_percentage),
#             "total_loans": len(staging_data)
#         }
        
#     except Exception as e:
#         logger.exception(f"Error during local impairment calculation: {str(e)}")
#         get_task_manager().update_progress(
#             task_id,
#             progress=100,
#             status_message=f"Error during local impairment calculation: {str(e)}"
#         )
#         raise


# def retrieve_loans_for_staging(portfolio_id: int, db: Session):
#     """
#     Retrieve loans for staging based on the portfolio ID.
#     Optimized for potentially large datasets.
#     """
#     # Get the latest ECL staging result
#     latest_staging = (
#         db.query(StagingResult)
#         .filter(
#             StagingResult.portfolio_id == portfolio_id,
#             StagingResult.staging_type == "ecl"
#         )
#         .order_by(StagingResult.created_at.desc())
#         .limit(1)
#         .first()
#     )
    
#     if not latest_staging:
#         return [], [], []
    
#     # Get all loans for this portfolio
#     all_loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
    
#     # Check if we have detailed loan data in the result_summary
#     if "loans" in latest_staging.result_summary:
#         # Extract loan IDs by stage
#         stage_1_ids = []
#         stage_2_ids = []
#         stage_3_ids = []
        
#         for loan in latest_staging.result_summary.get("loans", []):
#             if loan.get("stage") == 1:
#                 stage_1_ids.append(loan.get("id"))
#             elif loan.get("stage") == 2:
#                 stage_2_ids.append(loan.get("id"))
#             elif loan.get("stage") == 3:
#                 stage_3_ids.append(loan.get("id"))
        
#         # Get staged loans based on IDs
#         stage_1_loans = [loan for loan in all_loans if loan.id in stage_1_ids]
#         stage_2_loans = [loan for loan in all_loans if loan.id in stage_2_ids]
#         stage_3_loans = [loan for loan in all_loans if loan.id in stage_3_ids]
#     else:
#         # Alternative approach: use summary statistics to approximate
#         # This is a simplified approach and may not match exactly with the original staging
#         logger.warning("No detailed loan data in staging result, using summary statistics to approximate staging")
        
#         # Get summary counts
#         summary = latest_staging.result_summary
#         stage_1_count = summary.get("Stage 1", {}).get("num_loans", 0)
#         stage_2_count = summary.get("Stage 2", {}).get("num_loans", 0)
#         stage_3_count = summary.get("Stage 3", {}).get("num_loans", 0)
        
#         # Sort loans by outstanding loan balance for a simple approximation
#         sorted_loans = sorted(all_loans, key=lambda x: safe_float(x.outstanding_loan_balance) if x.outstanding_loan_balance is not None else 0, reverse=True)
        
#         # Distribute loans based on counts
#         stage_1_loans = []
#         stage_2_loans = []
#         stage_3_loans = []
        
#         idx = 0
#         for _ in range(stage_1_count):
#             if idx < len(sorted_loans):
#                 stage_1_loans.append(sorted_loans[idx])
#                 idx += 1
        
#         for _ in range(stage_2_count):
#             if idx < len(sorted_loans):
#                 stage_2_loans.append(sorted_loans[idx])
#                 idx += 1
        
#         for _ in range(stage_3_count):
#             if idx < len(sorted_loans):
#                 stage_3_loans.append(sorted_loans[idx])
#                 idx += 1
    
#     return stage_1_loans, stage_2_loans, stage_3_loans

def calculate_stage_totals(loans):
    """
    Calculate total outstanding balance and provision amount for a set of loans.
    """
    total_outstanding = sum(safe_float(loan.outstanding_loan_balance) if loan.outstanding_loan_balance is not None else 0 for loan in loans)
    
    # Since the Loan model doesn't have PD, LGD, and EAD attributes,
    # we'll use simplified calculations based on available attributes
    total_provision = 0
    for loan in loans:
        # Use loan_amount as a base for calculations
        loan_amount = safe_float(loan.loan_amount) if loan.loan_amount is not None else 0
        outstanding_balance = safe_float(loan.outstanding_loan_balance) if loan.outstanding_loan_balance is not None else 0
        
        # Simple default calculation: 5% of outstanding balance
        # This is a placeholder - in a real system, these would be calculated based on 
        # risk factors, days past due, etc.
        provision = outstanding_balance * 0.05
        total_provision += provision
    
    return total_outstanding, total_provision



async def process_local_impairment_calculation_sync(
    portfolio_id: int,
    reporting_date: date,
    db: Session
) -> Dict[str, Any]:
    """
    Calculate and update local impairment ECL for all loans in a portfolio.
    Provision is determined by the loan's stage and the associated fixed rate.
    """
    logger.info(f"Calculating local impairment ECL for portfolio {portfolio_id}")

    try:
        loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).yield_per(2000)
        
        # Standard local impairment provision rates
        stage_provision_rates = {
            "Current": Decimal("0.01"),
            "OLEM": Decimal("0.05"),
            "Substandard": Decimal("0.25"),
            "Doubtful": Decimal("0.50"),
            "Loss": Decimal("1.00")
        }

        total_provision = Decimal("0.0")
        loan_count = 0
        failed_loans = []
        updates = []

        async def process_loan(loan):
            nonlocal total_provision, loan_count
            try:
                stage = loan.bog_stage
                ead = Decimal(loan.adjusted_amortised_bal or 0)
                provision_rate = stage_provision_rates.get(stage, Decimal("0.0"))
                local_impairment = ead * provision_rate
                loan.local_ecl = round(safe_float(local_impairment), 2)
                total_provision += local_impairment
                loan_count += 1
                updates.append(loan)
            except Exception as e:
                logger.warning(f"Failed to update loan {getattr(loan, 'id', 'unknown')}: {e}")
                failed_loans.append(getattr(loan, 'id', 'unknown'))

        tasks = []
        for loan in loans:
            tasks.append(process_loan(loan))
            if len(tasks) >= 1000:
                await asyncio.gather(*tasks)
                db.bulk_save_objects(updates)
                db.commit()
                updates.clear()
                tasks.clear()

        if tasks:
            await asyncio.gather(*tasks)
            db.bulk_save_objects(updates)
            db.commit()

        logger.info(f"Local impairment ECL calculation completed for {loan_count} loans")

        return {
            "status": "success",
            "portfolio_id": portfolio_id,
            "total_provision": round(safe_float(total_provision), 2),
            "loan_count": loan_count,
            "failed_loans": failed_loans
        }

    except Exception as e:
        logger.error(f"Local impairment calculation failed: {e}")
        return {
            "status": "error",
            "message": str(e),
            "portfolio_id": portfolio_id
        }