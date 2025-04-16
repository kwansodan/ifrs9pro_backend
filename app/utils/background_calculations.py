import asyncio
import logging
import threading
import random
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
    calculate_loss_given_default, calculate_probability_of_default,
    calculate_exposure_at_default_percentage, calculate_marginal_ecl, is_in_range,
    get_amortization_schedule, get_ecl_by_stage, calculate_effective_interest_rate_lender
)
from app.utils.staging import parse_days_range

logger = logging.getLogger(__name__)

async def process_ecl_calculation(
    task_id: str,
    portfolio_id: int,
    reporting_date: date,
    db: Session
):
    """
    Process ECL calculation in the background with progress reporting.
    """
    try:
        logger.info(f"Starting ECL calculation for portfolio {portfolio_id} with reporting date {reporting_date}")
        get_task_manager().update_progress(
            task_id,
            progress=5,
            status_message=f"Starting ECL calculation for portfolio {portfolio_id}"
        )
        await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
        # Verify portfolio exists
        portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
        if not portfolio:
            logger.error(f"Portfolio with ID {portfolio_id} not found")
            raise ValueError(f"Portfolio with ID {portfolio_id} not found")
        
        logger.info(f"Portfolio {portfolio_id} found: {portfolio.name}")
        
        get_task_manager().update_progress(
            task_id,
            progress=10,
            status_message="Retrieving latest ECL staging data"
        )
        await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
        # Get the latest ECL staging result
        latest_staging = (
            db.query(StagingResult)
            .filter(
                StagingResult.portfolio_id == portfolio_id,
                StagingResult.staging_type == "ecl"
            )
            .order_by(StagingResult.created_at.desc())
            .first()
        )
        
        if not latest_staging:
            logger.error(f"No ECL staging found for portfolio {portfolio_id}")
            raise ValueError("No ECL staging found. Please stage loans first.")
        
        logger.info(f"Found ECL staging result ID {latest_staging.id} from {latest_staging.created_at}")
        
        # Extract config from the staging result
        config = latest_staging.config
        if not config:
            logger.error(f"Invalid staging configuration for portfolio {portfolio_id}")
            raise ValueError("Invalid staging configuration")
        
        logger.info(f"ECL staging config: {config}")
        
        # Log the staging result summary
        logger.info(f"ECL staging result summary: {latest_staging.result_summary}")
        
        get_task_manager().update_progress(
            task_id,
            progress=20,
            status_message="Processing loan staging data"
        )
        await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
        # Get the loan staging data from the result_summary
        staging_data = []
        if "loans" in latest_staging.result_summary:
            # New format with detailed loan data
            staging_data = latest_staging.result_summary["loans"]
        else:
            # Without detailed loan data, we need to re-stage based on summary stats
            logger.warning("No detailed loan staging data in result_summary, reconstructing staging using database query")
            
            # Recreate basic staging info from loan query using the config
            try:
                stage_1_range = parse_days_range(config["stage_1"]["days_range"])
                stage_2_range = parse_days_range(config["stage_2"]["days_range"])
                stage_3_range = parse_days_range(config["stage_3"]["days_range"])
            except (KeyError, ValueError) as e:
                logger.error(f"Error parsing stage ranges: {str(e)}")
                raise ValueError(f"Could not parse staging configuration: {str(e)}")
                
            # Get the loans
            loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
            
            logger.info(f"Found {len(loans)} loans for portfolio {portfolio_id}")
            
            get_task_manager().update_progress(
                task_id,
                progress=30,
                status_message=f"Re-staging {len(loans)} loans based on configuration"
            )
            await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
            
            # Re-stage them
            for loan in loans:
                if loan.ndia is None:
                    continue
                    
                ndia = loan.ndia
                    
                # Stage based on NDIA
                if is_in_range(ndia, stage_1_range):
                    stage = "Stage 1"
                elif is_in_range(ndia, stage_2_range):
                    stage = "Stage 2"
                elif is_in_range(ndia, stage_3_range):
                    stage = "Stage 3"
                else:
                    stage = "Stage 3"
                    
                # Create a basic staging entry
                staging_data.append({
                    "loan_id": loan.id,
                    "employee_id": loan.employee_id,
                    "stage": stage,
                    "outstanding_loan_balance": float(loan.outstanding_loan_balance) if loan.outstanding_loan_balance else 0,
                })
                
        if not staging_data:
            # If we still don't have staging data, return an error
            raise ValueError("No loan staging data found. Please re-run the staging process.")
            
        # Calculate stage statistics
        stage_stats = {}
        for stage in ["Stage 1", "Stage 2", "Stage 3"]:
            stage_loans = [loan for loan in staging_data if loan["stage"] == stage]
            total_balance = sum(loan["outstanding_loan_balance"] for loan in stage_loans)
            stage_stats[stage] = {
                "num_loans": len(stage_loans),
                "total_loan_value": round(total_balance, 2),
            }
            
        logger.info(f"ECL stage statistics for portfolio {portfolio_id}:")
        logger.info(f"Stage 1: {stage_stats['Stage 1']['num_loans']} loans, balance: {stage_stats['Stage 1']['total_loan_value']}")
        logger.info(f"Stage 2: {stage_stats['Stage 2']['num_loans']} loans, balance: {stage_stats['Stage 2']['total_loan_value']}")
        logger.info(f"Stage 3: {stage_stats['Stage 3']['num_loans']} loans, balance: {stage_stats['Stage 3']['total_loan_value']}")
        
        get_task_manager().update_progress(
            task_id,
            progress=40,
            status_message="Retrieving loan and client data"
        )
        await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
        # Get all loans in the portfolio
        loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
        
        # Create a map of loan_id to loan object for faster lookup
        loan_map = {loan.id: loan for loan in loans}

        # Initialize category tracking
        stage_1_loans = []
        stage_2_loans = []
        stage_3_loans = []

        # Calculate totals for each category
        stage_1_total = 0
        stage_2_total = 0
        stage_3_total = 0

        # Calculate provisions for each category
        stage_1_provision = 0
        stage_2_provision = 0
        stage_3_provision = 0

        # Summary metrics
        total_lgd = 0
        total_pd = 0
        total_ead_value = 0
        total_loans = 0

        get_task_manager().update_progress(
            task_id,
            progress=50,
            status_message=f"Retrieving client securities data for {len(staging_data)} loans"
        )
        await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
        # Get all client IDs to fetch securities
        client_ids = {loan.employee_id for loan in loans if loan.employee_id}

        # Get securities for all clients
        client_securities = {}
        if client_ids:
            securities = (
                db.query(Security)
                .join(Client, Security.client_id == Client.id)
                .filter(Client.employee_id.in_(client_ids))
                .all()
            )

            # Group securities by client employee_id
            for security in securities:
                client = db.query(Client).filter(Client.id == security.client_id).first()
                if client and client.employee_id:
                    if client.employee_id not in client_securities:
                        client_securities[client.employee_id] = []
                    client_securities[client.employee_id].append(security)

        get_task_manager().update_progress(
            task_id,
            progress=60,
            status_message=f"Calculating ECL for {len(staging_data)} loans"
        )
        await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
        # Process loans using staging data
        total_items = len(staging_data)
        for i, stage_info in enumerate(staging_data):
            if i % 50 == 0:  # Update progress every 50 loans (more frequent updates)
                progress = 60 + (i / total_items) * 30  # Progress from 60% to 90%
                get_task_manager().update_progress(
                    task_id,
                    progress=round(progress, 2),  # Round to 2 decimal places
                    processed_items=i,
                    total_items=total_items,
                    status_message=f"Calculating ECL: Processed {i}/{total_items} loans ({round(i/total_items*100, 1)}%)"
                )
                await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
                
            loan_id = stage_info.get("loan_id")
            stage = stage_info.get("stage")
            
            if not loan_id or not stage:
                logger.warning(f"Missing loan_id or stage in staging data: {stage_info}")
                continue
                
            loan = loan_map.get(loan_id)
            if not loan or loan.outstanding_loan_balance is None:
                logger.warning(f"Loan {loan_id} not found or has no outstanding balance")
                continue
                
            outstanding_loan_balance = loan.outstanding_loan_balance
            
            # Get securities for this loan's client
            client_securities_list = client_securities.get(loan.employee_id, [])

            # Calculate ECL components for the loan
            lgd = calculate_loss_given_default(loan, client_securities_list)
            pd = calculate_probability_of_default(loan, db)
            ead_value = calculate_exposure_at_default_percentage(loan, reporting_date)
            
            # Convert stage string to numeric value for get_ecl_by_stage function
            stage_num = 1
            if stage == "Stage 2":
                stage_num = 2
            elif stage == "Stage 3":
                stage_num = 3
            
            # Format the reporting date for the amortization schedule
            reporting_date_str = reporting_date.strftime("%d/%m/%Y")
            
            # Extract loan details for amortization schedule
            loan_amount = float(loan.loan_amount) if loan.loan_amount else 0
            loan_term = int(loan.loan_term) if loan.loan_term else 12  # Default to 12 months
            
            # Get monthly installment
            monthly_installment = float(loan.monthly_installment) if loan.monthly_installment else 0
            
            # Get effective interest rate
            admin_fees = float(loan.administrative_fees) if loan.administrative_fees else 0
            effective_interest_rate = calculate_effective_interest_rate_lender(
                loan_amount, admin_fees, loan_term, monthly_installment
            )
            
            # Default to 24% if calculation fails
            if effective_interest_rate is None:
                effective_interest_rate = 24.0
            
            # Format loan issue date
            if loan.loan_issue_date:
                if isinstance(loan.loan_issue_date, str):
                    try:
                        date_obj = datetime.strptime(loan.loan_issue_date, "%Y-%m-%d")
                        start_date = date_obj.strftime("%d/%m/%Y")
                    except ValueError:
                        start_date = datetime.now().replace(day=1).strftime("%d/%m/%Y")
                else:
                    start_date = loan.loan_issue_date.strftime("%d/%m/%Y")
            else:
                start_date = datetime.now().replace(day=1).strftime("%d/%m/%Y")
            
            # Calculate amortization schedule and ECL values
            try:
                schedule, ecl_12_month, ecl_lifetime = get_amortization_schedule(
                    loan_amount=loan_amount,
                    loan_term=loan_term,
                    annual_interest_rate=effective_interest_rate,
                    monthly_installment=monthly_installment,
                    start_date=start_date,
                    reporting_date=reporting_date_str,
                    pd=pd,
                    db=db,
                    loan=loan
                )
                
                # Get the appropriate ECL based on loan stage
                ecl = get_ecl_by_stage(schedule, ecl_12_month, ecl_lifetime, stage_num)
                
                logger.info(f"ECL calculation for loan {loan_id}: LGD={lgd}, PD={pd}, EAD={ead_value}, ECL={ecl}")
            except Exception as e:
                error_msg = f"Error calculating ECL for loan {loan_id}: {str(e)}"
                logger.error(error_msg)
                # Raise the error to stop the calculation process
                raise ValueError(error_msg) from e
            
            # Update stage totals based on the assigned stage
            if stage == "Stage 1":
                stage_1_loans.append(loan)
                stage_1_total += outstanding_loan_balance
                stage_1_provision += ecl
            elif stage == "Stage 2":
                stage_2_loans.append(loan)
                stage_2_total += outstanding_loan_balance
                stage_2_provision += ecl
            elif stage == "Stage 3":
                stage_3_loans.append(loan)
                stage_3_total += outstanding_loan_balance
                stage_3_provision += ecl
            else:
                # Default to Stage 3 if stage is something unexpected
                logger.warning(f"Unexpected stage '{stage}' for loan {loan_id}, treating as Stage 3")
                stage_3_loans.append(loan)
                stage_3_total += outstanding_loan_balance
                stage_3_provision += ecl

            # Update summary statistics
            total_lgd += lgd
            total_pd += pd
            total_ead_value += ead_value
            total_loans += 1

        get_task_manager().update_progress(
            task_id,
            progress=90,
            status_message="Finalizing ECL calculation results"
        )
        await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
        # Calculate averages for summary metrics
        avg_lgd = total_lgd / total_loans if total_loans > 0 else 0
        avg_pd = total_pd / total_loans if total_loans > 0 else 0
        avg_ead_value = total_ead_value / total_loans if total_loans > 0 else 0

        # Calculate total loan value and provision amount
        total_loan_value = stage_1_total + stage_2_total + stage_3_total
        
        # Use different provision rates for different stages
        stage_1_rate = Decimal("0.01")  # 1% for Stage 1
        stage_2_rate = Decimal("0.05")  # 5% for Stage 2
        stage_3_rate = Decimal("0.15")  # 15% for Stage 3
        
        # Recalculate provisions using the appropriate rates
        # Convert to Decimal before multiplication to avoid type mismatch
        stage_1_provision = Decimal(str(stage_1_total)) * stage_1_rate
        stage_2_provision = Decimal(str(stage_2_total)) * stage_2_rate
        stage_3_provision = Decimal(str(stage_3_total)) * stage_3_rate
        
        # Recalculate total provision
        total_provision = stage_1_provision + stage_2_provision + stage_3_provision

        # Calculate provision percentage
        provision_percentage = (
            (Decimal(str(total_provision)) / Decimal(str(total_loan_value)) * 100)
            if total_loan_value > 0
            else 0
        )

        get_task_manager().update_progress(
            task_id,
            progress=95,
            status_message="Saving calculation results to database"
        )
        await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
        # Create a new CalculationResult record
        calculation_result = CalculationResult(
            portfolio_id=portfolio_id,
            calculation_type="ecl",
            config=config,  # Use the config from staging
            result_summary={
                "Stage 1": {
                    "num_loans": len(stage_1_loans),
                    "total_loan_value": float(stage_1_total),
                    "outstanding_loan_balance": float(stage_1_total),
                    "provision_amount": float(stage_1_provision),
                    "provision_rate": float(stage_1_rate),
                },
                "Stage 2": {
                    "num_loans": len(stage_2_loans),
                    "total_loan_value": float(stage_2_total),
                    "outstanding_loan_balance": float(stage_2_total),
                    "provision_amount": float(stage_2_provision),
                    "provision_rate": float(stage_2_rate),
                },
                "Stage 3": {
                    "num_loans": len(stage_3_loans),
                    "total_loan_value": float(stage_3_total),
                    "outstanding_loan_balance": float(stage_3_total),
                    "provision_amount": float(stage_3_provision),
                    "provision_rate": float(stage_3_rate),
                },
                "total_loans": total_loans
            },
            total_provision=float(total_provision),
            provision_percentage=float(provision_percentage),
            reporting_date=reporting_date
        )
        db.add(calculation_result)
        db.commit()

        get_task_manager().update_progress(
            task_id,
            progress=100,
            status_message="ECL calculation completed successfully"
        )
        await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
        # Return the calculation result ID
        return {
            "calculation_id": calculation_result.id,
            "portfolio_id": portfolio_id,
            "total_provision": float(total_provision),
            "provision_percentage": float(provision_percentage),
            "total_loans": total_loans
        }
        
    except Exception as e:
        logger.exception(f"Error during ECL calculation: {str(e)}")
        get_task_manager().update_progress(
            task_id,
            progress=100,
            status_message=f"Error during ECL calculation: {str(e)}"
        )
        raise

async def process_local_impairment_calculation(
    task_id: str,
    portfolio_id: int,
    reporting_date: date,
    db: Session
):
    """
    Process local impairment calculation in the background with progress reporting.
    """
    try:
        get_task_manager().update_progress(
            task_id,
            progress=5,
            status_message=f"Starting local impairment calculation for portfolio {portfolio_id}"
        )
        await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
        # Verify portfolio exists
        portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
        if not portfolio:
            raise ValueError(f"Portfolio with ID {portfolio_id} not found")
        
        get_task_manager().update_progress(
            task_id,
            progress=10,
            status_message="Retrieving latest local impairment staging data"
        )
        await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
        # Get the latest local impairment staging result
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
            raise ValueError("No local impairment staging found. Please stage loans first.")
        
        # Extract config from the staging result
        config = latest_staging.config
        if not config:
            raise ValueError("Invalid staging configuration")
        
        get_task_manager().update_progress(
            task_id,
            progress=20,
            status_message="Processing loan staging data"
        )
        await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
        # Get the loan staging data from the result_summary
        staging_data = []
        if "loans" in latest_staging.result_summary:
            # New format with detailed loan data
            staging_data = latest_staging.result_summary["loans"]
        else:
            # Without detailed loan data, we need to re-stage based on summary stats
            logger.warning("No detailed loan staging data in result_summary, reconstructing staging using database query")
            
            # Recreate basic staging info from loan query using the config
            try:
                current_range = parse_days_range(config["current"]["days_range"])
                olem_range = parse_days_range(config["olem"]["days_range"])
                substandard_range = parse_days_range(config["substandard"]["days_range"])
                doubtful_range = parse_days_range(config["doubtful"]["days_range"])
                loss_range = parse_days_range(config["loss"]["days_range"])
            except (KeyError, ValueError) as e:
                logger.error(f"Error parsing day ranges: {str(e)}")
                raise ValueError(f"Could not parse staging configuration: {str(e)}")
                
            # Get the loans
            loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
            
            logger.info(f"Found {len(loans)} loans for portfolio {portfolio_id}")
            
            get_task_manager().update_progress(
                task_id,
                progress=30,
                status_message=f"Re-staging {len(loans)} loans based on configuration"
            )
            await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
            
            # Re-stage them
            for loan in loans:
                if loan.ndia is None:
                    continue
                    
                ndia = loan.ndia
                    
                # Stage based on NDIA
                if is_in_range(ndia, current_range):
                    stage = "Current"
                elif is_in_range(ndia, olem_range):
                    stage = "OLEM"
                elif is_in_range(ndia, substandard_range):
                    stage = "Substandard"
                elif is_in_range(ndia, doubtful_range):
                    stage = "Doubtful"
                elif is_in_range(ndia, loss_range):
                    stage = "Loss"
                else:
                    stage = "Loss"  # Default to Loss if outside all ranges
                    
                # Create a basic staging entry
                staging_data.append({
                    "loan_id": loan.id,
                    "employee_id": loan.employee_id,
                    "stage": stage,
                    "outstanding_loan_balance": float(loan.outstanding_loan_balance) if loan.outstanding_loan_balance else 0,
                })
                
        if not staging_data:
            # If we still don't have staging data, return an error
            raise ValueError("No loan staging data found. Please re-run the staging process.")
            
        get_task_manager().update_progress(
            task_id,
            progress=40,
            status_message="Retrieving loan data"
        )
        await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
        # Get all loans in the portfolio
        loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
        
        # Create a map of loan_id to loan object for faster lookup
        loan_map = {loan.id: loan for loan in loans}

        # Initialize category tracking
        current_loans = []
        olem_loans = []
        substandard_loans = []
        doubtful_loans = []
        loss_loans = []

        # Calculate totals for each category
        current_total = 0
        olem_total = 0
        substandard_total = 0
        doubtful_total = 0
        loss_total = 0

        # Get provision rates from config
        try:
            # Check if we have a provision_config object
            provision_config = config.get("provision_config", {})
            if provision_config:
                # Get rates from provision_config
                current_rate = Decimal(provision_config.get("Current", 0.01))
                olem_rate = Decimal(provision_config.get("OLEM", 0.03))
                substandard_rate = Decimal(provision_config.get("Substandard", 0.2))
                doubtful_rate = Decimal(provision_config.get("Doubtful", 0.5))
                loss_rate = Decimal(provision_config.get("Loss", 1.0))
            else:
                # Fall back to old structure
                current_rate = Decimal(config.get("current", {}).get("rate", 0.01))
                olem_rate = Decimal(config.get("olem", {}).get("rate", 0.03))
                substandard_rate = Decimal(config.get("substandard", {}).get("rate", 0.2))
                doubtful_rate = Decimal(config.get("doubtful", {}).get("rate", 0.5))
                loss_rate = Decimal(config.get("loss", {}).get("rate", 1.0))
            
            # Log the rates being used
            logger.info(f"Using provision rates - Current: {current_rate}, OLEM: {olem_rate}, Substandard: {substandard_rate}, Doubtful: {doubtful_rate}, Loss: {loss_rate}")
            
        except (KeyError, ValueError) as e:
            logger.error(f"Error parsing provision rates: {str(e)}")
            raise ValueError(f"Could not parse provision rates from configuration: {str(e)}")

        # Calculate stage statistics
        stage_stats = {}
        for stage in ["Current", "OLEM", "Substandard", "Doubtful", "Loss"]:
            stage_loans = [loan for loan in staging_data if loan["stage"] == stage]
            total_balance = sum(loan["outstanding_loan_balance"] for loan in stage_loans)
            stage_stats[stage] = {
                "num_loans": len(stage_loans),
                "total_loan_value": round(total_balance, 2),
            }
            
        logger.info(f"Local impairment stage statistics for portfolio {portfolio_id}:")
        logger.info(f"Current: {stage_stats['Current']['num_loans']} loans, balance: {stage_stats['Current']['total_loan_value']}")
        logger.info(f"OLEM: {stage_stats['OLEM']['num_loans']} loans, balance: {stage_stats['OLEM']['total_loan_value']}")
        logger.info(f"Substandard: {stage_stats['Substandard']['num_loans']} loans, balance: {stage_stats['Substandard']['total_loan_value']}")
        logger.info(f"Doubtful: {stage_stats['Doubtful']['num_loans']} loans, balance: {stage_stats['Doubtful']['total_loan_value']}")
        logger.info(f"Loss: {stage_stats['Loss']['num_loans']} loans, balance: {stage_stats['Loss']['total_loan_value']}")
        
        get_task_manager().update_progress(
            task_id,
            progress=60,
            status_message=f"Calculating local impairment for {len(staging_data)} loans"
        )
        await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
        # Process loans using staging data
        total_items = len(staging_data)
        for i, stage_info in enumerate(staging_data):
            if i % 50 == 0:  # Update progress every 50 loans (more frequent updates)
                progress = 60 + (i / total_items) * 30  # Progress from 60% to 90%
                get_task_manager().update_progress(
                    task_id,
                    progress=round(progress, 2),  # Round to 2 decimal places
                    processed_items=i,
                    total_items=total_items,
                    status_message=f"Calculating local impairment: Processed {i}/{total_items} loans ({round(i/total_items*100, 1)}%)"
                )
                await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
                
            loan_id = stage_info.get("loan_id")
            stage = stage_info.get("stage")
            
            if not loan_id or not stage:
                logger.warning(f"Missing loan_id or stage in staging data: {stage_info}")
                continue
                
            loan = loan_map.get(loan_id)
            if not loan or loan.outstanding_loan_balance is None:
                logger.warning(f"Loan {loan_id} not found or has no outstanding balance")
                continue
                
            outstanding_loan_balance = loan.outstanding_loan_balance
            
            # Update category totals based on the assigned stage
            if stage == "Current":
                current_loans.append(loan)
                current_total += outstanding_loan_balance
            elif stage == "OLEM":
                olem_loans.append(loan)
                olem_total += outstanding_loan_balance
            elif stage == "Substandard":
                substandard_loans.append(loan)
                substandard_total += outstanding_loan_balance
            elif stage == "Doubtful":
                doubtful_loans.append(loan)
                doubtful_total += outstanding_loan_balance
            elif stage == "Loss":
                loss_loans.append(loan)
                loss_total += outstanding_loan_balance
            else:
                # Default to Loss if stage is something unexpected
                logger.warning(f"Unexpected stage '{stage}' for loan {loan_id}, treating as Loss")
                loss_loans.append(loan)
                loss_total += outstanding_loan_balance

        get_task_manager().update_progress(
            task_id,
            progress=90,
            status_message="Finalizing local impairment calculation results",
        )
        await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
        # Calculate provisions for each category
        current_provision = current_total * current_rate
        olem_provision = olem_total * olem_rate
        substandard_provision = substandard_total * substandard_rate
        doubtful_provision = doubtful_total * doubtful_rate
        loss_provision = loss_total * loss_rate

        logger.info(f"Local impairment provisions for portfolio {portfolio_id}:")
        logger.info(f"Current: rate={current_rate}, value={current_total}, provision={current_provision}")
        logger.info(f"OLEM: rate={olem_rate}, value={olem_total}, provision={olem_provision}")
        logger.info(f"Substandard: rate={substandard_rate}, value={substandard_total}, provision={substandard_provision}")
        logger.info(f"Doubtful: rate={doubtful_rate}, value={doubtful_total}, provision={doubtful_provision}")
        logger.info(f"Loss: rate={loss_rate}, value={loss_total}, provision={loss_provision}")
        
        # Calculate total loan value and provision amount
        total_loan_value = current_total + olem_total + substandard_total + doubtful_total + loss_total
        total_provision = current_provision + olem_provision + substandard_provision + doubtful_provision + loss_provision

        # Calculate provision percentage
        provision_percentage = (
            (total_provision / Decimal(str(total_loan_value)) * 100)
            if total_loan_value > 0
            else 0
        )

        # Create a new CalculationResult record
        calculation_result = CalculationResult(
            portfolio_id=portfolio_id,
            calculation_type="local_impairment",
            config=config,  # Use the config from staging
            result_summary={
                "Current": {
                    "num_loans": len(current_loans),
                    "total_loan_value": float(current_total),
                    "outstanding_loan_balance": float(current_total),
                    "provision_amount": float(current_provision),
                    "provision_rate": float(current_rate),
                },
                "OLEM": {
                    "num_loans": len(olem_loans),
                    "total_loan_value": float(olem_total),
                    "outstanding_loan_balance": float(olem_total),
                    "provision_amount": float(olem_provision),
                    "provision_rate": float(olem_rate),
                },
                "Substandard": {
                    "num_loans": len(substandard_loans),
                    "total_loan_value": float(substandard_total),
                    "outstanding_loan_balance": float(substandard_total),
                    "provision_amount": float(substandard_provision),
                    "provision_rate": float(substandard_rate),
                },
                "Doubtful": {
                    "num_loans": len(doubtful_loans),
                    "total_loan_value": float(doubtful_total),
                    "outstanding_loan_balance": float(doubtful_total),
                    "provision_amount": float(doubtful_provision),
                    "provision_rate": float(doubtful_rate),
                },
                "Loss": {
                    "num_loans": len(loss_loans),
                    "total_loan_value": float(loss_total),
                    "outstanding_loan_balance": float(loss_total),
                    "provision_amount": float(loss_provision),
                    "provision_rate": float(loss_rate),
                },
                "total_loans": len(staging_data)
            },
            total_provision=float(total_provision),
            provision_percentage=float(provision_percentage),
            reporting_date=reporting_date
        )
        db.add(calculation_result)
        db.commit()

        get_task_manager().update_progress(
            task_id,
            progress=95,
            status_message="Saving local impairment calculation results to database"
        )
        await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
        get_task_manager().update_progress(
            task_id,
            progress=100,
            status_message="Local impairment calculation completed successfully"
        )
        await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
        # Return the calculation result ID
        return {
            "calculation_id": calculation_result.id,
            "portfolio_id": portfolio_id,
            "total_provision": float(total_provision),
            "provision_percentage": float(provision_percentage),
            "total_loans": len(staging_data)
        }
        
    except Exception as e:
        logger.exception(f"Error during local impairment calculation: {str(e)}")
        get_task_manager().update_progress(
            task_id,
            progress=100,
            status_message=f"Error during local impairment calculation: {str(e)}"
        )
        raise

async def start_background_ecl_calculation(
    portfolio_id: int,
    reporting_date: date,
    db: Session
) -> str:
    """
    Start a background task for ECL calculation.
    
    Returns the task ID that can be used to track progress.
    """
    # Create a new task
    task_id = get_task_manager().create_task(
        task_type="ecl_calculation",
        description=f"Calculating ECL for portfolio {portfolio_id}"
    )
    
    # Define a function to run the background task in a separate thread
    def run_task_in_thread():
        # Create a new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Create a new database session for this thread
        thread_db = SessionLocal()
        
        try:
            # Run the background task in this thread's event loop
            loop.run_until_complete(
                run_background_task(
                    task_id,
                    process_ecl_calculation,
                    portfolio_id=portfolio_id,
                    reporting_date=reporting_date,
                    db=thread_db
                )
            )
            
            # Properly await any pending notifications before closing the loop
            pending = asyncio.all_tasks(loop)
            if pending:
                loop.run_until_complete(asyncio.gather(*pending))
                
        except Exception as e:
            logger.exception(f"Error in background task thread: {e}")
            get_task_manager().mark_as_failed(task_id, str(e))
        finally:
            # Close the database session
            thread_db.close()
            loop.close()
    
    # Start the task in a separate thread
    thread = threading.Thread(target=run_task_in_thread)
    thread.daemon = True  # Allow the thread to be terminated when the main program exits
    thread.start()
    
    return task_id

async def start_background_local_impairment_calculation(
    portfolio_id: int,
    reporting_date: date,
    db: Session
) -> str:
    """
    Start a background task for local impairment calculation.
    
    Returns the task ID that can be used to track progress.
    """
    # Create a new task
    task_id = get_task_manager().create_task(
        task_type="local_impairment_calculation",
        description=f"Calculating local impairment for portfolio {portfolio_id}"
    )
    
    # Define a function to run the background task in a separate thread
    def run_task_in_thread():
        # Create a new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Create a new database session for this thread
        thread_db = SessionLocal()
        
        try:
            # Run the background task in this thread's event loop
            loop.run_until_complete(
                run_background_task(
                    task_id,
                    process_local_impairment_calculation,
                    portfolio_id=portfolio_id,
                    reporting_date=reporting_date,
                    db=thread_db
                )
            )
            
            # Properly await any pending notifications before closing the loop
            pending = asyncio.all_tasks(loop)
            if pending:
                loop.run_until_complete(asyncio.gather(*pending))
                
        except Exception as e:
            logger.exception(f"Error in background task thread: {e}")
            get_task_manager().mark_as_failed(task_id, str(e))
        finally:
            # Close the database session
            thread_db.close()
            loop.close()
    
    # Start the task in a separate thread
    thread = threading.Thread(target=run_task_in_thread)
    thread.daemon = True  # Allow the thread to be terminated when the main program exits
    thread.start()
    
    return task_id

def retrieve_loans_for_staging(portfolio_id: int, db: Session):
    """
    Retrieve loans for staging based on the portfolio ID.
    Optimized for potentially large datasets.
    """
    # Get the latest ECL staging result
    latest_staging = (
        db.query(StagingResult)
        .filter(
            StagingResult.portfolio_id == portfolio_id,
            StagingResult.staging_type == "ecl"
        )
        .order_by(StagingResult.created_at.desc())
        .limit(1)
        .first()
    )
    
    if not latest_staging:
        return [], [], []
    
    # Get all loans for this portfolio
    all_loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
    
    # Check if we have detailed loan data in the result_summary
    if "loans" in latest_staging.result_summary:
        # Extract loan IDs by stage
        stage_1_ids = []
        stage_2_ids = []
        stage_3_ids = []
        
        for loan in latest_staging.result_summary.get("loans", []):
            if loan.get("stage") == 1:
                stage_1_ids.append(loan.get("id"))
            elif loan.get("stage") == 2:
                stage_2_ids.append(loan.get("id"))
            elif loan.get("stage") == 3:
                stage_3_ids.append(loan.get("id"))
        
        # Get staged loans based on IDs
        stage_1_loans = [loan for loan in all_loans if loan.id in stage_1_ids]
        stage_2_loans = [loan for loan in all_loans if loan.id in stage_2_ids]
        stage_3_loans = [loan for loan in all_loans if loan.id in stage_3_ids]
    else:
        # Alternative approach: use summary statistics to approximate
        # This is a simplified approach and may not match exactly with the original staging
        logger.warning("No detailed loan data in staging result, using summary statistics to approximate staging")
        
        # Get summary counts
        summary = latest_staging.result_summary
        stage_1_count = summary.get("Stage 1", {}).get("num_loans", 0)
        stage_2_count = summary.get("Stage 2", {}).get("num_loans", 0)
        stage_3_count = summary.get("Stage 3", {}).get("num_loans", 0)
        
        # Sort loans by outstanding loan balance for a simple approximation
        sorted_loans = sorted(all_loans, key=lambda x: float(x.outstanding_loan_balance if x.outstanding_loan_balance is not None else 0), reverse=True)
        
        # Distribute loans based on counts
        stage_1_loans = []
        stage_2_loans = []
        stage_3_loans = []
        
        idx = 0
        for _ in range(stage_1_count):
            if idx < len(sorted_loans):
                stage_1_loans.append(sorted_loans[idx])
                idx += 1
        
        for _ in range(stage_2_count):
            if idx < len(sorted_loans):
                stage_2_loans.append(sorted_loans[idx])
                idx += 1
        
        for _ in range(stage_3_count):
            if idx < len(sorted_loans):
                stage_3_loans.append(sorted_loans[idx])
                idx += 1
    
    return stage_1_loans, stage_2_loans, stage_3_loans

def calculate_stage_totals(loans):
    """
    Calculate total outstanding balance and provision amount for a set of loans.
    """
    total_outstanding = sum(float(loan.outstanding_loan_balance if loan.outstanding_loan_balance is not None else 0) for loan in loans)
    
    # Since the Loan model doesn't have PD, LGD, and EAD attributes,
    # we'll use simplified calculations based on available attributes
    total_provision = 0
    for loan in loans:
        # Use loan_amount as a base for calculations
        loan_amount = float(loan.loan_amount if loan.loan_amount is not None else 0)
        outstanding_balance = float(loan.outstanding_loan_balance if loan.outstanding_loan_balance is not None else 0)
        
        # Simple default calculation: 5% of outstanding balance
        # This is a placeholder - in a real system, these would be calculated based on 
        # risk factors, days past due, etc.
        provision = outstanding_balance * 0.05
        total_provision += provision
    
    return total_outstanding, total_provision

def process_ecl_calculation_sync(
    portfolio_id: int,
    reporting_date: date,
    staging_result: StagingResult,
    db: Session
) -> Dict[str, Any]:
    """
    Synchronously process ECL calculation and return the result.
    """
    logger.info(f"Starting synchronous ECL calculation for portfolio {portfolio_id}")
    
    # Verify portfolio exists
    portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
    if not portfolio:
        logger.error(f"Portfolio with ID {portfolio_id} not found")
        raise ValueError(f"Portfolio with ID {portfolio_id} not found")
    
    # Get all loans for this portfolio
    all_loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
    
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
    
    # Calculate provisions based on fixed rates
    stage_1_rate = Decimal("0.01")  # Fixed 1% for Stage 1
    stage_2_rate = Decimal("0.05")  # Fixed 5% for Stage 2
    stage_3_rate = Decimal("0.15")  # Fixed 15% for Stage 3
    
    stage_1_provision = Decimal(str(stage_1_balance)) * stage_1_rate
    stage_2_provision = Decimal(str(stage_2_balance)) * stage_2_rate
    stage_3_provision = Decimal(str(stage_3_balance)) * stage_3_rate
    
    # Calculate total provision
    total_provision = stage_1_provision + stage_2_provision + stage_3_provision
    total_balance = Decimal(str(stage_1_balance)) + Decimal(str(stage_2_balance)) + Decimal(str(stage_3_balance))
    provision_percentage = (
        (Decimal(str(total_provision)) / Decimal(str(total_balance)) * 100)
        if total_balance > 0
        else 0
    )

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
            }
        },
        total_provision=float(total_provision),
        provision_percentage=float(provision_percentage)
    )
    
    db.add(calculation_result)
    db.commit()
    
    logger.info(f"ECL calculation completed for portfolio {portfolio_id}")
    
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
