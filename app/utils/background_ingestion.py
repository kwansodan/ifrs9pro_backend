import asyncio
import logging
from typing import Optional, Dict, Any, List
from fastapi import UploadFile
from sqlalchemy.orm import Session
import io
import random
from datetime import datetime, date
from decimal import Decimal

from app.models import (
    Loan,
    Guarantee,
    Client,
    Security,
    Portfolio,
    StagingResult,
    CalculationResult,
    QualityIssue
)
from app.utils.background_tasks import get_task_manager, run_background_task
from app.utils.background_processors import (
    process_loan_details_with_progress,
    process_client_data_with_progress
)
from app.utils.sync_processors import (
    process_loan_details_sync,
    process_client_data_sync,
    run_quality_checks_sync
)
from app.database import SessionLocal
from app.schemas import ECLStagingConfig, LocalImpairmentConfig, DaysRangeConfig
from app.utils.staging import (
    stage_loans_ecl_orm, 
    stage_loans_local_impairment_orm,
    stage_loans_ecl_orm_sync,
    stage_loans_local_impairment_orm_sync
)
from app.utils.quality_checks import create_quality_issues_if_needed, create_and_save_quality_issues

logger = logging.getLogger(__name__)

async def process_portfolio_ingestion(
    task_id: str,
    portfolio_id: int,
    loan_details_content: Optional[bytes] = None,
    loan_details_filename: Optional[str] = None,
    client_data_content: Optional[bytes] = None,
    client_data_filename: Optional[str] = None,
    loan_guarantee_data_content: Optional[bytes] = None,
    loan_guarantee_data_filename: Optional[str] = None,
    loan_collateral_data_content: Optional[bytes] = None,
    loan_collateral_data_filename: Optional[str] = None,
    db: Session = None
):
    """
    Process portfolio data ingestion in the background with progress reporting.
    
    This function orchestrates the processing of multiple data files for a portfolio,
    updating progress as each file is processed. It also performs staging and quality checks.
    """
    try:
        # Initialize result tracking
        results = {
            "portfolio_id": portfolio_id,
            "files_processed": 0,
            "total_files": 0,
            "details": {}
        }
        
        # Check for and delete existing data for this portfolio
        try:
            get_task_manager().update_progress(
                task_id,
                progress=5,
                status_message=f"Checking for existing data in portfolio {portfolio_id}"
            )
            
            # Delete existing data in reverse order of dependencies
            # First delete staging results and calculation results
            staging_count = db.query(StagingResult).filter(StagingResult.portfolio_id == portfolio_id).delete()
            calculation_count = db.query(CalculationResult).filter(CalculationResult.portfolio_id == portfolio_id).delete()
            
            # Delete quality issues
            quality_count = db.query(QualityIssue).filter(QualityIssue.portfolio_id == portfolio_id).delete()
            
            # Delete loans, guarantees, and clients
            loan_count = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).delete()
            guarantee_count = db.query(Guarantee).filter(Guarantee.portfolio_id == portfolio_id).delete()
            client_count = db.query(Client).filter(Client.portfolio_id == portfolio_id).delete()
            
            # Commit the deletions
            db.commit()
            
            logger.info(f"Cleared existing data: {loan_count} loans, {client_count} clients, {guarantee_count} guarantees")
            
            # Log the deletion results but don't add to response
            logger.info(f"Data cleared: loans={loan_count}, clients={client_count}, guarantees={guarantee_count}, " +
                       f"quality_issues={quality_count}, staging_results={staging_count}, calculation_results={calculation_count}")
            
        except Exception as e:
            logger.error(f"Error clearing existing data: {str(e)}")
            results["errors"] = results.get("errors", []) + [f"Error clearing existing data: {str(e)}"]
        
        # Count total files to process
        files_to_process = []
        if loan_details_content:
            files_to_process.append(("loan_details", loan_details_content, loan_details_filename))
            results["total_files"] += 1
        
        if client_data_content:
            files_to_process.append(("client_data", client_data_content, client_data_filename))
            results["total_files"] += 1
            
        if loan_guarantee_data_content:
            files_to_process.append(("loan_guarantee_data", loan_guarantee_data_content, loan_guarantee_data_filename))
            results["total_files"] += 1
            
        if loan_collateral_data_content:
            files_to_process.append(("loan_collateral_data", loan_collateral_data_content, loan_collateral_data_filename))
            results["total_files"] += 1
        
        # If no files provided, return early
        if not files_to_process:
            get_task_manager().update_task(
                task_id, 
                status_message="No files provided for ingestion",
                progress=100
            )
            return results
        
        # Calculate progress allocation per file
        # Data ingestion takes 80% of the total progress (10-90%)
        # Each file gets an equal portion of that 80%
        progress_per_file = 80.0 / len(files_to_process)
        
        # Process each file in sequence
        for i, (file_type, file_content, filename) in enumerate(files_to_process):
            # Calculate progress range for this file
            file_start_progress = 10 + (i * progress_per_file)
            file_end_progress = 10 + ((i + 1) * progress_per_file)
            
            # Update overall task progress
            get_task_manager().update_progress(
                task_id,
                progress=file_start_progress,
                status_message=f"Processing {file_type} ({i+1}/{len(files_to_process)})"
            )
            
            # Process based on file type
            if file_type == "loan_details":
                get_task_manager().update_task(task_id, status_message=f"Processing loan details from {filename}")
                
                # Create a progress wrapper function
                async def progress_wrapper(progress, processed_items=None, status_message=None):
                    # Map the file's internal progress (0-100%) to its allocated range
                    overall_progress = file_start_progress + (progress / 100.0) * (file_end_progress - file_start_progress)
                    get_task_manager().update_progress(
                        task_id,
                        progress=round(overall_progress, 2),
                        processed_items=processed_items,
                        status_message=status_message
                    )
                
                # Pass the wrapper to the processor
                file_result = await process_loan_details_with_progress(
                    task_id, 
                    file_content, 
                    portfolio_id, 
                    db,
                    progress_callback=progress_wrapper
                )
                results["details"]["loan_details"] = file_result
                
            elif file_type == "client_data":
                get_task_manager().update_task(task_id, status_message=f"Processing client data from {filename}")
                
                # Create a progress wrapper function
                async def progress_wrapper(progress, processed_items=None, status_message=None):
                    # Map the file's internal progress (0-100%) to its allocated range
                    overall_progress = file_start_progress + (progress / 100.0) * (file_end_progress - file_start_progress)
                    get_task_manager().update_progress(
                        task_id,
                        progress=round(overall_progress, 2),
                        processed_items=processed_items,
                        status_message=status_message
                    )
                
                # Pass the wrapper to the processor
                file_result = await process_client_data_with_progress(
                    task_id, 
                    file_content, 
                    portfolio_id, 
                    db,
                    progress_callback=progress_wrapper
                )
                results["details"]["client_data"] = file_result
                
            # Increment processed files count
            results["files_processed"] += 1
            
            # Update overall progress
            get_task_manager().update_progress(
                task_id,
                progress=file_end_progress,
                status_message=f"Completed processing {file_type}"
            )
        
        # Now perform staging operations - 10% of progress
        get_task_manager().update_progress(
            task_id,
            progress=90,
            status_message="Starting loan staging operations"
        )
        
        staging_results = {}
        
        # 1. Perform ECL staging
        try:
            get_task_manager().update_progress(
                task_id,
                progress=92,
                status_message="Performing ECL staging"
            )
            
            # Create default ECL staging config
            ecl_config = ECLStagingConfig(
                stage_1=DaysRangeConfig(days_range="0-120"),
                stage_2=DaysRangeConfig(days_range="120-240"),
                stage_3=DaysRangeConfig(days_range="240+")
            )
            
            # Create a new staging result entry
            ecl_staging_result = StagingResult(
                portfolio_id=portfolio_id,
                staging_type="ecl",
                config=ecl_config.dict(),
                result_summary={
                    "status": "processing",
                    "timestamp": datetime.now().isoformat()
                }
            )
            db.add(ecl_staging_result)
            db.flush()
            
            # Perform ECL staging
            ecl_result = await stage_loans_ecl_orm(
                portfolio_id=portfolio_id,
                config=ecl_config,
                db=db
            )
            
            staging_results["ecl"] = ecl_result
            
        except Exception as e:
            logger.error(f"Error during ECL staging: {str(e)}")
            staging_results["ecl"] = {
                "status": "error",
                "error": str(e)
            }
        
        # 2. Perform local impairment staging
        try:
            get_task_manager().update_progress(
                task_id,
                progress=94,
                status_message="Performing local impairment staging"
            )
            
            # Create default local impairment config
            local_config = LocalImpairmentConfig(
                current=DaysRangeConfig(days_range="0-30"),
                olem=DaysRangeConfig(days_range="31-90"),
                substandard=DaysRangeConfig(days_range="91-180"),
                doubtful=DaysRangeConfig(days_range="181-365"),
                loss=DaysRangeConfig(days_range="366+")
            )
            
            # Create a new staging result entry
            local_staging_result = StagingResult(
                portfolio_id=portfolio_id,
                staging_type="local_impairment",
                config=local_config.dict(),
                result_summary={
                    "status": "processing",
                    "timestamp": datetime.now().isoformat()
                }
            )
            db.add(local_staging_result)
            db.flush()
            
            # Perform local impairment staging
            local_result = await stage_loans_local_impairment_orm(
                portfolio_id=portfolio_id,
                config=local_config,
                db=db
            )
            
            staging_results["local_impairment"] = local_result
            
        except Exception as e:
            logger.error(f"Error during local impairment staging: {str(e)}")
            staging_results["local_impairment"] = {
                "status": "error",
                "error": str(e)
            }
        
        # Add staging results to the overall results
        results["staging"] = staging_results
        
        # Commit after all staging operations
        db.commit()
        
        # Perform calculations - 10% of progress
        calculation_results = {}
        
        # 1. Perform ECL calculation
        try:
            get_task_manager().update_progress(
                task_id,
                progress=96,
                status_message="Calculating ECL provisions"
            )
            
            # Get the portfolio's ECL calculation config
            portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
            
            # Create a simplified ECL calculation result
            # Get the latest ECL staging result
            latest_ecl_staging = (
                db.query(StagingResult)
                .filter(
                    StagingResult.portfolio_id == portfolio_id,
                    StagingResult.staging_type == "ecl"
                )
                .order_by(StagingResult.created_at.desc())
                .first()
            )
            
            if latest_ecl_staging and latest_ecl_staging.result_summary:
                # Get the staging summary
                staging_summary = latest_ecl_staging.result_summary
                
                # Extract loan counts and balances from staging summary
                stage_1_count = staging_summary.get("Stage 1", {}).get("num_loans", 0)
                stage_1_balance = staging_summary.get("Stage 1", {}).get("outstanding_loan_balance", 0)
                stage_2_count = staging_summary.get("Stage 2", {}).get("num_loans", 0)
                stage_2_balance = staging_summary.get("Stage 2", {}).get("outstanding_loan_balance", 0)
                stage_3_count = staging_summary.get("Stage 3", {}).get("num_loans", 0)
                stage_3_balance = staging_summary.get("Stage 3", {}).get("outstanding_loan_balance", 0)
                
                # Calculate provisions based on simplified rates
                stage_1_provision_rate = 0.01  # 1%
                stage_2_provision_rate = 0.05  # 5%
                stage_3_provision_rate = 0.15  # 15%
                
                stage_1_provision = stage_1_balance * stage_1_provision_rate
                stage_2_provision = stage_2_balance * stage_2_provision_rate
                stage_3_provision = stage_3_balance * stage_3_provision_rate
                
                # Calculate total provision and percentage
                total_provision = stage_1_provision + stage_2_provision + stage_3_provision
                total_balance = stage_1_balance + stage_2_balance + stage_3_balance
                provision_percentage = (total_provision / total_balance) * 100 if total_balance > 0 else 0
                
                # Create calculation result
                ecl_calculation_result = CalculationResult(
                    portfolio_id=portfolio_id,
                    calculation_type="ecl",
                    total_provision=total_provision,
                    provision_percentage=provision_percentage,
                    reporting_date=datetime.now().date(),
                    config={
                        "Stage 1": {"provision_rate": stage_1_provision_rate},
                        "Stage 2": {"provision_rate": stage_2_provision_rate},
                        "Stage 3": {"provision_rate": stage_3_provision_rate}
                    },
                    result_summary={
                        "Stage 1": {
                            "num_loans": stage_1_count,
                            "total_loan_value": float(round(stage_1_balance, 2)),
                            "provision_amount": float(stage_1_provision),
                            "provision_rate": float(stage_1_provision_rate * 100),
                        },
                        "Stage 2": {
                            "num_loans": stage_2_count,
                            "total_loan_value": float(round(stage_2_balance, 2)),
                            "provision_amount": float(stage_2_provision),
                            "provision_rate": float(stage_2_provision_rate * 100),
                        },
                        "Stage 3": {
                            "num_loans": stage_3_count,
                            "total_loan_value": float(round(stage_3_balance, 2)),
                            "provision_amount": float(stage_3_provision),
                            "provision_rate": float(stage_3_provision_rate * 100),
                        },
                        "total_loans": stage_1_count + stage_2_count + stage_3_count
                    }
                )
                db.add(ecl_calculation_result)
                db.commit()
                
                calculation_results["ecl"] = {
                    "status": "success",
                    "total_provision": total_provision,
                    "provision_percentage": provision_percentage
                }
            else:
                calculation_results["ecl"] = {
                    "status": "error",
                    "error": "No ECL staging results found"
                }
            
        except Exception as e:
            logger.error(f"Error during ECL calculation: {str(e)}")
            calculation_results["ecl"] = {
                "status": "error",
                "error": str(e)
            }
        
        # 2. Perform local impairment calculation
        try:
            get_task_manager().update_progress(
                task_id,
                progress=98,
                status_message="Calculating local impairment provisions"
            )
            
            # Get the latest local impairment staging result
            latest_local_staging = (
                db.query(StagingResult)
                .filter(
                    StagingResult.portfolio_id == portfolio_id,
                    StagingResult.staging_type == "local_impairment"
                )
                .order_by(StagingResult.created_at.desc())
                .first()
            )
            
            if latest_local_staging and latest_local_staging.result_summary:
                # Extract category data
                local_result = latest_local_staging.result_summary
                
                # Get category data
                current_data = local_result.get("Current", {})
                olem_data = local_result.get("OLEM", {})
                substandard_data = local_result.get("Substandard", {})
                doubtful_data = local_result.get("Doubtful", {})
                loss_data = local_result.get("Loss", {})
                
                # Get loan counts and balances
                current_count = current_data.get("num_loans", 0)
                current_balance = current_data.get("outstanding_loan_balance", 0)
                olem_count = olem_data.get("num_loans", 0)
                olem_balance = olem_data.get("outstanding_loan_balance", 0)
                substandard_count = substandard_data.get("num_loans", 0)
                substandard_balance = substandard_data.get("outstanding_loan_balance", 0)
                doubtful_count = doubtful_data.get("num_loans", 0)
                doubtful_balance = doubtful_data.get("outstanding_loan_balance", 0)
                loss_count = loss_data.get("num_loans", 0)
                loss_balance = loss_data.get("outstanding_loan_balance", 0)
                
                # Calculate provisions (simplified)
                current_provision_rate = 0.01  # 1% for current
                olem_provision_rate = 0.05  # 5% for OLEM
                substandard_provision_rate = 0.20  # 20% for substandard
                doubtful_provision_rate = 0.50  # 50% for doubtful
                loss_provision_rate = 1.00  # 100% for loss
                
                current_provision = current_balance * current_provision_rate
                olem_provision = olem_balance * olem_provision_rate
                substandard_provision = substandard_balance * substandard_provision_rate
                doubtful_provision = doubtful_balance * doubtful_provision_rate
                loss_provision = loss_balance * loss_provision_rate
                
                # Calculate total provision and percentage
                total_provision = current_provision + olem_provision + substandard_provision + doubtful_provision + loss_provision
                total_balance = current_balance + olem_balance + substandard_balance + doubtful_balance + loss_balance
                provision_percentage = (total_provision / total_balance) * 100 if total_balance > 0 else 0
                
                # Round all values to 2 decimal places
                current_provision = round(current_provision, 2)
                olem_provision = round(olem_provision, 2)
                substandard_provision = round(substandard_provision, 2)
                doubtful_provision = round(doubtful_provision, 2)
                loss_provision = round(loss_provision, 2)
                total_provision = round(total_provision, 2)
                provision_percentage = round(provision_percentage, 2)
                
                # Create calculation result
                local_calculation_result = CalculationResult(
                    portfolio_id=portfolio_id,
                    calculation_type="local_impairment",
                    total_provision=total_provision,
                    provision_percentage=provision_percentage,
                    reporting_date=datetime.now().date(),
                    config={
                        "Current": {"provision_rate": current_provision_rate},
                        "OLEM": {"provision_rate": olem_provision_rate},
                        "Substandard": {"provision_rate": substandard_provision_rate},
                        "Doubtful": {"provision_rate": doubtful_provision_rate},
                        "Loss": {"provision_rate": loss_provision_rate}
                    },
                    result_summary={
                        "Current": {
                            "num_loans": current_count,
                            "total_loan_value": round(current_balance, 2),
                            "provision_amount": current_provision,
                            "provision_rate": int(current_provision_rate * 100)
                        },
                        "OLEM": {
                            "num_loans": olem_count,
                            "total_loan_value": round(olem_balance, 2),
                            "provision_amount": olem_provision,
                            "provision_rate": int(olem_provision_rate * 100)
                        },
                        "Substandard": {
                            "num_loans": substandard_count,
                            "total_loan_value": round(substandard_balance, 2),
                            "provision_amount": substandard_provision,
                            "provision_rate": int(substandard_provision_rate * 100)
                        },
                        "Doubtful": {
                            "num_loans": doubtful_count,
                            "total_loan_value": round(doubtful_balance, 2),
                            "provision_amount": doubtful_provision,
                            "provision_rate": int(doubtful_provision_rate * 100)
                        },
                        "Loss": {
                            "num_loans": loss_count,
                            "total_loan_value": round(loss_balance, 2),
                            "provision_amount": loss_provision,
                            "provision_rate": int(loss_provision_rate * 100)
                        },
                        "total_loans": current_count + olem_count + substandard_count + doubtful_count + loss_count
                    }
                )
                db.add(local_calculation_result)
                db.commit()
                
                calculation_results["local_impairment"] = {
                    "status": "success",
                    "total_provision": total_provision,
                    "provision_percentage": provision_percentage
                }
            else:
                calculation_results["local_impairment"] = {
                    "status": "error",
                    "error": "No local impairment staging results found"
                }
            
        except Exception as e:
            logger.error(f"Error during local impairment calculation: {str(e)}")
            calculation_results["local_impairment"] = {
                "status": "error",
                "error": str(e)
            }
        
        # Add calculation results to the overall results
        results["calculation"] = calculation_results
        
        # Commit after all calculation operations
        db.commit()
        
        # Finally, create quality issues - 2% of progress
        try:
            get_task_manager().update_progress(
                task_id,
                progress=99,
                status_message="Checking data quality"
            )
            await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
            
            # Create quality issues
            quality_result = create_and_save_quality_issues(db, portfolio_id, task_id)
            results["quality_checks"] = quality_result
            db.commit()
            
        except Exception as e:
            logger.error(f"Error creating quality issues: {str(e)}")
            results["quality_checks"] = {
                "status": "error",
                "error": str(e)
            }
            db.rollback()
        
        # Complete the task
        get_task_manager().update_progress(
            task_id,
            progress=100,
            status_message=f"Completed processing {results['files_processed']} files"
        )
        await asyncio.sleep(0.1)  # Small delay to ensure WebSocket message is sent
        
        return results
        
    except Exception as e:
        logger.error(f"Error in portfolio ingestion: {str(e)}")
        get_task_manager().update_task(
            task_id,
            status="error",
            error=str(e),
            status_message=f"Error during ingestion: {str(e)}",
            progress=100
        )
        raise

def process_portfolio_ingestion_sync(
    portfolio_id: int,
    loan_details_content: Optional[bytes] = None,
    client_data_content: Optional[bytes] = None,
    loan_guarantee_data_content: Optional[bytes] = None,
    loan_collateral_data_content: Optional[bytes] = None,
    db: Session = None
) -> Dict[str, Any]:
    """
    Process portfolio data ingestion synchronously.
    
    This function orchestrates the processing of multiple data files for a portfolio
    and returns the results directly.
    """
    try:
        # Initialize result tracking
        results = {
            "portfolio_id": portfolio_id,
            "files_processed": 0,
            "total_files": 0,
            "details": {}
        }
        
        # Check for and delete existing data for this portfolio
        try:
            logger.info(f"Checking for existing data in portfolio {portfolio_id}")
            
            # Delete existing data in reverse order of dependencies
            # First delete staging results and calculation results
            staging_count = db.query(StagingResult).filter(StagingResult.portfolio_id == portfolio_id).delete()
            calculation_count = db.query(CalculationResult).filter(CalculationResult.portfolio_id == portfolio_id).delete()
            
            # Delete quality issues
            quality_count = db.query(QualityIssue).filter(QualityIssue.portfolio_id == portfolio_id).delete()
            
            # Delete loans, guarantees, and clients
            loan_count = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).delete()
            guarantee_count = db.query(Guarantee).filter(Guarantee.portfolio_id == portfolio_id).delete()
            client_count = db.query(Client).filter(Client.portfolio_id == portfolio_id).delete()
            
            # Commit the deletions
            db.commit()
            
            logger.info(f"Cleared existing data: {loan_count} loans, {client_count} clients, {guarantee_count} guarantees")
            
            # Log the deletion results but don't add to response
            logger.info(f"Data cleared: loans={loan_count}, clients={client_count}, guarantees={guarantee_count}, " +
                       f"quality_issues={quality_count}, staging_results={staging_count}, calculation_results={calculation_count}")
            
        except Exception as e:
            logger.error(f"Error clearing existing data: {str(e)}")
            results["errors"] = results.get("errors", []) + [f"Error clearing existing data: {str(e)}"]
        
        # Count files to process
        files_to_process = 0
        if loan_details_content:
            files_to_process += 1
        if client_data_content:
            files_to_process += 1
        if loan_guarantee_data_content:
            files_to_process += 1
        if loan_collateral_data_content:
            files_to_process += 1
        
        results["total_files"] = files_to_process
        
        # Process loan details if provided
        if loan_details_content:
            try:
                logger.info(f"Processing loan details for portfolio {portfolio_id}")
                
                # Create BytesIO object from content
                loan_details_io = io.BytesIO(loan_details_content)
                
                # Process the loan details
                loan_results = process_loan_details_sync(loan_details_io, portfolio_id, db)
                
                # Add results to the overall results
                results["details"]["loan_details"] = loan_results
                results["files_processed"] += 1
                
                logger.info(f"Processed {loan_results.get('processed', 0)} loan records")
                
            except Exception as e:
                logger.error(f"Error processing loan details: {str(e)}")
                results["details"]["loan_details"] = {"error": str(e)}
                results["errors"] = results.get("errors", []) + [f"Error processing loan details: {str(e)}"]
        
        # Process client data if provided
        if client_data_content:
            try:
                logger.info(f"Processing client data for portfolio {portfolio_id}")
                
                # Create BytesIO object from content
                client_data_io = io.BytesIO(client_data_content)
                
                # Process the client data
                client_results = process_client_data_sync(client_data_io, portfolio_id, db)
                
                # Add results to the overall results
                results["details"]["client_data"] = client_results
                results["files_processed"] += 1
                
                logger.info(f"Processed {client_results.get('processed', 0)} client records")
                
            except Exception as e:
                logger.error(f"Error processing client data: {str(e)}")
                results["details"]["client_data"] = {"error": str(e)}
                results["errors"] = results.get("errors", []) + [f"Error processing client data: {str(e)}"]
        
        # Process loan guarantee data if provided
        if loan_guarantee_data_content:
            try:
                logger.info(f"Processing loan guarantee data for portfolio {portfolio_id}")
                
                # Create BytesIO object from content
                loan_guarantee_data_io = io.BytesIO(loan_guarantee_data_content)
                
                # Process the loan guarantee data
                guarantee_results = process_loan_guarantees_sync(loan_guarantee_data_io, portfolio_id, db)
                
                # Add results to the overall results
                results["details"]["loan_guarantee_data"] = guarantee_results
                results["files_processed"] += 1
                
                logger.info(f"Processed {guarantee_results.get('processed', 0)} guarantee records")
                
            except Exception as e:
                logger.error(f"Error processing loan guarantee data: {str(e)}")
                results["details"]["loan_guarantee_data"] = {"error": str(e)}
                results["errors"] = results.get("errors", []) + [f"Error processing loan guarantee data: {str(e)}"]
        
        # Process loan collateral data if provided
        if loan_collateral_data_content:
            try:
                logger.info(f"Processing loan collateral data for portfolio {portfolio_id}")
                
                # Create BytesIO object from content
                loan_collateral_data_io = io.BytesIO(loan_collateral_data_content)
                
                # Process the loan collateral data
                collateral_results = process_collateral_data_sync(loan_collateral_data_io, portfolio_id, db)
                
                # Add results to the overall results
                results["details"]["loan_collateral_data"] = collateral_results
                results["files_processed"] += 1
                
                logger.info(f"Processed {collateral_results.get('processed', 0)} collateral records")
                
            except Exception as e:
                logger.error(f"Error processing loan collateral data: {str(e)}")
                results["details"]["loan_collateral_data"] = {"error": str(e)}
                results["errors"] = results.get("errors", []) + [f"Error processing loan collateral data: {str(e)}"]
        
        # Perform quality checks
        try:
            logger.info(f"Performing quality checks for portfolio {portfolio_id}")
            
            # Run quality checks
            quality_results = run_quality_checks_sync(portfolio_id, db)
            
            # Add results to the overall results
            results["quality_checks"] = quality_results
            
            logger.info(f"Found {quality_results.get('total_issues', 0)} quality issues")
            
        except Exception as e:
            logger.error(f"Error running quality checks: {str(e)}")
            results["quality_checks"] = {"error": str(e)}
            results["errors"] = results.get("errors", []) + [f"Error running quality checks: {str(e)}"]
        
        # Perform loan staging
        try:
            logger.info(f"Performing loan staging for portfolio {portfolio_id}")
            
            # Get the current date as the reporting date
            reporting_date = date.today()
            
            # Perform ECL staging
            ecl_config = ECLStagingConfig(
                stage_1=DaysRangeConfig(days_range="0-30"),
                stage_2=DaysRangeConfig(days_range="31-90"),
                stage_3=DaysRangeConfig(days_range="91+")
            )
            ecl_staging_result = stage_loans_ecl_orm_sync(portfolio_id, ecl_config, db)
            
            # Perform local impairment staging
            local_config = LocalImpairmentConfig(
                current=DaysRangeConfig(days_range="0-30"),
                olem=DaysRangeConfig(days_range="31-90"),
                substandard=DaysRangeConfig(days_range="91-180"),
                doubtful=DaysRangeConfig(days_range="181-360"),
                loss=DaysRangeConfig(days_range="361+")
            )
            local_staging_result = stage_loans_local_impairment_orm_sync(portfolio_id, local_config, db)
            
            # Add staging results to the overall results
            results["staging"] = {
                "ecl": {
                    "status": "success",
                    "date": reporting_date.isoformat()
                },
                "local_impairment": {
                    "status": "success",
                    "date": reporting_date.isoformat()
                }
            }
            
            logger.info(f"Successfully completed staging for portfolio {portfolio_id}")
            
        except Exception as e:
            logger.error(f"Error during loan staging: {str(e)}")
            results["staging"] = {"error": str(e)}
            results["errors"] = results.get("errors", []) + [f"Error during loan staging: {str(e)}"]
        
        # Final status
        if "errors" in results and results["errors"]:
            results["status"] = "completed_with_errors"
        else:
            results["status"] = "completed"
        
        logger.info(f"Portfolio ingestion completed with status: {results['status']}")
        
        return results
        
    except Exception as e:
        logger.error(f"Error in portfolio ingestion: {str(e)}")
        return {
            "status": "error",
            "error": str(e),
            "portfolio_id": portfolio_id
        }

async def start_background_ingestion(
    portfolio_id: int,
    loan_details: Optional[UploadFile] = None,
    client_data: Optional[UploadFile] = None,
    loan_guarantee_data: Optional[UploadFile] = None,
    loan_collateral_data: Optional[UploadFile] = None,
    db: Session = None
) -> str:
    """
    Start a background task for portfolio data ingestion.
    
    Returns the task ID that can be used to track progress.
    """
    # Create a new task
    task_id = get_task_manager().create_task(
        task_type="portfolio_ingestion",
        description=f"Ingesting data for portfolio {portfolio_id}"
    )
    
    # Read file contents immediately to prevent file closure issues
    loan_details_content = None
    loan_details_filename = None
    client_data_content = None
    client_data_filename = None
    loan_guarantee_data_content = None
    loan_guarantee_data_filename = None
    loan_collateral_data_content = None
    loan_collateral_data_filename = None
    
    if loan_details:
        loan_details_content = await loan_details.read()
        loan_details_filename = loan_details.filename
    
    if client_data:
        client_data_content = await client_data.read()
        client_data_filename = client_data.filename
    
    if loan_guarantee_data:
        loan_guarantee_data_content = await loan_guarantee_data.read()
        loan_guarantee_data_filename = loan_guarantee_data.filename
    
    if loan_collateral_data:
        loan_collateral_data_content = await loan_collateral_data.read()
        loan_collateral_data_filename = loan_collateral_data.filename
    
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
                    process_portfolio_ingestion,
                    portfolio_id=portfolio_id,
                    loan_details_content=loan_details_content,
                    loan_details_filename=loan_details_filename,
                    client_data_content=client_data_content,
                    client_data_filename=client_data_filename,
                    loan_guarantee_data_content=loan_guarantee_data_content,
                    loan_guarantee_data_filename=loan_guarantee_data_filename,
                    loan_collateral_data_content=loan_collateral_data_content,
                    loan_collateral_data_filename=loan_collateral_data_filename,
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
