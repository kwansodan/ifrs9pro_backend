import asyncio
import logging
from typing import Optional, Dict, Any, List
from fastapi import UploadFile
from sqlalchemy.orm import Session
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from app.utils.background_tasks import task_manager, run_background_task
from app.utils.background_processors import (
    process_loan_details_with_progress,
    process_client_data_with_progress
)
from app.models import Portfolio, StagingResult
from app.database import SessionLocal
from app.schemas import ECLStagingConfig, LocalImpairmentConfig, DaysRangeConfig
from app.utils.staging import stage_loans_ecl_orm, stage_loans_local_impairment_orm
from app.utils.quality_checks import create_quality_issues_if_needed

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
            task_manager.update_task(
                task_id, 
                status_message="No files provided for ingestion",
                progress=100
            )
            return results
        
        # Process each file in sequence - this takes 70% of the progress
        for i, (file_type, file_content, filename) in enumerate(files_to_process):
            # Update overall task progress
            overall_progress = (i / len(files_to_process)) * 70
            task_manager.update_progress(
                task_id,
                progress=overall_progress,
                status_message=f"Processing {file_type} ({i+1}/{len(files_to_process)})"
            )
            
            # Process based on file type
            if file_type == "loan_details":
                task_manager.update_task(task_id, status_message=f"Processing loan details from {filename}")
                file_result = await process_loan_details_with_progress(task_id, file_content, portfolio_id, db)
                results["details"]["loan_details"] = file_result
                
            elif file_type == "client_data":
                task_manager.update_task(task_id, status_message=f"Processing client data from {filename}")
                file_result = await process_client_data_with_progress(task_id, file_content, portfolio_id, db)
                results["details"]["client_data"] = file_result
                
            # Increment processed files count
            results["files_processed"] += 1
            
            # Update overall progress
            overall_progress = ((i + 1) / len(files_to_process)) * 70
            task_manager.update_progress(
                task_id,
                progress=overall_progress,
                status_message=f"Completed processing {file_type}"
            )
        
        # Now perform staging operations - 20% of progress
        task_manager.update_progress(
            task_id,
            progress=75,
            status_message="Starting loan staging operations"
        )
        
        staging_results = {}
        
        # 1. Perform ECL staging
        try:
            task_manager.update_progress(
                task_id,
                progress=80,
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
            task_manager.update_progress(
                task_id,
                progress=85,
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
        
        # Finally, create quality issues - 10% of progress
        try:
            task_manager.update_progress(
                task_id,
                progress=90,
                status_message="Checking data quality"
            )
            
            # Create quality issues
            quality_result = create_quality_issues_if_needed(db, portfolio_id)
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
        task_manager.update_progress(
            task_id,
            progress=100,
            status_message=f"Completed processing {results['files_processed']} files"
        )
        
        return results
        
    except Exception as e:
        logger.error(f"Error in portfolio ingestion: {str(e)}")
        task_manager.update_task(
            task_id,
            status="error",
            error=str(e),
            status_message=f"Error during ingestion: {str(e)}",
            progress=100
        )
        raise

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
    task_id = task_manager.create_task(
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
            task_manager.mark_as_failed(task_id, str(e))
        finally:
            # Close the database session
            thread_db.close()
            loop.close()
    
    # Start the task in a separate thread
    thread = threading.Thread(target=run_task_in_thread)
    thread.daemon = True  # Allow the thread to be terminated when the main program exits
    thread.start()
    
    return task_id
