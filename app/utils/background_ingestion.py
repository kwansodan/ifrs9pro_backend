import asyncio
import logging
from typing import Optional, Dict, Any, List
from fastapi import UploadFile
from sqlalchemy.orm import Session
import io
import random
import time
from datetime import datetime, date
from decimal import Decimal

from app.models import (
    Loan,
    Guarantee,
    Client,
    Report,
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

from app.utils.process_email_notifyer import send_ingestion_success_email, send_ingestion_failed_email, send_ingestion_began_email
from app.utils.processors import process_loan_guarantees, process_collateral_data
from app.utils.background_calculations import (
    process_ecl_calculation_sync,
    process_local_impairment_calculation_sync
)
from app.database import SessionLocal
from app.schemas import ECLStagingConfig, LocalImpairmentConfig, DaysRangeConfig
from app.utils.staging import (
    stage_loans_ecl_orm, 
    stage_loans_local_impairment_orm
)
from app.utils.quality_checks import create_quality_issues_if_needed, create_and_save_quality_issues

logger = logging.getLogger(__name__)


async def process_portfolio_ingestion_sync(
    task_id: str,
    portfolio_id: int,
    loan_details_content: bytes = None,
    loan_details_filename: str = None,
    client_data_content: bytes = None,
    client_data_filename: str = None,
    loan_guarantee_data_content: bytes = None,
    loan_guarantee_data_filename: str = None,
    loan_collateral_data_content: bytes = None,
    loan_collateral_data_filename: str = None,
    db: Session = None,
    first_name:str = None,
    user_email:str = None,
    uploaded_filenames:str = None,
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
        try:
            await send_ingestion_success_email(user_email, first_name, portfolio_id, uploaded_filenames, cc_emails=["support@service4gh.com"])
        except:
            logger.error("Failed to send ingestion success email")

        # Check for and delete existing data for this portfolio
        try:
            start = time.perf_counter()
            logger.info(f"Checking for existing data in portfolio {portfolio_id}")
            
            
            # First delete staging results and calculation results
            
            calculation_count = db.query(CalculationResult).filter(CalculationResult.portfolio_id == portfolio_id).delete()
            
            # Delete quality issues
            quality_count = db.query(QualityIssue).filter(QualityIssue.portfolio_id == portfolio_id).delete()
            
            # Delete loans, guarantees, and clients
            loan_count = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).delete()
            guarantee_count = db.query(Guarantee).filter(Guarantee.portfolio_id == portfolio_id).delete()
            client_count = db.query(Client).filter(Client.portfolio_id == portfolio_id).delete()
            report_count = db.query(Report).filter(Report.portfolio_id == portfolio_id).delete()
            
            # Commit the deletions
            db.commit()
            
            # Log the deletion results but don't add to response
            logger.info(f"The following previously held data in the current portfolio cleared: Loans:{loan_count}, Clients:{client_count}, Loan guarantees:{guarantee_count}, " +
                       f"Quality_issues:{quality_count}, Calculation_results:{calculation_count}, Generated reports:{report_count}")
            
        except Exception as e:
            logger.error(f"Error clearing existing data: {str(e)}")
            results["errors"] = results.get("errors", []) + [f"Error clearing existing data: {str(e)}"]
        end = time.perf_counter()
        logger.info(f"Data clearing took {end - start:0.4f} seconds")


        start = time.perf_counter()
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
        end = time.perf_counter()
        logger.info(f"File counting took {end - start:0.4f} seconds")

        # Process loan details if provided
        if loan_details_content:
            start = time.perf_counter()
            try:
                logger.info(f"Processing loan details for portfolio no {portfolio_id}")
                
                # Create BytesIO object from content
                loan_details_io = io.BytesIO(loan_details_content)
                
                # Process the loan details
                loan_results = await process_loan_details_sync(loan_details_io, portfolio_id, db)


                # Add results to the overall results
                results["details"]["loan_details"] = loan_results
                results["files_processed"] += 1
                
                logger.info(f"Successfully cleaned and stored {loan_results.get('processed', 0)} loan records")
                
            except Exception as e:
                logger.error(f"Error encountered while attempting to clean and store loan details: {str(e)}")
                results["details"]["loan_details"] = {"error": str(e)}
                results["errors"] = results.get("errors", []) + [f"Error encountered while attempting to clean and store loan details: {str(e)}"]
            end = time.perf_counter()
            logger.info(f"Loan details processing took {end - start:0.4f} seconds")
        
        # Process client data if provided
        if client_data_content:
            start = time.perf_counter()
            try:
                logger.info(f"Processing client data for portfolio {portfolio_id}")
                
                # Create BytesIO object from content
                client_data_io = io.BytesIO(client_data_content)
                
                # Process the client data
                client_results = await process_client_data_sync(client_data_io, portfolio_id, db)
                
                # Add results to the overall results
                results["details"]["client_data"] = client_results
                results["files_processed"] += 1
                
                logger.info(f"Processed {client_results.get('processed', 0)} client records")
                
            except Exception as e:
                logger.error(f"Error processing client data: {str(e)}")
                results["details"]["client_data"] = {"error": str(e)}
                results["errors"] = results.get("errors", []) + [f"Error processing client data: {str(e)}"]
            end = time.perf_counter()
            logger.info(f"Client data processing took {end - start:0.4f} seconds")

        # Process loan guarantee data if provided
        if loan_guarantee_data_content:
            start = time.perf_counter()
            try:
                logger.info(f"Processing loan guarantee data for portfolio {portfolio_id}")
                
                # Create BytesIO object from content
                loan_guarantee_data_io = io.BytesIO(loan_guarantee_data_content)
                
                # Process the loan guarantee data
                guarantee_results=await process_loan_guarantees(io.BytesIO(loan_guarantee_data_content), portfolio_id=portfolio_id, db=db)
                
                # Add results to the overall results
                results["details"]["loan_guarantee_data"] = guarantee_results
                results["files_processed"] += 1
                
                logger.info(f"Processed {guarantee_results.get('processed', 0)} guarantee records")
                
            except Exception as e:
                logger.error(f"Error processing loan guarantee data: {str(e)}")
                results["details"]["loan_guarantee_data"] = {"error": str(e)}
                results["errors"] = results.get("errors", []) + [f"Error processing loan guarantee data: {str(e)}"]
            end = time.perf_counter()
            logger.info(f"Loan guarantee data processing took {end - start:0.4f} seconds")

        # Process loan collateral data if provided
        if loan_collateral_data_content:
            start = time.perf_counter()
            try:
                logger.info(f"Processing loan collateral data for portfolio {portfolio_id}")
                
                # Create BytesIO object from content
                loan_collateral_data_io = io.BytesIO(loan_collateral_data_content)
                
                # Process the loan collateral data
                collateral_results = await process_collateral_data(loan_collateral_data_io, portfolio_id, db)
                
                # Add results to the overall results
                results["details"]["loan_collateral_data"] = collateral_results
                results["files_processed"] += 1
                
                logger.info(f"Processed {collateral_results.get('processed', 0)} collateral records")
                
            except Exception as e:
                logger.error(f"Error processing loan collateral data: {str(e)}")
                results["details"]["loan_collateral_data"] = {"error": str(e)}
                results["errors"] = results.get("errors", []) + [f"Error processing loan collateral data: {str(e)}"]
            end = time.perf_counter()
            logger.info(f"Loan collateral data processing took {end - start:0.4f} seconds")

        # Perform quality checks
        start = time.perf_counter()
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
        end = time.perf_counter()
        logger.info(f"Quality checks took {end - start:0.4f} seconds")

        # Perform loan staging
        start = time.perf_counter()
        try:
            logger.info(f"Starting loan staging for portfolio {portfolio_id}")
            
            # Get the current date as the reporting date
            reporting_date = date.today()

            await stage_loans_ecl_orm(portfolio_id, db)
            await stage_loans_local_impairment_orm(portfolio_id, db)

            db.commit()
            logger.info(f"Successfully completed staging for portfolio {portfolio_id}")
             
        except Exception as e:
            logger.error(f"Error during loan staging: {str(e)}")
            results["staging"] = {"error": str(e)}
            results["errors"] = results.get("errors", []) + [f"Error during loan staging: {str(e)}"]
        end = time.perf_counter()

        # Final status
        if "errors" in results and results["errors"]:
            results["status"] = "completed_with_errors"
        else:
            results["status"] = "completed"
        
        logger.info(f"Portfolio ingestion completed with status: {results['status']}")
        
        return results
        
    except Exception as e:
        logger.error(f"Error in portfolio ingestion: {str(e)}")
        try:
            await send_ingestion_failed_email(user_email, first_name, portfolio_id, uploaded_filenames, cc_emails=["support@service4gh.com"])
        except:
            logger.error("Failed to send ingestion failed email")
        return {
            "status": "error",
            "error": str(e),
            "portfolio_id": portfolio_id
        }

    finally:
        try:
            await send_ingestion_success_email(user_email, first_name, portfolio_id, uploaded_filenames, cc_emails=["support@service4gh.com"])
        except:
            logger.error("Failed to send ingestion success email")


async def start_background_ingestion(
    portfolio_id: int,
    loan_details: Optional[UploadFile] = None,
    client_data: Optional[UploadFile] = None,
    loan_guarantee_data: Optional[UploadFile] = None,
    loan_collateral_data: Optional[UploadFile] = None,
    db: Session = None,
    first_name: str = None,
    user_email: str = None,
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
    import threading

    # Read file contents immediately to prevent file closure issues
    loan_details_content = None
    loan_details_filename = None
    client_data_content = None
    client_data_filename = None
    loan_guarantee_data_content = None
    loan_guarantee_data_filename = None
    loan_collateral_data_content = None
    loan_collateral_data_filename = None
    
    uploaded_filenames = []
    if loan_details:
        loan_details_content = await loan_details.read()
        loan_details_filename = loan_details.filename
        uploaded_filenames.append(loan_details_filename)
    
    if client_data:
        client_data_content = await client_data.read()
        client_data_filename = client_data.filename
        uploaded_filenames.append(client_data_filename)
    
    if loan_guarantee_data:
        loan_guarantee_data_content = await loan_guarantee_data.read()
        loan_guarantee_data_filename = loan_guarantee_data.filename
        uploaded_filenames.append(loan_guarantee_data_filename)
    
    if loan_collateral_data:
        loan_collateral_data_content = await loan_collateral_data.read()
        loan_collateral_data_filename = loan_collateral_data.filename
        uploaded_filenames.append(loan_collateral_data_filename)
    
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
                    process_portfolio_ingestion_sync,
                    portfolio_id=portfolio_id,
                    loan_details_content=loan_details_content,
                    loan_details_filename=loan_details_filename,
                    client_data_content=client_data_content,
                    client_data_filename=client_data_filename,
                    loan_guarantee_data_content=loan_guarantee_data_content,
                    loan_guarantee_data_filename=loan_guarantee_data_filename,
                    loan_collateral_data_content=loan_collateral_data_content,
                    loan_collateral_data_filename=loan_collateral_data_filename,
                    db=thread_db,
                    first_name=first_name,
                    user_email=user_email,
                    uploaded_filenames=uploaded_filenames
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
    
    return {
        "task_id": task_id,
        "message": f"Portfolio {portfolio_id} ingestion started successfully.",
        "uploaded_files": uploaded_filenames,
        "status_check_url": f"/tasks/{task_id}/status"
    }

