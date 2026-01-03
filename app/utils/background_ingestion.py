import asyncio
import logging
from typing import Optional, Dict, Any, List
from fastapi import UploadFile, HTTPException
from sqlalchemy.orm import Session
import io
import tempfile
from io import BytesIO
import random
import pandas as pd
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
    QualityIssue,
    TenantSubscription,
    SubscriptionUsage,
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
from app.config import settings
from app.utils.minio_reports_factory import s3_client
from app.utils.quality_checks import create_quality_issues_if_needed, create_and_save_quality_issues
from sqlalchemy.inspection import inspect as sqlalchemy_inspect
from sqlalchemy.exc import NoInspectionAvailable
from pydantic import BaseModel
from app.schemas import IngestPayload  

logger = logging.getLogger(__name__)


def get_model_columns(model):
    # If user passed a Pydantic model, get fields from schema
    if isinstance(model, type) and issubclass(model, BaseModel):
        return list(model.__fields__.keys())

    # Otherwise treat as SQLAlchemy model
    try:
        mapper = sqlalchemy_inspect(model)
        return mapper.columns.keys()
    except NoInspectionAvailable:
        raise ValueError(f"Model {model} is not SQLAlchemy or Pydantic")



async def process_portfolio_ingestion_sync(
    task_id: str,
    portfolio_id: int,
    tenant_id: int,
    loan_details_content: pd.DataFrame = None,
    client_data_content: pd.DataFrame = None,
    loan_guarantee_content: pd.DataFrame = None,
    loan_collateral_data_content: pd.DataFrame = None,
    db: Session = None,
    first_name: str = None,
    user_email: str = None,
    uploaded_filenames: str = None,
) -> Dict[str, Any]:
    """
    Process portfolio data ingestion synchronously using pandas DataFrames.
    
    Sends:
        - start email when ingestion begins
        - fail email if any error occurs
        - success email only after the entire process completes without errors
    """
    results = {
        "portfolio_id": portfolio_id,
        "files_processed": 0,
        "total_files": 0,
        "details": {}
    }

    try:
        # ---------- Clear existing portfolio data ----------
        try:
            start = time.perf_counter()
            logger.info(f"Checking for existing data in portfolio {portfolio_id}")

            calculation_count = db.query(CalculationResult).filter(
                CalculationResult.portfolio_id == portfolio_id
            ).delete()
            quality_count = db.query(QualityIssue).filter(
                QualityIssue.portfolio_id == portfolio_id
            ).delete()
            # Track how many loans currently exist for this portfolio
            existing_loans_for_portfolio = db.query(Loan).filter(
                Loan.portfolio_id == portfolio_id
            ).count()

            loan_count = db.query(Loan).filter(
                Loan.portfolio_id == portfolio_id
            ).delete()
            guarantee_count = db.query(Guarantee).filter(
                Guarantee.portfolio_id == portfolio_id
            ).delete()
            client_count = db.query(Client).filter(
                Client.portfolio_id == portfolio_id
            ).delete()
            report_count = db.query(Report).filter(
                Report.portfolio_id == portfolio_id
            ).delete()

            db.commit()

            logger.info(
                f"Cleared existing data: Loans:{loan_count}, Clients:{client_count}, "
                f"Guarantees:{guarantee_count}, Quality Issues:{quality_count}, "
                f"Calculation Results:{calculation_count}, Reports:{report_count}"
            )
        except Exception as e:
            logger.error(f"Error clearing existing data: {str(e)}")
            results.setdefault("errors", []).append(f"Error clearing existing data: {str(e)}")
        end = time.perf_counter()
        logger.info(f"Data clearing took {end - start:0.4f} seconds")

        # ---------- Count files ----------
        files_to_process = sum(
            df is not None for df in [
                loan_details_content, client_data_content,
                loan_guarantee_content, loan_collateral_data_content
            ]
        )
        results["total_files"] = files_to_process
        logger.info(f"Total files to process: {files_to_process}")

        # ---------- Process loan details ----------
        if loan_details_content is not None:
            start = time.perf_counter()
            try:
                logger.info(f"Processing loan details for portfolio {portfolio_id}")
                loan_results = await process_loan_details_sync(loan_details_content, portfolio_id, tenant_id, db)
                results["details"]["loan_details"] = loan_results
                results["files_processed"] += 1
                logger.info(f"Processed {loan_results.get('processed', 0)} loan records")
            except Exception as e:
                logger.error(f"Error processing loan details: {str(e)}")
                results["details"]["loan_details"] = {"error": str(e)}
                results.setdefault("errors", []).append(f"Error processing loan details: {str(e)}")
            end = time.perf_counter()
            logger.info(f"Loan details processing took {end - start:0.4f} seconds")

        # ---------- Process client data ----------
        if client_data_content is not None:
            start = time.perf_counter()
            try:
                logger.info(f"Processing client data for portfolio {portfolio_id}")
                client_results = await process_client_data_sync(client_data_content, portfolio_id, tenant_id, db)
                results["details"]["client_data"] = client_results
                results["files_processed"] += 1
                logger.info(f"Processed {client_results.get('processed', 0)} client records")
            except Exception as e:
                logger.error(f"Error processing client data: {str(e)}")
                results["details"]["client_data"] = {"error": str(e)}
                results.setdefault("errors", []).append(f"Error processing client data: {str(e)}")
            end = time.perf_counter()
            logger.info(f"Client data processing took {end - start:0.4f} seconds")

        # ---------- Process loan guarantee data ----------
        if loan_guarantee_content is not None:
            start = time.perf_counter()
            try:
                logger.info(f"Processing loan guarantee data for portfolio {portfolio_id}")
                guarantee_results = await process_loan_guarantees(loan_guarantee_content, portfolio_id, db)
                results["details"]["loan_guarantee_data"] = guarantee_results
                results["files_processed"] += 1
                logger.info(f"Processed {guarantee_results.get('processed', 0)} guarantee records")
            except Exception as e:
                logger.error(f"Error processing loan guarantee data: {str(e)}")
                results["details"]["loan_guarantee_data"] = {"error": str(e)}
                results.setdefault("errors", []).append(f"Error processing loan guarantee data: {str(e)}")
            end = time.perf_counter()
            logger.info(f"Loan guarantee processing took {end - start:0.4f} seconds")

        # ---------- Process loan collateral data ----------
        if loan_collateral_data_content is not None:
            start = time.perf_counter()
            try:
                logger.info(f"Processing loan collateral data for portfolio {portfolio_id}")
                collateral_results = await process_collateral_data(loan_collateral_data_content, portfolio_id, db)
                results["details"]["loan_collateral_data"] = collateral_results
                results["files_processed"] += 1
                logger.info(f"Processed {collateral_results.get('processed', 0)} collateral records")
            except Exception as e:
                logger.error(f"Error processing loan collateral data: {str(e)}")
                results["details"]["loan_collateral_data"] = {"error": str(e)}
                results.setdefault("errors", []).append(f"Error processing loan collateral data: {str(e)}")
            end = time.perf_counter()
            logger.info(f"Loan collateral processing took {end - start:0.4f} seconds")

        # ---------- Quality checks ----------
        start = time.perf_counter()
        try:
            logger.info(f"Performing quality checks for portfolio {portfolio_id}")
            quality_results = run_quality_checks_sync(portfolio_id, db)
            results["quality_checks"] = quality_results
            logger.info(f"Found {quality_results.get('total_issues', 0)} quality issues")
        except Exception as e:
            logger.error(f"Error running quality checks: {str(e)}")
            results["quality_checks"] = {"error": str(e)}
            results.setdefault("errors", []).append(f"Error running quality checks: {str(e)}")
        end = time.perf_counter()
        logger.info(f"Quality checks took {end - start:0.4f} seconds")

        # ---------- Loan staging ----------
        start = time.perf_counter()
        try:
            logger.info(f"Starting loan staging for portfolio {portfolio_id}")
            await stage_loans_ecl_orm(portfolio_id, db, user_email=user_email, first_name=first_name)
            await stage_loans_local_impairment_orm(portfolio_id, db, user_email=user_email, first_name=first_name)
            db.commit()
            logger.info(f"Successfully completed staging for portfolio {portfolio_id}")
        except Exception as e:
            logger.error(f"Error during loan staging: {str(e)}")
            results["staging"] = {"error": str(e)}
            results.setdefault("errors", []).append(f"Error during loan staging: {str(e)}")
        end = time.perf_counter()

        # ---------- Recalculate subscription loan usage ----------
        try:
            portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
            if portfolio and portfolio.subscription_id:
                subscription = (
                    db.query(TenantSubscription)
                    .filter(TenantSubscription.id == portfolio.subscription_id)
                    .first()
                )
                if subscription:
                    usage = (
                        db.query(SubscriptionUsage)
                        .filter(SubscriptionUsage.subscription_id == subscription.id)
                        .with_for_update()
                        .first()
                    )
                    if usage:
                        # Authoritative loan count: all loans belonging to this subscription
                        total_loans = (
                            db.query(Loan)
                            .filter(Loan.subscription_id == subscription.id)
                            .count()
                        )
                        usage.current_loan_count = total_loans
                        from datetime import datetime, timezone

                        usage.last_calculated_at = datetime.now(timezone.utc)
                        db.add(usage)
                        db.commit()
        except Exception as e:
            logger.error(f"Failed to recalculate subscription usage after ingestion: {str(e)}")

        # ---------- Final status & emails ----------
        if results.get("errors"):
            results["status"] = "completed_with_errors"
            try:
                await send_ingestion_failed_email(user_email, first_name, portfolio_id, uploaded_filenames,
                                                  cc_emails=["support@service4gh.com"])
            except:
                logger.error("Failed to send ingestion failed email")
        else:
            results["status"] = "completed"
            try:
                await send_ingestion_success_email(user_email, first_name, portfolio_id, uploaded_filenames,
                                                   cc_emails=["support@service4gh.com"])
            except:
                logger.error("Failed to send ingestion success email")

        logger.info(f"Portfolio ingestion completed with status: {results['status']}")
        return results

    except Exception as e:
        logger.error(f"Error in portfolio ingestion: {str(e)}")
        try:
            await send_ingestion_failed_email(user_email, first_name, portfolio_id, uploaded_filenames,
                                              cc_emails=["support@service4gh.com"])
        except:
            logger.error("Failed to send ingestion failed email")
        return {
            "status": "error",
            "error": str(e),
            "portfolio_id": portfolio_id
        }

async def start_background_ingestion(
    portfolio_id: int,
    tenant_id = int,
    loan_details: Optional[pd.DataFrame] = None,
    client_data: Optional[pd.DataFrame] = None,
    loan_guarantee_data: Optional[pd.DataFrame] = None,
    loan_collateral_data: Optional[pd.DataFrame] = None,
    db: Session = None,
    first_name: str = None,
    user_email: str = None,
) -> dict:
    """
    Start a background task for portfolio data ingestion using pandas DataFrames.

    Returns a dict containing the task ID and status info.
    """
    # Create a new task
    task_id = get_task_manager().create_task(
        task_type="portfolio_ingestion",
        description=f"Ingesting data for portfolio {portfolio_id}"
    )
    import threading

    # Use the DataFrames directly, no need to read file bytes
    uploaded_names = []
    loan_details_content = loan_details
    if loan_details_content is not None and not loan_details_content.empty:
        uploaded_names.append("loan_details")

    client_data_content = client_data
    if client_data_content is not None and not client_data_content.empty:
        uploaded_names.append("client_data")

    loan_guarantee_data_content = loan_guarantee_data
    if loan_guarantee_data_content is not None and not loan_guarantee_data_content.empty:
        uploaded_names.append("loan_guarantee_data")

    loan_collateral_data_content = loan_collateral_data
    if loan_collateral_data_content is not None and not loan_collateral_data_content.empty:
        uploaded_names.append("loan_collateral_data")

    # Function to run the background task in a separate thread
    def run_task_in_thread():
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
                    tenant_id=tenant_id,
                    portfolio_id=portfolio_id,
                    loan_details_content=loan_details_content,
                    client_data_content=client_data_content,
                    loan_guarantee_content=loan_guarantee_data_content,
                    loan_collateral_data_content=loan_collateral_data_content,
                    db=thread_db,
                    first_name=first_name,
                    user_email=user_email,
                    uploaded_filenames=uploaded_names
                )
            )

            # Await any remaining tasks in the loop
            pending = asyncio.all_tasks(loop)
            if pending:
                loop.run_until_complete(asyncio.gather(*pending))

        except Exception as e:
            logger.exception(f"Error in background task thread: {e}")
            get_task_manager().mark_as_failed(task_id, str(e))
        finally:
            thread_db.close()
            loop.close()

    # Start the thread
    thread = threading.Thread(target=run_task_in_thread)
    thread.daemon = True
    thread.start()

    return {
        "task_id": task_id,
        "message": f"Portfolio {portfolio_id} ingestion started successfully.",
        "uploaded_dataframes": uploaded_names,
        "status_check_url": f"/tasks/{task_id}/status"
    }


async def fetch_excel_from_minio(payload: IngestPayload, db: Session, user_email, first_name, portfolio_id):
    """
    Processes ingestion of cleaned Excel files from MinIO using Pydantic payload:
    - Validates payload
    - Downloads files from MinIO
    - Parses Excel
    - Applies column mappings
    - Deletes original MinIO files
    - Returns dictionary of cleaned DataFrames
    """
    logger.info(f"Fetching excel data from minio to begin processing")
    # Send ingestion start email
    try:
        await send_ingestion_began_email(
            user_email, first_name, portfolio_id,
            cc_emails=["support@service4gh.com"]
        )
    except Exception as e:
        logger.error(f"Failed to send ingestion start email: {str(e)}")

    if not payload.files:
        raise HTTPException(status_code=400, detail="No file mappings provided")

    dataframes = {
        "loan_details": None,
        "client_data": None,
        "loan_guarantee_data": None,
        "loan_collateral_data": None,
    }

    FILE_KEY_MAPPING = dataframes.keys()
    BUCKET_NAME = settings.MINIO_BUCKET_NAME

    for file_info in payload.files:
        file_type = file_info.type
        if file_type not in FILE_KEY_MAPPING:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file type: {file_type}. Must be one of {list(FILE_KEY_MAPPING)}"
            )

        file_key = file_info.object_name
        mapping = file_info.mapping or {}

        if not file_key:
            raise HTTPException(
                status_code=400,
                detail=f"File object_name missing for {file_type}"
            )

        # --------- Download from MinIO ----------
        try:
            obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
            file_bytes = obj["Body"].read()
            obj["Body"].close()
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to download {file_key} from MinIO: {e}"
            )

        # --------- Load Excel ----------
        try:
            df = pd.read_excel(BytesIO(file_bytes))
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to parse Excel file {file_key}: {e}"
            )

        # --------- Apply mapping ----------
        if mapping:
            df.rename(columns=mapping, inplace=True)

        # --------- Optional: Save as temp file ----------
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            df.to_excel(tmp.name, index=False)

        # --------- Store DataFrame ----------
        dataframes[file_type] = df

        # --------- Delete original file ----------
        try:
            s3_client.delete_object(Bucket=BUCKET_NAME, Key=file_key)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to delete original file {file_key}: {e}"
            )

    # --------- Validate required files ----------
    for required_file in ["loan_details", "client_data"]:
        if dataframes[required_file] is None or dataframes[required_file].empty:
            raise HTTPException(
                status_code=400,
                detail=f"{required_file} is required and cannot be empty"
            )

    return dataframes