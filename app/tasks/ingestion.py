from app.celery_app import celery_app
from app.database import SessionLocal
from app.utils.minio_reports_factory import s3_client
from app.config import settings
from app.utils.background_ingestion import process_portfolio_ingestion_sync
from io import BytesIO
import pandas as pd
import logging

logger = logging.getLogger(__name__)

@celery_app.task(bind=True)
def run_ingestion_task(self, portfolio_id: int, tenant_id: int, file_mappings: dict, user_email: str, first_name: str):
    """
    Celery task to handle portfolio ingestion.
    Downloads files from MinIO, processes them, and cleans up.
    """
    logger.info(f"Starting Celery ingestion task for portfolio {portfolio_id}")
    
    dataframes = {
        "loan_details_content": None,
        "client_data_content": None,
        "loan_guarantee_content": None,
        "loan_collateral_data_content": None
    }
    
    uploaded_filenames = []
    
    try:
        # Download and parse files
        for file_type, file_key in file_mappings.items():
            if not file_key:
                continue
                
            try:
                obj = s3_client.get_object(Bucket=settings.MINIO_BUCKET_NAME, Key=file_key)
                file_bytes = obj["Body"].read()
                df = pd.read_excel(BytesIO(file_bytes))
                
                if file_type == "loan_details":
                    dataframes["loan_details_content"] = df
                    uploaded_filenames.append("loan_details")
                elif file_type == "client_data":
                    dataframes["client_data_content"] = df
                    uploaded_filenames.append("client_data")
                elif file_type == "loan_guarantee_data":
                    dataframes["loan_guarantee_content"] = df
                    uploaded_filenames.append("loan_guarantee_data")
                elif file_type == "loan_collateral_data":
                    dataframes["loan_collateral_data_content"] = df
                    uploaded_filenames.append("loan_collateral_data")
                    
            except Exception as e:
                logger.error(f"Failed to process file {file_key}: {e}")
                # We might want to notify or fail here, but for now log and continue/fail later
                raise e

        # Run the synchronous processing logic
        with SessionLocal() as db:
             # Since process_portfolio_ingestion_sync is an async function in the utils, 
             # but we are in a sync Celery worker, we have two options:
             # 1. Refactor process_portfolio_ingestion_sync to be sync (Ideal for Celery)
             # 2. Run it with asyncio.run() (Quickest for now)
             
             import asyncio
             
             # Create loop for async execution
             loop = asyncio.new_event_loop()
             asyncio.set_event_loop(loop)
             
             try:
                 results = loop.run_until_complete(
                     process_portfolio_ingestion_sync(
                         task_id=self.request.id,
                         portfolio_id=portfolio_id,
                         tenant_id=tenant_id,
                         db=db,
                         user_email=user_email,
                         first_name=first_name,
                         uploaded_filenames=uploaded_filenames,
                         **dataframes
                     )
                 )
             finally:
                 loop.close()
                 
        # Cleanup MinIO files
        for file_key in file_mappings.values():
            if file_key:
                try:
                    s3_client.delete_object(Bucket=settings.MINIO_BUCKET_NAME, Key=file_key)
                except Exception as e:
                    logger.warning(f"Failed to cleanup file {file_key}: {e}")

        return results

    except Exception as e:
        logger.error(f"Celery ingestion task failed: {e}")
        # Send failure email handled inside process_portfolio_ingestion_sync mostly, 
        # but if it crashed before that, we might need handling.
        # For now, let Celery retry or log.
        raise e
