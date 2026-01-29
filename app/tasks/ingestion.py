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
    
    import os
    import tempfile
    
    file_paths = {
        "loan_details_content": None,
        "client_data_content": None,
        "loan_guarantee_content": None,
        "loan_collateral_data_content": None
    }
    
    uploaded_filenames = []
    temp_files = []
    
    try:
        # 1. Download files from MinIO to local temp storage
        for file_type, file_key in file_mappings.items():
            if not file_key:
                continue
                
            try:
                fd, temp_path = tempfile.mkstemp(suffix=".xlsx", prefix=f"celery_{file_type}_")
                with os.fdopen(fd, 'wb') as tmp:
                    s3_client.download_fileobj(settings.MINIO_BUCKET_NAME, file_key, tmp)
                
                temp_files.append(temp_path)
                logger.info(f"Downloaded {file_key} to {temp_path}")
                
                # Map to correct argument name for the sync utility
                if file_type == "loan_details":
                    file_paths["loan_details_content"] = temp_path
                    uploaded_filenames.append("loan_details")
                elif file_type == "client_data":
                    file_paths["client_data_content"] = temp_path
                    uploaded_filenames.append("client_data")
                elif file_type == "loan_guarantee_data":
                    file_paths["loan_guarantee_content"] = temp_path
                    uploaded_filenames.append("loan_guarantee_data")
                elif file_type == "loan_collateral_data":
                    file_paths["loan_collateral_data_content"] = temp_path
                    uploaded_filenames.append("loan_collateral_data")
                    
            except Exception as e:
                logger.error(f"Failed to download file {file_key}: {e}")
                raise e

        # 2. Run the processing utility (supports path inputs)
        with SessionLocal() as db:
             import asyncio
             
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
                         **file_paths
                     )
                 )
             finally:
                 loop.close()

        return results

    except Exception as e:
        logger.error(f"Celery ingestion task failed: {e}")
        raise e
    finally:
        # Cleanup local temp files
        for tf in temp_files:
            try:
                if os.path.exists(tf):
                    os.remove(tf)
                    logger.debug(f"Cleaned up Celery temp file: {tf}")
            except: pass
