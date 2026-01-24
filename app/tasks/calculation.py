from app.celery_app import celery_app
from app.database import SessionLocal
from app.utils.staging import stage_loans_ecl_orm, stage_loans_local_impairment_orm
import logging
import asyncio

logger = logging.getLogger(__name__)

@celery_app.task(bind=True)
def run_ecl_staging_task(self, portfolio_id: int, user_email: str, first_name: str):
    """
    Celery task for ECL Staging.
    """
    logger.info(f"Starting Celery ECL staging task for portfolio {portfolio_id}")
    
    with SessionLocal() as db:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                stage_loans_ecl_orm(portfolio_id, db, user_email, first_name)
            )
            return result
        except Exception as e:
            logger.error(f"ECL staging task failed: {e}")
            raise e
        finally:
            loop.close()

@celery_app.task(bind=True)
def run_bog_staging_task(self, portfolio_id: int, user_email: str, first_name: str):
    """
    Celery task for BOG (Local Impairment) Staging.
    """
    logger.info(f"Starting Celery BOG staging task for portfolio {portfolio_id}")
    
    with SessionLocal() as db:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                stage_loans_local_impairment_orm(portfolio_id, db, user_email, first_name)
            )
            return result
        except Exception as e:
            logger.error(f"BOG staging task failed: {e}")
            raise e
        finally:
            loop.close()

@celery_app.task(bind=True)
def run_ecl_calculation_task(self, portfolio_id: int, reporting_date_str: str, user_email: str, first_name: str):
    """
    Celery task for ECL Calculation (Probability of Default, LGD, EAD, Final ECL).
    Wraps process_ecl_calculation_sync.
    """
    logger.info(f"Starting Celery ECL calculation task for portfolio {portfolio_id}")
    
    # Imports inside task to avoid circular deps if any
    from app.utils.background_calculations import process_ecl_calculation_sync
    
    with SessionLocal() as db:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                process_ecl_calculation_sync(
                    portfolio_id=portfolio_id,
                    reporting_date=reporting_date_str,
                    db=db,
                    user_email=user_email,
                    first_name=first_name
                )
            )
            return result
        except Exception as e:
            logger.error(f"ECL calculation task failed: {e}")
            # Ensure failure is raised so Celery knows it failed
            raise e
        finally:
            loop.close()

@celery_app.task(bind=True)
def run_bog_calculation_task(self, portfolio_id: int, reporting_date_str: str, user_email: str, first_name: str):
    """
    Celery task for BOG (Local Impairment) Calculation.
    Wraps process_bog_impairment_calculation_sync.
    """
    logger.info(f"Starting Celery BOG calculation task for portfolio {portfolio_id}")
    
    from app.utils.background_calculations import process_bog_impairment_calculation_sync
    
    with SessionLocal() as db:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                process_bog_impairment_calculation_sync(
                    portfolio_id=portfolio_id,
                    reporting_date=reporting_date_str,
                    db=db,
                    user_email=user_email,
                    first_name=first_name
                )
            )
            return result
        except Exception as e:
            logger.error(f"BOG calculation task failed: {e}")
            raise e
        finally:
            loop.close()
