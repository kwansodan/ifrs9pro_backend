"""
Utility functions for loan staging operations.
Contains implementations of ECL and local impairment staging.
"""
import logging
from datetime import datetime
from sqlalchemy.orm import Session
from typing import Dict, Any, List, Tuple
from sqlalchemy import func
from decimal import Decimal

from app.models import Portfolio, Loan
from app.schemas import ECLStagingConfig, LocalImpairmentConfig
from app.utils.validate_bog import validate_and_fix_bog_config
from app.utils.process_email_notifyer import (
    send_stage_loans_ecl_started_email,
    send_stage_loans_ecl_success_email, 
    send_stage_loans_ecl_failed_email,
    send_stage_loans_local_started_email,
    send_stage_loans_local_success_email,
    send_stage_loans_local_failed_email
)

logger = logging.getLogger(__name__)

async def stage_loans_ecl_orm(portfolio_id: int, db: Session, user_email, first_name) -> Dict[str, Any]:
    """
    Implementation of ECL staging using SQLAlchemy ORM for large datasets.
    """
    try:
        await send_stage_loans_ecl_started_email(user_email, first_name, portfolio_id, cc_emails=["support@service4gh.com"])
    except:
        logger.error("Failed to send loans ecl staging began email")
    try:
        logger.info(f"Starting ECL staging for portfolio {portfolio_id}")
        
     # Fetch ECL staging config from DB
        latest_ecl_config = (
    db.query(Portfolio.ecl_staging_config)
    .filter(Portfolio.id == portfolio_id)
    .scalar()
)


        if not latest_ecl_config:
            logger.error(f"No ECL staging config found for portfolio {portfolio_id}")
            return {
                "status": "error",
                "error": "Missing ECL staging configuration"
            }
            
        config = latest_ecl_config  # Assuming it's a dict

        stage_1_range = config.get("stage_1", {}).get("days_range", "")
        stage_2_range = config.get("stage_2", {}).get("days_range", "")
        stage_3_range = config.get("stage_3", {}).get("days_range", "")
       
        logger.info(f"ECL staging ranges: Stage 1: {stage_1_range}, Stage 2: {stage_2_range}, Stage 3: {stage_3_range}")



        # Extract min and max days for each stage
        stage_1_min, stage_1_max = parse_days_range(stage_1_range)
        stage_2_min, stage_2_max = parse_days_range(stage_2_range)
        stage_3_min, stage_3_max = parse_days_range(stage_3_range)
        
        logger.info(f"Parsed day ranges: Stage 1: {stage_1_min}-{stage_1_max}, Stage 2: {stage_2_min}-{stage_2_max}, Stage 3: {stage_3_min}-{stage_3_max}")
        
        stage_balances = {1: 0.0, 2: 0.0, 3: 0.0}
        timestamp = datetime.now()
        
        # Use batch processing to reduce memory usage
        batch_size = 500
        offset = 0
        
        
        while True:
            # Get a batch of loans
            loan_batch = db.query(Loan).filter(
                Loan.portfolio_id == portfolio_id
            ).order_by(Loan.id).offset(offset).limit(batch_size).all()
            
            # If no more loans, break the loop
            if not loan_batch:
                break
                
            # Process each loan in the batch
            for loan in loan_batch:
                # Get the ndia value (days past due)
                ndia = loan.ndia if loan.ndia is not None else 0

                # Determine the stage based on ndia
                if ndia >= stage_3_min:
                    loan.ifrs9_stage = "Stage 3"
                    
                elif ndia >= stage_2_min and (stage_2_max is None or ndia < stage_2_max):
                    loan.ifrs9_stage = "Stage 2"
                    
                else:
                    loan.ifrs9_stage = "Stage 1"
                    
                
                # Update the last staged timestamp
                loan.last_staged_at = timestamp
            
            # Commit changes for this batch
            db.commit()
            
            # Update offset for next batch
            offset += batch_size
            
            # Log progress
            logger.info(f"Processed {offset} loans out of for ECL staging")
        try:
            await send_stage_loans_ecl_success_email(user_email, first_name, portfolio_id, cc_emails=["support@service4gh.com"])
        except:
            logger.error("Failed to send loans ecl staging success email")
       
    except Exception as e:
        try:
            await send_stage_loans_ecl_failed_email(user_email, first_name, portfolio_id, cc_emails=["support@service4gh.com"])
        except:
            logger.error("Failed to send loans ecl staging began email")
        db.rollback()
        logger.error(f"Error in ECL staging: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }
    
 

async def stage_loans_local_impairment_orm(portfolio_id: int, db: Session, user_email, first_name) -> Dict[str, Any]:
    """
    Implementation of ECL staging using SQLAlchemy ORM for large datasets.
    """
    try:
        await send_stage_loans_local_started_email(user_email, first_name, portfolio_id, cc_emails=["support@service4gh.com"])
    except:
        logger.error("Failed to send loans local staging began email")

    try:
        logger.info(f"Starting BOG staging for portfolio {portfolio_id}")
        
     # Fetch BOG staging config from DB
        
        latest_bog_config = (
    db.query(Portfolio.bog_staging_config)
    .filter(Portfolio.id == portfolio_id)
    .scalar()
)
        
        if not latest_bog_config:
            logger.error(f"No BOG staging config found for portfolio {portfolio_id}")
            return {
                "status": "error",
                "error": "Missing BOG staging configuration"
            }
        # Validate & fix before staging
        validated_config = validate_and_fix_bog_config(latest_bog_config)

        config = validated_config 

        current_range = config.get("Current", {}).get("days_range", "")
        olem_range = config.get("OLEM", {}).get("days_range", "")
        substandard_range = config.get("Substandard", {}).get("days_range", "")
        doubtful_range = config.get("Doubtful", {}).get("days_range", "")
        loss_range = config.get("Loss", {}).get("days_range", "")
       
        logger.info(f"BOG staging ranges: Current {current_range}, Olem: {olem_range}, substandard: {substandard_range}, doubtful: {doubtful_range}, loss: {loss_range}")



        # Extract min and max days for each stage
        current_min, current_max = parse_days_range(current_range)
        olem_min, olem_max = parse_days_range(olem_range)
        substandard_min, substandard_max = parse_days_range(substandard_range)
        doubtful_min, doubtful_max = parse_days_range(doubtful_range)
        loss_min, loss_max = parse_days_range(loss_range)
        
        
        timestamp = datetime.now()
        
        # Use batch processing to reduce memory usage
        batch_size = 500
        offset = 0
        
        
        while True:
            # Get a batch of loans
            loan_batch = db.query(Loan).filter(
                Loan.portfolio_id == portfolio_id
            ).order_by(Loan.id).offset(offset).limit(batch_size).all()
            
            # If no more loans, break the loop
            if not loan_batch:
                break
                
            # Process each loan in the batch
            for loan in loan_batch:
                # Get the ndia value (days past due)
                ndia = loan.ndia if loan.ndia is not None else 0

                # Determine the stage based on ndia
                if ndia >= loss_min:
                    loan.bog_stage = "Loss"

                    
                elif ndia >= doubtful_min and (doubtful_max is None or ndia < doubtful_max):
                    loan.bog_stage = "Doubtful"

                elif ndia >= substandard_min and (substandard_max is None or ndia < substandard_max):
                    loan.bog_stage = "substandard"
                    
                elif ndia >= olem_min and (olem_max is None or ndia < olem_max):
                    loan.bog_stage = "Olem"    
                    
                else:
                    loan.bog_stage = "Current"
                    
                
                # Update the last staged timestamp
                loan.last_staged_at = timestamp
            
            # Commit changes for this batch
            db.commit()
            
            # Update offset for next batch
            offset += batch_size
            
            # Log progress
            logger.info(f"Processed {offset} loans out of for BOG staging")
            try:
                await send_stage_loans_ecl_success_email(user_email, first_name, portfolio_id, cc_emails=["support@service4gh.com"])
            except:
                logger.error("Failed to send loans ecl staging success email")
       
    except Exception as e:
        try:
            await send_stage_loans_ecl_failed_email(user_email, first_name, portfolio_id, cc_emails=["support@service4gh.com"])
        except:
            logger.error("Failed to send loans ecl staging failed email")
        db.rollback()
        logger.error(f"Error in BOG staging: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }

def parse_days_range(days_range: str) -> Tuple[int, int]:
    """
    Parse a days range string like "0-30" or "90+" into min and max values.
    Returns a tuple of (min_days, max_days) where max_days is None for unbounded ranges.
    """
    if not days_range:
        return (0, None)
    
    if days_range.endswith("+"):
        min_days = int(days_range[:-1])
        max_days = None
    else:
        parts = days_range.split("-")
        if len(parts) != 2:
            raise ValueError(f"Invalid days range format: {days_range}")
        
        min_days = int(parts[0])
        max_days = int(parts[1])
    
    return (min_days, max_days)
