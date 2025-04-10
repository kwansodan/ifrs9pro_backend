"""
Utility functions for loan staging operations.
Contains implementations of ECL and local impairment staging.
"""
import logging
from datetime import datetime
from sqlalchemy.orm import Session
from typing import Dict, Any, List, Tuple

from app.models import StagingResult, Loan
from app.schemas import ECLStagingConfig, LocalImpairmentConfig

logger = logging.getLogger(__name__)

async def stage_loans_ecl_orm(portfolio_id: int, config: ECLStagingConfig, db: Session) -> Dict[str, Any]:
    """
    Implementation of ECL staging using SQLAlchemy ORM for large datasets.
    """
    try:
        logger.info(f"Starting ECL staging for portfolio {portfolio_id}")
        
        # Parse days ranges from config
        stage_1_range = config.stage_1.days_range
        stage_2_range = config.stage_2.days_range
        stage_3_range = config.stage_3.days_range
        
        # Extract min and max days for each stage
        stage_1_min, stage_1_max = parse_days_range(stage_1_range)
        stage_2_min, stage_2_max = parse_days_range(stage_2_range)
        stage_3_min, stage_3_max = parse_days_range(stage_3_range)
        
        # Get all loans for the portfolio
        loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
        
        # Initialize counters
        stage_counts = {1: 0, 2: 0, 3: 0}
        timestamp = datetime.now()
        
        # Process each loan
        for loan in loans:
            # Get the ndia value (days past due)
            ndia = loan.ndia if loan.ndia is not None else 0
            
            # Determine the stage based on ndia
            if ndia >= stage_3_min:
                loan.ecl_stage = 3
                stage_counts[3] += 1
            elif ndia >= stage_2_min and (stage_2_max is None or ndia < stage_2_max):
                loan.ecl_stage = 2
                stage_counts[2] += 1
            else:
                loan.ecl_stage = 1
                stage_counts[1] += 1
            
            # Update the last staged timestamp
            loan.last_staged_at = timestamp
        
        # Commit all changes
        db.commit()
        
        # Update the staging result
        staging_result = db.query(StagingResult).filter(
            StagingResult.portfolio_id == portfolio_id,
            StagingResult.staging_type == "ecl"
        ).order_by(StagingResult.created_at.desc()).first()
        
        if staging_result:
            staging_result.result_summary = {
                "status": "completed",
                "timestamp": timestamp.isoformat(),
                "total_loans": len(loans),
                "stage_1": stage_counts.get(1, 0),
                "stage_2": stage_counts.get(2, 0),
                "stage_3": stage_counts.get(3, 0),
                "config": {
                    "stage_1": stage_1_range,
                    "stage_2": stage_2_range,
                    "stage_3": stage_3_range
                }
            }
            db.add(staging_result)
            db.commit()
        
        logger.info(f"Completed ECL staging for portfolio {portfolio_id}: {stage_counts}")
        
        # Return summary
        return {
            "status": "success",
            "total_loans": len(loans),
            "stage_1": stage_counts.get(1, 0),
            "stage_2": stage_counts.get(2, 0),
            "stage_3": stage_counts.get(3, 0)
        }
    
    except Exception as e:
        db.rollback()
        logger.error(f"Error in ECL staging: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }

async def stage_loans_local_impairment_orm(portfolio_id: int, config: LocalImpairmentConfig, db: Session) -> Dict[str, Any]:
    """
    Implementation of local impairment staging using SQLAlchemy ORM for large datasets.
    """
    try:
        logger.info(f"Starting local impairment staging for portfolio {portfolio_id}")
        
        # Parse days ranges from config
        current_range = config.current.days_range
        olem_range = config.olem.days_range
        substandard_range = config.substandard.days_range
        doubtful_range = config.doubtful.days_range
        loss_range = config.loss.days_range
        
        # Extract min and max days for each category
        current_min, current_max = parse_days_range(current_range)
        olem_min, olem_max = parse_days_range(olem_range)
        substandard_min, substandard_max = parse_days_range(substandard_range)
        doubtful_min, doubtful_max = parse_days_range(doubtful_range)
        loss_min, loss_max = parse_days_range(loss_range)
        
        # Get all loans for the portfolio
        loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
        
        # Initialize counters
        category_counts = {
            "current": 0,
            "olem": 0,
            "substandard": 0,
            "doubtful": 0,
            "loss": 0
        }
        timestamp = datetime.now()
        
        # Process each loan
        for loan in loans:
            # Get the ndia value (days past due)
            ndia = loan.ndia if loan.ndia is not None else 0
            
            # Determine the impairment category based on ndia
            if ndia >= loss_min:
                loan.impairment_category = "loss"
                category_counts["loss"] += 1
            elif ndia >= doubtful_min and (doubtful_max is None or ndia < doubtful_max):
                loan.impairment_category = "doubtful"
                category_counts["doubtful"] += 1
            elif ndia >= substandard_min and (substandard_max is None or ndia < substandard_max):
                loan.impairment_category = "substandard"
                category_counts["substandard"] += 1
            elif ndia >= olem_min and (olem_max is None or ndia < olem_max):
                loan.impairment_category = "olem"
                category_counts["olem"] += 1
            else:
                loan.impairment_category = "current"
                category_counts["current"] += 1
            
            # Update the last staged timestamp
            loan.last_staged_at = timestamp
        
        # Commit all changes
        db.commit()
        
        # Update the staging result
        staging_result = db.query(StagingResult).filter(
            StagingResult.portfolio_id == portfolio_id,
            StagingResult.staging_type == "local_impairment"
        ).order_by(StagingResult.created_at.desc()).first()
        
        if staging_result:
            staging_result.result_summary = {
                "status": "completed",
                "timestamp": timestamp.isoformat(),
                "total_loans": len(loans),
                "current": category_counts["current"],
                "olem": category_counts["olem"],
                "substandard": category_counts["substandard"],
                "doubtful": category_counts["doubtful"],
                "loss": category_counts["loss"],
                "config": {
                    "current": current_range,
                    "olem": olem_range,
                    "substandard": substandard_range,
                    "doubtful": doubtful_range,
                    "loss": loss_range
                }
            }
            db.add(staging_result)
            db.commit()
        
        logger.info(f"Completed local impairment staging for portfolio {portfolio_id}: {category_counts}")
        
        # Return summary
        return {
            "status": "success",
            "total_loans": len(loans),
            "current": category_counts["current"],
            "olem": category_counts["olem"],
            "substandard": category_counts["substandard"],
            "doubtful": category_counts["doubtful"],
            "loss": category_counts["loss"]
        }
    
    except Exception as e:
        db.rollback()
        logger.error(f"Error in local impairment staging: {str(e)}")
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
