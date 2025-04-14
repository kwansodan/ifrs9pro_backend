"""
Utility functions for loan staging operations.
Contains implementations of ECL and local impairment staging.
"""
import logging
from datetime import datetime
from sqlalchemy.orm import Session
from typing import Dict, Any, List, Tuple
from sqlalchemy import func

from app.models import StagingResult, Loan
from app.schemas import ECLStagingConfig, LocalImpairmentConfig

logger = logging.getLogger(__name__)

async def stage_loans_ecl_orm(portfolio_id: int, config: ECLStagingConfig, db: Session) -> Dict[str, Any]:
    """
    Implementation of ECL staging using SQLAlchemy ORM for large datasets.
    """
    try:
        logger.info(f"Starting ECL staging for portfolio {portfolio_id}")
        logger.info(f"ECL staging config: {config.dict()}")
        
        # Parse days ranges from config
        stage_1_range = config.stage_1.days_range
        stage_2_range = config.stage_2.days_range
        stage_3_range = config.stage_3.days_range
        
        logger.info(f"ECL staging ranges: Stage 1: {stage_1_range}, Stage 2: {stage_2_range}, Stage 3: {stage_3_range}")
        
        # Extract min and max days for each stage
        stage_1_min, stage_1_max = parse_days_range(stage_1_range)
        stage_2_min, stage_2_max = parse_days_range(stage_2_range)
        stage_3_min, stage_3_max = parse_days_range(stage_3_range)
        
        logger.info(f"Parsed day ranges: Stage 1: {stage_1_min}-{stage_1_max}, Stage 2: {stage_2_min}-{stage_2_max}, Stage 3: {stage_3_min}-{stage_3_max}")
        
        # Get total loan count for the portfolio
        total_loans = db.query(func.count(Loan.id)).filter(Loan.portfolio_id == portfolio_id).scalar() or 0
        logger.info(f"Total loans in portfolio {portfolio_id}: {total_loans}")
        
        # Initialize counters
        stage_counts = {1: 0, 2: 0, 3: 0}
        stage_balances = {1: 0.0, 2: 0.0, 3: 0.0}
        timestamp = datetime.now()
        
        # Use batch processing to reduce memory usage
        batch_size = 5000
        offset = 0
        
        # Sample logging for NDIA values
        ndia_sample = []
        sample_size = min(20, total_loans)
        
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
                
                # Sample some NDIA values for debugging
                if len(ndia_sample) < sample_size:
                    ndia_sample.append((loan.loan_no, ndia))
                
                # Get outstanding loan balance
                balance = float(loan.outstanding_loan_balance) if loan.outstanding_loan_balance is not None else 0.0
                
                # Determine the stage based on ndia
                if ndia >= stage_3_min:
                    loan.ecl_stage = 3
                    stage_counts[3] += 1
                    stage_balances[3] += balance
                elif ndia >= stage_2_min and (stage_2_max is None or ndia < stage_2_max):
                    loan.ecl_stage = 2
                    stage_counts[2] += 1
                    stage_balances[2] += balance
                else:
                    loan.ecl_stage = 1
                    stage_counts[1] += 1
                    stage_balances[1] += balance
                
                # Update the last staged timestamp
                loan.last_staged_at = timestamp
            
            # Commit changes for this batch
            db.commit()
            
            # Update offset for next batch
            offset += batch_size
            
            # Log progress
            logger.info(f"Processed {offset} loans out of {total_loans} for ECL staging")
        
        # Log sample NDIA values
        logger.info(f"Sample NDIA values from portfolio {portfolio_id}: {ndia_sample}")
        
        # Round balances to 2 decimal places
        stage_balances = {k: round(v, 2) for k, v in stage_balances.items()}
        
        # Log final stage counts and balances
        logger.info(f"ECL staging results for portfolio {portfolio_id}:")
        logger.info(f"Stage 1: {stage_counts[1]} loans, balance: {stage_balances[1]}")
        logger.info(f"Stage 2: {stage_counts[2]} loans, balance: {stage_balances[2]}")
        logger.info(f"Stage 3: {stage_counts[3]} loans, balance: {stage_balances[3]}")
        
        # Update the staging result
        staging_result = db.query(StagingResult).filter(
            StagingResult.portfolio_id == portfolio_id,
            StagingResult.staging_type == "ecl"
        ).order_by(StagingResult.created_at.desc()).first()
        
        if staging_result:
            staging_result.result_summary = {
                "status": "completed",
                "timestamp": timestamp.isoformat(),
                "total_loans": total_loans,
                "Stage 1": {
                    "num_loans": stage_counts.get(1, 0),
                    "outstanding_loan_balance": stage_balances.get(1, 0)
                },
                "Stage 2": {
                    "num_loans": stage_counts.get(2, 0),
                    "outstanding_loan_balance": stage_balances.get(2, 0)
                },
                "Stage 3": {
                    "num_loans": stage_counts.get(3, 0),
                    "outstanding_loan_balance": stage_balances.get(3, 0)
                },
                "config": {
                    "stage_1": {"days_range": stage_1_range},
                    "stage_2": {"days_range": stage_2_range},
                    "stage_3": {"days_range": stage_3_range}
                }
            }
            db.add(staging_result)
            db.commit()
        
        logger.info(f"Completed ECL staging for portfolio {portfolio_id}: {stage_counts}")
        
        # Return summary
        return {
            "status": "success",
            "total_loans": total_loans,
            "Stage 1": {
                "num_loans": stage_counts.get(1, 0),
                "outstanding_loan_balance": stage_balances.get(1, 0)
            },
            "Stage 2": {
                "num_loans": stage_counts.get(2, 0),
                "outstanding_loan_balance": stage_balances.get(2, 0)
            },
            "Stage 3": {
                "num_loans": stage_counts.get(3, 0),
                "outstanding_loan_balance": stage_balances.get(3, 0)
            }
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
        logger.info(f"Local impairment config: {config.dict()}")
        
        # Parse days ranges from config
        current_range = config.current.days_range
        olem_range = config.olem.days_range
        substandard_range = config.substandard.days_range
        doubtful_range = config.doubtful.days_range
        loss_range = config.loss.days_range
        
        logger.info(f"Local impairment ranges: Current: {current_range}, OLEM: {olem_range}, Substandard: {substandard_range}, Doubtful: {doubtful_range}, Loss: {loss_range}")
        
        # Extract min and max days for each category
        current_min, current_max = parse_days_range(current_range)
        olem_min, olem_max = parse_days_range(olem_range)
        substandard_min, substandard_max = parse_days_range(substandard_range)
        doubtful_min, doubtful_max = parse_days_range(doubtful_range)
        loss_min, loss_max = parse_days_range(loss_range)
        
        logger.info(f"Parsed day ranges: Current: {current_min}-{current_max}, OLEM: {olem_min}-{olem_max}, Substandard: {substandard_min}-{substandard_max}, Doubtful: {doubtful_min}-{doubtful_max}, Loss: {loss_min}-{loss_max}")
        
        # Get total loan count for the portfolio
        total_loans = db.query(func.count(Loan.id)).filter(Loan.portfolio_id == portfolio_id).scalar() or 0
        logger.info(f"Total loans in portfolio {portfolio_id} for local impairment: {total_loans}")
        
        # Initialize counters
        category_counts = {
            "Current": 0, 
            "OLEM": 0, 
            "Substandard": 0, 
            "Doubtful": 0, 
            "Loss": 0
        }
        category_balances = {
            "Current": 0.0, 
            "OLEM": 0.0, 
            "Substandard": 0.0, 
            "Doubtful": 0.0, 
            "Loss": 0.0
        }
        timestamp = datetime.now()
        
        # Use batch processing to reduce memory usage
        batch_size = 5000
        offset = 0
        
        # Sample logging for NDIA values
        ndia_sample = []
        sample_size = min(20, total_loans)
        
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
                
                # Sample some NDIA values for debugging
                if len(ndia_sample) < sample_size:
                    ndia_sample.append((loan.loan_no, ndia))
                
                # Get outstanding loan balance
                balance = float(loan.outstanding_loan_balance) if loan.outstanding_loan_balance is not None else 0.0
                
                # Determine the impairment category based on ndia
                if ndia >= loss_min:
                    loan.impairment_category = "Loss"
                    category_counts["Loss"] += 1
                    category_balances["Loss"] += balance
                elif ndia >= doubtful_min and (doubtful_max is None or ndia < doubtful_max):
                    loan.impairment_category = "Doubtful"
                    category_counts["Doubtful"] += 1
                    category_balances["Doubtful"] += balance
                elif ndia >= substandard_min and (substandard_max is None or ndia < substandard_max):
                    loan.impairment_category = "Substandard"
                    category_counts["Substandard"] += 1
                    category_balances["Substandard"] += balance
                elif ndia >= olem_min and (olem_max is None or ndia < olem_max):
                    loan.impairment_category = "OLEM"
                    category_counts["OLEM"] += 1
                    category_balances["OLEM"] += balance
                else:
                    loan.impairment_category = "Current"
                    category_counts["Current"] += 1
                    category_balances["Current"] += balance
                
                # Update the last staged timestamp
                loan.last_staged_at = timestamp
            
            # Commit changes for this batch
            db.commit()
            
            # Update offset for next batch
            offset += batch_size
            
            # Log progress
            logger.info(f"Processed {offset} loans out of {total_loans} for local impairment staging")
        
        # Log sample NDIA values
        logger.info(f"Sample NDIA values from portfolio {portfolio_id} for local impairment: {ndia_sample}")
        
        # Round balances to 2 decimal places
        category_balances = {k: round(v, 2) for k, v in category_balances.items()}
        
        # Log final category counts and balances
        logger.info(f"Local impairment staging results for portfolio {portfolio_id}:")
        logger.info(f"Current: {category_counts['Current']} loans, balance: {category_balances['Current']}")
        logger.info(f"OLEM: {category_counts['OLEM']} loans, balance: {category_balances['OLEM']}")
        logger.info(f"Substandard: {category_counts['Substandard']} loans, balance: {category_balances['Substandard']}")
        logger.info(f"Doubtful: {category_counts['Doubtful']} loans, balance: {category_balances['Doubtful']}")
        logger.info(f"Loss: {category_counts['Loss']} loans, balance: {category_balances['Loss']}")
        
        # Update the staging result
        staging_result = db.query(StagingResult).filter(
            StagingResult.portfolio_id == portfolio_id,
            StagingResult.staging_type == "local_impairment"
        ).order_by(StagingResult.created_at.desc()).first()
        
        if staging_result:
            staging_result.result_summary = {
                "status": "completed",
                "timestamp": timestamp.isoformat(),
                "total_loans": total_loans,
                "Current": {
                    "num_loans": category_counts["Current"],
                    "outstanding_loan_balance": category_balances["Current"]
                },
                "OLEM": {
                    "num_loans": category_counts["OLEM"],
                    "outstanding_loan_balance": category_balances["OLEM"]
                },
                "Substandard": {
                    "num_loans": category_counts["Substandard"],
                    "outstanding_loan_balance": category_balances["Substandard"]
                },
                "Doubtful": {
                    "num_loans": category_counts["Doubtful"],
                    "outstanding_loan_balance": category_balances["Doubtful"]
                },
                "Loss": {
                    "num_loans": category_counts["Loss"],
                    "outstanding_loan_balance": category_balances["Loss"]
                },
                "config": {
                    "current": {"days_range": current_range},
                    "olem": {"days_range": olem_range},
                    "substandard": {"days_range": substandard_range},
                    "doubtful": {"days_range": doubtful_range},
                    "loss": {"days_range": loss_range}
                }
            }
            db.add(staging_result)
            db.commit()
        
        logger.info(f"Completed local impairment staging for portfolio {portfolio_id}: {category_counts}")
        
        # Return summary
        return {
            "status": "success",
            "total_loans": total_loans,
            "Current": {
                "num_loans": category_counts["Current"],
                "outstanding_loan_balance": category_balances["Current"]
            },
            "OLEM": {
                "num_loans": category_counts["OLEM"],
                "outstanding_loan_balance": category_balances["OLEM"]
            },
            "Substandard": {
                "num_loans": category_counts["Substandard"],
                "outstanding_loan_balance": category_balances["Substandard"]
            },
            "Doubtful": {
                "num_loans": category_counts["Doubtful"],
                "outstanding_loan_balance": category_balances["Doubtful"]
            },
            "Loss": {
                "num_loans": category_counts["Loss"],
                "outstanding_loan_balance": category_balances["Loss"]
            }
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
