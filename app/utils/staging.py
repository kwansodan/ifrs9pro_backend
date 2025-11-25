"""
Utility functions for loan staging operations.
Contains implementations of ECL and local impairment staging.
"""
import logging
from datetime import datetime
from sqlalchemy.orm import Session
from typing import Dict, Any, List, Tuple
from sqlalchemy import func
from typing import Optional

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

    # ----------------------------
    # SEND "PROCESS BEGAN" EMAIL
    # ----------------------------
    try:
        await send_stage_loans_ecl_started_email(
            user_email, 
            first_name, 
            portfolio_id,
            cc_emails=["support@service4gh.com"]
        )
    except Exception as e:
        logger.error(f"Failed to send ECL staging began email: {e}")

    try:
        logger.info(f"Starting ECL staging for portfolio {portfolio_id}")

        # ------------------------------------
        # FETCH & VALIDATE CONFIG
        # ------------------------------------
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

        config = latest_ecl_config

        stage_1_range = config.get("stage_1", {}).get("days_range", "")
        stage_2_range = config.get("stage_2", {}).get("days_range", "")
        stage_3_range = config.get("stage_3", {}).get("days_range", "")

        logger.info(
            f"ECL staging ranges: "
            f"Stage 1: {stage_1_range}, "
            f"Stage 2: {stage_2_range}, "
            f"Stage 3: {stage_3_range}"
        )

        stage_1_min, stage_1_max = parse_days_range(stage_1_range)
        stage_2_min, stage_2_max = parse_days_range(stage_2_range)
        stage_3_min, stage_3_max = parse_days_range(stage_3_range)

        logger.info(
            f"Parsed day ranges: "
            f"Stage 1: {stage_1_min}-{stage_1_max}, "
            f"Stage 2: {stage_2_min}-{stage_2_max}, "
            f"Stage 3: {stage_3_min}-{stage_3_max}"
        )

        timestamp = datetime.now()

        # ------------------------------------
        # BATCH PROCESSING
        # ------------------------------------
        batch_size = 500
        offset = 0

        while True:

            loan_batch = (
                db.query(Loan)
                .filter(Loan.portfolio_id == portfolio_id)
                .order_by(Loan.id)
                .offset(offset)
                .limit(batch_size)
                .all()
            )

            if not loan_batch:
                break

            for loan in loan_batch:
                ndia = loan.ndia or 0

                if ndia >= stage_3_min:
                    loan.ifrs9_stage = "Stage 3"
                elif ndia >= stage_2_min and (stage_2_max is None or ndia < stage_2_max):
                    loan.ifrs9_stage = "Stage 2"
                else:
                    loan.ifrs9_stage = "Stage 1"

                loan.last_staged_at = timestamp

            db.commit()
            offset += batch_size

            logger.info(f"Processed {offset} loans for ECL staging.")

        # ------------------------------------
        # SEND SUCCESS EMAIL (ONLY IF NO ERROR)
        # ------------------------------------
        try:
            await send_stage_loans_ecl_success_email(
                user_email, 
                first_name, 
                portfolio_id,
                cc_emails=["support@service4gh.com"]
            )
        except Exception as e:
            logger.error(f"Failed to send ECL staging success email: {e}")

        return {"status": "success", "message": "ECL staging completed successfully"}

    except Exception as e:
        db.rollback()
        logger.error(f"Error in ECL staging: {str(e)}")

        # ------------------------------------
        # SEND FAILED EMAIL (ONLY ON ERROR)
        # ------------------------------------
        try:
            await send_stage_loans_ecl_failed_email(
                user_email,
                first_name,
                portfolio_id,
                cc_emails=["support@service4gh.com"]
            )
        except Exception as e2:
            logger.error(f"Failed to send ECL staging failed email: {e2}")

        return {"status": "error", "error": str(e)}
    
 

async def stage_loans_local_impairment_orm(
    portfolio_id: int, db: Session, user_email, first_name
) -> Dict[str, Any]:

    # ----------------------------
    # SEND "STARTED" EMAIL
    # ----------------------------
    try:
        await send_stage_loans_local_started_email(
            user_email,
            first_name,
            portfolio_id,
            cc_emails=["support@service4gh.com"]
        )
    except Exception as e:
        logger.error(f"Failed to send loans local staging began email: {e}")

    try:
        logger.info(f"Starting BOG staging for portfolio {portfolio_id}")

        # ----------------------------
        # FETCH CONFIG
        # ----------------------------
        latest_bog_config = (
            db.query(Portfolio.bog_staging_config)
            .filter(Portfolio.id == portfolio_id)
            .scalar()
        )

        if not latest_bog_config:
            logger.error(f"No BOG staging config found for portfolio {portfolio_id}")
            return {"status": "error", "error": "Missing BOG staging configuration"}

        # Validate & normalize keys to lowercase
        validated_config = validate_and_fix_bog_config(latest_bog_config)
        config = {k.lower(): v for k, v in validated_config.items()}

        # These keys MUST exist: current, olem, substandard, doubtful, loss
        current_range = config.get("current", {}).get("days_range", "")
        olem_range = config.get("olem", {}).get("days_range", "")
        sub_range = config.get("substandard", {}).get("days_range", "")
        doubtful_range = config.get("doubtful", {}).get("days_range", "")
        loss_range = config.get("loss", {}).get("days_range", "")

        logger.info(
            f"BOG staging ranges:"
            f" Current {current_range},"
            f" OLEM {olem_range},"
            f" Substandard {sub_range},"
            f" Doubtful {doubtful_range},"
            f" Loss {loss_range}"
        )

        # Parse ranges
        current_min, current_max = parse_days_range(current_range)
        olem_min, olem_max = parse_days_range(olem_range)
        sub_min, sub_max = parse_days_range(sub_range)
        doubtful_min, doubtful_max = parse_days_range(doubtful_range)
        loss_min, loss_max = parse_days_range(loss_range)

        timestamp = datetime.now()

        # ----------------------------
        # BATCH PROCESSING
        # ----------------------------
        batch_size = 500
        offset = 0

        while True:
            loan_batch = (
                db.query(Loan)
                .filter(Loan.portfolio_id == portfolio_id)
                .order_by(Loan.id)
                .offset(offset)
                .limit(batch_size)
                .all()
            )

            if not loan_batch:
                break

            for loan in loan_batch:
                ndia = loan.ndia or 0

                # Assign BOG stage
                if ndia >= loss_min:
                    loan.bog_stage = "Loss"

                elif ndia >= doubtful_min and (doubtful_max is None or ndia < doubtful_max):
                    loan.bog_stage = "Doubtful"

                elif ndia >= sub_min and (sub_max is None or ndia < sub_max):
                    loan.bog_stage = "Substandard"

                elif ndia >= olem_min and (olem_max is None or ndia < olem_max):
                    loan.bog_stage = "OLEM"

                else:
                    loan.bog_stage = "Current"

                loan.last_staged_at = timestamp

            db.commit()
            offset += batch_size

            logger.info(f"Processed {offset} loans for BOG staging")

        # ----------------------------
        # SEND SUCCESS EMAIL (ONLY IF NO ERROR)
        # ----------------------------
        try:
            await send_stage_loans_local_success_email(
                user_email,
                first_name,
                portfolio_id,
                cc_emails=["support@service4gh.com"]
            )
        except Exception as e:
            logger.error(f"Failed to send loans local staging success email: {e}")

        return {"status": "success", "message": "BOG staging completed"}

    except Exception as e:
        db.rollback()
        logger.error(f"Error in BOG staging: {str(e)}")

        # ----------------------------
        # SEND FAILED EMAIL ON ERROR
        # ----------------------------
        try:
            await send_stage_loans_local_failed_email(
                user_email,
                first_name,
                portfolio_id,
                cc_emails=["support@service4gh.com"]
            )
        except Exception as e2:
            logger.error(f"Failed to send loans local staging failed email: {e2}")

        return {"status": "error", "error": str(e)}


def parse_days_range(days_range: str) -> Tuple[int, Optional[int]]:
    """
    Parse a days range string like "0-30" or "90+" into (min_days, max_days).
    max_days = None for open-ended ranges such as "360+".
    """

    if not days_range or not isinstance(days_range, str):
        raise ValueError(f"Days range is empty or invalid: {days_range}")

    days_range = days_range.strip().replace(" ", "")  # remove all spaces

    # Handle open-ended ranges like "360+"
    if days_range.endswith("+"):
        value = days_range[:-1]
        if not value.isdigit():
            raise ValueError(f"Invalid open-ended days range: {days_range}")
        return int(value), None

    # Standard range "min-max"
    if "-" not in days_range:
        raise ValueError(f"Missing '-' in days range: {days_range}")

    parts = days_range.split("-")
    if len(parts) != 2:
        raise ValueError(f"Incorrect format (too many '-'): {days_range}")

    min_part, max_part = parts[0], parts[1]

    if not min_part.isdigit() or not max_part.isdigit():
        raise ValueError(f"Non-numeric values in days range: {days_range}")

    min_days = int(min_part)
    max_days = int(max_part)

    if min_days > max_days:
        raise ValueError(f"Min days cannot exceed max days: {days_range}")

    return min_days, max_days

