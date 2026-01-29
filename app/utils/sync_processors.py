import io
import json
import logging
import decimal
import os
import tempfile
from datetime import datetime
from sqlalchemy import text
import polars as pl
import traceback
from xlsx2csv import Xlsx2csv

from app.models import (
    Loan,
    Guarantee,
    Client,
    Security,
    Portfolio,
    QualityIssue,
    DeductionStatus,
    TenantSubscription,
)
from app.utils.quality_checks import create_and_save_quality_issues

logger = logging.getLogger(__name__)

def excel_to_csv_task(excel_path: str) -> str:
    """Helper to convert Excel file to CSV for memory-efficient streaming."""
    fd, csv_path = tempfile.mkstemp(suffix=".csv", prefix="ingest_conv_")
    os.close(fd) # Close file descriptor, xlsx2csv will open it
    try:
        Xlsx2csv(excel_path, skip_empty_lines=True).convert(csv_path)
        return csv_path
    except Exception as e:
        if os.path.exists(csv_path): os.remove(csv_path)
        raise e

async def process_loan_details_sync(file_path, portfolio_id, tenant_id, db):
    """Chunked processing for loan details to minimize RAM usage."""
    csv_path = None
    try:
        # 1. Convert Excel to CSV for streaming
        logger.info(f"Converting loan details Excel to CSV: {file_path}")
        csv_path = excel_to_csv_task(file_path)

        # 2. Define schema and reader
        # Note: We use polars batched reader to keep memory low
        reader = pl.read_csv_batched(csv_path, infer_schema_length=10000, ignore_errors=True)
        
        # 3. Get subscription ID
        portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
        sub_id = int(portfolio.subscription_id) if portfolio and portfolio.subscription_id else 0

        # Mapping configuration
        target_columns = {
            "loan_no": "loan_no", "employee_id": "employee_id", "loan_amount": "loan_amount",
            "outstanding_loan_balance": "outstanding_loan_balance", "deduction_start_period": "deduction_start_period",
            "submission_period": "submission_period", "maturity_period": "maturity_period",
            "loan_issue_date": "loan_issue_date", "monthly_installment": "monthly_installment",
            "accumulated_arrears": "accumulated_arrears"
        }

        total_processed = 0
        batch_count = 0
        
        while (batches := reader.next_batches(1)): 
            batch_count += 1
            df = batches[0]
            
            # Normalize and rename columns
            df.columns = [c.strip().lower().replace(".", "").replace(" ", "_") for c in df.columns]
            
            # Full Mapping
            target_columns = {
                "loan_no": "loan_no", "employee_id": "employee_id", "loan_amount": "loan_amount",
                "loan_term": "loan_term", "monthly_installment": "monthly_installment",
                "accumulated_arrears": "accumulated_arrears", "outstanding_loan_balance": "outstanding_loan_balance",
                "loan_issue_date": "loan_issue_date", "deduction_start_period": "deduction_start_period",
                "submission_period": "submission_period", "maturity_period": "maturity_period"
            }
            rename_map = {k: v for k, v in target_columns.items() if k in df.columns}
            if rename_map: df = df.rename(rename_map)

            # Numeric Conversion
            num_cols = ["loan_amount", "loan_term", "monthly_installment", "accumulated_arrears", "outstanding_loan_balance"]
            for col in num_cols:
                if col in df.columns:
                    df = df.with_columns(
                        pl.col(col).cast(pl.Utf8).str.replace_all(r"[^\d\.\-]", "").cast(pl.Float64, strict=False).fill_null(0.0)
                    )

            # NDIA Recalculation
            if "monthly_installment" in df.columns and "accumulated_arrears" in df.columns:
                df = df.with_columns([
                    pl.when(pl.col("monthly_installment") > 0)
                    .then((pl.col("accumulated_arrears") / pl.col("monthly_installment")) * 30)
                    .otherwise(0.0)
                    .alias("ndia")
                ])
            else:
                df = df.with_columns(pl.lit(0.0).alias("ndia"))

            # Add metadata
            df = df.with_columns([
                pl.lit(portfolio_id).alias("portfolio_id"),
                pl.lit(tenant_id).alias("tenant_id"),
                pl.lit(sub_id).alias("subscription_id")
            ])

            # Inject using COPY
            connection = db.connection().connection
            cursor = connection.cursor()
            
            # Define columns for COPY
            copy_cols = ["portfolio_id", "tenant_id", "subscription_id", "loan_no", "employee_id", 
                         "loan_amount", "outstanding_loan_balance", "ndia", "monthly_installment", "accumulated_arrears"]
            copy_cols = [c for c in copy_cols if c in df.columns]
            
            # Format batch for COPY
            rows_data = []
            for row in df.select(copy_cols).to_dicts():
                line = "\t".join(str(row.get(c, "")) for c in copy_cols)
                rows_data.append(line)
            
            if rows_data:
                batch_buffer = io.StringIO("\n".join(rows_data) + "\n")
                cursor.copy_from(batch_buffer, "loans", columns=copy_cols, sep="\t", null="")
                connection.commit()
                total_processed += len(rows_data)
                logger.info(f"Processed batch {batch_count}: +{len(rows_data)} records (Total: {total_processed})")

        return {"processed": total_processed, "success": True}

    finally:
        if csv_path and os.path.exists(csv_path): os.remove(csv_path)



async def process_client_data_sync(file_path, portfolio_id, tenant_id, db):
    """Chunked processing for client data to minimize RAM usage."""
    csv_path = None
    try:
        # 1. Convert Excel to CSV
        logger.info(f"Converting client data Excel to CSV: {file_path}")
        csv_path = excel_to_csv_task(file_path)

        # 2. Batched reading
        reader = pl.read_csv_batched(csv_path, infer_schema_length=10000, ignore_errors=True)
        
        target_mapping = {
            "employee_id": "employee_id", "last_name": "last_name", "other_names": "other_names",
            "residential_address": "residential_address", "phone_number": "phone_number",
            "date_of_birth": "date_of_birth", "client_type": "client_type"
        }

        total_processed = 0
        batch_count = 0
        
        while (batches := reader.next_batches(1)):
            batch_count += 1
            df = batches[0]
            
            # Normalize columns
            df.columns = [c.strip().lower().replace(".", "").replace(" ", "_") for c in df.columns]
            
            # (Mapping logic for variants like 'lastname' -> 'last_name')
            # ...
            
            df = df.with_columns([
                pl.lit(portfolio_id).alias("portfolio_id"),
                pl.lit(tenant_id).alias("tenant_id")
            ])

            # Inject using COPY
            connection = db.connection().connection
            cursor = connection.cursor()
            
            rows_data = []
            for row in df.to_dicts():
                processed_row = {
                    "portfolio_id": str(portfolio_id),
                    "tenant_id": str(tenant_id),
                    "employee_id": str(row.get("employee_id", "")),
                    "last_name": str(row.get("last_name", row.get("lastname", ""))),
                    "other_names": str(row.get("other_names", row.get("othernames", ""))),
                    "client_type": "individual"
                }
                line = "\t".join(processed_row.values())
                rows_data.append(line)
            
            if rows_data:
                batch_buffer = io.StringIO("\n".join(rows_data) + "\n")
                cursor.copy_from(batch_buffer, "clients", columns=list(processed_row.keys()), sep="\t", null="")
                connection.commit()
                total_processed += len(rows_data)
                logger.info(f"Processed client batch {batch_count}: +{len(rows_data)} records (Total: {total_processed})")

        return {"processed": total_processed, "success": True}

    finally:
        if csv_path and os.path.exists(csv_path): os.remove(csv_path)

def run_quality_checks_sync(portfolio_id, db):
    """Synchronous function to run quality checks on portfolio data."""
    try:
        # Run quality checks and create issues
        issue_counts = create_and_save_quality_issues(db, portfolio_id)
        
        # Calculate total issues
        total_issues = sum(issue_counts.values())
        
        # Log the number of issues found
        logger.info(f"Found {total_issues} quality issues for portfolio {portfolio_id}")
        logger.info(f"Issue breakdown: {issue_counts}")
        
        return {
            "total_issues": total_issues,
            "issue_counts": issue_counts,
            "success": True,
            "message": f"Found {total_issues} quality issues"
        }
        
    except Exception as e:
        logger.error(f"Error running quality checks: {str(e)}")
        return {"error": str(e)}
