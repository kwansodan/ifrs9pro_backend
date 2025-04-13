import io
import csv
import pandas as pd
import polars as pl
import numpy as np
from datetime import datetime
import concurrent.futures
from sqlalchemy import text
import logging
import asyncio
from typing import Optional, Dict, Any, List

from app.models import (
    Loan,
    Guarantee,
    Client,
    Security
)
from app.utils.background_tasks import get_task_manager

logger = logging.getLogger(__name__)

async def process_loan_details_with_progress(task_id: str, file_content: bytes, portfolio_id: int, db):
    """Function to process loan details with progress reporting."""
    try:
        # Get task manager
        task_manager = get_task_manager()
        
        # Update task status
        task_manager.update_task(task_id, status_message="Reading loan details file")
        
        # Target column names (lowercase for matching)
        target_columns = {
            "loan no.": "loan_no",
            "employee id": "employee_id",
            "employee name": "employee_name",
            "employer": "employer",
            "loan issue date": "loan_issue_date",
            "deduction start period": "deduction_start_period",
            "submission period": "submission_period",
            "maturity period": "maturity_period",
            "location code": "location_code",
            "dalex paddy": "dalex_paddy",
            "team leader": "team_leader",
            "loan type": "loan_type",
            "loan amount": "loan_amount",
            "loan term": "loan_term",
            "administrative fees": "administrative_fees",
            "total interest": "total_interest",
            "total collectible": "total_collectible",
            "net loan amount": "net_loan_amount",
            "monthly installment": "monthly_installment",
            "principal due": "principal_due",
            "interest due": "interest_due",
            "total due": "total_due",
            "principal paid": "principal_paid",
            "interest paid": "interest_paid",
            "total paid": "total_paid",
            "principal paid2": "principal_paid2",
            "interest paid2": "interest_paid2",
            "total paid2": "total_paid2",
            "paid": "paid",
            "cancelled": "cancelled",
            "outstanding loan balance": "outstanding_loan_balance",
            "accumulated arrears": "accumulated_arrears",
            "ndia": "ndia",
            "prevailing posted repayment": "prevailing_posted_repayment",
            "prevailing due payment": "prevailing_due_payment",
            "current missed deduction": "current_missed_deduction",
            "admin charge": "admin_charge",
            "recovery rate": "recovery_rate",
            "deduction status": "deduction_status",
        }
        
        # Update task status
        task_manager.update_task(task_id, status_message="Checking existing loans")
        
        # Get existing loan numbers from the database using raw SQL for performance
        existing_loans_query = text("""
            SELECT loan_no, id FROM loans 
            WHERE portfolio_id = :portfolio_id AND loan_no IS NOT NULL
        """)
        result = db.execute(existing_loans_query, {"portfolio_id": portfolio_id})
        existing_loan_nos = {loan_no: loan_id for loan_no, loan_id in result if loan_no}
        
        logger.info(f"Found {len(existing_loan_nos)} existing loan numbers in portfolio {portfolio_id}")
        
        # Update task status
        task_manager.update_task(task_id, status_message="Reading Excel file")
        
        # CHUNKED PROCESSING: Read Excel file in chunks to reduce memory usage
        xlsx = pd.ExcelFile(io.BytesIO(file_content))
        sheet_name = xlsx.sheet_names[0]
        
        # Get total rows for progress tracking
        total_rows = len(pd.read_excel(xlsx, sheet_name=sheet_name, usecols=[0], nrows=None)) - 1
        
        # Update task with total items
        task_manager.update_task(task_id, total_items=total_rows, status_message="Processing data")
        
        # Process in chunks to reduce memory usage
        chunk_size = 500  # Adjust based on your memory constraints
        num_chunks = (total_rows + chunk_size - 1) // chunk_size
        
        # Track overall processing stats
        inserted_count = 0
        rows_updated = 0
        
        for chunk_idx in range(num_chunks):
            # Calculate chunk range
            start_row = chunk_idx * chunk_size
            end_row = min((chunk_idx + 1) * chunk_size, total_rows)
            
            # Update status
            task_manager.update_task(
                task_id, 
                status_message=f"Processing chunk {chunk_idx + 1}/{num_chunks} (rows {start_row}-{end_row})"
            )
            
            # Read chunk with Polars
            skiprows = 1 + start_row  # +1 for header
            nrows = end_row - start_row
            
            # Read chunk with pandas first (more reliable for chunked Excel reading)
            pd_chunk = pd.read_excel(
                xlsx, 
                sheet_name=sheet_name,
                skiprows=skiprows,
                nrows=nrows
            )
            
            # Convert to Polars for processing
            df = pl.from_pandas(pd_chunk)
            
            # Free pandas memory
            del pd_chunk
            
            # Create a mapping of actual column names to our target column names
            case_insensitive_mapping = {}
            for col in df.columns:
                col_lower = str(col).lower().strip() if col is not None else ""
                if col_lower in target_columns:
                    case_insensitive_mapping[col] = target_columns[col_lower]
            
            # Rename columns using our case-insensitive mapping
            for old_col, new_col in case_insensitive_mapping.items():
                if old_col in df.columns:
                    df = df.rename({old_col: new_col})

            # Process date columns
            date_columns = ["loan_issue_date", "deduction_start_period", "submission_period", "maturity_period"]
            for date_col in date_columns:
                if date_col in df.columns:
                    # Try to parse dates with multiple common formats
                    # First clean the data
                    df = df.with_columns(
                        pl.col(date_col)
                        .cast(pl.Utf8)
                        .str.replace(r"^\s*-\s*$|^\s*$|None|NaN|nan|-", "")
                    )
                    
                    # Create a clean column for date parsing
                    clean_col = df[date_col]
                    
                    # Initialize a column with null values
                    parsed_dates = pl.Series(name=date_col, values=[None] * len(df))
                    
                    # Try different date formats
                    date_formats = [
                        "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y", 
                        "%Y/%m/%d", "%d.%m.%Y", "%m.%d.%Y", "%Y.%m.%d",
                        "%b %d, %Y", "%d %b %Y", "%Y %b %d",
                        "%B %d, %Y", "%d %B %Y", "%Y %B %d"
                    ]
                    
                    # Try each format and use the first one that works
                    for fmt in date_formats:
                        try:
                            # Try to parse with this format
                            temp_dates = clean_col.str.strptime(pl.Date, fmt=fmt, strict=False)
                            # If we have any non-null values, use this format
                            if temp_dates.null_count() < len(temp_dates):
                                parsed_dates = temp_dates
                                logger.info(f"Successfully parsed {date_col} using format {fmt}")
                                break
                        except Exception as e:
                            # This format didn't work, try the next one
                            continue
                    
                    # Use current date for any remaining null values
                    current_date = datetime.now().date()
                    df = df.with_columns(
                        parsed_dates.fill_null(current_date).alias(date_col)
                    )

            # Handle loan_term separately as it needs to be an integer
            if "loan_term" in df.columns:
                df = df.with_columns(
                    pl.col("loan_term")
                    .cast(pl.Utf8)
                    .str.replace(r"^\s*-\s*$|^\s*$|None|NaN|nan|-", "")
                    .cast(pl.Float64, strict=False)
                    .fill_null(0.0)
                    .cast(pl.Int64)  # Convert to integer
                )
            
            # Process numeric columns (except loan_term which is handled separately)
            numeric_columns = [
                "loan_amount", "administrative_fees", "total_interest", "total_collectible",
                "net_loan_amount", "monthly_installment", "principal_due", "interest_due",
                "total_due", "principal_paid", "interest_paid", "total_paid",
                "principal_paid2", "interest_paid2", "total_paid2", "outstanding_loan_balance",
                "accumulated_arrears", "ndia", "prevailing_posted_repayment", 
                "prevailing_due_payment", "current_missed_deduction", "admin_charge",
                "recovery_rate"
            ]
            
            for col in numeric_columns:
                if col in df.columns:
                    # Replace problematic values and convert to float
                    # First ensure the column is string type, then clean and convert to float
                    df = df.with_columns(
                        pl.col(col)
                        .cast(pl.Utf8)
                        .str.replace(r"^\s*-\s*$|^\s*$|None|NaN|nan|-", "")
                        .cast(pl.Float64, strict=False)
                        .fill_null(0.0)
                    )

            # Convert boolean columns
            bool_values = ["Yes", "TRUE", "True", "true", "1", "Y", "y"]
            for col in ["paid", "cancelled"]:
                if col in df.columns:
                    df = df.with_columns(
                        pl.col(col).cast(pl.Utf8).is_in(bool_values).fill_null(False)
                    )
            
            # Add portfolio_id to all records
            df = df.with_columns(pl.lit(portfolio_id).alias("portfolio_id"))
            
            # Filter out rows with no loan_no
            if "loan_no" in df.columns:
                df = df.filter(pl.col("loan_no").is_not_null())
                
                # Convert to pandas for easier processing with existing_loan_nos
                pdf = df.to_pandas()
                
                # Free polars memory
                del df
                
                # Split into updates and inserts
                mask_update = pdf["loan_no"].isin(existing_loan_nos.keys())
                df_update = pdf[mask_update].copy()
                df_insert = pdf[~mask_update].copy()
                
                # Free memory
                del pdf
                del mask_update
                
                # Process this chunk
                chunk_inserted = len(df_insert)
                inserted_count += chunk_inserted
                
                # Process updates if any
                chunk_updated = 0
                if not df_update.empty:
                    # Process updates in batches
                    batch_size = 1000
                    for i in range(0, len(df_update), batch_size):
                        batch = df_update.iloc[i:i+batch_size]
                        
                        # Prepare update statements
                        for _, row in batch.iterrows():
                            # Build update statement
                            update_values = {}
                            for col in df_update.columns:
                                if col not in ["id", "portfolio_id", "loan_no"] and not pd.isna(row[col]):
                                    update_values[col] = row[col]
                            
                            if update_values:
                                loan = db.query(Loan).filter(Loan.id == row["id"]).first()
                                if loan:
                                    for key, value in update_values.items():
                                        setattr(loan, key, value)
                                    chunk_updated += 1
                        
                        # Commit batch
                        db.commit()
                
                rows_updated += chunk_updated
                
                # Free memory
                del df_update
                del df_insert
                
                # Update progress for this chunk
                chunk_progress = min(95, (chunk_idx + 1) / num_chunks * 95)
                task_manager.update_progress(
                    task_id, 
                    progress=chunk_progress,
                    processed_items=(chunk_idx + 1) * chunk_size,
                    status_message=f"Processed chunk {chunk_idx + 1}/{num_chunks}: {chunk_inserted} inserted, {chunk_updated} updated"
                )
            
            else:
                # Skip this chunk if no loan_no column
                logger.warning(f"No loan_no column found in chunk {chunk_idx + 1}")
                continue
        
        # Final commit and progress update
        db.commit()
        
        # Set final progress
        task_manager.update_progress(
            task_id, 
            progress=100,
            processed_items=total_rows,
            status_message=f"Completed: {inserted_count} loans inserted, {rows_updated} updated"
        )
        
        return {
            "inserted": inserted_count,
            "updated": rows_updated,
            "total_processed": total_rows
        }
        
    except Exception as e:
        logger.exception(f"Error processing loan details: {str(e)}")
        task_manager.update_task(task_id, status_message=f"Error: {str(e)}")
        raise

async def process_client_data_with_progress(task_id: str, file_content: bytes, portfolio_id: int, db):
    """Process client data file with progress reporting."""
    try:
        # Get task manager
        task_manager = get_task_manager()
        
        # Update task status
        task_manager.update_task(task_id, status_message="Reading client data file")
        
        # Target column names (lowercase for matching)
        target_columns = {
            "employee id": "employee_id",
            "last name": "last_name",
            "other names": "other_names",
            "gender": "gender",
            "date of birth": "date_of_birth",
            "age": "age",
            "marital status": "marital_status",
            "employer": "employer",
            "department": "department",
            "job title": "job_title",
            "employment type": "employment_type",
            "employment start date": "employment_date",  # Match the column name in the model
            "years in service": "years_in_service",
            "monthly salary": "monthly_salary",
            "phone number": "phone_number",
            "email": "email",
            "residential address": "residential_address",
            "postal address": "postal_address",
            "id type": "id_type",
            "id number": "id_number",
        }
        
        # Update task status
        task_manager.update_task(task_id, status_message="Checking existing clients")
        
        # Get existing employee IDs from the database using raw SQL for performance
        existing_clients_query = text("""
            SELECT employee_id, id FROM clients 
            WHERE portfolio_id = :portfolio_id AND employee_id IS NOT NULL
        """)
        result = db.execute(existing_clients_query, {"portfolio_id": portfolio_id})
        existing_employee_ids = {employee_id: client_id for employee_id, client_id in result if employee_id}
        
        logger.info(f"Found {len(existing_employee_ids)} existing employee IDs in portfolio {portfolio_id}")
        
        # Update task status
        task_manager.update_task(task_id, status_message="Reading Excel file")
        
        # CHUNKED PROCESSING: Read Excel file in chunks to reduce memory usage
        xlsx = pd.ExcelFile(io.BytesIO(file_content))
        sheet_name = xlsx.sheet_names[0]
        
        # Get total rows for progress tracking
        total_rows = len(pd.read_excel(xlsx, sheet_name=sheet_name, usecols=[0], nrows=None)) - 1
        
        # Update task with total items
        task_manager.update_task(task_id, total_items=total_rows, status_message="Processing data")
        
        # Process in chunks to reduce memory usage
        chunk_size = 500  # Adjust based on your memory constraints
        num_chunks = (total_rows + chunk_size - 1) // chunk_size
        
        # Track overall processing stats
        inserted_count = 0
        rows_updated = 0
        
        for chunk_idx in range(num_chunks):
            # Calculate chunk range
            start_row = chunk_idx * chunk_size
            end_row = min((chunk_idx + 1) * chunk_size, total_rows)
            
            # Update status
            task_manager.update_task(
                task_id, 
                status_message=f"Processing chunk {chunk_idx + 1}/{num_chunks} (rows {start_row}-{end_row})"
            )
            
            # Read chunk with pandas first (more reliable for chunked Excel reading)
            skiprows = 1 + start_row  # +1 for header
            nrows = end_row - start_row
            
            pd_chunk = pd.read_excel(
                xlsx, 
                sheet_name=sheet_name,
                skiprows=skiprows,
                nrows=nrows
            )
            
            # Convert to Polars for processing
            df = pl.from_pandas(pd_chunk)
            
            # Free pandas memory
            del pd_chunk
            
            # Create a mapping of actual column names to our target column names
            case_insensitive_mapping = {}
            for col in df.columns:
                col_lower = str(col).lower().strip() if col is not None else ""
                if col_lower in target_columns:
                    case_insensitive_mapping[col] = target_columns[col_lower]
            
            # Rename columns using our case-insensitive mapping
            for old_col, new_col in case_insensitive_mapping.items():
                if old_col in df.columns:
                    df = df.rename({old_col: new_col})
            
            # Process date columns
            date_columns = ["date_of_birth"]
            for date_col in date_columns:
                if date_col in df.columns:
                    # Try to parse dates with multiple common formats
                    # First clean the data
                    df = df.with_columns(
                        pl.col(date_col)
                        .cast(pl.Utf8)
                        .str.replace(r"^\s*-\s*$|^\s*$|None|NaN|nan|-", "")
                    )
                    
                    # Create a clean column for date parsing
                    clean_col = df[date_col]
                    
                    # Initialize a column with null values
                    parsed_dates = pl.Series(name=date_col, values=[None] * len(df))
                    
                    # Try different date formats
                    date_formats = [
                        "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y", 
                        "%Y/%m/%d", "%d.%m.%Y", "%m.%d.%Y", "%Y.%m.%d",
                        "%b %d, %Y", "%d %b %Y", "%Y %b %d",
                        "%B %d, %Y", "%d %B %Y", "%Y %B %d"
                    ]
                    
                    # Try each format and use the first one that works
                    for fmt in date_formats:
                        try:
                            # Try to parse with this format
                            temp_dates = clean_col.str.strptime(pl.Date, fmt=fmt, strict=False)
                            # If we have any non-null values, use this format
                            if temp_dates.null_count() < len(temp_dates):
                                parsed_dates = temp_dates
                                logger.info(f"Successfully parsed {date_col} using format {fmt}")
                                break
                        except Exception as e:
                            # This format didn't work, try the next one
                            continue
                    
                    # Use a default date for any remaining null values
                    default_date = datetime(1970, 1, 1).date()
                    df = df.with_columns(
                        parsed_dates.fill_null(default_date).alias(date_col)
                    )
            
            # Process age as integer
            if "age" in df.columns:
                df = df.with_columns(
                    pl.col("age")
                    .cast(pl.Utf8)
                    .str.replace(r"^\s*-\s*$|^\s*$|None|NaN|nan|-", "")
                    .cast(pl.Float64, strict=False)
                    .fill_null(0.0)
                    .cast(pl.Int64)  # Convert to integer
                )
            
            # Add portfolio_id to all records
            df = df.with_columns(pl.lit(portfolio_id).alias("portfolio_id"))
            
            # Filter out rows with no employee_id
            if "employee_id" in df.columns:
                df = df.filter(pl.col("employee_id").is_not_null())
                
                # Convert to pandas for easier processing with existing_employee_ids
                pdf = df.to_pandas()
                
                # Free polars memory
                del df
                
                # Split into updates and inserts
                mask_update = pdf["employee_id"].isin(existing_employee_ids.keys())
                df_update = pdf[mask_update].copy()
                df_insert = pdf[~mask_update].copy()
                
                # Free memory
                del pdf
                del mask_update
                
                # Add id column to updates
                if not df_update.empty:
                    df_update["id"] = df_update["employee_id"].map(existing_employee_ids)
                
                # Process this chunk
                chunk_inserted = len(df_insert)
                inserted_count += chunk_inserted
                
                # Process updates if any
                chunk_updated = 0
                if not df_update.empty:
                    # Process updates in batches
                    batch_size = 1000
                    for i in range(0, len(df_update), batch_size):
                        batch = df_update.iloc[i:i+batch_size]
                        
                        # Prepare update statements
                        for _, row in batch.iterrows():
                            # Build update statement
                            update_values = {}
                            for col in df_update.columns:
                                if col not in ["id", "portfolio_id", "employee_id"] and not pd.isna(row[col]):
                                    update_values[col] = row[col]
                            
                            if update_values:
                                client = db.query(Client).filter(Client.id == row["id"]).first()
                                if client:
                                    for key, value in update_values.items():
                                        setattr(client, key, value)
                                    chunk_updated += 1
                        
                        # Commit batch
                        db.commit()
                
                rows_updated += chunk_updated
                
                # Process inserts if any
                if not df_insert.empty:
                    # Create a CSV-like string buffer
                    csv_buffer = io.StringIO()
                    
                    # Write data to the buffer in CSV format
                    for _, row in df_insert.iterrows():
                        values = []
                        for col in ["portfolio_id", "employee_id", "last_name", "other_names", 
                                   "gender", "date_of_birth", "age", "marital_status", "number_of_dependents",
                                   "education_level", "employment_type", "employment_sector", "monthly_income",
                                   "employer_name", "department", "position", "employment_start_date",
                                   "employment_end_date", "employment_status", "residence_type", "residence_ownership",
                                   "residence_location", "contact_number", "email_address"]:
                            if col in row:
                                if col in date_columns and pd.notna(row[col]):
                                    values.append(str(row[col]))
                                elif col == "age" and pd.notna(row[col]):
                                    # Ensure age is an integer
                                    values.append(str(int(float(row[col]))))
                                elif pd.isna(row[col]):
                                    values.append("")  # NULL in COPY format
                                else:
                                    val = str(row[col])
                                    # Escape special characters for COPY
                                    val = val.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
                                    values.append(val)
                            else:
                                values.append("")  # NULL in COPY format
                        csv_buffer.write("\t".join(values) + "\n")
                    
                    # Reset buffer position to start
                    csv_buffer.seek(0)
                    
                    # Get raw connection from SQLAlchemy session
                    connection = db.connection().connection
                    
                    # Use a cursor for the COPY command
                    with connection.cursor() as cursor:
                        # Start a COPY command
                        cursor.copy_from(
                            csv_buffer,
                            "clients",
                            columns=[
                                "portfolio_id", "employee_id", "last_name", "other_names", 
                                "gender", "date_of_birth", "age", "marital_status", "number_of_dependents",
                                "education_level", "employment_type", "employment_sector", "monthly_income",
                                "employer_name", "department", "position", "employment_start_date",
                                "employment_end_date", "employment_status", "residence_type", "residence_ownership",
                                "residence_location", "contact_number", "email_address"
                            ],
                            sep="\t",
                            null=""
                        )
                
                # Free memory
                del df_update
                del df_insert
                
                # Update progress for this chunk
                chunk_progress = min(95, (chunk_idx + 1) / num_chunks * 95)
                task_manager.update_progress(
                    task_id, 
                    progress=chunk_progress,
                    processed_items=(chunk_idx + 1) * chunk_size,
                    status_message=f"Processed chunk {chunk_idx + 1}/{num_chunks}: {chunk_inserted} inserted, {chunk_updated} updated"
                )
            
            else:
                # Skip this chunk if no employee_id column
                logger.warning(f"No employee_id column found in chunk {chunk_idx + 1}")
                continue
        
        # Final commit and progress update
        db.commit()
        
        # Set final progress
        task_manager.update_progress(
            task_id, 
            progress=100,
            processed_items=total_rows,
            status_message=f"Completed: {inserted_count} clients inserted, {rows_updated} updated"
        )
        
        return {
            "inserted": inserted_count,
            "updated": rows_updated,
            "total_processed": total_rows
        }
        
    except Exception as e:
        logger.exception(f"Error processing client data: {str(e)}")
        task_manager.update_task(task_id, status_message=f"Error: {str(e)}")
        raise
