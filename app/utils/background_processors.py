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
            "deduction status": "deduction_status"
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
        
        # Read the header row to determine column mapping once for all chunks
        header_df = pd.read_excel(xlsx, sheet_name=sheet_name, nrows=1)
        
        # Create a mapping of actual column names to our target column names
        case_insensitive_mapping = {}
        
        # Log all available columns for debugging
        logger.info(f"Available columns in Excel file: {list(header_df.columns)}")
        
        for col in header_df.columns:
            col_lower = str(col).lower().strip() if col is not None else ""
            
            # Normalize column name by removing spaces, dots, underscores, etc.
            col_normalized = col_lower.replace(" ", "").replace(".", "").replace("_", "").replace("-", "").replace("#", "")
            
            # First try direct match
            if col_lower in target_columns:
                case_insensitive_mapping[col] = target_columns[col_lower]
                logger.info(f"Direct match for column '{col}' to '{target_columns[col_lower]}'")
            # Then try normalized match
            else:
                for target_col, target_name in target_columns.items():
                    target_normalized = target_col.replace(" ", "").replace(".", "").replace("_", "").replace("-", "").replace("#", "")
                    if col_normalized == target_normalized:
                        case_insensitive_mapping[col] = target_name
                        logger.info(f"Matched column '{col}' to target '{target_col}' using normalized comparison")
                        break
        
        # Log the final column mapping for debugging
        logger.info(f"Final column mapping: {case_insensitive_mapping}")
        
        # Create a list of target column names in the order they appear in the Excel file
        target_column_order = []
        for col in header_df.columns:
            if col in case_insensitive_mapping:
                target_column_order.append(case_insensitive_mapping[col])
            else:
                # If no mapping exists, use the original column name
                target_column_order.append(col)
        
        logger.info(f"Target column order: {target_column_order}")
        
        # If we don't have a loan_no column, use the first column as loan_no
        has_loan_no = "loan_no" in target_column_order
        if not has_loan_no and len(target_column_order) > 0:
            target_column_order[0] = "loan_no"
            logger.info(f"No loan_no column found, using first column as loan_no")
        
        # Process in chunks to reduce memory usage
        chunk_size = 50  # Adjust based on your memory constraints
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
                nrows=nrows,
                header=None  # Don't use the first row as header
            )
            
            # Assign the target column names to the dataframe
            if len(pd_chunk.columns) == len(target_column_order):
                pd_chunk.columns = target_column_order
            else:
                # If column counts don't match, log an error and try to handle it
                logger.error(f"Column count mismatch in chunk {chunk_idx + 1}: Expected {len(target_column_order)}, got {len(pd_chunk.columns)}")
                # Use as many columns as we have, padding with None if needed
                if len(pd_chunk.columns) < len(target_column_order):
                    pd_chunk.columns = target_column_order[:len(pd_chunk.columns)]
                else:
                    # More columns than expected, use target columns and add extras
                    columns = target_column_order.copy()
                    for i in range(len(target_column_order), len(pd_chunk.columns)):
                        columns.append(f"extra_col_{i}")
                    pd_chunk.columns = columns
            
            # Convert to Polars for processing
            df = pl.from_pandas(pd_chunk)
            
            # Free pandas memory
            del pd_chunk
            
            # Log column names for debugging
            logger.info(f"Columns in chunk {chunk_idx + 1} after applying target column order: {df.columns}")
            
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
                "prevailing_due_payment", "current_missed_deduction", 
                "admin_charge", "recovery_rate"
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
            
            # Check if loan_no column exists after mapping
            if "loan_no" not in df.columns:
                # If loan_no doesn't exist, use the first column as loan_no
                if len(df.columns) > 0:
                    first_col = df.columns[0]
                    logger.info(f"loan_no column not found after mapping, using first column '{first_col}' as loan_no")
                    df = df.rename({first_col: "loan_no"})
                else:
                    logger.error("No columns available in the dataframe")
                    continue
            
            # Filter out rows with no loan_no
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
                
            # Process inserts if any
            if not df_insert.empty:
                # Log the first few rows for debugging
                logger.info(f"First row of data to insert: {df_insert.iloc[0].to_dict() if len(df_insert) > 0 else 'No data'}")
                
                # Ensure loan_amount has a default value (required field)
                if 'loan_amount' in df_insert.columns:
                    df_insert['loan_amount'] = df_insert['loan_amount'].fillna(0)
                else:
                    df_insert['loan_amount'] = 0
                
                # Create a CSV-like string buffer
                csv_buffer = io.StringIO()
                
                # Write data to the buffer in CSV format
                for _, row in df_insert.iterrows():
                    values = []
                    for col in ["portfolio_id", "loan_no", "employee_id", "employee_name", 
                               "employer", "loan_issue_date", "deduction_start_period", 
                               "submission_period", "maturity_period", "location_code", 
                               "dalex_paddy", "team_leader", "loan_type", "loan_amount", 
                               "loan_term", "administrative_fees", "total_interest", 
                               "total_collectible", "net_loan_amount", "monthly_installment", 
                               "principal_due", "interest_due", "total_due", "principal_paid", 
                               "interest_paid", "total_paid", "principal_paid2", "interest_paid2", 
                               "total_paid2", "paid", "cancelled", "outstanding_loan_balance", 
                               "accumulated_arrears", "ndia", "prevailing_posted_repayment", 
                               "prevailing_due_payment", "current_missed_deduction", 
                               "admin_charge", "recovery_rate", "deduction_status"]:
                        if col in row:
                            if col in date_columns and pd.notna(row[col]):
                                values.append(str(row[col]))
                            elif col == "loan_term" and pd.notna(row[col]):
                                # Ensure loan_term is an integer
                                values.append(str(int(float(row[col]))))
                            elif col in ["paid", "cancelled"] and pd.notna(row[col]):
                                # Convert boolean to string representation
                                values.append(str(row[col]).lower())
                            elif pd.isna(row[col]):
                                # Handle required fields with default values
                                if col == "loan_amount":
                                    values.append("0")  # Default to 0 for loan_amount
                                else:
                                    values.append("")  # NULL in COPY format
                            else:
                                val = str(row[col])
                                # Escape special characters for COPY
                                val = val.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
                                values.append(val)
                        else:
                            # Handle required fields with default values
                            if col == "loan_amount":
                                values.append("0")  # Default to 0 for loan_amount
                            elif col == "loan_term":
                                values.append("0")  # Default to 0 for loan_term
                            elif col in ["paid", "cancelled"]:
                                values.append("false")  # Default to false for boolean fields
                            else:
                                values.append("")  # NULL in COPY format
                    csv_buffer.write("\t".join(values) + "\n")
                
                # Reset buffer position to start
                csv_buffer.seek(0)
                
                try:
                    # Get raw connection from SQLAlchemy session
                    connection = db.connection().connection
                    
                    # Use a cursor for the COPY command
                    with connection.cursor() as cursor:
                        # Start a COPY command
                        cursor.copy_from(
                            csv_buffer,
                            "loans",
                            columns=[
                                "portfolio_id", "loan_no", "employee_id", "employee_name", 
                                "employer", "loan_issue_date", "deduction_start_period", 
                                "submission_period", "maturity_period", "location_code", 
                                "dalex_paddy", "team_leader", "loan_type", "loan_amount", 
                                "loan_term", "administrative_fees", "total_interest", 
                                "total_collectible", "net_loan_amount", "monthly_installment", 
                                "principal_due", "interest_due", "total_due", "principal_paid", 
                                "interest_paid", "total_paid", "principal_paid2", "interest_paid2", 
                                "total_paid2", "paid", "cancelled", "outstanding_loan_balance", 
                                "accumulated_arrears", "ndia", "prevailing_posted_repayment", 
                                "prevailing_due_payment", "current_missed_deduction", 
                                "admin_charge", "recovery_rate", "deduction_status"
                            ],
                            sep="\t",
                            null=""
                        )
                except Exception as e:
                    logger.error(f"Error during bulk insert: {str(e)}")
                    # Try individual inserts as a fallback
                    logger.info("Attempting individual inserts as fallback")
                    for i, row in df_insert.iterrows():
                        try:
                            # Ensure required fields have values
                            if pd.isna(row.get('loan_amount')):
                                row['loan_amount'] = 0
                                
                            # Create a new Loan object
                            loan = Loan(
                                portfolio_id=portfolio_id,
                                loan_no=row.get('loan_no'),
                                loan_amount=row.get('loan_amount', 0)
                            )
                            
                            # Add other fields if they exist
                            for col in df_insert.columns:
                                if col not in ['portfolio_id', 'loan_no', 'loan_amount'] and not pd.isna(row.get(col)):
                                    setattr(loan, col, row[col])
                            
                            db.add(loan)
                            # Commit every 100 rows to avoid large transactions
                            if i % 100 == 0:
                                db.commit()
                        except Exception as inner_e:
                            logger.error(f"Error inserting row {i}: {str(inner_e)}")
                            continue
                    
                    # Final commit for remaining rows
                    db.commit()
            
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
            "lastname": "last_name",
            "othernames": "other_names",
            "residential address": "residential_address",
            "postal address": "postal_address",
            "client phone no.": "phone_number",
            "title": "title",
            "marital status": "marital_status",
            "gender": "gender",
            "date of birth": "date_of_birth",
            "employer": "employer",
            "previous employee no.": "previous_employee_no",
            "social security no.": "social_security_no",
            "voters id no.": "voters_id_no",
            "employment date": "employment_date",
            "next of kin": "next_of_kin",
            "next of kin contact:": "next_of_kin_contact",
            "next of kin address": "next_of_kin_address",
            "search name": "search_name"
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
        
        # Read the header row to determine column mapping once for all chunks
        header_df = pd.read_excel(xlsx, sheet_name=sheet_name, nrows=1)
        
        # Create a mapping of actual column names to our target column names
        case_insensitive_mapping = {}
        
        # Log all available columns for debugging
        logger.info(f"Available columns in Excel file: {list(header_df.columns)}")
        
        for col in header_df.columns:
            col_lower = str(col).lower().strip() if col is not None else ""
            
            # Normalize column name by removing spaces, dots, underscores, etc.
            col_normalized = col_lower.replace(" ", "").replace(".", "").replace("_", "").replace("-", "").replace("#", "")
            
            # First try direct match
            if col_lower in target_columns:
                case_insensitive_mapping[col] = target_columns[col_lower]
                logger.info(f"Direct match for column '{col}' to '{target_columns[col_lower]}'")
            # Then try normalized match
            else:
                for target_col, target_name in target_columns.items():
                    target_normalized = target_col.replace(" ", "").replace(".", "").replace("_", "").replace("-", "").replace("#", "")
                    if col_normalized == target_normalized:
                        case_insensitive_mapping[col] = target_name
                        logger.info(f"Matched column '{col}' to target '{target_col}' using normalized comparison")
                        break
        
        # Log the final column mapping for debugging
        logger.info(f"Final column mapping: {case_insensitive_mapping}")
        
        # Create a list of target column names in the order they appear in the Excel file
        target_column_order = []
        for col in header_df.columns:
            if col in case_insensitive_mapping:
                target_column_order.append(case_insensitive_mapping[col])
            else:
                # If no mapping exists, use the original column name
                target_column_order.append(col)
        
        logger.info(f"Target column order: {target_column_order}")
        
        # If we don't have an employee_id column, use the first column as employee_id
        has_employee_id = "employee_id" in target_column_order
        if not has_employee_id and len(target_column_order) > 0:
            target_column_order[0] = "employee_id"
            logger.info(f"No employee_id column found, using first column as employee_id")
        
        # Process in chunks to reduce memory usage
        chunk_size = 50  # Adjust based on your memory constraints
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
                nrows=nrows,
                header=None  # Don't use the first row as header
            )
            
            # Assign the target column names to the dataframe
            if len(pd_chunk.columns) == len(target_column_order):
                pd_chunk.columns = target_column_order
            else:
                # If column counts don't match, log an error and try to handle it
                logger.error(f"Column count mismatch in chunk {chunk_idx + 1}: Expected {len(target_column_order)}, got {len(pd_chunk.columns)}")
                # Use as many columns as we have, padding with None if needed
                if len(pd_chunk.columns) < len(target_column_order):
                    pd_chunk.columns = target_column_order[:len(pd_chunk.columns)]
                else:
                    # More columns than expected, use target columns and add extras
                    columns = target_column_order.copy()
                    for i in range(len(target_column_order), len(pd_chunk.columns)):
                        columns.append(f"extra_col_{i}")
                    pd_chunk.columns = columns
            
            # Convert to Polars for processing
            df = pl.from_pandas(pd_chunk)
            
            # Free pandas memory
            del pd_chunk
            
            # Log column names for debugging
            logger.info(f"Columns in chunk {chunk_idx + 1} after applying target column order: {df.columns}")
            
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
            
            # Add portfolio_id to all records
            df = df.with_columns(pl.lit(portfolio_id).alias("portfolio_id"))
            
            # Check if employee_id column exists after mapping
            if "employee_id" not in df.columns:
                # If employee_id doesn't exist, use the first column as employee_id
                if len(df.columns) > 0:
                    first_col = df.columns[0]
                    logger.info(f"employee_id column not found after mapping, using first column '{first_col}' as employee_id")
                    df = df.rename({first_col: "employee_id"})
                else:
                    logger.error("No columns available in the dataframe")
                    continue
            
            # Filter out rows with no employee_id
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
                # Log the first few rows for debugging
                logger.info(f"First row of client data to insert: {df_insert.iloc[0].to_dict() if len(df_insert) > 0 else 'No data'}")
                
                # Create a CSV-like string buffer
                csv_buffer = io.StringIO()
                
                # Write data to the buffer in CSV format
                for _, row in df_insert.iterrows():
                    values = []
                    for col in ["portfolio_id", "employee_id", "last_name", "other_names", 
                               "residential_address", "postal_address", "phone_number", "title", 
                               "marital_status", "gender", "date_of_birth", "employer", 
                               "previous_employee_no", "social_security_no", "voters_id_no", 
                               "employment_date", "next_of_kin", "next_of_kin_contact", 
                               "next_of_kin_address"]:
                        if col in row:
                            if col in date_columns and pd.notna(row[col]):
                                values.append(str(row[col]))
                            elif pd.isna(row[col]):
                                # For required fields, provide default values
                                if col in ["last_name", "other_names"]:
                                    values.append("Unknown")  # Default name for required fields
                                else:
                                    values.append("")  # NULL in COPY format
                            else:
                                val = str(row[col])
                                # Escape special characters for COPY
                                val = val.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
                                values.append(val)
                        else:
                            # For required fields, provide default values
                            if col in ["last_name", "other_names"]:
                                values.append("Unknown")  # Default name for required fields
                            else:
                                values.append("")  # NULL in COPY format
                    csv_buffer.write("\t".join(values) + "\n")
                
                # Reset buffer position to start
                csv_buffer.seek(0)
                
                try:
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
                                "residential_address", "postal_address", "phone_number", "title", 
                                "marital_status", "gender", "date_of_birth", "employer", 
                                "previous_employee_no", "social_security_no", "voters_id_no", 
                                "employment_date", "next_of_kin", "next_of_kin_contact", 
                                "next_of_kin_address"
                            ],
                            sep="\t",
                            null=""
                        )
                except Exception as e:
                    logger.error(f"Error during bulk client insert: {str(e)}")
                    # Try individual inserts as a fallback
                    logger.info("Attempting individual client inserts as fallback")
                    
                    # Rollback the failed transaction
                    db.rollback()
                    
                    for i, row in df_insert.iterrows():
                        try:
                            # Create a new Client object with required fields
                            client = Client(
                                portfolio_id=portfolio_id,
                                employee_id=row.get('employee_id'),
                                last_name=row.get('last_name', 'Unknown'),
                                other_names=row.get('other_names', 'Unknown')
                            )
                            
                            # Add other fields if they exist
                            for col in df_insert.columns:
                                if col not in ['portfolio_id', 'employee_id', 'last_name', 'other_names'] and col in [
                                    "residential_address", "postal_address", "phone_number", "title", 
                                    "marital_status", "gender", "date_of_birth", "employer", 
                                    "previous_employee_no", "social_security_no", "voters_id_no", 
                                    "employment_date", "next_of_kin", "next_of_kin_contact", 
                                    "next_of_kin_address"
                                ] and not pd.isna(row.get(col)):
                                    setattr(client, col, row[col])
                            
                            db.add(client)
                            # Commit every 100 rows to avoid large transactions
                            if i % 100 == 0:
                                db.commit()
                        except Exception as inner_e:
                            logger.error(f"Error inserting client row {i}: {str(inner_e)}")
                            # Rollback on error
                            db.rollback()
                            continue
                    
                    # Final commit for remaining rows
                    try:
                        db.commit()
                    except Exception as commit_e:
                        logger.error(f"Error during final commit: {str(commit_e)}")
                        db.rollback()
            
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
