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
        
        # Track overall processing stats
        rows_processed = 0
        rows_skipped = 0
        rows_updated = 0
        inserted_count = 0

        # Update task status
        task_manager.update_task(task_id, status_message="Reading Excel file")
        
        # POLARS OPTIMIZATION: Read Excel file in one go with Polars
        # First read with pandas to get number of rows
        xlsx = pd.ExcelFile(io.BytesIO(file_content))
        sheet_name = xlsx.sheet_names[0]
        total_rows = len(pd.read_excel(xlsx, sheet_name=sheet_name, usecols=[0])) - 1
        
        # Update task with total items
        task_manager.update_task(task_id, total_items=total_rows, status_message="Processing data")
        
        # Now read with Polars for better performance
        df = pl.read_excel(io.BytesIO(file_content), sheet_name=sheet_name)
        
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
            
            # Split into updates and inserts
            mask_update = pdf["loan_no"].isin(existing_loan_nos.keys())
            df_update = pdf[mask_update].copy()
            df_insert = pdf[~mask_update].copy()
            
            # Add id column to updates
            if not df_update.empty:
                df_update["id"] = df_update["loan_no"].map(existing_loan_nos)
            
            # Update task status
            task_manager.update_task(
                task_id, 
                status_message=f"Processing {len(df_insert)} new loans and {len(df_update)} updates"
            )
            
            # ULTRA-OPTIMIZED: Use PostgreSQL COPY command for bulk inserts
            # This is much faster than individual INSERT statements
            
            # Create a CSV-like string buffer
            csv_buffer = io.StringIO()
            
            # Process in batches to update progress
            batch_size = max(1, min(1000, len(df_insert) // 10))  # Adjust batch size based on total
            
            # Write data to the buffer in CSV format
            for i, (_, row) in enumerate(df_insert.iterrows()):
                values = []
                for col in ["portfolio_id", "loan_no", "employee_id", "employee_name", "employer", 
                           "loan_issue_date", "deduction_start_period", "submission_period", "maturity_period",
                           "location_code", "dalex_paddy", "team_leader", "loan_type", "loan_amount", "loan_term",
                           "administrative_fees", "total_interest", "total_collectible", "net_loan_amount",
                           "monthly_installment", "principal_due", "interest_due", "total_due",
                           "principal_paid", "interest_paid", "total_paid", "principal_paid2", "interest_paid2",
                           "total_paid2", "paid", "cancelled", "outstanding_loan_balance", "accumulated_arrears",
                           "ndia", "prevailing_posted_repayment", "prevailing_due_payment", "current_missed_deduction",
                           "admin_charge", "recovery_rate", "deduction_status"]:
                    if col in row:
                        if col in date_columns and pd.notna(row[col]):
                            values.append(str(row[col]))
                        elif col in ["paid", "cancelled"]:
                            values.append("t" if row[col] else "f")
                        elif col == "loan_term" and pd.notna(row[col]):
                            # Ensure loan_term is an integer
                            values.append(str(int(float(row[col]))))
                        elif col in numeric_columns and pd.notna(row[col]):
                            values.append(str(row[col]))
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
                
                # Update progress periodically
                if i > 0 and i % batch_size == 0:
                    progress = min(90, (i / total_rows) * 100)  # Cap at 90% to leave room for final steps
                    task_manager.update_progress(
                        task_id, 
                        progress=progress,
                        processed_items=i,
                        status_message=f"Processed {i} of {len(df_insert)} new loans"
                    )
            
            # Reset buffer position to start
            csv_buffer.seek(0)
            
            # Get raw connection from SQLAlchemy session
            connection = db.connection().connection
            
            # Use a cursor for the COPY command
            with connection.cursor() as cursor:
                # Start a COPY command
                cursor.copy_from(
                    csv_buffer,
                    "loans",
                    columns=[
                        "portfolio_id", "loan_no", "employee_id", "employee_name", "employer", 
                        "loan_issue_date", "deduction_start_period", "submission_period", "maturity_period",
                        "location_code", "dalex_paddy", "team_leader", "loan_type", "loan_amount", "loan_term",
                        "administrative_fees", "total_interest", "total_collectible", "net_loan_amount",
                        "monthly_installment", "principal_due", "interest_due", "total_due",
                        "principal_paid", "interest_paid", "total_paid", "principal_paid2", "interest_paid2",
                        "total_paid2", "paid", "cancelled", "outstanding_loan_balance", "accumulated_arrears",
                        "ndia", "prevailing_posted_repayment", "prevailing_due_payment", "current_missed_deduction",
                        "admin_charge", "recovery_rate", "deduction_status"
                    ],
                    sep="\t",
                    null=""
                )
            
            inserted_count = len(df_insert)
            
            # Update progress for inserts completion
            task_manager.update_progress(
                task_id, 
                progress=95,
                processed_items=inserted_count,
                status_message=f"Inserted {inserted_count} new loans, processing updates"
            )
            
            # Process updates if any
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
                                rows_updated += 1
                    
                    # Commit batch
                    db.commit()
                    
                    # Update progress
                    progress = min(98, 90 + ((i + len(batch)) / len(df_update)) * 8)
                    task_manager.update_progress(
                        task_id, 
                        progress=progress,
                        processed_items=inserted_count + i + len(batch),
                        status_message=f"Updated {i + len(batch)} of {len(df_update)} existing loans"
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
        else:
            task_manager.update_task(
                task_id, 
                status_message="Error: No loan_no column found in the data"
            )
            return {"error": "No loan_no column found in the data"}
            
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
        
        # Get existing client IDs from the database
        existing_clients_query = text("""
            SELECT employee_id, id FROM clients 
            WHERE portfolio_id = :portfolio_id AND employee_id IS NOT NULL
        """)
        result = db.execute(existing_clients_query, {"portfolio_id": portfolio_id})
        existing_client_ids = {emp_id: client_id for emp_id, client_id in result if emp_id}
        
        logger.info(f"Found {len(existing_client_ids)} existing client IDs in portfolio {portfolio_id}")
        
        # Update task status
        task_manager.update_task(task_id, status_message="Reading Excel file")
        
        # Read Excel file with Polars
        # First read with pandas to get number of rows
        xlsx = pd.ExcelFile(io.BytesIO(file_content))
        sheet_name = xlsx.sheet_names[0]
        total_rows = len(pd.read_excel(xlsx, sheet_name=sheet_name, usecols=[0])) - 1
        
        # Update task with total items
        task_manager.update_task(task_id, total_items=total_rows, status_message="Processing data")
        
        # Now read with Polars for better performance
        df = pl.read_excel(io.BytesIO(file_content), sheet_name=sheet_name)
        
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
        date_columns = ["date_of_birth", "employment_date"]
        for col in date_columns:
            if col in df.columns:
                # Try to parse dates with multiple common formats
                # First clean the data
                df = df.with_columns(
                    pl.col(col)
                    .cast(pl.Utf8)
                    .str.replace(r"^\s*-\s*$|^\s*$|None|NaN|nan|-", "")
                )
                
                # Create a clean column for date parsing
                clean_col = df[col]
                
                # Initialize a column with null values
                parsed_dates = pl.Series(name=col, values=[None] * len(df))
                
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
                            logger.info(f"Successfully parsed {col} using format {fmt}")
                            break
                    except Exception as e:
                        # This format didn't work, try the next one
                        continue
                
                # Use current date for any remaining null values
                current_date = datetime.now().date()
                df = df.with_columns(
                    parsed_dates.fill_null(current_date).alias(col)
                )

        # Clean numeric columns
        numeric_columns = ["age", "years_in_service", "monthly_salary"]
        for col in numeric_columns:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .cast(pl.Utf8)
                    .str.replace(r"^\s*-\s*$|^\s*$|None|NaN|nan|-", "")
                    .cast(pl.Float64, strict=False)
                    .fill_null(0.0)
                )
        
        # Add portfolio_id to all records
        df = df.with_columns(pl.lit(portfolio_id).alias("portfolio_id"))
        
        # Filter out rows with no employee_id
        if "employee_id" in df.columns:
            df = df.filter(pl.col("employee_id").is_not_null())
            
            # Process the dataframe in batches
            batch_size = 500
            total_batches = (len(df) + batch_size - 1) // batch_size
            
            # Create a CSV file for bulk insert
            csv_file = io.StringIO()
            
            # Don't write CSV header - PostgreSQL COPY doesn't expect headers
            csv_header = ["portfolio_id", "employee_id", "last_name", "other_names", "gender", 
                         "date_of_birth", "marital_status", "employer", "employment_date", 
                         "phone_number", "residential_address", "postal_address", 
                         "search_name"]
            
            # Use tab as separator to avoid issues with commas in the data
            csv_writer = csv.writer(csv_file, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            # Don't write the header row to avoid "invalid input syntax" errors
            
            # Process each batch
            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min((batch_idx + 1) * batch_size, len(df))
                batch_df = df.slice(start_idx, end_idx - start_idx)
                
                # Update progress - scale to 0-90% to leave room for final processing
                progress = (batch_idx / total_batches) * 90
                processed_items = min(end_idx, len(df))
                task_manager.update_progress(
                    task_id, 
                    progress=progress,
                    processed_items=processed_items,
                    status_message=f"Processing client records {start_idx+1}-{end_idx} of {len(df)}"
                )
                
                # Process each row in the batch
                for row_idx, row in enumerate(batch_df.iter_rows(named=True)):
                    # Skip rows that already exist
                    employee_id = row.get("employee_id")
                    if employee_id and employee_id in existing_client_ids:
                        # Update existing client
                        client_id = existing_client_ids[employee_id]
                        
                        # Build update values
                        update_values = {}
                        
                        # Add fields
                        for col, val in row.items():
                            if col != "employee_id" and val is not None:
                                # Map to correct database column names
                                if col == "employment_date" and val:
                                    # Ensure date format
                                    update_values[col] = val
                                elif col == "date_of_birth" and val:
                                    # Ensure date format
                                    update_values[col] = val
                                elif col in ["last_name", "other_names", "gender", "marital_status", 
                                           "employer", "phone_number", "residential_address", 
                                           "postal_address"]:
                                    update_values[col] = str(val) if val else None
                        
                        # Update search_name
                        if "last_name" in update_values or "other_names" in update_values:
                            last_name = update_values.get("last_name", "")
                            other_names = update_values.get("other_names", "")
                            update_values["search_name"] = f"{last_name} {other_names}".strip().lower()
                        
                        # Update the client if we have values to update
                        if update_values:
                            db.query(Client).filter(Client.id == client_id).update(update_values)
                    else:
                        # Create new client record for CSV
                        values = [portfolio_id]
                        
                        # Add employee_id
                        values.append(str(employee_id) if employee_id else "")
                        
                        # Add last_name and other_names directly
                        values.append(str(row.get("last_name", "")) if row.get("last_name") else "")
                        values.append(str(row.get("other_names", "")) if row.get("other_names") else "")
                        
                        # Add other fields in the order of csv_header
                        for field in csv_header[4:]:  # Skip portfolio_id, employee_id, last_name, other_names
                            if field == "search_name":
                                # Combine last_name and other_names for search_name
                                search_name = f"{values[2]} {values[3]}".strip().lower()
                                values.append(search_name)
                            elif field == "date_of_birth" and "date_of_birth" in row and row["date_of_birth"]:
                                values.append(str(row["date_of_birth"]))
                            elif field == "employment_date" and "employment_date" in row and row["employment_date"]:
                                values.append(str(row["employment_date"]))
                            elif field in row and row[field] is not None:
                                # Clean the value to avoid issues with tabs or newlines
                                val = str(row[field])
                                val = val.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                                values.append(val)
                            else:
                                values.append("")
                        
                        # Write to CSV
                        csv_writer.writerow(values)
            
            # Reset buffer position to start
            csv_file.seek(0)
            
            # Only proceed if we have data to insert
            if len(df) > 0:
                # Update progress before database operation
                task_manager.update_progress(
                    task_id, 
                    progress=95,
                    processed_items=len(df),
                    status_message=f"Inserting {len(df)} new client records into database"
                )
                
                # Get raw connection from SQLAlchemy session
                connection = db.connection().connection
                
                # Use a cursor for the COPY command
                with connection.cursor() as cursor:
                    # Start a COPY command
                    cursor.copy_from(
                        csv_file,
                        "clients",
                        columns=[
                            "portfolio_id", "employee_id", "last_name", "other_names", "gender", 
                            "date_of_birth", "marital_status", "employer", "employment_date", 
                            "phone_number", "residential_address", "postal_address", 
                            "search_name"
                        ],
                        sep='\t',  # Use tab as separator
                        null=""
                    )
                
                # Update progress after database operation
                task_manager.update_progress(
                    task_id, 
                    progress=98,
                    processed_items=len(df),
                    status_message=f"Database commit completed, finalizing..."
                )
                
                # Final commit and progress update
                db.commit()
            
            # Set final progress
            task_manager.update_progress(
                task_id, 
                progress=100,
                processed_items=total_rows,
                status_message=f"Completed: {len(df)} clients processed"
            )
            
            return {
                "processed": len(df),
                "total_processed": total_rows
            }
        else:
            task_manager.update_task(
                task_id, 
                status_message="Error: No employee_id column found in the data"
            )
            return {"error": "No employee_id column found in the data"}
            
    except Exception as e:
        logger.exception(f"Error processing client data: {str(e)}")
        task_manager.update_task(task_id, status_message=f"Error: {str(e)}")
        raise
