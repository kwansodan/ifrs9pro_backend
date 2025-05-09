import io
import json
import logging
import decimal
from datetime import datetime
from sqlalchemy import text
import polars as pl

from app.models import (
    Loan,
    Guarantee,
    Client,
    Security,
    Portfolio,
    QualityIssue,
    DeductionStatus
)
from app.utils.quality_checks import create_and_save_quality_issues

logger = logging.getLogger(__name__)

async def process_loan_details_sync(file_content, portfolio_id, db):
    import io, decimal, polars as pl
    from sqlalchemy import text
    from app.models import Loan, Client
    import logging

    logger = logging.getLogger(__name__)

    try:
        # Read and preprocess Excel data (assumed done earlier)
        df = pl.read_excel(file_content)

        # Add portfolio_id to all records
        df = df.with_columns(pl.lit(portfolio_id).alias("portfolio_id"))

        # Clear existing loans for this portfolio
        try:
            db.execute(text(f"DELETE FROM loans WHERE portfolio_id = {portfolio_id}"))
            db.commit()
            logger.info(f"Cleared existing loans for portfolio {portfolio_id}")
        except Exception as e:
            db.rollback()
            logger.error(f"Error clearing existing loans: {str(e)}")
            return {"error": str(e)}

        # Use COPY unless too large
        use_copy = df.height < 50000

        try:
            connection = db.connection().connection
            cursor = connection.cursor()
            cursor.execute("SET statement_timeout TO '300s';")

            if use_copy:
                csv_buffer = io.StringIO()
                loan_columns = [
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
                ]
                integer_columns = [c.name for c in Loan.__table__.columns if c.type.python_type == int]

                for row in df.rows(named=True):
                    values = []
                    for col in loan_columns:
                        val = row.get(col, None)
                        if val is None:
                            values.append("0" if col in integer_columns else "")
                        elif isinstance(val, (str, pl.Utf8)):
                            val_str = str(val).replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
                            values.append(val_str)
                        elif isinstance(val, bool):
                            values.append(str(val).lower())
                        elif col in integer_columns:
                            try:
                                values.append(str(int(float(val))))
                            except (ValueError, TypeError):
                                values.append("0")
                        else:
                            values.append(str(val))
                    csv_buffer.write("\t".join(values) + "\n")
                csv_buffer.seek(0)
                cursor.copy_from(csv_buffer, "loans", columns=loan_columns, sep="\t", null="")
                connection.commit()
                logger.info(f"Bulk inserted {df.height} loans using COPY command")
                return {"processed": df.height, "success": True, "message": f"Successfully processed {df.height} loan records"}

        except Exception as copy_error:
            try:
                connection.rollback()
            except Exception:
                pass
            logger.warning(f"COPY failed: {str(copy_error)} — falling back to bulk_save_objects")

        # fallback to ORM
        try:
            batch_size = 10000
            offset = 0
            processed_total = 0

            while True:
                batch_df = df.slice(offset, batch_size)
                if batch_df.height == 0:
                    break

                records = batch_df.to_dicts()
                numeric_cols_in_df = [
                    "loan_amount", "loan_term", "administrative_fees", "total_interest",
                    "total_collectible", "net_loan_amount", "monthly_installment",
                    "principal_due", "interest_due", "total_due", "principal_paid",
                    "interest_paid", "total_paid", "principal_paid2", "interest_paid2",
                    "total_paid2", "outstanding_loan_balance", "accumulated_arrears",
                    "ndia", "prevailing_posted_repayment", "prevailing_due_payment",
                    "current_missed_deduction", "admin_charge", "recovery_rate"
                ]
                required_not_null = ["loan_no", "employee_id", "loan_amount"]
                loans = []
                for record in records:
                    try:
                        for col in required_not_null:
                            if col not in record or record[col] in [None, ""]:
                                if col in numeric_cols_in_df:
                                    record[col] = decimal.Decimal("0")
                                else:
                                    record[col] = "UNKNOWN"
                        for col in numeric_cols_in_df:
                            if col in record:
                                if record[col] is None:
                                    record[col] = decimal.Decimal("0")
                                else:
                                    record[col] = decimal.Decimal(str(record[col]))
                        loan = Loan(**{k: v for k, v in record.items() if k in Loan.__table__.columns.keys()})
                        loans.append(loan)
                    except Exception as e:
                        logger.warning(f"Loan skipped due to error: {str(e)}")

                db.bulk_save_objects(loans)
                db.commit()
                processed_total += len(loans)
                offset += batch_size

            logger.info(f"Inserted {processed_total} loans with bulk_save_objects")
            return {"processed": processed_total, "success": True, "message": f"Successfully processed {processed_total} loan records"}

        except Exception as final_error:
            db.rollback()
            logger.error(f"Final fallback insert failed: {str(final_error)}")
            return {"error": str(final_error)}

    except Exception as e:
        db.rollback()
        logger.error(f"Outer exception in processing: {str(e)}")
        return {"error": str(e)}



async def process_client_data_sync(file_content, portfolio_id, db):
    """Synchronous function to process client data with high-performance optimizations for large datasets using Polars."""
    try:
        # Target column names (lowercase for matching)
        target_columns = {
            "employee id": "employee_id",
            "last name": "last_name",
            "other names": "other_names",
            "lastname": "last_name",
            "othernames": "other_names",
            "residential address": "residential_address",
            "postal address": "postal_address",
            "phone number": "phone_number",
            "title": "title",
            "marital status": "marital_status",
            "gender": "gender",
            "date of birth": "date_of_birth",
            "employer": "employer",
            "previous employee no": "previous_employee_no",
            "social security no": "social_security_no",
            "voters id no": "voters_id_no",
            "employment date": "employment_date",
            "next of kin": "next_of_kin",
            "next of kin contact": "next_of_kin_contact",
            "next of kin address": "next_of_kin_address",
            "client type": "client_type"
        }
        
        # Read file content as Excel file
        try:
            # Read Excel file
            if isinstance(file_content, io.BytesIO):
                content = file_content
            else:
                content = io.BytesIO(file_content)
                
            # Read with polars
            df = pl.read_excel(content)
            logger.info(f"Successfully read Excel file with polars, found {df.height} rows")
        except Exception as excel_error:
            logger.error(f"Failed to read Excel file: {str(excel_error)}")
            raise ValueError(f"Unable to read Excel file: {str(excel_error)}")
        
        # Lowercase column names for consistent matching
        df.columns = [col.lower() for col in df.columns]
        
        # Rename columns based on target mapping
        rename_map = {}
        for orig_col in df.columns:
            if orig_col in target_columns:
                rename_map[orig_col] = target_columns[orig_col]
        
        if rename_map:
            df = df.rename(rename_map)
        
        # Convert date columns - process all at once for better performance
        date_columns = ["date_of_birth", "employment_date"]
        date_cols_in_df = [col for col in date_columns if col in df.columns]
        
        if date_cols_in_df:
            try:
                # First strip any leading/trailing quotes from date strings
                for col in date_cols_in_df:
                    if col in df.columns:
                        # Strip quotes from string values
                        df = df.with_columns(
                            pl.col(col).cast(pl.Utf8).str.replace_all("^['\"]|['\"]$", "").alias(col)
                        )
                
                # Try multiple date formats in sequence, prioritizing '%Y-%m-%d'
                date_formats = ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%d-%m-%Y", "%m-%d-%Y"]
                for date_format in date_formats:
                    try:
                        # Attempt parsing
                        new_cols = [pl.col(col).str.strptime(pl.Date, date_format, strict=False) for col in date_cols_in_df]
                        temp_df = df.with_columns(new_cols)
                        # Check if at least one value was parsed (not all null)
                        any_parsed = False
                        for col in date_cols_in_df:
                            if temp_df[col].null_count() < df.height:
                                any_parsed = True
                                break
                        if any_parsed:
                            df = temp_df
                            logger.info(f"Successfully parsed dates using format: {date_format}")
                            break  # Only break if parsing succeeded
                    except Exception as e:
                        logger.debug(f"Failed to parse dates with format {date_format}: {str(e)}")
                        continue
                
                # If still not parsed, try the default Polars date parsing as fallback
                for col in date_cols_in_df:
                    if df[col].dtype != pl.Date:
                        df = df.with_columns(pl.col(col).cast(pl.Date, strict=False))
                        
            except Exception as e:
                logger.warning(f"Failed to convert date columns: {str(e)}")
                # Fall back to individual column processing with multiple formats
                for col in date_cols_in_df:
                    try:
                        # First strip quotes
                        df = df.with_columns(
                            pl.col(col).cast(pl.Utf8).str.replace_all("^['\"]|['\"]$", "").alias(col)
                        )
                        
                        # Try each format
                        for date_format in date_formats:
                            try:
                                df = df.with_columns(
                                    pl.col(col).str.strptime(pl.Date, date_format, strict=False).alias(col)
                                )
                                break  # Stop if successful
                            except:
                                continue
                                
                        # If still not parsed, try the default as fallback
                        if df[col].dtype != pl.Date:
                            df = df.with_columns(pl.col(col).cast(pl.Date, strict=False))
                            
                    except Exception as e:
                        logger.warning(f"Failed to convert {col} to date: {str(e)}")
        
        # Create search name column
        if "last_name" in df.columns and "other_names" in df.columns:
            try:
                df = df.with_columns(
                    (pl.col("last_name").fill_null("") + " " + pl.col("other_names").fill_null("")).alias("search_name")
                )
            except Exception as e:
                logger.warning(f"Failed to create search_name column: {str(e)}")
                # Fallback for search name
                df = df.with_columns([
                    pl.lit("").alias("search_name")
                ])
        
        # Clear existing clients for this portfolio
        try:
            db.execute(text(f"DELETE FROM clients WHERE portfolio_id = {portfolio_id}"))
            db.commit()
            logger.info(f"Cleared existing clients for portfolio {portfolio_id}")
        except Exception as e:
            db.rollback()  # Explicitly rollback on error
            logger.error(f"Error clearing existing clients: {str(e)}")
            return {"error": str(e)}
        
        # Add portfolio_id to all records
        df = df.with_columns(pl.lit(portfolio_id).alias("portfolio_id"))
        
        # Set default client_type if not present
        if "client_type" not in df.columns:
            df = df.with_columns(pl.lit("individual").alias("client_type"))
        else:
            # Ensure client_type has a default value
            df = df.with_columns(pl.col("client_type").fill_null("individual"))
        
        # Use PostgreSQL's COPY command for bulk insert (much faster than ORM)
        try:
            # Get raw connection
            connection = db.connection().connection
            cursor = connection.cursor()
            
            # Create a CSV-like string buffer
            csv_buffer = io.StringIO()
            
            # Define the columns we want to insert
            client_columns = [
                "portfolio_id", "employee_id", "last_name", "other_names", 
                "residential_address", "postal_address", "phone_number", 
                "title", "marital_status", "gender", "date_of_birth", 
                "employer", "previous_employee_no", "social_security_no", 
                "voters_id_no", "employment_date", "next_of_kin", 
                "next_of_kin_contact", "next_of_kin_address", 
                "search_name", "client_type"
            ]
            
            # Get integer columns from the Client model
            integer_columns = [c.name for c in Client.__table__.columns if c.type.python_type == int]
            
            # Write data to buffer in CSV format
            for row in df.rows(named=True):
                values = []
                for col in client_columns:
                    val = row.get(col, None)
                    if val is None:
                        values.append("")  # NULL in COPY format
                    elif isinstance(val, (str, pl.Utf8)):
                        # Escape special characters for COPY
                        val_str = str(val).replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
                        values.append(val_str)
                    elif isinstance(val, bool):
                        values.append(str(val).lower())
                    elif col in integer_columns:
                        # Convert to integer for integer columns
                        try:
                            values.append(str(int(float(val))))
                        except (ValueError, TypeError):
                            values.append("0")  # Default to 0 for invalid values
                    else:
                        values.append(str(val))
                
                csv_buffer.write("\t".join(values) + "\n")
            
            # Reset buffer position to start
            csv_buffer.seek(0)
            
            # Execute COPY command
            cursor.copy_from(
                csv_buffer,
                "clients",
                columns=client_columns,
                sep="\t",
                null=""
            )
            
            # Commit the transaction
            connection.commit()  # Commit at the connection level
            
            processed_count = df.height
            logger.info(f"Bulk inserted {processed_count} clients using COPY command")
            
            return {
                "processed": processed_count,
                "errors": [],
                "success": True
            }
            
        except Exception as copy_error:
            # Explicitly rollback the connection on error
            try:
                connection.rollback()
            except Exception as rollback_error:
                logger.error(f"Error during connection rollback: {str(rollback_error)}")
                
            logger.error(f"Error during COPY bulk insert: {str(copy_error)}")
            logger.info("Falling back to bulk_save_objects method")
            
            # Fallback to bulk_save_objects if COPY fails
            try:
                # Convert to records for bulk insert
                records = df.to_dicts()
                
                # Create Client objects
                clients = []
                for record in records:
                    # Create a client object with only valid columns
                    try:
                        client = Client(**{k: v for k, v in record.items() 
                                        if k in Client.__table__.columns.keys()})
                        clients.append(client)
                    except Exception as record_error:
                        logger.warning(f"Error creating client object: {str(record_error)}, skipping record")
                
                # Bulk insert all clients at once
                db.bulk_save_objects(clients)
                db.commit()
                
                processed_count = len(clients)
                logger.info(f"Bulk inserted {processed_count} clients using bulk_save_objects")
                
                return {
                    "processed": processed_count,
                    "errors": [],
                    "success": True
                }
            except Exception as bulk_error:
                db.rollback()  # Explicitly rollback on error
                logger.error(f"Error during bulk_save_objects: {str(bulk_error)}")
                raise bulk_error
        
    except Exception as e:
        # Ensure transaction is rolled back
        try:
            db.rollback()
        except Exception:
            pass
            
        logger.error(f"Error processing client data: {str(e)}")
        return {"error": str(e)}


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
