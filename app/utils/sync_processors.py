import io
import polars as pl
import logging
from typing import Dict, Any
from sqlalchemy import text

from app.models import (
    Loan,
    Guarantee,
    Client,
    Security,
    Portfolio,
    QualityIssue
)
from app.utils.quality_checks import create_and_save_quality_issues

logger = logging.getLogger(__name__)

def process_loan_details_sync(file_content, portfolio_id, db):
    """Synchronous function to process loan details with high-performance optimizations for large datasets using Polars."""
    try:
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
            "employer address": "employer_address",
            "employer city": "employer_city",
            "employer region": "employer_region",
            "employer country": "employer_country",
            "ndia": "ndia"
        }
        
        # Read file content as Excel file
        try:
            # Read Excel file
            if isinstance(file_content, io.BytesIO):
                content = file_content.read()
            else:
                content = file_content
                
            df = pl.read_excel(content)
            logger.info("Successfully read Excel file")
        except Exception as excel_error:
            logger.error(f"Failed to read Excel file: {str(excel_error)}")
            raise ValueError(f"Unable to read Excel file: {str(excel_error)}")
        
        # Convert column names to lowercase for case-insensitive matching
        df.columns = [col.lower() for col in df.columns]
        
        # Map columns to target names
        rename_dict = {}
        for source, target in target_columns.items():
            if source in df.columns:
                rename_dict[source] = target
        
        # Rename columns
        if rename_dict:
            df = df.rename(rename_dict)
        
        # Check if required columns are present
        required_columns = ["loan_no", "employee_id", "loan_amount", "outstanding_loan_balance"]
        column_display_names = {
            "loan_no": "Loan No.",
            "employee_id": "Employee Id",
            "loan_amount": "Loan Amount",
            "outstanding_loan_balance": "Outstanding Loan Balance"
        }
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            # Convert to user-friendly column names
            readable_missing_columns = [column_display_names.get(col, col) for col in missing_columns]
            logger.error(f"Missing required columns: {readable_missing_columns}")
            return {"error": f"Missing required columns: {readable_missing_columns}"}
        
        # Convert date columns to proper format
        date_columns = ["loan_issue_date", "deduction_start_period", "submission_period", "maturity_period", "date_of_birth"]
        for col in date_columns:
            if col in df.columns:
                try:
                    df = df.with_columns(pl.col(col).cast(pl.Date, strict=False))
                except Exception as e:
                    logger.warning(f"Failed to convert {col} to date: {str(e)}")
        
        # Convert numeric columns
        numeric_columns = [
            "loan_amount", "loan_term", "administrative_fees", "total_interest", 
            "total_collectible", "net_loan_amount", "monthly_installment", 
            "principal_due", "interest_due", "total_due", "principal_paid", 
            "interest_paid", "total_paid", "principal_paid2", "interest_paid2", 
            "total_paid2", "outstanding_principal", "outstanding_interest", 
            "outstanding_loan_balance", "days_in_arrears", "ndia"
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                try:
                    df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
                except Exception as e:
                    logger.warning(f"Failed to convert {col} to numeric: {str(e)}")
        
        # Clear existing loans for this portfolio
        try:
            db.execute(text(f"DELETE FROM loans WHERE portfolio_id = {portfolio_id}"))
            db.commit()
            logger.info(f"Cleared existing loans for portfolio {portfolio_id}")
        except Exception as e:
            logger.error(f"Error clearing existing loans: {str(e)}")
            return {"error": str(e)}
        
        # Convert to records for bulk insert
        records = df.to_dicts()
        
        # Prepare loan objects
        loans = []
        for record in records:
            # Add portfolio_id to each record
            record["portfolio_id"] = portfolio_id
            
            # Create Loan object
            loan = Loan(**{k: v for k, v in record.items() if k in Loan.__table__.columns.keys()})
            loans.append(loan)
        
        # Bulk insert loans
        db.bulk_save_objects(loans)
        db.commit()
        
        return {
            "processed": len(loans),
            "success": True,
            "message": f"Successfully processed {len(loans)} loan records"
        }
        
    except Exception as e:
        logger.error(f"Error processing loan details: {str(e)}")
        return {"error": str(e)}

def process_client_data_sync(file_content, portfolio_id, db):
    """Synchronous function to process client data with high-performance optimizations for large datasets using Polars."""
    try:
        # Target column names (lowercase for matching)
        target_columns = {
            "employee id": "employee_id",
            "last name": "last_name",
            "other names": "other_names",
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
            logger.info("Successfully read Excel file with polars")
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
        
        # Convert date columns
        date_columns = ["date_of_birth", "employment_date"]
        for col in date_columns:
            if col in df.columns:
                try:
                    # Use the correct method for polars DataFrame (with_columns, not with_column)
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
            logger.error(f"Error clearing existing clients: {str(e)}")
            return {"error": str(e)}
        
        # Convert to records for bulk insert
        records = df.to_dicts()
        
        # Process each client
        processed_count = 0
        errors = []
        
        for client_data in records:
            try:
                # Create a new client record
                client = Client(
                    portfolio_id=portfolio_id,
                    employee_id=client_data.get("employee_id"),
                    last_name=client_data.get("last_name"),
                    other_names=client_data.get("other_names"),
                    residential_address=client_data.get("residential_address"),
                    postal_address=client_data.get("postal_address"),
                    phone_number=client_data.get("phone_number"),
                    title=client_data.get("title"),
                    marital_status=client_data.get("marital_status"),
                    gender=client_data.get("gender"),
                    date_of_birth=client_data.get("date_of_birth"),
                    employer=client_data.get("employer"),
                    previous_employee_no=client_data.get("previous_employee_no"),
                    social_security_no=client_data.get("social_security_no"),
                    voters_id_no=client_data.get("voters_id_no"),
                    employment_date=client_data.get("employment_date"),
                    next_of_kin=client_data.get("next_of_kin"),
                    next_of_kin_contact=client_data.get("next_of_kin_contact"),
                    next_of_kin_address=client_data.get("next_of_kin_address"),
                    search_name=client_data.get("search_name"),
                    client_type=client_data.get("client_type", "individual")
                )
                
                # Add the client to the database
                db.add(client)
                processed_count += 1
                
                # Commit in batches to improve performance
                if processed_count % 100 == 0:
                    db.commit()
                    
            except Exception as e:
                logger.error(f"Error processing client: {str(e)}")
                errors.append(str(e))
        
        # Final commit for any remaining records
        db.commit()
        
        return {
            "processed": processed_count,
            "errors": errors,
            "success": len(errors) == 0
        }
        
    except Exception as e:
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
