import io
import pandas as pd
import polars as pl
import numpy as np
from datetime import datetime  # Import datetime for type checking
import concurrent.futures
from sqlalchemy import text
from app.models import (
    Loan,
    Guarantee,
    Client,
    Security,
    Portfolio
)

async def process_loan_details(loan_details, portfolio_id, db):
    """Function to process loan details with high-performance optimizations for large datasets using Polars."""
    try:
        content = await loan_details.read()
        
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
        
        # Get existing loan numbers from the database using raw SQL for performance
        existing_loans_query = text("""
            SELECT loan_no, id FROM loans 
            WHERE portfolio_id = :portfolio_id AND loan_no IS NOT NULL
        """)
        result = db.execute(existing_loans_query, {"portfolio_id": portfolio_id})
        existing_loan_nos = {loan_no: loan_id for loan_no, loan_id in result if loan_no}
        
        print(f"Found {len(existing_loan_nos)} existing loan numbers in portfolio {portfolio_id}")
        
        # Track overall processing stats
        rows_processed = 0
        rows_skipped = 0
        rows_updated = 0
        inserted_count = 0

        # POLARS OPTIMIZATION: Read Excel file in one go with Polars
        # First read with pandas to get number of rows
        xlsx = pd.ExcelFile(io.BytesIO(content))
        sheet_name = xlsx.sheet_names[0]
        total_rows = len(pd.read_excel(xlsx, sheet_name=sheet_name, usecols=[0])) - 1
        
        # Now read with Polars for better performance
        df = pl.read_excel(io.BytesIO(content), sheet_name=sheet_name)
        
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
        for col in date_columns:
            if col in df.columns:
                # Convert to datetime and handle null values
                # First ensure the column is treated as string before conversion
                df = df.with_columns(
                    pl.col(col).cast(pl.Utf8).str.to_datetime("%Y-%m-%d", strict=False)
                )

        # Clean numeric columns - convert string values like " -   " to None or 0
        numeric_columns = [
            "loan_amount", "loan_term", "administrative_fees", "total_interest",
            "total_collectible", "net_loan_amount", "monthly_installment",
            "principal_due", "interest_due", "total_due", "principal_paid",
            "interest_paid", "total_paid", "principal_paid2", "interest_paid2",
            "total_paid2", "outstanding_loan_balance", "accumulated_arrears",
            "ndia", "prevailing_posted_repayment", "prevailing_due_payment",
            "current_missed_deduction", "admin_charge", "recovery_rate"
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
            
            # ULTRA-OPTIMIZED: Use PostgreSQL COPY command for bulk inserts
            # This is much faster than individual INSERT statements
            
            # Create a CSV-like string buffer
            csv_buffer = io.StringIO()
            
            # Write data to the buffer in CSV format
            for _, row in df_insert.iterrows():
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
            
            # Reset buffer position to start
            csv_buffer.seek(0)
            
            # Get raw connection from SQLAlchemy session
            connection = db.connection().connection
            
            # Create a cursor
            cursor = connection.cursor()
            
            # Execute COPY command
            cursor.copy_from(
                csv_buffer,
                'loans',
                columns=["portfolio_id", "loan_no", "employee_id", "employee_name", "employer", 
                        "loan_issue_date", "deduction_start_period", "submission_period", "maturity_period",
                        "location_code", "dalex_paddy", "team_leader", "loan_type", "loan_amount", "loan_term",
                        "administrative_fees", "total_interest", "total_collectible", "net_loan_amount",
                        "monthly_installment", "principal_due", "interest_due", "total_due",
                        "principal_paid", "interest_paid", "total_paid", "principal_paid2", "interest_paid2",
                        "total_paid2", "paid", "cancelled", "outstanding_loan_balance", "accumulated_arrears",
                        "ndia", "prevailing_posted_repayment", "prevailing_due_payment", "current_missed_deduction",
                        "admin_charge", "recovery_rate", "deduction_status"]
            )
            
            inserted_count = len(df_insert)
        
            # Prepare values for SQL UPDATE
            values_list = []
            for _, row in df_update.iterrows():
                values = []
                for col in ["id", "employee_id", "employee_name", "employer", 
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
                            values.append(f"'{row[col]}'")
                        elif col in ["paid", "cancelled"]:
                            values.append("TRUE" if row[col] else "FALSE")
                        elif col in numeric_columns and pd.notna(row[col]):
                            values.append(str(row[col]))
                        elif pd.isna(row[col]):
                            values.append("NULL")
                        else:
                            val = str(row[col])
                            val = val.replace("'", "''")  # SQL standard for escaping single quotes
                            values.append(f"'{val}'")
                    else:
                        values.append("NULL")
                values_list.append(f"({', '.join(values)})")
            
            # Execute in batches of 5000 to avoid transaction issues
            for i in range(0, len(values_list), 5000):
                batch_values = values_list[i:i+5000]
                update_sql = """
                    UPDATE loans SET
                        employee_id = data.employee_id,
                        employee_name = data.employee_name,
                        employer = data.employer,
                        loan_issue_date = data.loan_issue_date,
                        deduction_start_period = data.deduction_start_period,
                        submission_period = data.submission_period,
                        maturity_period = data.maturity_period,
                        location_code = data.location_code,
                        dalex_paddy = data.dalex_paddy,
                        team_leader = data.team_leader,
                        loan_type = data.loan_type,
                        loan_amount = data.loan_amount,
                        loan_term = data.loan_term,
                        administrative_fees = data.administrative_fees,
                        total_interest = data.total_interest,
                        total_collectible = data.total_collectible,
                        net_loan_amount = data.net_loan_amount,
                        monthly_installment = data.monthly_installment,
                        principal_due = data.principal_due,
                        interest_due = data.interest_due,
                        total_due = data.total_due,
                        principal_paid = data.principal_paid,
                        interest_paid = data.interest_paid,
                        total_paid = data.total_paid,
                        principal_paid2 = data.principal_paid2,
                        interest_paid2 = data.interest_paid2,
                        total_paid2 = data.total_paid2,
                        paid = data.paid,
                        cancelled = data.cancelled,
                        outstanding_loan_balance = data.outstanding_loan_balance,
                        accumulated_arrears = data.accumulated_arrears,
                        ndia = data.ndia,
                        prevailing_posted_repayment = data.prevailing_posted_repayment,
                        prevailing_due_payment = data.prevailing_due_payment,
                        current_missed_deduction = data.current_missed_deduction,
                        admin_charge = data.admin_charge,
                        recovery_rate = data.recovery_rate,
                        deduction_status = data.deduction_status
                    FROM (VALUES
                """ + ",\n".join(batch_values) + ") AS data(id, employee_id, employee_name, employer, loan_issue_date, deduction_start_period, submission_period, maturity_period, location_code, dalex_paddy, team_leader, loan_type, loan_amount, loan_term, administrative_fees, total_interest, total_collectible, net_loan_amount, monthly_installment, principal_due, interest_due, total_due, principal_paid, interest_paid, total_paid, principal_paid2, interest_paid2, total_paid2, paid, cancelled, outstanding_loan_balance, accumulated_arrears, ndia, prevailing_posted_repayment, prevailing_due_payment, current_missed_deduction, admin_charge, recovery_rate, deduction_status) WHERE loans.id = data.id"
                db.execute(text(update_sql))
                db.flush()
        
        # Commit all changes at once
        db.commit()
        
        return {
            "status": "success",
            "rows_processed": rows_processed,
            "rows_inserted": inserted_count,
            "rows_updated": rows_updated,
            "rows_skipped": rows_skipped,
            "filename": loan_details.filename,
        }
    except Exception as e:
        db.rollback()
        return {
            "status": "error",
            "message": str(e),
            "filename": loan_details.filename,
        }

async def process_client_data(client_data, portfolio_id, db):
    """Process client data file using high-performance optimizations with Polars."""
    try:
        content = await client_data.read()
        
        # Get the portfolio's customer_type
        portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
        portfolio_customer_type = portfolio.customer_type if portfolio and portfolio.customer_type else "individuals"
        
        # Create a case-insensitive column mapping
        target_columns = {
            "employee id": "employee_id",
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
            "next of kin contact:": "next_of_kin_contact",  
            "next of kin address": "next_of_kin_address",
            "search name": "search_name",
            "client type": "client_type",
        }
        
        # Initialize tracking variables
        rows_processed = 0
        rows_inserted = 0
        
        # Get existing clients from the database using raw SQL for performance
        existing_clients_query = text("""
            SELECT employee_id FROM clients 
            WHERE portfolio_id = :portfolio_id AND employee_id IS NOT NULL
        """)
        result = db.execute(existing_clients_query, {"portfolio_id": portfolio_id})
        existing_clients_set = {emp_id for emp_id, in result if emp_id}
        
        # Read Excel file with Polars
        df = pl.read_excel(io.BytesIO(content))
        
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
                df = df.with_columns(
                    pl.col(col).cast(pl.Utf8).str.to_datetime("%Y-%m-%d", strict=False)
                )
        
        # Convert employee_id to string
        if "employee_id" in df.columns:
            df = df.with_columns(pl.col("employee_id").cast(pl.Utf8))
        
        # Add portfolio_id to all records
        df = df.with_columns(pl.lit(portfolio_id).alias("portfolio_id"))
        
        # Set client_type based on portfolio's customer_type
        df = df.with_columns(pl.lit(portfolio_customer_type).alias("client_type"))
        
        # Remove rows with missing employee_id
        df = df.filter(pl.col("employee_id").is_not_null())
        
        # Convert to pandas for filtering with existing_clients_set
        pdf = df.to_pandas()
        
        # Filter new clients - only keep employees not in existing_clients_set
        mask_new = ~pdf["employee_id"].isin(existing_clients_set)
        new_clients = pdf[mask_new].copy()
        
        rows_processed = len(pdf)
        rows_skipped = len(pdf) - len(new_clients)
        
        # Prepare new clients for insertion
        if not new_clients.empty:
            # ULTRA-OPTIMIZED: Use PostgreSQL COPY command for bulk inserts
            # This is much faster than individual INSERT statements
            
            # Create a CSV-like string buffer
            csv_buffer = io.StringIO()
            
            # Write data to the buffer in CSV format
            for _, row in new_clients.iterrows():
                values = []
                for col in ["portfolio_id", "employee_id", "last_name", "other_names", "residential_address", 
                          "postal_address", "phone_number", "title", "marital_status", "gender", 
                          "date_of_birth", "employer", "previous_employee_no", "social_security_no", 
                          "voters_id_no", "employment_date", "next_of_kin", "next_of_kin_contact", 
                          "next_of_kin_address", "search_name", "client_type"]:
                    if col in row:
                        if col in date_columns and pd.notna(row[col]):
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
            
            # Reset buffer position to start
            csv_buffer.seek(0)
            
            # Get raw connection from SQLAlchemy session
            connection = db.connection().connection
            
            # Create a cursor
            cursor = connection.cursor()
            
            # Execute COPY command
            cursor.copy_from(
                csv_buffer,
                'clients',
                columns=["portfolio_id", "employee_id", "last_name", "other_names", "residential_address", 
                        "postal_address", "phone_number", "title", "marital_status", "gender", 
                        "date_of_birth", "employer", "previous_employee_no", "social_security_no", 
                        "voters_id_no", "employment_date", "next_of_kin", "next_of_kin_contact", 
                        "next_of_kin_address", "search_name", "client_type"]
            )
            
            # Update counter and existing_clients_set for future reference
            rows_inserted = len(new_clients)
            existing_clients_set.update(new_clients["employee_id"].to_list())
        
        # Commit all changes
        db.commit()
        
        return {
            "status": "success",
            "rows_processed": rows_processed,
            "rows_inserted": rows_inserted,
            "rows_skipped": rows_skipped,
            "filename": client_data.filename,
        }
    except Exception as e:
        db.rollback()
        return {
            "status": "error",
            "message": str(e),
            "filename": client_data.filename,
        }


async def process_loan_guarantees(loan_guarantee_data, portfolio_id, db):
    """Process loan guarantees file using Polars for high-performance data processing."""
    try:
        content = loan_guarantee_data.read()
        
        # Target column names (lowercase for matching)
        target_columns = {
            "loan no.": "loan_no",
            "guarantor name": "guarantor_name",
            "guarantor phone": "guarantor_phone",
            "guarantor address": "guarantor_address",
            "guarantor id": "guarantor_id",
            "relationship": "relationship",
            "guarantee amount": "guarantee_amount",
        }
        
        # Get existing guarantees from the database using raw SQL for performance
        existing_guarantees_query = text("""
            SELECT loan_no, guarantor_name, id FROM guarantees 
            WHERE loan_no IS NOT NULL AND guarantor_name IS NOT NULL
        """)
        result = db.execute(existing_guarantees_query)
        existing_guarantees = {(loan_no, guarantor_name): guarantor_id for loan_no, guarantor_name, guarantor_id in result}
        
        # Read Excel file with Polars
        df = pl.read_excel(io.BytesIO(content))
        
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
        
        # Clean numeric columns
        if "guarantee_amount" in df.columns:
            df = df.with_columns(
                pl.col("guarantee_amount")
                .cast(pl.Utf8)
                .str.replace(r"^\s*-\s*$|^\s*$|None|NaN|nan|-", "")
                .cast(pl.Float64, strict=False)
                .fill_null(0.0)
            )
        
        # Filter out rows with no loan_no or guarantor_name
        if "loan_no" in df.columns and "guarantor_name" in df.columns:
            df = df.filter(
                pl.col("loan_no").is_not_null() & 
                pl.col("guarantor_name").is_not_null()
            )
            
            # Convert to pandas for easier processing with existing_guarantees
            pdf = df.to_pandas()
            
            # Create a composite key for matching
            pdf["composite_key"] = pdf.apply(lambda row: (row["loan_no"], row["guarantor_name"]), axis=1)
            
            # Split into updates and inserts
            mask_update = pdf["composite_key"].isin(existing_guarantees.keys())
            df_update = pdf[mask_update].copy()
            df_insert = pdf[~mask_update].copy()
            
            # Add id column to updates
            if not df_update.empty:
                df_update["id"] = df_update["composite_key"].map(existing_guarantees)
            
            # ULTRA-OPTIMIZED: Use PostgreSQL COPY command for bulk inserts
            # This is much faster than individual INSERT statements
            
            # Create a CSV-like string buffer
            csv_buffer = io.StringIO()
            
            # Write data to the buffer in CSV format
            for _, row in df_insert.iterrows():
                values = []
                for col in ["loan_no", "guarantor_name", "guarantor_phone", "guarantor_address", 
                           "guarantor_id", "relationship", "guarantee_amount"]:
                    if col in row:
                        if pd.isna(row[col]):
                            values.append("")  # NULL in COPY format
                        elif col == "guarantee_amount" and pd.notna(row[col]):
                            values.append(str(row[col]))
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
            
            # Create a cursor
            cursor = connection.cursor()
            
            # Execute COPY command
            cursor.copy_from(
                csv_buffer,
                'guarantees',
                columns=["loan_no", "guarantor_name", "guarantor_phone", "guarantor_address", 
                        "guarantor_id", "relationship", "guarantee_amount"]
            )
            
            # Prepare SQL templates
            update_template = """
                UPDATE guarantees SET
                    guarantor_phone = data.guarantor_phone,
                    guarantor_address = data.guarantor_address,
                    guarantor_id = data.guarantor_id,
                    relationship = data.relationship,
                    guarantee_amount = data.guarantee_amount
                FROM (VALUES
            """
            
            if not df_update.empty:
                # Prepare values for SQL UPDATE
                values_list = []
                for _, row in df_update.iterrows():
                    values = []
                    for col in ["id", "guarantor_phone", "guarantor_address",
                               "guarantor_id", "relationship", "guarantee_amount"]:
                        if col in row:
                            if col == "guarantee_amount" and pd.notna(row[col]):
                                values.append(str(row[col]))
                            elif pd.isna(row[col]):
                                values.append("NULL")
                            else:
                                val = str(row[col])
                                val = val.replace("'", "''")  # SQL standard for escaping single quotes
                                values.append(f"'{val}'")
                        else:
                            values.append("NULL")
                    values_list.append(f"({', '.join(values)})")
                
                # Execute in batches of 5000 to avoid transaction issues
                for i in range(0, len(values_list), 5000):
                    batch_values = values_list[i:i+5000]
                    update_sql = update_template + ",\n".join(batch_values) + ") AS data(id, guarantor_phone, guarantor_address, guarantor_id, relationship, guarantee_amount) WHERE guarantees.id = data.id"
                    db.execute(text(update_sql))
                    db.flush()
        
        # Commit all changes
        db.commit()
        
        return {
            "status": "success",
            "rows_processed": len(df) if isinstance(df, pl.DataFrame) else 0,
            "filename": getattr(loan_guarantee_data, "filename", "in-memory-file"),
        }
    except Exception as e:
        db.rollback()
        return {
            "status": "error",
            "message": str(e),
            "filename": getattr(loan_guarantee_data, "filename", "in-memory-file"),
        }

async def process_collateral_data(collateral_data, portfolio_id, db):
    """Process loan collateral (securities) data using optimized bulk operations with Polars."""
    try:
        content = collateral_data.read()
        
        # Target column names (lowercase for matching)
        target_columns = {
            "loan no.": "loan_no",
            "security type": "security_type",
            "security description": "security_description",
            "security value": "security_value",
            "valuation date": "valuation_date",
            "location": "location",
            "registration details": "registration_details",
            "ownership": "ownership",
        }
        
        # Get existing securities from the database using raw SQL for performance
        existing_securities_query = text("""
            SELECT * FROM securities 
        """)
        result = db.execute(existing_securities_query)
        existing_securities = {(loan_no, security_type, security_description): security_id for loan_no, security_type, security_description, security_id in result}
        
        # Read Excel file with Polars
        df = pl.read_excel(io.BytesIO(content))
        
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
        if "valuation_date" in df.columns:
            df = df.with_columns(
                pl.col("valuation_date").cast(pl.Utf8).str.to_datetime("%Y-%m-%d", strict=False)
            )
        
        # Clean numeric columns
        if "security_value" in df.columns:
            df = df.with_columns(
                pl.col("security_value")
                .cast(pl.Utf8)
                .str.replace(r"^\s*-\s*$|^\s*$|None|NaN|nan|-", "")
                .cast(pl.Float64, strict=False)
                .fill_null(0.0)
            )
        
        # Filter out rows with no loan_no, security_type, or security_description
        if all(col in df.columns for col in ["loan_no", "security_type", "security_description"]):
            df = df.filter(
                pl.col("loan_no").is_not_null() & 
                pl.col("security_type").is_not_null() &
                pl.col("security_description").is_not_null()
            )
            
            # Convert to pandas for easier processing with existing_securities
            pdf = df.to_pandas()
            
            # Create a composite key for matching
            pdf["composite_key"] = pdf.apply(
                lambda row: (row["loan_no"], row["security_type"], row["security_description"]), 
                axis=1
            )
            
            # Split into updates and inserts
            mask_update = pdf["composite_key"].isin(existing_securities.keys())
            df_update = pdf[mask_update].copy()
            df_insert = pdf[~mask_update].copy()
            
            # Add id column to updates
            if not df_update.empty:
                df_update["id"] = df_update["composite_key"].map(existing_securities)
            
            # ULTRA-OPTIMIZED: Use PostgreSQL COPY command for bulk inserts
            # This is much faster than individual INSERT statements
            
            # Create a CSV-like string buffer
            csv_buffer = io.StringIO()
            
            # Write data to the buffer in CSV format
            for _, row in df_insert.iterrows():
                values = []
                for col in ["loan_no", "security_type", "security_description", "security_value", 
                           "valuation_date", "location", "registration_details", "ownership"]:
                    if col in row:
                        if col == "valuation_date" and pd.notna(row[col]):
                            values.append(str(row[col]))
                        elif col == "security_value" and pd.notna(row[col]):
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
            values.append(str(portfolio_id)) 
            # Reset buffer position to start
            csv_buffer.seek(0)
            
            # Get raw connection from SQLAlchemy session
            connection = db.connection().connection
            
            # Create a cursor
            cursor = connection.cursor()
            
            # Execute COPY command
            cursor.copy_from(
                csv_buffer,
                'securities',
                columns=["loan_no", "security_type", "security_description", "security_value", 
             "valuation_date", "location", "registration_details", "ownership", "portfolio_id"]
            )
            
            # Prepare SQL templates
            update_template = """
                UPDATE securities SET
                    security_value = data.security_value,
                    valuation_date = data.valuation_date,
                    location = data.location,
                    registration_details = data.registration_details,
                    ownership = data.ownership
                FROM (VALUES
            """
            
            if not df_update.empty:
                # Prepare values for SQL UPDATE
                values_list = []
                for _, row in df_update.iterrows():
                    values = []
                    for col in ["id", "security_value", "valuation_date", 
                               "location", "registration_details", "ownership"]:
                        if col in row:
                            if col == "valuation_date" and pd.notna(row[col]):
                                values.append(f"'{row[col]}'")
                            elif col == "security_value" and pd.notna(row[col]):
                                values.append(str(row[col]))
                            elif pd.isna(row[col]):
                                values.append("NULL")
                            else:
                                val = str(row[col])
                                val = val.replace("'", "''")  # SQL standard for escaping single quotes
                                values.append(f"'{val}'")
                        else:
                            values.append("NULL")
                    values_list.append(f"({', '.join(values)})")
                
                # Execute in batches of 5000 to avoid transaction issues
                for i in range(0, len(values_list), 5000):
                    batch_values = values_list[i:i+5000]
                    update_sql = update_template + ",\n".join(batch_values) + ") AS data(id, security_value, valuation_date, location, registration_details, ownership) WHERE securities.id = data.id"
                    db.execute(text(update_sql))
                    db.flush()
        
        # Commit all changes
        db.commit()
        
        return {
            "status": "success",
            "rows_processed": len(df) if isinstance(df, pl.DataFrame) else 0,
            "filename": getattr(collateral_data, "filename", "in-memory-file"),
        }
    except Exception as e:
        db.rollback()
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "message": str(e),
            "filename": getattr(collateral_data, "filename", "in-memory-file"),
        }
