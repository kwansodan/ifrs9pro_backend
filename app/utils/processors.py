import io
import pandas as pd
from app.models import (
    Loan,
    Guarantee,
    Client,
)


async def process_loan_details(loan_details, portfolio_id, db):
    """Highly optimized function to process loan details."""
    try:
        content = await loan_details.read()
        df = pd.read_excel(io.BytesIO(content), dtype=str)  # Read directly as strings

        # Column mapping
        column_mapping = {
            "Loan No.": "loan_no",
            "Employee Id": "employee_id",
            "Employee Name": "employee_name",
            "Employer": "employer",
            "Loan Issue Date": "loan_issue_date",
            "Deduction Start Period": "deduction_start_period",
            "Submission Period": "submission_period",
            "Maturity Period": "maturity_period",
            "Location Code": "location_code",
            "Dalex Paddy": "dalex_paddy",
            "Team Leader": "team_leader",
            "Loan Type": "loan_type",
            "Loan Amount": "loan_amount",
            "Loan Term": "loan_term",
            "Administrative Fees": "administrative_fees",
            "Total Interest": "total_interest",
            "Total Collectible": "total_collectible",
            "Net Loan Amount": "net_loan_amount",
            "Monthly Installment": "monthly_installment",
            "Principal Due": "principal_due",
            "Interest Due": "interest_due",
            "Total Due": "total_due",
            "Principal Paid": "principal_paid",
            "Interest Paid": "interest_paid",
            "Total Paid": "total_paid",
            "Principal Paid2": "principal_paid2",
            "Interest Paid2": "interest_paid2",
            "Total Paid2": "total_paid2",
            "Paid": "paid",
            "Cancelled": "cancelled",
            "Outstanding Loan Balance": "outstanding_loan_balance",
            "Accumulated Arrears": "accumulated_arrears",
            "NDIA": "ndia",
            "Prevailing Posted Repayment": "prevailing_posted_repayment",
            "Prevailing Due Payment": "prevailing_due_payment",
            "Current Missed Deduction": "current_missed_deduction",
            "Admin Charge": "admin_charge",
            "Recovery Rate": "recovery_rate",
            "Deduction Status": "deduction_status",
        }
        df.rename(columns=column_mapping, inplace=True)

        # Convert dates efficiently
        df["loan_issue_date"] = pd.to_datetime(df["loan_issue_date"], errors="coerce")

        # Handle date formats for period columns with mixed formats
        for col in ["deduction_start_period", "submission_period", "maturity_period"]:
            # First, standardize the format for parsing
            if col in df.columns:
                # Handle both formats: "Sep-22" and "SEP2022"
                # Function to convert various date formats to standard format
                def standardize_date_format(date_str):
                    if pd.isna(date_str):
                        return None
                    
                    date_str = str(date_str).strip().upper()
                    
                    # Format: "SEP2022" (no hyphen)
                    if len(date_str) == 7 and not "-" in date_str:
                        month = date_str[:3]
                        year = date_str[3:]
                        if len(year) == 4:  # Full year format (2022)
                            return f"{month}-{year[2:]}"  # Convert to "SEP-22"
                        return date_str
                    
                    return date_str
                
                # Apply standardization
                df[col] = df[col].apply(standardize_date_format)
                
                # Parse the standardized dates
                df[col] = pd.to_datetime(
                    df[col], format="%b-%y", errors="coerce"
                ) + pd.offsets.MonthEnd(0)
                
                # Replace NaT values with None for SQL compatibility
                df[col] = df[col].where(pd.notna(df[col]), None)

        # Convert boolean columns efficiently
        df["paid"] = df["paid"].isin(["Yes", "True"])
        df["cancelled"] = df["cancelled"].isin(["Yes", "True"])

        # Get existing loan numbers in bulk
        existing_loans = (
            db.query(Loan.loan_no).filter(Loan.portfolio_id == portfolio_id).all()
        )
        existing_loan_nos = {loan_no for (loan_no,) in existing_loans}

        # Convert DataFrame to list of dictionaries
        loan_records = df.astype(object).to_dict(orient="records")

        rows_processed, rows_skipped = 0, 0
        loans_to_add = []
        updates = []

        # Process loans in bulk
        for record in loan_records:
            try:
                loan_no = record.get("loan_no")

                # Replace NaT values with None for SQL compatibility
                for date_col in ["loan_issue_date", "deduction_start_period", "submission_period", "maturity_period"]:
                    if date_col in record and pd.isna(record[date_col]):
                        record[date_col] = None

                if loan_no in existing_loan_nos:
                    updates.append(record)
                else:
                    loans_to_add.append(Loan(**record, portfolio_id=portfolio_id))

                rows_processed += 1
            except Exception as e:
                rows_skipped += 1
                print(f"Error processing loan: {e}")

        # Bulk insert new loans
        if loans_to_add:
            db.bulk_save_objects(loans_to_add)

        return {
            "status": "success",
            "rows_processed": rows_processed,
            "rows_skipped": rows_skipped,
            "filename": loan_details.filename,
        }

    except Exception as e:
        return {"status": "error", "message": str(e), "filename": loan_details.filename}
    
async def process_loan_guarantees(loan_guarantee_data, portfolio_id, db):
    """Process loan guarantees file using a simplified approach."""
    try:
        content = await loan_guarantee_data.read()
        df = pd.read_excel(io.BytesIO(content), dtype=str)
        
        # Print column names for debugging
        print(f"Actual columns in file: {df.columns.tolist()}")
        
        # Create a lowercase version of column names for easy matching
        lowercase_columns = {col.lower(): col for col in df.columns}
        
        # Find 'guarantor' and 'amount' columns using simple pattern matching
        guarantor_col = None
        amount_col = None
        
        if 'guarantor name' in lowercase_columns or 'guarantor' in lowercase_columns:
            guarantor_col = lowercase_columns.get('guarantor name') or lowercase_columns.get('guarantor')
        
        if 'pledged amount' in lowercase_columns or 'amount' in lowercase_columns:
            amount_col = lowercase_columns.get('pledged amount') or lowercase_columns.get('amount')
        
        # If we found the columns, rename them to our standard names
        rename_dict = {}
        if guarantor_col:
            rename_dict[guarantor_col] = "guarantor"
        else:
            raise ValueError(f"Could not find guarantor column. Available columns: {df.columns.tolist()}")
            
        if amount_col:
            rename_dict[amount_col] = "pledged_amount"
        
        # Apply the renaming
        df.rename(columns=rename_dict, inplace=True)
        
        # Convert pledged_amount to numeric if the column exists
        if "pledged_amount" in df.columns:
            df["pledged_amount"] = pd.to_numeric(df["pledged_amount"], errors="coerce")
        
        # Fetch only necessary fields from DB
        existing_guarantors = (
            db.query(Guarantee.guarantor)
            .filter(Guarantee.portfolio_id == portfolio_id)
            .all()
        )
        existing_guarantors_set = {g[0] for g in existing_guarantors}
        
        rows_processed = len(df)
        new_guarantees = df[~df["guarantor"].isin(existing_guarantors_set)].copy()
        
        # Prepare new guarantees for bulk insert
        if not new_guarantees.empty:
            new_guarantees["portfolio_id"] = portfolio_id
            guarantees_to_add = new_guarantees.to_dict(orient="records")
            db.bulk_insert_mappings(Guarantee, guarantees_to_add)
            
        return {
            "status": "success",
            "rows_processed": rows_processed,
            "rows_skipped": len(df) - len(new_guarantees),
            "filename": loan_guarantee_data.filename,
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "filename": loan_guarantee_data.filename,
        }

async def process_loan_collateral(loan_collateral_data, portfolio_id, db):
    """Process loan collateral data file using optimized bulk operations."""
    try:
        content = await loan_collateral_data.read()
        df = pd.read_excel(
            io.BytesIO(content), dtype=str
        )  # Read all columns as strings

        # Column mapping
        column_mapping = {
            "Employee Id": "employee_id",
            "Lastname": "last_name",
            "Othernames": "other_names",
            "Residential Address": "residential_address",
            "Postal Address": "postal_address",
            "Phone Number": "phone_number",
            "Title": "title",
            "Marital Status": "marital_status",
            "Gender": "gender",
            "Date of Birth": "date_of_birth",
            "Employer": "employer",
            "Previous Employee No": "previous_employee_no",
            "Social Security No": "social_security_no",
            "Voters ID No": "voters_id_no",
            "Employment Date": "employment_date",
            "Next of Kin": "next_of_kin",
            "Next of Kin Contact": "next_of_kin_contact",
            "Next of Kin Address": "next_of_kin_address",
            "Search Name": "search_name",
            "Client Type": "client_type",
        }

        df.rename(columns=column_mapping, inplace=True)

        # Convert date columns
        date_columns = ["date_of_birth", "employment_date"]
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # Convert employee_id to string (if it's numeric)
        if "employee_id" in df.columns:
            df["employee_id"] = df["employee_id"].astype(str)

        # Fetch existing employee IDs
        existing_clients = (
            db.query(Client.employee_id)
            .filter(Client.portfolio_id == portfolio_id)
            .all()
        )
        existing_clients_set = {
            c[0] for c in existing_clients
        }  # Extract values from tuples

        rows_processed = len(df)
        new_clients = df[~df["employee_id"].isin(existing_clients_set)].copy()

        # Set default client_type if missing
        if "client_type" in new_clients.columns:
            new_clients["client_type"].fillna("consumer", inplace=True)

        # Prepare new clients for bulk insert
        if not new_clients.empty:
            new_clients["portfolio_id"] = portfolio_id
            clients_to_add = new_clients.to_dict(orient="records")
            db.bulk_insert_mappings(Client, clients_to_add)

        return {
            "status": "success",
            "rows_processed": rows_processed,
            "rows_skipped": len(df) - len(new_clients),
            "filename": loan_collateral_data.filename,
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "filename": loan_collateral_data.filename,
        }
