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

        for col in ["deduction_start_period", "submission_period", "maturity_period"]:
            df[col] = pd.to_datetime(
                df[col], format="%b-%y", errors="coerce"
            ) + pd.offsets.MonthEnd(0)

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

                if loan_no in existing_loan_nos:
                    updates.append(record)
                else:
                    loans_to_add.append(Loan(**record, portfolio_id=portfolio_id))

                rows_processed += 1
            except Exception:
                rows_skipped += 1

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
    """Process loan guarantees file using bulk operations."""
    try:
        content = await loan_guarantee_data.read()
        df = pd.read_excel(io.BytesIO(content))

        # Column mapping
        column_mapping = {
            "Guarantor Name": "guarantor",
            "Pledged Amount": "pledged_amount",
        }

        # Rename columns
        df = df.rename(columns=column_mapping)

        # Get existing guarantees
        existing_guarantees = (
            db.query(Guarantee).filter(Guarantee.portfolio_id == portfolio_id).all()
        )
        existing_guarantees_dict = {g.guarantor: g for g in existing_guarantees}

        rows_processed = 0
        rows_skipped = 0
        guarantees_to_add = []

        # Process each row
        for _, row in df.iterrows():
            try:
                guarantor = row.get("guarantor")

                if guarantor in existing_guarantees_dict:
                    # Update existing guarantee
                    existing_guarantee = existing_guarantees_dict[guarantor]
                    for field in column_mapping.values():
                        if field in row and pd.notna(row[field]):
                            setattr(existing_guarantee, field, row[field])
                else:
                    # Filter valid columns
                    guarantee_data = {
                        field: row[field]
                        for field in column_mapping.values()
                        if field in row and pd.notna(row[field])
                    }

                    # Create new guarantee
                    new_guarantee = Guarantee(**guarantee_data)
                    new_guarantee.portfolio_id = portfolio_id
                    guarantees_to_add.append(new_guarantee)

                rows_processed += 1
            except Exception as e:
                rows_skipped += 1
                print(f"Error processing guarantee row: {str(e)}")

        # Bulk insert new guarantees
        if guarantees_to_add:
            db.bulk_save_objects(guarantees_to_add)

        return {
            "status": "success",
            "rows_processed": rows_processed,
            "rows_skipped": rows_skipped,
            "filename": loan_guarantee_data.filename,
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "filename": loan_guarantee_data.filename,
        }


async def process_loan_collateral(loan_collateral_data, portfolio_id, db):
    """Process loan collateral data file (client information) using bulk operations."""
    try:
        content = await loan_collateral_data.read()
        df = pd.read_excel(io.BytesIO(content))

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

        # Rename columns
        df = df.rename(columns=column_mapping)

        # Process date columns
        date_columns = ["date_of_birth", "employment_date"]
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # Convert employee_id to string
        if "employee_id" in df.columns:
            df["employee_id"] = df["employee_id"].astype(str)

        # Get existing clients
        existing_clients = (
            db.query(Client).filter(Client.portfolio_id == portfolio_id).all()
        )
        existing_clients_dict = {
            client.employee_id: client for client in existing_clients
        }

        rows_processed = 0
        rows_skipped = 0
        clients_to_add = []

        # Process each row
        for _, row in df.iterrows():
            try:
                employee_id = row.get("employee_id")

                if employee_id in existing_clients_dict:
                    # Update existing client
                    existing_client = existing_clients_dict[employee_id]
                    for field in column_mapping.values():
                        if field in row and pd.notna(row[field]):
                            setattr(existing_client, field, row[field])
                else:
                    # Filter valid columns
                    client_data = {
                        field: row[field]
                        for field in column_mapping.values()
                        if field in row and pd.notna(row[field])
                    }

                    # Set default client_type if needed
                    if "client_type" not in client_data:
                        client_data["client_type"] = "consumer"

                    # Create new client
                    new_client = Client(**client_data)
                    new_client.portfolio_id = portfolio_id
                    clients_to_add.append(new_client)

                rows_processed += 1
            except Exception as e:
                rows_skipped += 1
                print(f"Error processing client row: {str(e)}")

        # Bulk insert new clients
        if clients_to_add:
            db.bulk_save_objects(clients_to_add)

        return {
            "status": "success",
            "rows_processed": rows_processed,
            "rows_skipped": rows_skipped,
            "filename": loan_collateral_data.filename,
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "filename": loan_collateral_data.filename,
        }
