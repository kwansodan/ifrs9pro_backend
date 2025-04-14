import io
import pandas as pd
from app.models import (
    Loan,
    Guarantee,
    Client,
    Security
)

async def process_loan_details(loan_details, portfolio_id, db):
    """Function to process loan details with case-insensitive column mapping."""
    try:
        content = await loan_details.read()
        df = pd.read_excel(io.BytesIO(content), dtype=str)  # Read directly as strings

        # Print original column names for debugging
        print(f"Original columns in file: {df.columns.tolist()}")

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
        
        # Create a mapping of actual column names to our target column names
        case_insensitive_mapping = {}
        for col in df.columns:
            col_lower = col.lower().strip()
            if col_lower in target_columns:
                case_insensitive_mapping[col] = target_columns[col_lower]
        
        # Report the mapping that will be used
        print(f"Using column mapping: {case_insensitive_mapping}")
            
        # Rename columns using our case-insensitive mapping
        df.rename(columns=case_insensitive_mapping, inplace=True)

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
        if "paid" in df.columns:
            df["paid"] = df["paid"].isin(["Yes", "TRUE", "True", "true", "1", "Y", "y"])
        if "cancelled" in df.columns:
            df["cancelled"] = df["cancelled"].isin(["Yes", "TRUE", "True", "true", "1", "Y", "y"])

        # Clean any existing transaction state
        db.rollback()

        # Get ALL existing loan numbers from the database
        existing_loans = db.query(Loan.loan_no, Loan.id).all()
        existing_loan_nos = {loan_no: loan_id for loan_no, loan_id in existing_loans if loan_no}

        print(f"Found {len(existing_loan_nos)} existing loan numbers in database")

        # Track loan numbers seen in this import to handle duplicates within the sheet
        seen_loan_nos_in_sheet = set()
        
        # Convert DataFrame to list of dictionaries
        loan_records = df.astype(object).to_dict(orient="records")

        rows_processed = 0
        rows_skipped = 0
        rows_updated = 0
        loans_to_add = []
        loans_to_update = []

        # Process loans in bulk
        for record in loan_records:
            try:
                loan_no = record.get("loan_no")
                
                # Skip completely if loan_no is None or empty
                if not loan_no:
                    rows_skipped += 1
                    print(f"Skipping record with no loan_no")
                    continue

                # Replace NaT values with None for SQL compatibility
                for date_col in ["loan_issue_date", "deduction_start_period", "submission_period", "maturity_period"]:
                    if date_col in record and pd.isna(record[date_col]):
                        record[date_col] = None

                # Add portfolio_id to the record
                record["portfolio_id"] = portfolio_id
                
                # Check if this loan exists in database
                if loan_no in existing_loan_nos:
                    print(f"Updating existing loan: {loan_no}")
                    loan_id = existing_loan_nos[loan_no]
                    record["id"] = loan_id  # Include ID for update
                    loans_to_update.append(record)
                    rows_updated += 1
                else:
                    # If it's a duplicate within this sheet and not in database, still add it
                    if loan_no in seen_loan_nos_in_sheet:
                        print(f"Adding duplicate loan from sheet: {loan_no}")
                    else:
                        print(f"Adding new loan: {loan_no}")
                        seen_loan_nos_in_sheet.add(loan_no)
                        
                    # Create a new Loan object
                    loans_to_add.append(Loan(**record))

                rows_processed += 1
            except Exception as e:
                rows_skipped += 1
                print(f"Error processing loan: {e}")
                continue  # Skip to next record on error

        # Insert new loans
        inserted_count = 0
        if loans_to_add:
            try:
                db.bulk_save_objects(loans_to_add)
                inserted_count = len(loans_to_add)
                print(f"Successfully inserted {inserted_count} new loans")
            except Exception as e:
                db.rollback()
                print(f"Error during bulk save: {e}")
                # Try one by one as a fallback
                print("Falling back to one-by-one insertion")
                inserted_count = 0
                for loan in loans_to_add:
                    try:
                        db.add(loan)
                        db.flush()  # Check for errors without committing
                        inserted_count += 1
                    except Exception as inner_e:
                        db.rollback()  # Roll back the failed insert
                        print(f"Error inserting loan {getattr(loan, 'loan_no', 'unknown')}: {inner_e}")
                        rows_skipped += 1
                        
                print(f"Successfully inserted {inserted_count} loans one by one")
        
        # Update existing loans
        updated_count = 0
        if loans_to_update:
            try:
                # Use bulk update
                db.bulk_update_mappings(Loan, loans_to_update)
                updated_count = len(loans_to_update)
                print(f"Successfully updated {updated_count} existing loans")
            except Exception as e:
                db.rollback()
                print(f"Error during bulk update: {e}")
                # Try one by one as a fallback
                print("Falling back to one-by-one updates")
                updated_count = 0
                for loan_data in loans_to_update:
                    try:
                        loan_id = loan_data.pop("id")
                        loan = db.query(Loan).filter(Loan.id == loan_id).first()
                        if loan:
                            for key, value in loan_data.items():
                                setattr(loan, key, value)
                            db.flush()  # Check for errors without committing
                            updated_count += 1
                    except Exception as inner_e:
                        db.rollback()  # Roll back the failed update
                        print(f"Error updating loan {loan_data.get('loan_no', 'unknown')}: {inner_e}")
                        rows_skipped += 1
                
                print(f"Successfully updated {updated_count} loans one by one")

        db.commit()
        
        return {
            "status": "success",
            "rows_processed": rows_processed,
            "rows_inserted": inserted_count,
            "rows_updated": updated_count,
            "rows_skipped": rows_skipped,
            "filename": loan_details.filename,
        }

    except Exception as e:
        # Make sure to rollback on error
        db.rollback()
        return {"status": "error", "message": str(e), "filename": loan_details.filename}
    
async def process_loan_guarantees(loan_guarantee_data, portfolio_id, db):
    """Process loan guarantees file using a case-insensitive approach."""
    try:
        content = await loan_guarantee_data.read()
        df = pd.read_excel(io.BytesIO(content), dtype=str)
        
        # Print column names for debugging
        print(f"Actual columns in file: {df.columns.tolist()}")
        
        # Create a case-insensitive mapping for columns
        # We'll check various potential column names for guarantor and amount
        possible_guarantor_cols = ["guarantor name", "guarantor", "guarantor's name", "name", "guarantor_name"]
        possible_amount_cols = ["pledged amount", "amount", "guarantee amount", "pledged_amount", "guarantee_amount"]
        
        guarantor_col = None
        amount_col = None
        
        # Create a lowercase version of column names for easy matching
        lowercase_columns = {col.lower().strip(): col for col in df.columns}
        
        # Find matching column names
        for col_key in possible_guarantor_cols:
            if col_key in lowercase_columns:
                guarantor_col = lowercase_columns[col_key]
                break
                
        for col_key in possible_amount_cols:
            if col_key in lowercase_columns:
                amount_col = lowercase_columns[col_key]
                break
        
        # If we found the columns, rename them to our standard names
        rename_dict = {}
        if guarantor_col:
            rename_dict[guarantor_col] = "guarantor"
            print(f"Using '{guarantor_col}' as guarantor column")
        else:
            raise ValueError(f"Could not find guarantor column. Available columns: {df.columns.tolist()}")
            
        if amount_col:
            rename_dict[amount_col] = "pledged_amount"
            print(f"Using '{amount_col}' as pledged amount column")
        else:
            # If amount column is not found, we'll provide a warning but continue
            # with a default value for pledged_amount
            print(f"Warning: Could not find amount column. Using default value of 0.")
            df["pledged_amount"] = 0
        
        # Apply the renaming
        df.rename(columns=rename_dict, inplace=True)
        
        # Convert pledged_amount to numeric if the column exists
        if "pledged_amount" in df.columns:
            df["pledged_amount"] = pd.to_numeric(df["pledged_amount"], errors="coerce").fillna(0)
        
        # Fetch only necessary fields from DB
        existing_guarantors = (
            db.query(Guarantee.guarantor)
            .filter(Guarantee.portfolio_id == portfolio_id)
            .all()
        )
        existing_guarantors_set = {g[0] for g in existing_guarantors}
        
        # Clean guarantor strings (trim whitespace)
        if "guarantor" in df.columns:
            df["guarantor"] = df["guarantor"].astype(str).str.strip()
            
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
            "rows_inserted": len(new_guarantees),
            "rows_skipped": len(df) - len(new_guarantees),
            "filename": loan_guarantee_data.filename,
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "filename": loan_guarantee_data.filename,
        }
async def process_client_data(client_data, portfolio_id, db):
    """Process client data file using optimized bulk operations."""
    try:
        content = await client_data.read()
        df = pd.read_excel(
            io.BytesIO(content), dtype=str
        )  # Read all columns as strings

        # Print original column names for debugging
        print(f"Original columns in file: {df.columns.tolist()}")
        
        # Create a case-insensitive column mapping
        case_insensitive_mapping = {}
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
        
        # Create a mapping of actual column names to our target column names
        for col in df.columns:
            col_lower = col.lower().strip()
            if col_lower in target_columns:
                case_insensitive_mapping[col] = target_columns[col_lower]
        
        # Report the mapping that will be used
        print(f"Using column mapping: {case_insensitive_mapping}")
        
        # Rename columns using our case-insensitive mapping
        df.rename(columns=case_insensitive_mapping, inplace=True)

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
            "filename": client_data.filename,
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "filename": client_data.filename,
        }

async def process_collateral_data(collateral_data, portfolio_id, db):
    """Process loan collateral (securities) data using optimized bulk operations."""
    try:
        content = await collateral_data.read()
        df = pd.read_excel(io.BytesIO(content), dtype=str)  # Read all columns as strings

        # Print original column names for debugging
        print(f"Original columns in file: {df.columns.tolist()}")
        
        # Define possible column names (lowercase for matching)
        possible_columns = {
            "employee id": "employee_id",  # To link with client
            "collateral description": "collateral_description",
            "collateral value": "collateral_value",
            "forced sale value": "forced_sale_value",
            "method of valuation": "method_of_valuation",
            "cash or non cash": "cash_or_non_cash"
        }
        
        # Create a case-insensitive mapping of actual columns to our target columns
        column_mapping = {}
        for col in df.columns:
            col_lower = col.lower().strip()
            for possible_col, target_col in possible_columns.items():
                if col_lower == possible_col or col_lower.replace(' ', '_') == possible_col.replace(' ', '_'):
                    column_mapping[col] = target_col
                    break
        
        # Report the mapping that will be used
        print(f"Using column mapping: {column_mapping}")
        
        # Rename the columns based on our mapping
        df.rename(columns=column_mapping, inplace=True)
        
        # Make sure required columns are present
        required_columns = ["employee_id", "collateral_value"]
        for column in required_columns:
            if column not in df.columns:
                raise ValueError(f"Required column '{column}' not found in the file")
        
        # Convert numeric columns
        numeric_columns = ["collateral_value", "forced_sale_value"]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        
        # Get all clients for this portfolio to match by employee_id
        clients = db.query(Client).filter(Client.portfolio_id == portfolio_id).all()
        
        # Create a mapping of employee_id to client_id
        employee_id_to_client_id = {client.employee_id: client.id for client in clients}
        
        # Add client_id column based on employee_id
        df["client_id"] = df["employee_id"].map(employee_id_to_client_id)
        
        # Filter out rows where client_id is missing
        valid_df = df[df["client_id"].notna()].copy()
        
        # Check if we have any valid records
        if valid_df.empty:
            return {
                "status": "warning",
                "message": "No matching clients found for securities in this portfolio",
                "rows_processed": len(df),
                "rows_inserted": 0,
                "rows_skipped": len(df),
                "filename": collateral_data.filename
            }
        
        # Set default values for optional columns
        if "method_of_valuation" not in valid_df.columns:
            valid_df["method_of_valuation"] = "market_value"
        if "cash_or_non_cash" not in valid_df.columns:
            valid_df["cash_or_non_cash"] = "non_cash"
        
        # Check for existing securities to avoid duplicates
        # We'll consider a security a duplicate if it has the same client_id, 
        # collateral_description, and collateral_value
        existing_securities = (
            db.query(Security)
            .filter(Security.client_id.in_(valid_df["client_id"].tolist()))
            .all()
        )
        
        # Create a set of existing security identifiers (client_id + desc + value)
        existing_identifiers = set()
        for security in existing_securities:
            # Create a unique identifier for this security
            identifier = (
                security.client_id,
                security.collateral_description if security.collateral_description else "",
                float(security.collateral_value)
            )
            existing_identifiers.add(identifier)
        
        # Filter out rows that already exist
        securities_to_add = []
        skipped_count = 0
        
        for _, row in valid_df.iterrows():
            # Create a unique identifier for this row
            row_identifier = (
                int(row["client_id"]),
                row.get("collateral_description", ""),
                float(row["collateral_value"])
            )
            
            # Skip if this security already exists
            if row_identifier in existing_identifiers:
                skipped_count += 1
                continue
            
            # Create a new security from this row
            security_data = {
                "client_id": int(row["client_id"]),
                "collateral_description": row.get("collateral_description"),
                "collateral_value": float(row["collateral_value"]),
                "forced_sale_value": float(row.get("forced_sale_value", 0)),
                "method_of_valuation": row.get("method_of_valuation", "market_value"),
                "cash_or_non_cash": row.get("cash_or_non_cash", "non_cash")
            }
            securities_to_add.append(security_data)
        
        # Bulk insert the new securities
        if securities_to_add:
            db.bulk_insert_mappings(Security, securities_to_add)
        
        return {
            "status": "success",
            "rows_processed": len(df),
            "rows_inserted": len(securities_to_add),
            "rows_skipped": skipped_count + (len(df) - len(valid_df)),
            "filename": collateral_data.filename
        }
    
    except Exception as e:
        # Make sure to rollback on error
        db.rollback()
        import traceback
        traceback.print_exc()
        return {
            "status": "error", 
            "message": str(e), 
            "filename": collateral_data.filename
        }
