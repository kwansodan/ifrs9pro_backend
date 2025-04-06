import io
import pandas as pd
import numpy as np
from datetime import datetime  # Import datetime for type checking
from app.models import (
    Loan,
    Guarantee,
    Client,
    Security
)

async def process_loan_details(loan_details, portfolio_id, db):
    """Function to process loan details with case-insensitive column mapping and improved memory efficiency."""
    try:
        content = await loan_details.read()
        
        # Use ExcelFile for better memory management
        xlsx = pd.ExcelFile(io.BytesIO(content))
        
        # Read first row to get column names
        df_columns = pd.read_excel(xlsx, nrows=0)
        print(f"Original columns in file: {df_columns.columns.tolist()}")
        
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
        
        # Get existing loan numbers from the database FOR THIS PORTFOLIO ONLY
        existing_loans = db.query(Loan.loan_no, Loan.id).filter(Loan.portfolio_id == portfolio_id).all()
        existing_loan_nos = {loan_no: loan_id for loan_no, loan_id in existing_loans if loan_no}
        
        print(f"Found {len(existing_loan_nos)} existing loan numbers in portfolio {portfolio_id}")
        
        # Track overall processing stats
        rows_processed = 0
        rows_skipped = 0
        rows_updated = 0
        inserted_count = 0

        # Instead of batch processing, just read the entire file
        print(f"Reading entire Excel file...")
        df = pd.read_excel(xlsx, dtype=str)
        
        # Show the shape to confirm how many rows were read
        print(f"Read {df.shape[0]} rows and {df.shape[1]} columns")
        
        # Create a mapping of actual column names to our target column names
        case_insensitive_mapping = {}
        for col in df.columns:
            # Ensure col is a string before calling lower()
            if isinstance(col, str):
                col_lower = col.lower().strip()
                if col_lower in target_columns:
                    case_insensitive_mapping[col] = target_columns[col_lower]
            else:
                # Handle non-string column names (like datetime objects)
                col_str = str(col)
                print(f"Warning: Non-string column name encountered: {col_str} of type {type(col)}")
                
        print(f"Using column mapping: {case_insensitive_mapping}")
            
        # Rename columns using our case-insensitive mapping
        df.rename(columns=case_insensitive_mapping, inplace=True)

        # Convert dates
        if "loan_issue_date" in df.columns:
            df["loan_issue_date"] = pd.to_datetime(df["loan_issue_date"], errors="coerce")

        # Process date columns with mixed formats
        for col in ["deduction_start_period", "submission_period", "maturity_period"]:
            if col in df.columns:
                # First, convert potential datetime objects to strings
                df[col] = df[col].apply(lambda x: x.strftime("%b-%Y") if isinstance(x, datetime) else x)
                
                # Only process non-null values
                mask = df[col].notna()
                if mask.any():
                    str_cols = df.loc[mask, col].astype(str)
                    
                    # Format without hyphen (e.g., "SEP2022")
                    no_hyphen_mask = str_cols.str.len() == 7
                    if no_hyphen_mask.any():
                        temp = str_cols[no_hyphen_mask]
                        month = temp.str[:3]
                        year = temp.str[3:]
                        full_year_mask = year.str.len() == 4
                        year.loc[full_year_mask] = year.loc[full_year_mask].str[2:]
                        str_cols.loc[no_hyphen_mask] = month + '-' + year
                    
                    # Update the original dataframe
                    df.loc[mask, col] = str_cols
                
                # Parse dates
                try:
                    df[col] = pd.to_datetime(df[col], format="%b-%y", errors="coerce") + pd.offsets.MonthEnd(0)
                except:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                
                # Handle NaT values
                df[col] = df[col].where(pd.notna(df[col]), None)

        # Convert boolean columns
        if "paid" in df.columns:
            bool_values = ["Yes", "TRUE", "True", "true", "1", "Y", "y"]
            df["paid"] = np.isin(df["paid"].values, bool_values)
            
        if "cancelled" in df.columns:
            bool_values = ["Yes", "TRUE", "True", "true", "1", "Y", "y"]
            df["cancelled"] = np.isin(df["cancelled"].values, bool_values)

        # Initialize counters
        rows_processed = 0
        rows_skipped = 0
        rows_updated = 0
        inserted_count = 0

        # Process rows in chunks for better performance
        loans_to_add = []
        loans_to_update = []
        
        # Track unique loan_no values to handle duplicates within file
        seen_loan_nos_in_batch = set()
        
        # Process each row
        for _, record in df.iterrows():
            try:
                # Convert Series to dict
                record_dict = record.to_dict()
                
                loan_no = record_dict.get("loan_no")
                
                # Skip completely if loan_no is None or empty
                if not loan_no:
                    rows_skipped += 1
                    print(f"Skipping record with no loan_no. Row data: {record_dict}")
                    continue

                # Replace NaT values with None for SQL compatibility
                for date_col in ["loan_issue_date", "deduction_start_period", "submission_period", "maturity_period"]:
                    if date_col in record_dict and (pd.isna(record_dict[date_col]) or record_dict[date_col] == 'NaT'):
                        record_dict[date_col] = None

                # Add portfolio_id to the record
                record_dict["portfolio_id"] = portfolio_id
                
                # Check if this loan exists in THIS portfolio
                if loan_no in existing_loan_nos:
                    loan_id = existing_loan_nos[loan_no]
                    record_dict["id"] = loan_id  # Include ID for update
                    loans_to_update.append(record_dict)
                    rows_updated += 1
                else:
                    # Add it even if it's a duplicate within this file
                    if loan_no in seen_loan_nos_in_batch:
                        print(f"Adding duplicate loan from file: {loan_no}")
                    else:
                        seen_loan_nos_in_batch.add(loan_no)
                        
                    # Create a new Loan object
                    loans_to_add.append(Loan(**record_dict))

                rows_processed += 1
                
                # Process in batches of 100 for better memory management
                if len(loans_to_add) >= 100:
                    try:
                        db.bulk_save_objects(loans_to_add)
                        db.flush()
                        inserted_count += len(loans_to_add)
                        loans_to_add = []  # Reset the list
                    except Exception as e:
                        db.rollback()
                        print(f"Error during bulk save: {e}")
                        # Individual inserts as fallback
                        for loan in loans_to_add:
                            try:
                                db.add(loan)
                                db.flush()
                                inserted_count += 1
                            except Exception as inner_e:
                                db.rollback()
                                print(f"Error inserting loan {getattr(loan, 'loan_no', 'unknown')}: {inner_e}")
                                rows_skipped += 1
                        loans_to_add = []  # Reset after processing
                
                if len(loans_to_update) >= 100:
                    try:
                        db.bulk_update_mappings(Loan, loans_to_update)
                        db.flush()
                        loans_to_update = []  # Reset the list
                    except Exception as e:
                        db.rollback()
                        print(f"Error during bulk update: {e}")
                        # Individual updates as fallback
                        for loan_data in loans_to_update:
                            try:
                                loan_id = loan_data.pop("id")
                                loan = db.query(Loan).filter(Loan.id == loan_id).first()
                                if loan:
                                    for key, value in loan_data.items():
                                        setattr(loan, key, value)
                                    db.flush()
                            except Exception as inner_e:
                                db.rollback()
                                print(f"Error updating loan {loan_data.get('loan_no', 'unknown')}: {inner_e}")
                                rows_skipped += 1
                        loans_to_update = []  # Reset after processing
                
            except Exception as e:
                rows_skipped += 1
                print(f"Error processing loan: {e}")
                continue

        # Process any remaining loans
        if loans_to_add:
            try:
                db.bulk_save_objects(loans_to_add)
                db.flush()
                inserted_count += len(loans_to_add)
            except Exception as e:
                db.rollback()
                print(f"Error during final bulk save: {e}")
                for loan in loans_to_add:
                    try:
                        db.add(loan)
                        db.flush()
                        inserted_count += 1
                    except Exception as inner_e:
                        db.rollback()
                        print(f"Error inserting loan {getattr(loan, 'loan_no', 'unknown')}: {inner_e}")
                        rows_skipped += 1
        
        if loans_to_update:
            try:
                db.bulk_update_mappings(Loan, loans_to_update)
                db.flush()
            except Exception as e:
                db.rollback()
                print(f"Error during final bulk update: {e}")
                for loan_data in loans_to_update:
                    try:
                        loan_id = loan_data.pop("id")
                        loan = db.query(Loan).filter(Loan.id == loan_id).first()
                        if loan:
                            for key, value in loan_data.items():
                                setattr(loan, key, value)
                            db.flush()
                    except Exception as inner_e:
                        db.rollback()
                        print(f"Error updating loan {loan_data.get('loan_no', 'unknown')}: {inner_e}")
                        rows_skipped += 1
        
        # Commit all changes at the end
        db.commit()
        
        # Commit all changes at the end
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
        # Make sure to rollback on error
        db.rollback()
        return {"status": "error", "message": str(e), "filename": loan_details.filename}

async def process_loan_guarantees(loan_guarantee_data, portfolio_id, db):
    """Process loan guarantees file using a case-insensitive approach with improved memory efficiency."""
    try:
        content = await loan_guarantee_data.read()
        
        # Use ExcelFile for better memory management
        xlsx = pd.ExcelFile(io.BytesIO(content))
        
        # Read first row to get column names
        df_columns = pd.read_excel(xlsx, nrows=0)
        print(f"Actual columns in file: {df_columns.columns.tolist()}")
        
        # Create a case-insensitive mapping for columns
        # We'll check various potential column names for guarantor and amount
        possible_guarantor_cols = ["guarantor name", "guarantor", "guarantor's name", "name", "guarantor_name"]
        possible_amount_cols = ["pledged amount", "amount", "guarantee amount", "pledged_amount", "guarantee_amount"]
        
        guarantor_col = None
        amount_col = None
        
        # Create a lowercase version of column names for easy matching
        lowercase_columns = {col.lower().strip(): col for col in df_columns.columns}
        
        # Find matching column names
        for col_key in possible_guarantor_cols:
            if col_key in lowercase_columns:
                guarantor_col = lowercase_columns[col_key]
                break
                
        for col_key in possible_amount_cols:
            if col_key in lowercase_columns:
                amount_col = lowercase_columns[col_key]
                break
        
        # If we found the columns, prepare rename dictionary
        rename_dict = {}
        if guarantor_col:
            rename_dict[guarantor_col] = "guarantor"
            print(f"Using '{guarantor_col}' as guarantor column")
        else:
            raise ValueError(f"Could not find guarantor column. Available columns: {df_columns.columns.tolist()}")
            
        if amount_col:
            rename_dict[amount_col] = "pledged_amount"
            print(f"Using '{amount_col}' as pledged amount column")
        else:
            # If amount column is not found, we'll provide a warning but continue
            print(f"Warning: Could not find amount column. Using default value of 0.")
        
        # Initialize tracking variables
        rows_processed = 0
        rows_inserted = 0
        rows_skipped = 0
        
        # Fetch existing guarantors FOR THIS PORTFOLIO ONLY
        existing_guarantors = (
            db.query(Guarantee.guarantor)
            .filter(Guarantee.portfolio_id == portfolio_id)
            .all()
        )
        existing_guarantors_set = {g[0] for g in existing_guarantors}
        
        print(f"Found {len(existing_guarantors_set)} existing guarantors in portfolio {portfolio_id}")
        
        # Get number of rows in the file (minus header)
        try:
            total_rows = len(pd.read_excel(xlsx, usecols=[0]))
        except:
            # Fallback in case of errors
            total_rows = 1000000  # Arbitrarily large number
        
        # Define batch size for reading Excel
        batch_size = 1000
        
        # Process the file in batches
        for batch_start in range(0, total_rows, batch_size):
            batch_end = min(batch_start + batch_size, total_rows)
            print(f"Processing batch {batch_start}-{batch_end} of {total_rows} rows")
            
            # Skip the header row on the first batch, otherwise skip header + processed rows
            skiprows = None if batch_start == 0 else 1 + batch_start
            
            # Read a batch of data
            if batch_start == 0:
                df = pd.read_excel(xlsx, dtype=str, nrows=batch_size)
            else:
                df = pd.read_excel(xlsx, dtype=str, skiprows=skiprows, nrows=batch_end - batch_start)
            
            # If we're at the end of the file and got no data, break
            if df.empty:
                break
            
            # Apply the renaming
            df.rename(columns=rename_dict, inplace=True)
            
            # Add pledged_amount column if not found
            if "pledged_amount" not in df.columns and amount_col not in df.columns:
                df["pledged_amount"] = 0
            
            # Convert pledged_amount to numeric if the column exists
            if "pledged_amount" in df.columns:
                df["pledged_amount"] = pd.to_numeric(df["pledged_amount"], errors="coerce").fillna(0)
            
            # Clean guarantor strings (trim whitespace)
            if "guarantor" in df.columns:
                df["guarantor"] = df["guarantor"].astype(str).str.strip()
                
            # Track batch stats
            rows_processed += len(df)
            
            # Process in smaller batches to reduce memory pressure
            sub_batch_size = 100
            df_length = len(df)
            
            for sub_batch_start in range(0, df_length, sub_batch_size):
                sub_batch_end = min(sub_batch_start + sub_batch_size, df_length)
                sub_batch_df = df.iloc[sub_batch_start:sub_batch_end]
                
                # Filter new guarantees - only keep guarantors not in existing_guarantors_set
                new_guarantees = sub_batch_df[~sub_batch_df["guarantor"].isin(existing_guarantors_set)].copy()
                
                rows_skipped += len(sub_batch_df) - len(new_guarantees)
                
                # Prepare new guarantees for bulk insert
                if not new_guarantees.empty:
                    new_guarantees["portfolio_id"] = portfolio_id
                    guarantees_to_add = new_guarantees.to_dict(orient="records")
                    
                    # Insert in database
                    db.bulk_insert_mappings(Guarantee, guarantees_to_add)
                    db.flush()
                    
                    # Update counter and existing_guarantors_set for future reference
                    rows_inserted += len(new_guarantees)
                    existing_guarantors_set.update(new_guarantees["guarantor"].tolist())
                    
                # Explicitly clean up to reduce memory usage
                del sub_batch_df
                if 'new_guarantees' in locals():
                    del new_guarantees
            
            # Explicitly clean up the DataFrame to free memory
            del df
        
        # Commit all changes at the end
        db.commit()
            
        return {
            "status": "success",
            "rows_processed": rows_processed,
            "rows_inserted": rows_inserted,
            "rows_skipped": rows_skipped,
            "filename": loan_guarantee_data.filename,
        }
    except Exception as e:
        db.rollback()
        return {
            "status": "error",
            "message": str(e),
            "filename": loan_guarantee_data.filename,
        }

async def process_client_data(client_data, portfolio_id, db):
    """Process client data file using optimized bulk operations with improved memory efficiency."""
    try:
        content = await client_data.read()
        
        # Use ExcelFile for better memory management
        xlsx = pd.ExcelFile(io.BytesIO(content))
        
        # Read first row to get column names
        df_columns = pd.read_excel(xlsx, nrows=0)
        print(f"Original columns in file: {df_columns.columns.tolist()}")
        
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
        rows_skipped = 0
        
        # Fetch existing employee IDs FOR THIS PORTFOLIO ONLY
        existing_clients = (
            db.query(Client.employee_id)
            .filter(Client.portfolio_id == portfolio_id)
            .all()
        )
        existing_clients_set = {c[0] for c in existing_clients}
        
        print(f"Found {len(existing_clients_set)} existing clients in portfolio {portfolio_id}")
        
        # Get number of rows in the file (minus header)
        try:
            total_rows = len(pd.read_excel(xlsx, usecols=[0]))
        except:
            # Fallback in case of errors
            total_rows = 1000000  # Arbitrarily large number
        
        # Define batch size for reading Excel
        batch_size = 1000
        
        # Process the file in batches
        for batch_start in range(0, total_rows, batch_size):
            batch_end = min(batch_start + batch_size, total_rows)
            print(f"Processing batch {batch_start}-{batch_end} of {total_rows} rows")
            
            # Skip the header row on the first batch, otherwise skip header + processed rows
            skiprows = None if batch_start == 0 else 1 + batch_start
            
            # Read a batch of data
            if batch_start == 0:
                df = pd.read_excel(xlsx, dtype=str, nrows=batch_size)
            else:
                df = pd.read_excel(xlsx, dtype=str, skiprows=skiprows, nrows=batch_end - batch_start)
            
            # If we're at the end of the file and got no data, break
            if df.empty:
                break
            
            # Create a mapping of actual column names to our target column names
            case_insensitive_mapping = {}
            for col in df.columns:
                col_lower = col.lower().strip()
                if col_lower in target_columns:
                    case_insensitive_mapping[col] = target_columns[col_lower]
            
            if batch_start == 0:
                # Report the mapping that will be used (first batch only)
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

            rows_processed += len(df)
            
            # Process in smaller batches to reduce memory pressure
            sub_batch_size = 100
            df_length = len(df)
            
            for sub_batch_start in range(0, df_length, sub_batch_size):
                sub_batch_end = min(sub_batch_start + sub_batch_size, df_length)
                sub_batch_df = df.iloc[sub_batch_start:sub_batch_end]
                
                # Filter new clients - only keep employees not in existing_clients_set
                new_clients = sub_batch_df[~sub_batch_df["employee_id"].isin(existing_clients_set)].copy()
                
                rows_skipped += len(sub_batch_df) - len(new_clients)
                
                # Prepare new clients for bulk insert
                if not new_clients.empty:
                    # Set default client_type if missing
                    if "client_type" in new_clients.columns:
                        new_clients["client_type"].fillna("consumer", inplace=True)
                        
                    new_clients["portfolio_id"] = portfolio_id
                    clients_to_add = new_clients.to_dict(orient="records")
                    
                    # Insert in database
                    db.bulk_insert_mappings(Client, clients_to_add)
                    db.flush()
                    
                    # Update counter and existing_clients_set for future reference
                    rows_inserted += len(new_clients)
                    existing_clients_set.update(new_clients["employee_id"].tolist())
                    
                # Explicitly clean up to reduce memory usage
                del sub_batch_df
                if 'new_clients' in locals():
                    del new_clients
            
            # Explicitly clean up the DataFrame to free memory
            del df
        
        # Commit all changes at the end
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

async def process_collateral_data(collateral_data, portfolio_id, db):
    """Process loan collateral (securities) data using optimized bulk operations with improved memory efficiency."""
    try:
        content = await collateral_data.read()
        
        # Use ExcelFile for better memory management
        xlsx = pd.ExcelFile(io.BytesIO(content))
        
        # Read first row to get column names
        df_columns = pd.read_excel(xlsx, nrows=0)
        print(f"Original columns in file: {df_columns.columns.tolist()}")
        
        # Define possible column names (lowercase for matching)
        possible_columns = {
            "employee id": "employee_id",  # To link with client
            "collateral description": "collateral_description",
            "collateral value": "collateral_value",
            "forced sale value": "forced_sale_value",
            "method of valuation": "method_of_valuation",
            "cash or non cash": "cash_or_non_cash"
        }
        
        # Initialize tracking variables
        rows_processed = 0
        rows_inserted = 0
        rows_skipped = 0
        
        # Get all clients for THIS PORTFOLIO ONLY to match by employee_id
        clients = db.query(Client).filter(Client.portfolio_id == portfolio_id).all()
        
        # Create a mapping of employee_id to client_id
        employee_id_to_client_id = {client.employee_id: client.id for client in clients}
        
        print(f"Found {len(employee_id_to_client_id)} clients in portfolio {portfolio_id}")
        
        # Check for existing securities to avoid duplicates - only for THIS PORTFOLIO's clients
        client_ids = list(employee_id_to_client_id.values())
        existing_securities = (
            db.query(Security)
            .filter(Security.client_id.in_(client_ids))
            .all() if client_ids else []
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
            
        print(f"Found {len(existing_identifiers)} existing securities for clients in this portfolio")
        
        # Get number of rows in the file (minus header)
        try:
            total_rows = len(pd.read_excel(xlsx, usecols=[0]))
        except:
            # Fallback in case of errors
            total_rows = 1000000  # Arbitrarily large number
        
        # Define batch size for reading Excel
        batch_size = 1000
        
        # Process the file in batches
        for batch_start in range(0, total_rows, batch_size):
            batch_end = min(batch_start + batch_size, total_rows)
            print(f"Processing batch {batch_start}-{batch_end} of {total_rows} rows")
            
            # Skip the header row on the first batch, otherwise skip header + processed rows
            skiprows = None if batch_start == 0 else 1 + batch_start
            
            # Read a batch of data
            if batch_start == 0:
                df = pd.read_excel(xlsx, dtype=str, nrows=batch_size)
            else:
                df = pd.read_excel(xlsx, dtype=str, skiprows=skiprows, nrows=batch_end - batch_start)
            
            # If we're at the end of the file and got no data, break
            if df.empty:
                break
            
            # Create a case-insensitive mapping of actual columns to our target columns
            column_mapping = {}
            for col in df.columns:
                col_lower = col.lower().strip()
                for possible_col, target_col in possible_columns.items():
                    if col_lower == possible_col or col_lower.replace(' ', '_') == possible_col.replace(' ', '_'):
                        column_mapping[col] = target_col
                        break
            
            if batch_start == 0:
                # Report the mapping that will be used (first batch only)
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
            
            # Add client_id column based on employee_id
            df["client_id"] = df["employee_id"].map(employee_id_to_client_id)
            
            # Filter out rows where client_id is missing
            valid_df = df[df["client_id"].notna()].copy()
            
            # Update processing count
            rows_processed += len(df)
            rows_skipped += len(df) - len(valid_df)
            
            # Set default values for optional columns
            if "method_of_valuation" not in valid_df.columns:
                valid_df["method_of_valuation"] = "market_value"
            if "cash_or_non_cash" not in valid_df.columns:
                valid_df["cash_or_non_cash"] = "non_cash"
            
            # Process in smaller batches to reduce memory pressure
            sub_batch_size = 100
            
            if not valid_df.empty:
                df_length = len(valid_df)
                
                for sub_batch_start in range(0, df_length, sub_batch_size):
                    sub_batch_end = min(sub_batch_start + sub_batch_size, df_length)
                    sub_batch_df = valid_df.iloc[sub_batch_start:sub_batch_end]
                    
                    # Filter out rows that already exist
                    securities_to_add = []
                    
                    for _, row in sub_batch_df.iterrows():
                        # Create a unique identifier for this row
                        row_identifier = (
                            int(row["client_id"]),
                            row.get("collateral_description", ""),
                            float(row["collateral_value"])
                        )
                        
                        # Skip if this security already exists
                        if row_identifier in existing_identifiers:
                            rows_skipped += 1
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
                        
                        # Add to existing identifiers to prevent duplicates in subsequent batches
                        existing_identifiers.add(row_identifier)
                    
                    # Bulk insert the new securities
                    if securities_to_add:
                        db.bulk_insert_mappings(Security, securities_to_add)
                        db.flush()
                        rows_inserted += len(securities_to_add)
                    
                    # Explicitly clean up to reduce memory usage
                    del sub_batch_df
                    if 'securities_to_add' in locals():
                        del securities_to_add
            
            # Explicitly clean up the DataFrame to free memory
            del df
            if 'valid_df' in locals():
                del valid_df
        
        # Commit all changes at the end
        db.commit()
        
        if rows_processed > 0 and rows_inserted == 0:
            return {
                "status": "warning",
                "message": "No matching clients found for securities in this portfolio",
                "rows_processed": rows_processed,
                "rows_inserted": rows_inserted,
                "rows_skipped": rows_skipped,
                "filename": collateral_data.filename
            }
        
        return {
            "status": "success",
            "rows_processed": rows_processed,
            "rows_inserted": rows_inserted,
            "rows_skipped": rows_skipped,
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
