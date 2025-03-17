from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
    UploadFile,
    File,
    Form,
    Body,
)
from sqlalchemy.orm import Session, joinedload
import numpy as np
import math
from decimal import Decimal
from datetime import datetime, timedelta, date
from pydantic import BaseModel
from typing import List, Dict, Optional, Union
import pandas as pd
import io
from app.database import get_db
from app.models import Portfolio, User
from app.auth.utils import get_current_active_user
from app.calculators.ecl import (
    calculate_effective_interest_rate,
    calculate_exposure_at_default_percentage,
    calculate_probability_of_default,
    calculate_loss_given_default,
    calculate_marginal_ecl,
)
from app.calculators.local_impairment import (
    parse_days_range,
    calculate_category_data,
    calculate_days_past_due,
    calculate_loan_impairment,
    calculate_impairment_summary,
)
from app.models import (
    Portfolio,
    User,
    AssetType,
    CustomerType,
    FundingSource,
    DataSource,
    Loan,
    Security,
    Client,
)
from app.schemas import (
    PortfolioCreate,
    PortfolioUpdate,
    PortfolioResponse,
    PortfolioList,
    PortfolioWithSummaryResponse,
    ECLSummary,
    ECLCategoryData,
    ECLSummaryMetrics,
    LocalImpairmentSummary,
    ImpairmentConfig,
)
from app.auth.utils import get_current_active_user

router = APIRouter(prefix="/portfolios", tags=["portfolios"])


@router.post("/", response_model=PortfolioResponse, status_code=status.HTTP_201_CREATED)
def create_portfolio(
    portfolio: PortfolioCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Create a new portfolio for the current user.
    """
    new_portfolio = Portfolio(
        name=portfolio.name,
        description=portfolio.description,
        asset_type=portfolio.asset_type.value,
        customer_type=portfolio.customer_type.value,
        funding_source=portfolio.funding_source.value,
        data_source=portfolio.data_source.value,
        repayment_source=portfolio.repayment_source,
        credit_source=portfolio.credit_source,
        loan_assets=portfolio.loan_assets,
        ecl_impairment_account=portfolio.ecl_impairment_account,
        user_id=current_user.id,
    )

    db.add(new_portfolio)
    db.commit()
    db.refresh(new_portfolio)
    return new_portfolio


@router.get("/", response_model=PortfolioList)
def get_portfolios(
    skip: int = 0,
    limit: int = 100,
    asset_type: Optional[str] = None,
    customer_type: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Retrieve all portfolios belonging to the current user.
    Optional filtering by asset_type and customer_type.
    """
    query = db.query(Portfolio).filter(Portfolio.user_id == current_user.id)

    # Apply filters if provided
    if asset_type:
        query = query.filter(Portfolio.asset_type == asset_type)
    if customer_type:
        query = query.filter(Portfolio.customer_type == customer_type)

    # Get total count for pagination
    total = query.count()

    # Apply pagination
    portfolios = query.offset(skip).limit(limit).all()

    return {"items": portfolios, "total": total}


@router.get("/{portfolio_id}", response_model=PortfolioWithSummaryResponse)
def get_portfolio(
    portfolio_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Retrieve a specific portfolio by ID including overview and customer summary.
    """
    # Query the portfolio with joined loans and clients
    portfolio = (
        db.query(Portfolio)
        .options(joinedload(Portfolio.loans), joinedload(Portfolio.clients))
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
        .first()
    )

    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )

    # Calculate overview metrics
    total_loans = len(portfolio.loans)
    total_loan_value = sum(
        loan.loan_amount for loan in portfolio.loans if loan.loan_amount is not None
    )
    average_loan_amount = total_loan_value / total_loans if total_loans > 0 else 0
    total_customers = len(portfolio.clients)

    # Calculate customer summary metrics
    individual_customers = sum(
        1 for client in portfolio.clients if client.client_type == "consumer"
    )
    institutions = sum(
        1 for client in portfolio.clients if client.client_type == "institution"
    )
    mixed = sum(
        1
        for client in portfolio.clients
        if client.client_type not in ["consumer", "institution"]
    )
    # Determine active customers (you may need to adjust this logic based on your definition of "active")
    active_customers = sum(
        1
        for client in portfolio.clients
        if any(
            loan.paid is False
            for loan in portfolio.loans
            if hasattr(loan, "employee_id") and loan.employee_id == client.employee_id
        )
    )

    # Create response dictionary with portfolio data and summaries
    response = {
        "id": portfolio.id,
        "name": portfolio.name,
        "description": portfolio.description,
        "asset_type": portfolio.asset_type,
        "customer_type": portfolio.customer_type,
        "funding_source": portfolio.funding_source,
        "created_at": portfolio.created_at,
        "updated_at": portfolio.updated_at,
        "overview": {
            "total_loans": total_loans,
            "total_loan_value": total_loan_value,
            "average_loan_amount": average_loan_amount,
            "total_customers": total_customers,
        },
        "customer_summary": {
            "individual_customers": individual_customers,
            "institutions": institutions,
            "mixed": mixed,
            "active_customers": active_customers,
        },
    }

    return response


@router.put("/{portfolio_id}", response_model=PortfolioResponse)
def update_portfolio(
    portfolio_id: int,
    portfolio_update: PortfolioUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Update a specific portfolio by ID.
    """
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
        .first()
    )

    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )

    # Update fields if provided
    update_data = portfolio_update.dict(exclude_unset=True)

    # Convert enum values to strings for storage
    if "asset_type" in update_data and update_data["asset_type"]:
        update_data["asset_type"] = update_data["asset_type"].value
    if "customer_type" in update_data and update_data["customer_type"]:
        update_data["customer_type"] = update_data["customer_type"].value
    if "funding_source" in update_data and update_data["funding_source"]:
        update_data["funding_source"] = update_data["funding_source"].value
    if "data_source" in update_data and update_data["data_source"]:
        update_data["data_source"] = update_data["data_source"].value

    for key, value in update_data.items():
        setattr(portfolio, key, value)

    db.commit()
    db.refresh(portfolio)
    return portfolio


@router.delete("/{portfolio_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_portfolio(
    portfolio_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Delete a specific portfolio by ID.
    """
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
        .first()
    )

    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )

    db.delete(portfolio)
    db.commit()

    return None


@router.post("/{portfolio_id}/ingest", status_code=status.HTTP_200_OK)
async def ingest_portfolio_data(
    portfolio_id: int,
    loan_details: Optional[UploadFile] = File(None),
    loan_guarantee_data: Optional[UploadFile] = File(None),
    loan_collateral_data: Optional[UploadFile] = File(None),
    historical_repayments_data: Optional[UploadFile] = File(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Ingest Excel files containing portfolio data.

    Accepts up to four Excel files:
    - loan_details: Primary loan information
    - loan_guarantee_data: Information about loan guarantees
    - loan_collateral_data: Information about loan collateral
    - historical_repayments_data: Historical repayment data
    """
    # Check if at least one file is provided
    if not any(
        [
            loan_details,
            loan_guarantee_data,
            loan_collateral_data,
            historical_repayments_data,
        ]
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one file must be provided",
        )

    # Verify portfolio exists and belongs to current user
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
        .first()
    )

    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )

    results = {}

    # Process loan details file
    if loan_details:
        try:
            content = await loan_details.read()
            df = pd.read_excel(io.BytesIO(content))

            # Data cleanup and transformation
            # Convert column names to match model field names
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

            # Rename columns based on mapping
            df = df.rename(columns=column_mapping)

            # Convert date columns to appropriate format
            date_columns = ["loan_issue_date", "maturity_period"]
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")

            # Special handling for period columns (they appear to be in Month-YY format)
            period_columns = ["deduction_start_period", "submission_period"]
            for col in period_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce", format="%b-%y")

            # Convert boolean columns
            boolean_columns = ["paid", "cancelled"]
            for col in boolean_columns:
                if col in df.columns:
                    df[col] = df[col].map({"Yes": True, "No": False})

            # Process and insert each row
            rows_processed = 0
            rows_skipped = 0
            for index, row in df.iterrows():
                try:
                    # Check if loan already exists
                    existing_loan = (
                        db.query(Loan)
                        .filter(Loan.loan_no == row.get("loan_no"))
                        .first()
                    )

                    if existing_loan:
                        # Update existing loan
                        for field in column_mapping.values():
                            if field in row and pd.notna(row[field]):
                                setattr(existing_loan, field, row[field])
                        rows_processed += 1
                    else:
                        # Filter to keep only columns that exist in the model
                        loan_data = {
                            field: row[field]
                            for field in column_mapping.values()
                            if field in row and pd.notna(row[field])
                        }

                        # Create new loan record
                        new_loan = Loan(**loan_data)
                        # Associate loan with the portfolio
                        new_loan.portfolio_id = portfolio_id
                        db.add(new_loan)
                        rows_processed += 1

                except Exception as e:
                    rows_skipped += 1
                    print(f"Error processing row {index}: {str(e)}")
                    continue

            # Commit changes to DB
            db.commit()

            results["loan_details"] = {
                "status": "success",
                "rows_processed": rows_processed,
                "rows_skipped": rows_skipped,
                "filename": loan_details.filename,
            }
        except Exception as e:
            results["loan_details"] = {
                "status": "error",
                "message": str(e),
                "filename": loan_details.filename,
            }

    # Process loan guarantee data file
    if loan_guarantee_data:
        try:
            content = await loan_guarantee_data.read()
            df = pd.read_excel(io.BytesIO(content))

            # Data cleanup and transformation
            # Convert column names to match model field names
            column_mapping = {
                "Guarantor Name": "guarantor",
                "Pledged Amount": "pledged_amount",
            }

            # Rename columns based on mapping
            df = df.rename(columns=column_mapping)

            # Process and insert each row
            rows_processed = 0
            rows_skipped = 0
            for index, row in df.iterrows():
                try:
                    # Check if guarantee already exists by guarantor name in this portfolio
                    existing_guarantee = (
                        db.query(Guarantee)
                        .filter(
                            Guarantee.guarantor == row.get("guarantor"),
                            Guarantee.portfolio_id == portfolio_id,
                        )
                        .first()
                    )

                    if existing_guarantee:
                        # Update existing guarantee
                        for field in column_mapping.values():
                            if field in row and pd.notna(row[field]):
                                setattr(existing_guarantee, field, row[field])
                        rows_processed += 1
                    else:
                        # Filter to keep only columns that exist in the model
                        guarantee_data = {
                            field: row[field]
                            for field in column_mapping.values()
                            if field in row and pd.notna(row[field])
                        }

                        # Create new guarantee record
                        new_guarantee = Guarantee(**guarantee_data)
                        # Associate guarantee with the portfolio
                        new_guarantee.portfolio_id = portfolio_id
                        db.add(new_guarantee)
                        rows_processed += 1

                except Exception as e:
                    rows_skipped += 1
                    print(f"Error processing row {index}: {str(e)}")
                    continue

            # Commit changes to DB
            db.commit()

            results["loan_guarantee_data"] = {
                "status": "success",
                "rows_processed": rows_processed,
                "rows_skipped": rows_skipped,
                "filename": loan_guarantee_data.filename,
            }
        except Exception as e:
            results["loan_guarantee_data"] = {
                "status": "error",
                "message": str(e),
                "filename": loan_guarantee_data.filename,
            }

    # Process loan collateral data file (which contains client information)
    if loan_collateral_data:
        try:
            content = await loan_collateral_data.read()
            df = pd.read_excel(io.BytesIO(content))

            # Data cleanup and transformation
            # Convert column names to match model field names
            column_mapping = {
                "Employee ID": "employee_id",
                "Last Name": "last_name",
                "Other Names": "other_names",
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

            # Rename columns based on mapping
            df = df.rename(columns=column_mapping)

            # Convert date columns to appropriate format
            date_columns = ["date_of_birth", "employment_date"]
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")

                # Process and insert each row
            rows_processed = 0
            rows_skipped = 0
            for index, row in df.iterrows():
                try:
                    # Check if client already exists by employee_id
                    existing_client = (
                        db.query(Client)
                        .filter(Client.employee_id == row.get("employee_id"))
                        .first()
                    )

                    if existing_client:
                        # Update existing client
                        for field in column_mapping.values():
                            if field in row and pd.notna(row[field]):
                                setattr(existing_client, field, row[field])
                        rows_processed += 1
                    else:
                        # Filter to keep only columns that exist in the model
                        client_data = {
                            field: row[field]
                            for field in column_mapping.values()
                            if field in row and pd.notna(row[field])
                        }

                        # Set client_type to default if it doesn't exist in the data
                        if "client_type" not in client_data:
                            client_data["client_type"] = "consumer"

                        # Create new client record
                        new_client = Client(**client_data)
                        # Associate client with the portfolio
                        new_client.portfolio_id = portfolio_id
                        db.add(new_client)
                        rows_processed += 1

                except Exception as e:
                    rows_skipped += 1
                    print(f"Error processing row {index}: {str(e)}")
                    continue

            # Commit changes to DB
            db.commit()

            results["loan_collateral_data"] = {
                "status": "success",
                "rows_processed": rows_processed,
                "rows_skipped": rows_skipped,
                "filename": loan_collateral_data.filename,
            }
        except Exception as e:
            results["loan_collateral_data"] = {
                "status": "error",
                "message": str(e),
                "filename": loan_collateral_data.filename,
            }

    return {"portfolio_id": portfolio_id, "results": results}


@router.get("/{portfolio_id}/calculate-ecl", response_model=ECLSummary)
def calculate_ecl(
    portfolio_id: int,
    reporting_date: Optional[date] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    # Verify portfolio exists and belongs to current user
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
        .first()
    )

    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )

    # Use provided reporting date or default to current date
    if not reporting_date:
        reporting_date = datetime.now().date()

    # Get all loans in the portfolio
    loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()

    # Create a dictionary to map client IDs to their securities
    client_securities = {}

    # Get all securities for clients with loans in this portfolio
    client_ids = {loan.employee_id for loan in loans}

    if client_ids:
        securities = (
            db.query(Security)
            .join(Client, Security.client_id == Client.id)
            .filter(Client.employee_id.in_(client_ids))
            .all()
        )

        # Group securities by client employee_id
        for security in securities:
            client = db.query(Client).filter(Client.id == security.client_id).first()
            if client and client.employee_id:
                if client.employee_id not in client_securities:
                    client_securities[client.employee_id] = []
                client_securities[client.employee_id].append(security)

    # Categorize loans by NDIA (Stage 1, 2, 3)
    # Based on IFRS 9 staging
    # Stage 1: 0-120 days (Current + OLEM)
    # Stage 2: 120-240 days (Substandard + Some Doubtful)
    # Stage 3: 240+ days (Most Doubtful + Loss)

    current_loans = []
    olem_loans = []
    substandard_loans = []
    doubtful_loans = []
    loss_loans = []

    # Calculate totals for each category
    current_total = 0
    olem_total = 0
    substandard_total = 0
    doubtful_total = 0
    loss_total = 0

    # Calculate provisions for each category
    current_provision = 0
    olem_provision = 0
    substandard_provision = 0
    doubtful_provision = 0
    loss_provision = 0

    # Summary metrics
    total_lgd = 0
    total_pd = 0
    total_ead_percentage = 0
    total_loans = 0

    for loan in loans:
        # Find securities for this loan's client
        client_securities_list = client_securities.get(loan.employee_id, [])

        if loan.ndia is None:
            # If NDIA is not available, try to calculate it from accumulated arrears and monthly installment
            if (
                loan.accumulated_arrears
                and loan.monthly_installment
                and loan.monthly_installment > 0
            ):
                ndia = int(
                    (loan.accumulated_arrears / loan.monthly_installment) * 30
                )  # Convert months to days
            else:
                ndia = 0
        else:
            ndia = loan.ndia

        # Calculate EIR for the loan
        eir = calculate_effective_interest_rate(
            loan_amount=loan.loan_amount,
            monthly_installment=loan.monthly_installment,
            loan_term=loan.loan_term,
        )

        # Calculate LGD for the loan using client's securities
        lgd = calculate_loss_given_default(loan, client_securities_list)

        # Calculate PD for the loan
        pd = calculate_probability_of_default(loan, ndia)

        # Calculate EAD percentage
        ead_percentage = calculate_exposure_at_default_percentage(loan, reporting_date)

        # Calculate ECL for the loan
        ecl = calculate_marginal_ecl(loan, pd, lgd, eir, reporting_date)

        # Update summary metrics
        total_lgd += lgd
        total_pd += pd
        total_ead_percentage += ead_percentage
        total_loans += 1

        # Categorize loan by NDIA
        if ndia < 30:  # Current: 0-30 days
            current_loans.append(loan)
            current_total += loan.outstanding_loan_balance or 0
            current_provision += ecl
        elif ndia < 120:  # OLEM: 31-120 days
            olem_loans.append(loan)
            olem_total += loan.outstanding_loan_balance or 0
            olem_provision += ecl
        elif ndia < 180:  # Substandard: 121-180 days
            substandard_loans.append(loan)
            substandard_total += loan.outstanding_loan_balance or 0
            substandard_provision += ecl
        elif ndia < 240:  # Doubtful: 181-240 days
            doubtful_loans.append(loan)
            doubtful_total += loan.outstanding_loan_balance or 0
            doubtful_provision += ecl
        else:  # Loss: 240+ days
            loss_loans.append(loan)
            loss_total += loan.outstanding_loan_balance or 0
            loss_provision += ecl

    # Calculate averages for summary metrics
    avg_lgd = total_lgd / total_loans if total_loans > 0 else 0
    avg_pd = total_pd / total_loans if total_loans > 0 else 0
    avg_ead_percentage = total_ead_percentage / total_loans if total_loans > 0 else 0

    # Calculate total loan value and provision amount
    total_loan_value = (
        current_total + olem_total + substandard_total + doubtful_total + loss_total
    )
    total_provision = (
        current_provision
        + olem_provision
        + substandard_provision
        + doubtful_provision
        + loss_provision
    )

    # Calculate provision percentage
    provision_percentage = (
        (total_provision / total_loan_value * 100) if total_loan_value > 0 else 0
    )

    # Construct response
    response = ECLSummary(
        portfolio_id=portfolio_id,
        calculation_date=reporting_date.strftime("%Y-%m-%d"),
        current=ECLCategoryData(
            num_loans=len(current_loans),
            total_loan_value=current_total,
            provision_amount=current_provision,
        ),
        olem=ECLCategoryData(
            num_loans=len(olem_loans),
            total_loan_value=olem_total,
            provision_amount=olem_provision,
        ),
        substandard=ECLCategoryData(
            num_loans=len(substandard_loans),
            total_loan_value=substandard_total,
            provision_amount=substandard_provision,
        ),
        doubtful=ECLCategoryData(
            num_loans=len(doubtful_loans),
            total_loan_value=doubtful_total,
            provision_amount=doubtful_provision,
        ),
        loss=ECLCategoryData(
            num_loans=len(loss_loans),
            total_loan_value=loss_total,
            provision_amount=loss_provision,
        ),
        summary_metrics=ECLSummaryMetrics(
            pd=round(avg_pd, 1),
            lgd=round(avg_lgd, 1),
            ead=round(avg_ead_percentage, 1),
            total_provision=round(total_provision, 2),
            provision_percentage=round(provision_percentage, 1),
        ),
    )

    return response


@router.post(
    "/{portfolio_id}/calculate-local-impairment", response_model=LocalImpairmentSummary
)
def calculate_local_impairment(
    portfolio_id: int,
    config: ImpairmentConfig = Body(...),
    reporting_date: Optional[date] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Calculate local impairment based on frontend-provided configuration.

    Expects a configuration object with day ranges and provision rates for each category:
    - Current: E.g., "0-30" days
    - OLEM (On Lender's Monitoring): E.g., "31-90" days
    - Substandard: E.g., "91-180" days
    - Doubtful: E.g., "181-359" days
    - Loss: E.g., "360+" days (typically 100% provision)
    """
    # Verify portfolio exists and belongs to current user
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
        .first()
    )

    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )

    # Use provided reporting date or default to current date
    if not reporting_date:
        reporting_date = datetime.now().date()

    try:
        # Get all loans in the portfolio
        all_loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()

        # Calculate the impairment summary
        impairment_summary = calculate_impairment_summary(
            portfolio_id=portfolio_id,
            loans=all_loans,
            config=config,
            reporting_date=reporting_date,
        )

        return impairment_summary

    except ValueError as e:
        # Handle errors from invalid day range formats
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
