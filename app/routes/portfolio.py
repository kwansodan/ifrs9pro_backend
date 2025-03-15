from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, Form
from sqlalchemy.orm import Session, joinedload
from typing import List, Optional
import pandas as pd
import io
from app.database import get_db
from app.models import Portfolio, User
from app.auth.utils import get_current_active_user
from app.models import (
    Portfolio,
    User,
    AssetType,
    CustomerType,
    FundingSource,
    DataSource,
    Loan,
)
from app.schemas import (
    PortfolioCreate,
    PortfolioUpdate,
    PortfolioResponse,
    PortfolioList,
    PortfolioWithLoansResponse
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


@router.get("/{portfolio_id}", response_model=PortfolioWithLoansResponse)
def get_portfolio(
    portfolio_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Retrieve a specific portfolio by ID including its loans.
    """
    # Query the portfolio with joined loans
    portfolio = (
        db.query(Portfolio)
        .options(joinedload(Portfolio.loans))  # Use joinedload to eagerly load the loans
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
        .first()
    )
    
    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )
    
    return portfolio

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
    if not any([loan_details, loan_guarantee_data, loan_collateral_data, historical_repayments_data]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one file must be provided"
        )
    
    # Verify portfolio exists and belongs to current user
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
        .first()
    )

    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail="Portfolio not found"
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
                'Loan No.': 'loan_no',
                'Employee Id': 'employee_id',
                'Employee Name': 'employee_name',
                'Employer': 'employer',
                'Loan Issue Date': 'loan_issue_date',
                'Deduction Start Period': 'deduction_start_period',
                'Submission Period': 'submission_period',
                'Maturity Period': 'maturity_period',
                'Location Code': 'location_code',
                'Dalex Paddy': 'dalex_paddy',
                'Team Leader': 'team_leader',
                'Loan Type': 'loan_type',
                'Loan Amount': 'loan_amount',
                'Loan Term': 'loan_term',
                'Administrative Fees': 'administrative_fees',
                'Total Interest': 'total_interest',
                'Total Collectible': 'total_collectible',
                'Net Loan Amount': 'net_loan_amount',
                'Monthly Installment': 'monthly_installment',
                'Principal Due': 'principal_due',
                'Interest Due': 'interest_due',
                'Total Due': 'total_due',
                'Principal Paid': 'principal_paid',
                'Interest Paid': 'interest_paid',
                'Total Paid': 'total_paid',
                'Principal Paid2': 'principal_paid2',
                'Interest Paid2': 'interest_paid2',
                'Total Paid2': 'total_paid2',
                'Paid': 'paid',
                'Cancelled': 'cancelled',
                'Outstanding Loan Balance': 'outstanding_loan_balance',
                'Accumulated Arrears': 'accumulated_arrears',
                'NDIA': 'ndia',
                'Prevailing Posted Repayment': 'prevailing_posted_repayment',
                'Prevailing Due Payment': 'prevailing_due_payment',
                'Current Missed Deduction': 'current_missed_deduction',
                'Admin Charge': 'admin_charge',
                'Recovery Rate': 'recovery_rate',
                'Deduction Status': 'deduction_status'
            }
            
            # Rename columns based on mapping
            df = df.rename(columns=column_mapping)
            
            # Convert date columns to appropriate format
            date_columns = ['loan_issue_date', 'maturity_period']
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Special handling for period columns (they appear to be in Month-YY format)
            period_columns = ['deduction_start_period', 'submission_period']
            for col in period_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce', format='%b-%y')
            
            # Convert boolean columns
            boolean_columns = ['paid', 'cancelled']
            for col in boolean_columns:
                if col in df.columns:
                    df[col] = df[col].map({'Yes': True, 'No': False})
            
            # Process and insert each row
            rows_processed = 0
            rows_skipped = 0
            for index, row in df.iterrows():
                try:
                    # Check if loan already exists
                    existing_loan = db.query(Loan).filter(Loan.loan_no == row.get('loan_no')).first()
                    
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
                "filename": loan_details.filename
            }
        except Exception as e:
            results["loan_details"] = {
                "status": "error",
                "message": str(e),
                "filename": loan_details.filename
            }
    
    # Process loan guarantee data file
    if loan_guarantee_data:
        try:
            content = await loan_guarantee_data.read()
            df = pd.read_excel(io.BytesIO(content))
            # Process the data
            results["loan_guarantee_data"] = {
                "status": "success",
                "rows_processed": len(df),
                "filename": loan_guarantee_data.filename
            }
        except Exception as e:
            results["loan_guarantee_data"] = {
                "status": "error",
                "message": str(e),
                "filename": loan_guarantee_data.filename
            }
    
    # Process loan collateral data file
    if loan_collateral_data:
        try:
            content = await loan_collateral_data.read()
            df = pd.read_excel(io.BytesIO(content))
            # Process the data
            results["loan_collateral_data"] = {
                "status": "success",
                "rows_processed": len(df),
                "filename": loan_collateral_data.filename
            }
        except Exception as e:
            results["loan_collateral_data"] = {
                "status": "error",
                "message": str(e),
                "filename": loan_collateral_data.filename
            }
    
    # Process historical repayments data file
    if historical_repayments_data:
        try:
            content = await historical_repayments_data.read()
            df = pd.read_excel(io.BytesIO(content))
            # Process the data
            results["historical_repayments_data"] = {
                "status": "success",
                "rows_processed": len(df),
                "filename": historical_repayments_data.filename
            }
        except Exception as e:
            results["historical_repayments_data"] = {
                "status": "error",
                "message": str(e),
                "filename": historical_repayments_data.filename
            }
    
    return {
        "portfolio_id": portfolio_id,
        "results": results
    }
