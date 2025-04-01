import logging
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
    is_in_range,
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
    QualityIssue,
    StagingResult,
    CalculationResult
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
    QualityIssueResponse,
    QualityIssueCreate,
    QualityIssueUpdate,
    QualityIssueComment,
    QualityIssueCommentCreate,
    QualityCheckSummary,
    ECLConfig,
    StagingResponse,
    ECLStagingConfig,
    LocalImpairmentConfig,
    CalculatorResponse,
    EADInput,
    PDInput,
    EIRInput,
    StagedLoans,
    ProvisionRateConfig,
    ECLComponentConfig,
    LoanStageInfo,
    CategoryData,
    PortfolioWithSummaryResponse,
    OverviewModel,
    CustomerSummaryModel,
    PortfolioLatestResults,


)
from app.auth.utils import get_current_active_user
from app.utils.quality_checks import create_quality_issues_if_needed
from app.utils.processors import (
    process_loan_details,
    process_loan_collateral,
    process_loan_guarantees,
)

logger = logging.getLogger(__name__)
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
        credit_risk_reserve=portfolio.credit_risk_reserve,
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

    # Apply pagination and get portfolios
    portfolios = query.offset(skip).limit(limit).all()
    
    # Convert to response objects
    response_items = []
    for portfolio in portfolios:
        # Check if portfolio has loans
        has_data = db.query(Loan).filter(Loan.portfolio_id == portfolio.id).limit(1).count() > 0
        
        # Convert to PortfolioResponse and set has_ingested_data
        portfolio_dict = portfolio.__dict__.copy()
        if '_sa_instance_state' in portfolio_dict:
            del portfolio_dict['_sa_instance_state']
            
            
        # Create response object with the data flag
        portfolio_response = PortfolioResponse(**portfolio_dict, has_ingested_data=has_data)
        response_items.append(portfolio_response)

    return {"items": response_items, "total": total}

@router.get("/{portfolio_id}", response_model=PortfolioWithSummaryResponse)
def get_portfolio(
    portfolio_id: int,
    include_quality_issues: bool = False,
    include_report_history: bool = False,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Retrieve a specific portfolio by ID including overview, customer summary, quality checks,
    latest staging and calculation results, and optionally report history.
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
    has_ingested_data = total_loans > 0  # Simple check if the portfolio has any loans
    
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
    # Determine active customers
    active_customers = sum(
        1
        for client in portfolio.clients
        if any(
            loan.paid is False
            for loan in portfolio.loans
            if hasattr(loan, "employee_id") and loan.employee_id == client.employee_id
        )
    )

    # Run quality checks and create issues if necessary
    quality_counts = create_quality_issues_if_needed(db, portfolio_id)

    quality_check_summary = QualityCheckSummary(
        duplicate_names=quality_counts["duplicate_names"],
        duplicate_addresses=quality_counts["duplicate_addresses"],
        missing_repayment_data=quality_counts["missing_repayment_data"],
        total_issues=quality_counts["total_issues"],
        high_severity_issues=quality_counts["high_severity_issues"],
        open_issues=quality_counts["open_issues"],
    )

    # Get quality issues if requested
    quality_issues = []
    if include_quality_issues:
        quality_issues = (
            db.query(QualityIssue)
            .filter(QualityIssue.portfolio_id == portfolio_id)
            .order_by(QualityIssue.severity.desc(), QualityIssue.created_at.desc())
            .all()
        )

    # Get report history if requested
    report_history = []
    if include_report_history:
        report_history = (
            db.query(Report)
            .filter(Report.portfolio_id == portfolio_id)
            .order_by(Report.created_at.desc())
            .all()
        )

    # Get latest staging results
    latest_local_impairment_staging = (
        db.query(StagingResult)
        .filter(
            StagingResult.portfolio_id == portfolio_id,
            StagingResult.staging_type == "local_impairment"
        )
        .order_by(StagingResult.created_at.desc())
        .first()
    )
    
    latest_ecl_staging = (
        db.query(StagingResult)
        .filter(
            StagingResult.portfolio_id == portfolio_id,
            StagingResult.staging_type == "ecl"
        )
        .order_by(StagingResult.created_at.desc())
        .first()
    )
      # Get latest staging results
    latest_local_impairment_staging = (
        db.query(StagingResult)
        .filter(
            StagingResult.portfolio_id == portfolio_id,
            StagingResult.staging_type == "local_impairment"
        )
        .order_by(StagingResult.created_at.desc())
        .first()
    )
    
    latest_ecl_staging = (
        db.query(StagingResult)
        .filter(
            StagingResult.portfolio_id == portfolio_id,
            StagingResult.staging_type == "ecl"
        )
        .order_by(StagingResult.created_at.desc())
        .first()
    )
    
    # Get latest calculation results
    latest_local_impairment_calculation = (
        db.query(CalculationResult)
        .filter(
            CalculationResult.portfolio_id == portfolio_id,
            CalculationResult.calculation_type == "local_impairment"
        )
        .order_by(CalculationResult.created_at.desc())
        .first()
    )
    
    latest_ecl_calculation = (
        db.query(CalculationResult)
        .filter(
            CalculationResult.portfolio_id == portfolio_id,
            CalculationResult.calculation_type == "ecl"
        )
        .order_by(CalculationResult.created_at.desc())
        .first()
    )
    # Create staging summary from the staging results
    staging_summary = None
    if latest_ecl_staging or latest_local_impairment_staging:
        staging_summary = {}
        
        # Process ECL staging if available
        if latest_ecl_staging:
            ecl_result = latest_ecl_staging.result_summary
            
            # Extract data for each stage
            stage_1_data = None
            stage_2_data = None
            stage_3_data = None
            
            # Check if we have the newer format with detailed loan data
            if "loans" in ecl_result:
                # Calculate totals from individual loan data
                stage_1_loans = [loan for loan in ecl_result["loans"] if loan.get("stage") == "Stage 1"]
                stage_2_loans = [loan for loan in ecl_result["loans"] if loan.get("stage") == "Stage 2"]
                stage_3_loans = [loan for loan in ecl_result["loans"] if loan.get("stage") == "Stage 3"]
                
                stage_1_balance = sum(float(loan.get("outstanding_loan_balance", 0)) for loan in stage_1_loans)
                stage_2_balance = sum(float(loan.get("outstanding_loan_balance", 0)) for loan in stage_2_loans)
                stage_3_balance = sum(float(loan.get("outstanding_loan_balance", 0)) for loan in stage_3_loans)
                
                stage_1_data = {
                    "num_loans": len(stage_1_loans),
                    "outstanding_loan_balance": stage_1_balance
                }
                
                stage_2_data = {
                    "num_loans": len(stage_2_loans),
                    "outstanding_loan_balance": stage_2_balance
                }
                
                stage_3_data = {
                    "num_loans": len(stage_3_loans),
                    "outstanding_loan_balance": stage_3_balance
                }
            else:
                # Use summary statistics if available
                stage_1_data = {
                    "num_loans": ecl_result.get("stage1_count", 0),
                    "outstanding_loan_balance": ecl_result.get("stage1_total", 0)
                }
                
                stage_2_data = {
                    "num_loans": ecl_result.get("stage2_count", 0),
                    "outstanding_loan_balance": ecl_result.get("stage2_total", 0)
                }
                
                stage_3_data = {
                    "num_loans": ecl_result.get("stage3_count", 0),
                    "outstanding_loan_balance": ecl_result.get("stage3_total", 0)
                }
            
            # Create ECL staging summary
            staging_summary["ecl"] = {
                "stage_1": stage_1_data,
                "stage_2": stage_2_data,
                "stage_3": stage_3_data,
                "staging_date": latest_ecl_staging.created_at
            }
        
        # Process local impairment staging if available
        if latest_local_impairment_staging:
            local_result = latest_local_impairment_staging.result_summary
            
            # Extract data for each category
            current_data = None
            olem_data = None
            substandard_data = None
            doubtful_data = None
            loss_data = None
            
            # Check if we have the newer format with detailed loan data
            if "loans" in local_result:
                # Calculate totals from individual loan data
                current_loans = [loan for loan in local_result["loans"] if loan.get("stage") == "Current"]
                olem_loans = [loan for loan in local_result["loans"] if loan.get("stage") == "OLEM"]
                substandard_loans = [loan for loan in local_result["loans"] if loan.get("stage") == "Substandard"]
                doubtful_loans = [loan for loan in local_result["loans"] if loan.get("stage") == "Doubtful"]
                loss_loans = [loan for loan in local_result["loans"] if loan.get("stage") == "Loss"]
                
                current_balance = sum(float(loan.get("outstanding_loan_balance", 0)) for loan in current_loans)
                olem_balance = sum(float(loan.get("outstanding_loan_balance", 0)) for loan in olem_loans)
                substandard_balance = sum(float(loan.get("outstanding_loan_balance", 0)) for loan in substandard_loans)
                doubtful_balance = sum(float(loan.get("outstanding_loan_balance", 0)) for loan in doubtful_loans)
                loss_balance = sum(float(loan.get("outstanding_loan_balance", 0)) for loan in loss_loans)
                
                current_data = {
                    "num_loans": len(current_loans),
                    "outstanding_loan_balance": current_balance
                }
                
                olem_data = {
                    "num_loans": len(olem_loans),
                    "outstanding_loan_balance": olem_balance
                }
                
                substandard_data = {
                    "num_loans": len(substandard_loans),
                    "outstanding_loan_balance": substandard_balance
                }
                
                doubtful_data = {
                    "num_loans": len(doubtful_loans),
                    "outstanding_loan_balance": doubtful_balance
                }
                
                loss_data = {
                    "num_loans": len(loss_loans),
                    "outstanding_loan_balance": loss_balance
                }
            else:
                # Use summary statistics if available
                current_data = {
                    "num_loans": local_result.get("current_count", 0),
                    "outstanding_loan_balance": local_result.get("current_total", 0)
                }
                
                olem_data = {
                    "num_loans": local_result.get("olem_count", 0),
                    "outstanding_loan_balance": local_result.get("olem_total", 0)
                }
                
                substandard_data = {
                    "num_loans": local_result.get("substandard_count", 0),
                    "outstanding_loan_balance": local_result.get("substandard_total", 0)
                }
                
                doubtful_data = {
                    "num_loans": local_result.get("doubtful_count", 0),
                    "outstanding_loan_balance": local_result.get("doubtful_total", 0)
                }
                
                loss_data = {
                    "num_loans": local_result.get("loss_count", 0),
                    "outstanding_loan_balance": local_result.get("loss_total", 0)
                }
            
            # Create local impairment staging summary
            staging_summary["local_impairment"] = {
                "current": current_data,
                "olem": olem_data,
                "substandard": substandard_data,
                "doubtful": doubtful_data,
                "loss": loss_data,
                "staging_date": latest_local_impairment_staging.created_at
            }

    # Calculate summary for calculations
    calculation_summary = None
    if latest_ecl_calculation or latest_local_impairment_calculation:
        # Get the total loan value from earlier calculation
        total_value = round(total_loan_value, 2)
        
        # Initialize calculation summary
        calculation_summary = {
            "total_loan_value": total_value,
        }
        
        # Add ECL detailed summary if available
        if latest_ecl_calculation:
            # Get the detailed result summary
            ecl_summary = latest_ecl_calculation.result_summary
            
            # Extract stage-specific data from the result_summary
            stage_1_data = {
                "num_loans": ecl_summary.get("stage1_count", 0),
                "total_loan_value": ecl_summary.get("stage1_total", 0),
                "provision_amount": ecl_summary.get("stage1_provision", 0),
                "provision_rate": ecl_summary.get("stage1_provision_rate", 0),
            }
            
            stage_2_data = {
                "num_loans": ecl_summary.get("stage2_count", 0),
                "total_loan_value": ecl_summary.get("stage2_total", 0),
                "provision_amount": ecl_summary.get("stage2_provision", 0),
                "provision_rate": ecl_summary.get("stage2_provision_rate", 0),
            }
            
            stage_3_data = {
                "num_loans": ecl_summary.get("stage3_count", 0),
                "total_loan_value": ecl_summary.get("stage3_total", 0),
                "provision_amount": ecl_summary.get("stage3_provision", 0),
                "provision_rate": ecl_summary.get("stage3_provision_rate", 0),
            }
            
            calculation_summary["ecl"] = {
                "stage_1": stage_1_data,
                "stage_2": stage_2_data,
                "stage_3": stage_3_data,
                "total_provision": float(latest_ecl_calculation.total_provision),
                "provision_percentage": float(latest_ecl_calculation.provision_percentage),
                "calculation_date": latest_ecl_calculation.created_at
            }
            
        # Add local impairment detailed summary if available
        if latest_local_impairment_calculation:
            # Get the detailed result summary
            local_summary = latest_local_impairment_calculation.result_summary
            
            # Extract category-specific data from the result_summary
            current_data = {
                "num_loans": local_summary.get("current_count", 0),
                "total_loan_value": local_summary.get("current_total", 0),
                "provision_amount": local_summary.get("current_provision", 0),
                "provision_rate": local_summary.get("current_provision_rate", 0),
            }
            
            olem_data = {
                "num_loans": local_summary.get("olem_count", 0),
                "total_loan_value": local_summary.get("olem_total", 0),
                "provision_amount": local_summary.get("olem_provision", 0),
                "provision_rate": local_summary.get("olem_provision_rate", 0),
            }
            
            substandard_data = {
                "num_loans": local_summary.get("substandard_count", 0),
                "total_loan_value": local_summary.get("substandard_total", 0),
                "provision_amount": local_summary.get("substandard_provision", 0),
                "provision_rate": local_summary.get("substandard_provision_rate", 0),
            }
            
            doubtful_data = {
                "num_loans": local_summary.get("doubtful_count", 0),
                "total_loan_value": local_summary.get("doubtful_total", 0),
                "provision_amount": local_summary.get("doubtful_provision", 0),
                "provision_rate": local_summary.get("doubtful_provision_rate", 0),
            }
            
            loss_data = {
                "num_loans": local_summary.get("loss_count", 0),
                "total_loan_value": local_summary.get("loss_total", 0),
                "provision_amount": local_summary.get("loss_provision", 0),
                "provision_rate": local_summary.get("loss_provision_rate", 0),
            }
            
            calculation_summary["local_impairment"] = {
                "current": current_data,
                "olem": olem_data,
                "substandard": substandard_data,
                "doubtful": doubtful_data,
                "loss": loss_data,
                "total_provision": float(latest_local_impairment_calculation.total_provision),
                "provision_percentage": float(latest_local_impairment_calculation.provision_percentage),
                "calculation_date": latest_local_impairment_calculation.created_at
            }

    # Convert repayment_source boolean to string representation
    # repayment_source_str = "At Source" if portfolio.repayment_source else "Manual Transfer"

    # Create response dictionary with portfolio data and summaries
    response = PortfolioWithSummaryResponse(
        id=portfolio.id,
        name=portfolio.name,
        description=portfolio.description,
        asset_type=portfolio.asset_type,
        customer_type=portfolio.customer_type,
        funding_source=portfolio.funding_source,
        data_source=portfolio.data_source,
        repayment_source=portfolio.repayment_source,
        credit_risk_reserve=portfolio.credit_risk_reserve,
        loan_assets=portfolio.loan_assets,
        ecl_impairment_account=portfolio.ecl_impairment_account,
        has_ingested_data=has_ingested_data, 
        created_at=portfolio.created_at,
        updated_at=portfolio.updated_at,
        overview=OverviewModel(
            total_loans=total_loans,
            total_loan_value=round(total_loan_value, 2),
            average_loan_amount=round(average_loan_amount, 2),
            total_customers=total_customers,
        ),
        customer_summary=CustomerSummaryModel(
            individual_customers=individual_customers,
            institutions=institutions,
            mixed=mixed,
            active_customers=active_customers,
        ),
        quality_check=quality_check_summary,
        quality_issues=quality_issues if include_quality_issues else None,
        report_history=report_history if include_report_history else None,
        calculation_summary=calculation_summary,
        staging_summary=staging_summary,
    )

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
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Ingest Excel files containing portfolio data and automatically perform both types of staging.

    Accepts up to three Excel files:
    - loan_details: Primary loan information
    - loan_guarantee_data: Information about loan guarantees
    - loan_collateral_data: Information about loan collateral
    
    The function automatically performs both ECL and local impairment staging after successful ingestion.
    """
    # Check if at least one file is provided
    if not any([loan_details, loan_guarantee_data, loan_collateral_data]):
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

    # Explicitly rollback any existing transaction to start fresh
    db.rollback()

    # Process files one by one with separate transactions for each
    try:
        # Process loan details file
        if loan_details:
            try:
                results["loan_details"] = await process_loan_details(
                    loan_details, portfolio_id, db
                )
                db.commit()
            except Exception as e:
                db.rollback()
                logger.error(f"Error processing loan details: {str(e)}")
                results["loan_details"] = {"status": "error", "message": str(e)}

        # Process loan guarantee data file
        if loan_guarantee_data:
            try:
                results["loan_guarantee_data"] = await process_loan_guarantees(
                    loan_guarantee_data, portfolio_id, db
                )
                db.commit()
            except Exception as e:
                db.rollback()
                logger.error(f"Error processing loan guarantees: {str(e)}")
                results["loan_guarantee_data"] = {"status": "error", "message": str(e)}

        # Process loan collateral data file
        if loan_collateral_data:
            try:
                results["loan_collateral_data"] = await process_loan_collateral(
                    loan_collateral_data, portfolio_id, db
                )
                db.commit()
            except Exception as e:
                db.rollback()
                logger.error(f"Error processing loan collateral: {str(e)}")
                results["loan_collateral_data"] = {"status": "error", "message": str(e)}

        # Only perform staging if at least one file was processed successfully
        if any(result.get("status") == "success" for result in results.values() if isinstance(result, dict)):
            # Automatically perform both types of staging
            staging_results = {}
            
            # 1. Perform ECL staging
            try:
                # Create default ECL staging config
                ecl_config = ECLStagingConfig(
                    stage_1={"days_range": "0-120"},
                    stage_2={"days_range": "120-240"},
                    stage_3={"days_range": "240+"}
                )
                
                # Call the staging function
                ecl_staging = stage_loans_ecl(
                    portfolio_id=portfolio_id,
                    config=ecl_config,
                    db=db,
                    current_user=current_user
                )
                
                staging_results["ecl"] = {
                    "status": "success",
                    "loans_staged": len(ecl_staging.loans)
                }
                
            except Exception as e:
                db.rollback()
                logger.error(f"Error during ECL staging: {str(e)}")
                staging_results["ecl"] = {
                    "status": "error",
                    "error": str(e)
                }
            
            # 2. Perform local impairment staging
            try:
                # Create default local impairment config
                local_config = LocalImpairmentConfig(
                    current={"days_range": "0-30", "rate": 1},
                    olem={"days_range": "31-90", "rate": 5},
                    substandard={"days_range": "91-180", "rate": 25},
                    doubtful={"days_range": "181-365", "rate": 50},
                    loss={"days_range": "366+", "rate": 100}
                )
                
                # Call the staging function
                local_staging = stage_loans_local_impairment(
                    portfolio_id=portfolio_id,
                    config=local_config,
                    db=db,
                    current_user=current_user
                )
                
                staging_results["local_impairment"] = {
                    "status": "success",
                    "loans_staged": len(local_staging.loans)
                }
                
            except Exception as e:
                db.rollback()
                logger.error(f"Error during local impairment staging: {str(e)}")
                staging_results["local_impairment"] = {
                    "status": "error",
                    "error": str(e)
                }
            
            # Add staging results to the response
            results["staging"] = staging_results

        return {
            "portfolio_id": portfolio_id, 
            "results": results, 
            "status": "success" if not any(result.get("status") == "error" for result in results.values() if isinstance(result, dict)) else "partial_success"
        }

    except Exception as e:
        # Rollback in case of a general error
        db.rollback()
        logger.error(f"General error during ingestion: {str(e)}")
        return {
            "portfolio_id": portfolio_id,
            "results": {"error": str(e)},
            "status": "error",
        }
@router.get("/{portfolio_id}/calculate-ecl", response_model=ECLSummary)
def calculate_ecl_provision(
    portfolio_id: int,
    reporting_date: Optional[date] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Calculate ECL provisions directly from a portfolio ID.
    This route uses the latest staging data stored in the database.
    """
    # Use provided reporting date or default to current date
    if not reporting_date:
        reporting_date = datetime.now().date()

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
    
    # Get the latest ECL staging result
    latest_staging = (
        db.query(StagingResult)
        .filter(
            StagingResult.portfolio_id == portfolio_id,
            StagingResult.staging_type == "ecl"
        )
        .order_by(StagingResult.created_at.desc())
        .first()
    )
    
    if not latest_staging:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail="No ECL staging found. Please stage loans first."
        )
    
    # Extract config from the staging result
    config = latest_staging.config
    if not config:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Invalid staging configuration"
        )
    
    # Get the loan staging data from the result_summary
    # Try to handle both formats - either direct "loans" key or individual loan info
    staging_data = []
    if "loans" in latest_staging.result_summary:
        # New format with detailed loan data
        staging_data = latest_staging.result_summary["loans"]
    else:
        # Without detailed loan data, we need to re-stage based on summary stats
        logger.warning("No detailed loan staging data in result_summary, reconstructing staging using database query")
        
        # Recreate basic staging info from loan query using the config
        try:
            stage_1_range = parse_days_range(config["stage_1"]["days_range"])
            stage_2_range = parse_days_range(config["stage_2"]["days_range"])
            stage_3_range = parse_days_range(config["stage_3"]["days_range"])
        except (KeyError, ValueError) as e:
            logger.error(f"Error parsing stage ranges: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Could not parse staging configuration: {str(e)}"
            )
            
        # Get the loans
        loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
        
        # Re-stage them
        for loan in loans:
            if loan.ndia is None:
                continue
                
            ndia = loan.ndia
            
            # Stage based on NDIA
            if is_in_range(ndia, stage_1_range):
                stage = "Stage 1"
            elif is_in_range(ndia, stage_2_range):
                stage = "Stage 2"
            elif is_in_range(ndia, stage_3_range):
                stage = "Stage 3"
            else:
                stage = "Stage 3"
                
            # Create a basic staging entry
            staging_data.append({
                "loan_id": loan.id,
                "employee_id": loan.employee_id,
                "stage": stage,
                "outstanding_loan_balance": float(loan.outstanding_loan_balance) if loan.outstanding_loan_balance else 0,
            })
            
    if not staging_data:
        # If we still don't have staging data, return an error
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No loan staging data found. Please re-run the staging process."
        )
        
    # Get all loans in the portfolio
    loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
    
    # Create a map of loan_id to loan object for faster lookup
    loan_map = {loan.id: loan for loan in loans}

    # Initialize category tracking
    stage_1_loans = []
    stage_2_loans = []
    stage_3_loans = []

    # Calculate totals for each category
    stage_1_total = 0
    stage_2_total = 0
    stage_3_total = 0

    # Calculate provisions for each category
    stage_1_provision = 0
    stage_2_provision = 0
    stage_3_provision = 0

    # Summary metrics
    total_lgd = 0
    total_pd = 0
    total_ead_percentage = 0
    total_loans = 0

    # Get all client IDs to fetch securities
    client_ids = {loan.employee_id for loan in loans if loan.employee_id}

    # Get securities for all clients
    client_securities = {}
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

    # Process loans using staging data
    for stage_info in staging_data:
        loan_id = stage_info.get("loan_id")
        stage = stage_info.get("stage")
        
        if not loan_id or not stage:
            logger.warning(f"Missing loan_id or stage in staging data: {stage_info}")
            continue
            
        loan = loan_map.get(loan_id)
        if not loan or loan.outstanding_loan_balance is None:
            logger.warning(f"Loan {loan_id} not found or has no outstanding balance")
            continue
            
        outstanding_loan_balance = loan.outstanding_loan_balance
        
        # Get securities for this loan's client
        client_securities_list = client_securities.get(loan.employee_id, [])

        # Calculate ECL components for the loan
        lgd = calculate_loss_given_default(loan, client_securities_list)
        pd = calculate_probability_of_default(loan, db)
        ead_percentage = calculate_exposure_at_default_percentage(loan, reporting_date)
        ecl = calculate_marginal_ecl(loan, ead_percentage, pd, lgd)

        # Update stage totals based on the assigned stage
        if stage == "Stage 1":
            stage_1_loans.append(loan)
            stage_1_total += outstanding_loan_balance
            stage_1_provision += ecl
        elif stage == "Stage 2":
            stage_2_loans.append(loan)
            stage_2_total += outstanding_loan_balance
            stage_2_provision += ecl
        elif stage == "Stage 3":
            stage_3_loans.append(loan)
            stage_3_total += outstanding_loan_balance
            stage_3_provision += ecl
        else:
            # Default to Stage 3 if stage is something unexpected
            logger.warning(f"Unexpected stage '{stage}' for loan {loan_id}, treating as Stage 3")
            stage_3_loans.append(loan)
            stage_3_total += outstanding_loan_balance
            stage_3_provision += ecl

        # Update summary statistics
        total_lgd += lgd
        total_pd += pd
        total_ead_percentage += ead_percentage
        total_loans += 1

    # Calculate averages for summary metrics
    avg_lgd = total_lgd / total_loans if total_loans > 0 else 0
    avg_pd = total_pd / total_loans if total_loans > 0 else 0
    avg_ead_percentage = total_ead_percentage / total_loans if total_loans > 0 else 0

    # Calculate total loan value and provision amount
    total_loan_value = stage_1_total + stage_2_total + stage_3_total
    total_provision = stage_1_provision + stage_2_provision + stage_3_provision

    # Calculate provision percentage
    provision_percentage = (
        (Decimal(total_provision) / Decimal(total_loan_value) * 100)
        if total_loan_value > 0
        else 0
    )

    # Calculate effective provision rates
    stage_1_rate = (
        Decimal(stage_1_provision) / Decimal(stage_1_total) if stage_1_total > 0 else 0
    )
    stage_2_rate = (
        Decimal(stage_2_provision) / Decimal(stage_2_total) if stage_2_total > 0 else 0
    )
    stage_3_rate = (
        Decimal(stage_3_provision) / Decimal(stage_3_total) if stage_3_total > 0 else 0
    )

    # Create a new CalculationResult record
    calculation_result = CalculationResult(
        portfolio_id=portfolio_id,
        calculation_type="ecl",
        config=config,  # Use the config from staging
        result_summary={
            "stage1_count": len(stage_1_loans),
            "stage1_total": float(stage_1_total),
            "stage1_provision": float(stage_1_provision),
            "stage1_provision_rate": float(stage_1_rate),
            "stage2_count": len(stage_2_loans),
            "stage2_total": float(stage_2_total),
            "stage2_provision": float(stage_2_provision),
            "stage2_provision_rate": float(stage_2_rate),
            "stage3_count": len(stage_3_loans),
            "stage3_total": float(stage_3_total),
            "stage3_provision": float(stage_3_provision),
            "stage3_provision_rate": float(stage_3_rate),
            "total_loans": total_loans
        },
        total_provision=float(total_provision),
        provision_percentage=float(provision_percentage),
        reporting_date=reporting_date
    )
    db.add(calculation_result)
    db.commit()

    # Construct response
    response = ECLSummary(
        portfolio_id=portfolio_id,
        calculation_date=reporting_date.strftime("%Y-%m-%d"),
        stage_1=CategoryData(
            num_loans=len(stage_1_loans),
            total_loan_value=round(stage_1_total, 2),
            provision_amount=round(stage_1_provision, 2),
            provision_rate=round(stage_1_rate, 4),
        ),
        stage_2=CategoryData(
            num_loans=len(stage_2_loans),
            total_loan_value=round(stage_2_total, 2),
            provision_amount=round(stage_2_provision, 2),
            provision_rate=round(stage_2_rate, 4),
        ),
        stage_3=CategoryData(
            num_loans=len(stage_3_loans),
            total_loan_value=round(stage_3_total, 2),
            provision_amount=round(stage_3_provision, 2),
            provision_rate=round(stage_3_rate, 4),
        ),
        summary_metrics=ECLSummaryMetrics(
            avg_pd=round(avg_pd, 4),
            avg_lgd=round(avg_lgd, 4),
            avg_ead=round(avg_ead_percentage, 4),
            total_provision=round(total_provision, 2),
            provision_percentage=round(provision_percentage, 2),
        ),
    )

    return response

@router.post("/{portfolio_id}/stage-loans-ecl", response_model=StagingResponse)
def stage_loans_ecl(
    portfolio_id: int,
    config: ECLStagingConfig = Body(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Classify loans in the portfolio according to ECL staging criteria (Stage 1, 2, 3).
    Stores the staging information in the database.
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
    # Parse day ranges from config
    try:
        stage_1_range = parse_days_range(config.stage_1.days_range)
        stage_2_range = parse_days_range(config.stage_2.days_range)
        stage_3_range = parse_days_range(config.stage_3.days_range)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    # Get all loans in the portfolio
    loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()

    # Stage the loans
    staged_loans = []
    staged_at = datetime.now()
    
    # Count loans in each stage
    stage1_count = 0
    stage2_count = 0
    stage3_count = 0
    total_count = 0

    for loan in loans:
        total_count += 1
        # Determine the stage
        if is_in_range(loan.ndia, stage_1_range):
            stage = "Stage 1"
            stage1_count += 1
        elif is_in_range(loan.ndia, stage_2_range):
            stage = "Stage 2"
            stage2_count += 1
        elif is_in_range(loan.ndia, stage_3_range):
            stage = "Stage 3"
            stage3_count += 1
        else:
            # Default to Stage 3 if no stage matches
            stage = "Stage 3"
            stage3_count += 1

        # Create the loan stage info for the response
        ndia_value = int(loan.ndia) if loan.ndia is not None else 0
        outstanding_loan_balance = loan.outstanding_loan_balance
        loan_issue_date = loan.loan_issue_date
        accumulated_arrears = loan.accumulated_arrears
        loan_amount = loan.loan_amount
        monthly_installment = loan.monthly_installment
        loan_term = loan.loan_term

        staged_loans.append(
            LoanStageInfo(
                loan_id=loan.id,
                employee_id=loan.employee_id,
                stage=stage,
                outstanding_loan_balance=outstanding_loan_balance,
                ndia=ndia_value,
                loan_issue_date=loan_issue_date,
                loan_amount=loan_amount,
                monthly_installment=monthly_installment,
                loan_term=loan_term,
                accumulated_arrears=accumulated_arrears,
            )
        )

    # Convert staged loans to a format that can be stored in JSON
    # We need to do this because LoanStageInfo objects aren't directly JSON serializable
    serialized_loans = []
    for loan_info in staged_loans:
        serialized_loan = {
            "loan_id": loan_info.loan_id,
            "employee_id": loan_info.employee_id,
            "stage": loan_info.stage,
            "outstanding_loan_balance": float(loan_info.outstanding_loan_balance) if loan_info.outstanding_loan_balance else 0,
            "ndia": loan_info.ndia,
            "loan_issue_date": loan_info.loan_issue_date.isoformat() if loan_info.loan_issue_date else None,
            "loan_amount": float(loan_info.loan_amount) if loan_info.loan_amount else 0,
            "monthly_installment": float(loan_info.monthly_installment) if loan_info.monthly_installment else 0,
            "loan_term": loan_info.loan_term,
            "accumulated_arrears": float(loan_info.accumulated_arrears) if loan_info.accumulated_arrears else 0,
        }
        serialized_loans.append(serialized_loan)

    # Create a new StagingResult record with the detailed loan info
    result_summary = {
        "total_loans": total_count,
        "stage1_count": stage1_count,
        "stage2_count": stage2_count,
        "stage3_count": stage3_count,
        "staged_at": staged_at.isoformat(),
        "loans": serialized_loans  # Add the detailed loan staging data
    }
    
    staging_result = StagingResult(
        portfolio_id=portfolio_id,
        staging_type="ecl",
        config=config.dict(),
        result_summary=result_summary
    )
    db.add(staging_result)
    
    # Commit the changes
    db.commit()

    return StagingResponse(loans=staged_loans)

@router.post("/{portfolio_id}/stage-loans-local", response_model=StagingResponse)
def stage_loans_local_impairment(
    portfolio_id: int,
    config: LocalImpairmentConfig = Body(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Classify loans in the portfolio according to local impairment categories:
    Current, OLEM, Substandard, Doubtful, and Loss.
    Stores the staging information in the database.
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

    # Parse day ranges from config
    try:
        current_range = parse_days_range(config.current.days_range)
        olem_range = parse_days_range(config.olem.days_range)
        substandard_range = parse_days_range(config.substandard.days_range)
        doubtful_range = parse_days_range(config.doubtful.days_range)
        loss_range = parse_days_range(config.loss.days_range)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    # Get all loans in the portfolio
    loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()

    # Stage the loans
    staged_loans = []
    staged_at = datetime.now()
    
    # Count loans in each category
    current_count = 0
    olem_count = 0
    substandard_count = 0
    doubtful_count = 0
    loss_count = 0
    total_count = 0

    for loan in loans:
        total_count += 1
        # Calculate NDIA if not available
        if loan.ndia is None:
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

        # Determine the category
        if is_in_range(ndia, current_range):
            stage = "Current"
            current_count += 1
        elif is_in_range(ndia, olem_range):
            stage = "OLEM"
            olem_count += 1
        elif is_in_range(ndia, substandard_range):
            stage = "Substandard"
            substandard_count += 1
        elif is_in_range(ndia, doubtful_range):
            stage = "Doubtful"
            doubtful_count += 1
        elif is_in_range(ndia, loss_range):
            stage = "Loss"
            loss_count += 1
        else:
            # Default to Loss if no category matches
            stage = "Loss"
            loss_count += 1

        # Create the loan stage info for the response
        ndia_value = int(loan.ndia) if loan.ndia is not None else 0
        outstanding_loan_balance = loan.outstanding_loan_balance
        loan_issue_date = loan.loan_issue_date
        accumulated_arrears = loan.accumulated_arrears
        loan_amount = loan.loan_amount
        monthly_installment = loan.monthly_installment
        loan_term = loan.loan_term

        staged_loans.append(
            LoanStageInfo(
                loan_id=loan.id,
                employee_id=loan.employee_id,
                stage=stage,
                outstanding_loan_balance=outstanding_loan_balance,
                ndia=ndia_value,
                loan_issue_date=loan_issue_date,
                loan_amount=loan_amount,
                monthly_installment=monthly_installment,
                loan_term=loan_term,
                accumulated_arrears=accumulated_arrears,
            )
        )
    
    # Convert staged loans to a format that can be stored in JSON
    serialized_loans = []
    for loan_info in staged_loans:
        serialized_loan = {
            "loan_id": loan_info.loan_id,
            "employee_id": loan_info.employee_id,
            "stage": loan_info.stage,
            "outstanding_loan_balance": float(loan_info.outstanding_loan_balance) if loan_info.outstanding_loan_balance else 0,
            "ndia": loan_info.ndia,
            "loan_issue_date": loan_info.loan_issue_date.isoformat() if loan_info.loan_issue_date else None,
            "loan_amount": float(loan_info.loan_amount) if loan_info.loan_amount else 0,
            "monthly_installment": float(loan_info.monthly_installment) if loan_info.monthly_installment else 0,
            "loan_term": loan_info.loan_term,
            "accumulated_arrears": float(loan_info.accumulated_arrears) if loan_info.accumulated_arrears else 0,
        }
        serialized_loans.append(serialized_loan)
    
    # Create a new StagingResult record with the detailed loan info
    result_summary = {
        "total_loans": total_count,
        "current_count": current_count,
        "olem_count": olem_count,
        "substandard_count": substandard_count,
        "doubtful_count": doubtful_count,
        "loss_count": loss_count,
        "staged_at": staged_at.isoformat(),
        "loans": serialized_loans  # Add the detailed loan staging data
    }
    
    staging_result = StagingResult(
        portfolio_id=portfolio_id,
        staging_type="local_impairment",
        config=config.dict(),
        result_summary=result_summary
    )
    db.add(staging_result)
    
    # Commit the changes
    db.commit()

    return StagingResponse(loans=staged_loans)


@router.get("/{portfolio_id}/calculate-local-impairment", response_model=LocalImpairmentSummary)
def calculate_local_provision(
    portfolio_id: int,
    reporting_date: Optional[date] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Calculate local impairment provisions directly from a portfolio ID.
    This route uses the latest staging data stored in the database.
    """
    # Use provided reporting date or default to current date
    if not reporting_date:
        reporting_date = datetime.now().date()

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
    
    # Get the latest local impairment staging result
    latest_staging = (
        db.query(StagingResult)
        .filter(
            StagingResult.portfolio_id == portfolio_id,
            StagingResult.staging_type == "local_impairment"
        )
        .order_by(StagingResult.created_at.desc())
        .first()
    )
    
    if not latest_staging:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail="No local impairment staging found. Please stage loans first."
        )
    
    # Extract config from the staging result
    config = latest_staging.config
    if not config:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Invalid staging configuration"
        )
    
    # Get the loan staging data from the result_summary
    # Try to handle both formats - either direct "loans" key or individual loan info
    staging_data = []
    if "loans" in latest_staging.result_summary:
        # New format with detailed loan data
        staging_data = latest_staging.result_summary["loans"]
    else:
        # Without detailed loan data, we need to re-stage based on summary stats
        logger.warning("No detailed loan staging data in result_summary, reconstructing staging using database query")
        
        # Recreate basic staging info from loan query using the config
        try:
            current_range = parse_days_range(config["current"]["days_range"])
            olem_range = parse_days_range(config["olem"]["days_range"])
            substandard_range = parse_days_range(config["substandard"]["days_range"])
            doubtful_range = parse_days_range(config["doubtful"]["days_range"])
            loss_range = parse_days_range(config["loss"]["days_range"])
        except (KeyError, ValueError) as e:
            logger.error(f"Error parsing day ranges: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Could not parse staging configuration: {str(e)}"
            )
            
        # Get the loans
        loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
        
        # Re-stage them
        for loan in loans:
            # Calculate NDIA if not available
            if loan.ndia is None:
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
                
            # Stage based on NDIA
            if is_in_range(ndia, current_range):
                stage = "Current"
            elif is_in_range(ndia, olem_range):
                stage = "OLEM"
            elif is_in_range(ndia, substandard_range):
                stage = "Substandard"
            elif is_in_range(ndia, doubtful_range):
                stage = "Doubtful"
            elif is_in_range(ndia, loss_range):
                stage = "Loss"
            else:
                stage = "Loss"
                
            # Create a basic staging entry
            staging_data.append({
                "loan_id": loan.id,
                "employee_id": loan.employee_id,
                "stage": stage,
                "outstanding_loan_balance": float(loan.outstanding_loan_balance) if loan.outstanding_loan_balance else 0,
            })
    
    if not staging_data:
        # If we still don't have staging data, return an error
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No loan staging data found. Please re-run the staging process."
        )
    
    # Parse provision rates from config
    try:
        current_rate = Decimal(config["current"]["rate"]) / Decimal(100) if "rate" in config["current"] else Decimal(0.01)
        olem_rate = Decimal(config["olem"]["rate"]) / Decimal(100) if "rate" in config["olem"] else Decimal(0.05)
        substandard_rate = Decimal(config["substandard"]["rate"]) / Decimal(100) if "rate" in config["substandard"] else Decimal(0.25)
        doubtful_rate = Decimal(config["doubtful"]["rate"]) / Decimal(100) if "rate" in config["doubtful"] else Decimal(0.5)
        loss_rate = Decimal(config["loss"]["rate"]) / Decimal(100) if "rate" in config["loss"] else Decimal(1.0)
    except (KeyError, ValueError) as e:
        # If rates aren't in the staging config, use defaults
        current_rate = Decimal(0.01)  # 1%
        olem_rate = Decimal(0.05)     # 5%
        substandard_rate = Decimal(0.25)  # 25%
        doubtful_rate = Decimal(0.5)  # 50%
        loss_rate = Decimal(1.0)      # 100%
        
    # Get all loans in the portfolio
    loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
    
    # Create a map of loan_id to loan object for faster lookup
    loan_map = {loan.id: loan for loan in loans}

    # Initialize category tracking
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

    # Process loans using staging data
    for stage_info in staging_data:
        loan_id = stage_info.get("loan_id")
        stage = stage_info.get("stage")
        
        if not loan_id or not stage:
            logger.warning(f"Missing loan_id or stage in staging data: {stage_info}")
            continue
            
        loan = loan_map.get(loan_id)
        if not loan or loan.outstanding_loan_balance is None:
            logger.warning(f"Loan {loan_id} not found or has no outstanding balance")
            continue
            
        outstanding_loan_balance = loan.outstanding_loan_balance
        
        if stage == "Current":
            current_loans.append(loan)
            current_total += outstanding_loan_balance
        elif stage == "OLEM":
            olem_loans.append(loan)
            olem_total += outstanding_loan_balance
        elif stage == "Substandard":
            substandard_loans.append(loan)
            substandard_total += outstanding_loan_balance
        elif stage == "Doubtful":
            doubtful_loans.append(loan)
            doubtful_total += outstanding_loan_balance
        elif stage == "Loss":
            loss_loans.append(loan)
            loss_total += outstanding_loan_balance
        else:
            # Default to Loss if stage is something unexpected
            logger.warning(f"Unexpected stage '{stage}' for loan {loan_id}, treating as Loss")
            loss_loans.append(loan)
            loss_total += outstanding_loan_balance

    # Calculate provisions using the provision rates
    current_provision = current_total * current_rate
    olem_provision = olem_total * olem_rate
    substandard_provision = substandard_total * substandard_rate
    doubtful_provision = doubtful_total * doubtful_rate
    loss_provision = loss_total * loss_rate

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

    # Create a new CalculationResult record
    calculation_result = CalculationResult(
        portfolio_id=portfolio_id,
        calculation_type="local_impairment",
        config=config,
        result_summary={
            "current_count": len(current_loans),
            "current_total": float(current_total),
            "current_provision": float(current_provision),
            "current_provision_rate": float(current_rate),
            
            "olem_count": len(olem_loans),
            "olem_total": float(olem_total),
            "olem_provision": float(olem_provision),
            "olem_provision_rate": float(olem_rate),
            
            "substandard_count": len(substandard_loans),
            "substandard_total": float(substandard_total),
            "substandard_provision": float(substandard_provision),
            "substandard_provision_rate": float(substandard_rate),
            
            "doubtful_count": len(doubtful_loans),
            "doubtful_total": float(doubtful_total),
            "doubtful_provision": float(doubtful_provision),
            "doubtful_provision_rate": float(doubtful_rate),
            
            "loss_count": len(loss_loans),
            "loss_total": float(loss_total),
            "loss_provision": float(loss_provision),
            "loss_provision_rate": float(loss_rate),
            
            "total_loans": len(current_loans) + len(olem_loans) + len(substandard_loans) + len(doubtful_loans) + len(loss_loans)
        },
        total_provision=float(total_provision),
        provision_percentage=float(provision_percentage),
        reporting_date=reporting_date
    )
    db.add(calculation_result)
    db.commit()

    # Construct response
    response = LocalImpairmentSummary(
        portfolio_id=portfolio_id,
        calculation_date=reporting_date.strftime("%Y-%m-%d"),
        current=CategoryData(
            num_loans=len(current_loans),
            total_loan_value=round(current_total, 2),
            provision_amount=round(current_provision, 2),
            provision_rate=current_rate,
        ),
        olem=CategoryData(
            num_loans=len(olem_loans),
            total_loan_value=round(olem_total, 2),
            provision_amount=round(olem_provision, 2),
            provision_rate=olem_rate,
        ),
        substandard=CategoryData(
            num_loans=len(substandard_loans),
            total_loan_value=round(substandard_total, 2),
            provision_amount=round(substandard_provision, 2),
            provision_rate=substandard_rate,
        ),
        doubtful=CategoryData(
            num_loans=len(doubtful_loans),
            total_loan_value=round(doubtful_total, 2),
            provision_amount=round(doubtful_provision, 2),
            provision_rate=doubtful_rate,
        ),
        loss=CategoryData(
            num_loans=len(loss_loans),
            total_loan_value=round(loss_total, 2),
            provision_amount=round(loss_provision, 2),
            provision_rate=loss_rate,
        ),
        total_provision=round(total_provision, 2),
        provision_percentage=round(provision_percentage, 1),
    )

    return response


