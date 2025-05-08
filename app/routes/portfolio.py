import asyncio
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
    BackgroundTasks,
)
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import text, func, case, cast, String, and_, select
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
    calculate_exposure_at_default_percentage,
    calculate_probability_of_default,
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
    CalculationResult,
    Report
    
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
    QualityIssueCommentCreate,
    QualityCheckSummary,
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
from app.utils.background_processors import process_loan_details_with_progress as process_loan_details, process_client_data_with_progress as process_client_data
from app.utils.background_ingestion import (
    start_background_ingestion,
    process_portfolio_ingestion_sync
)
from app.utils.staging import parse_days_range
from app.utils.background_calculations import (
    process_ecl_calculation_sync,
    process_bog_impairment_calculation_sync
)
from app.utils.ecl_calculator import calculate_loss_given_default
import os

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/portfolios", tags=["portfolios"])


@router.post("/", response_model=PortfolioResponse, status_code=status.HTTP_201_CREATED)
async def create_portfolio(
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
    query = db.query(Portfolio)

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
        
        # Check if ECL calculation exists
        total_final_ecl = db.query( func.sum(Loan.final_ecl) ).filter( Loan.portfolio_id == portfolio.id ).scalar() or 0
        has_calculated_ecl = total_final_ecl>0
        
        # Check if local impairment calculation exists
        total_bog_provision=db.query( func.sum(Loan.bog_provision) ).filter( Loan.portfolio_id == portfolio.id ).scalar() or 0
        has_calculated_local_impairment = total_bog_provision>0

    
        # Check if portfolio has quality issues at all
        has_issues = db.query(QualityIssue).filter(
            QualityIssue.portfolio_id == portfolio.id
        ).first() is not None

        # Only check approval status if there are issues
        has_all_issues_approved = None
        if has_issues:
            has_open_issues = db.query(QualityIssue).filter(
                QualityIssue.portfolio_id == portfolio.id,
                QualityIssue.status != "approved"
            ).first() is not None
            has_all_issues_approved = not has_open_issues
            
        # Convert to PortfolioResponse and set flags
        portfolio_dict = portfolio.__dict__.copy()
        if '_sa_instance_state' in portfolio_dict:
            del portfolio_dict['_sa_instance_state']
                
        # Create response object with all flags
        portfolio_response = PortfolioResponse(
            **portfolio_dict, 
            has_ingested_data=has_data,
            has_calculated_ecl=has_calculated_ecl,
            has_calculated_local_impairment=has_calculated_local_impairment,
            has_all_issues_approved=has_all_issues_approved
        )
        response_items.append(portfolio_response)

    return {"items": response_items, "total": total}

@router.get("/{portfolio_id}", response_model=PortfolioWithSummaryResponse)
def get_portfolio(
    portfolio_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    optimized endpoint for retrieving portfolio details with 70K+ loans/clients.
    Uses direct SQL queries and minimal processing to ensure fast response times.
    """
    try:
        today = date.today()
        # Verify portfolio exists and user has access
        portfolio = db.query(Portfolio).filter(
            Portfolio.id == portfolio_id
        ).first()
        
        if not portfolio:
            raise HTTPException(status_code=404, detail="Portfolio not found")
        
        #portfolio stats
        total_loans = db.query(func.count(Loan.id)).filter(Loan.portfolio_id == portfolio_id).scalar() or 0
        has_ingested_data = total_loans > 0
        
        # Check flags
        total_final_ecl = db.query( func.sum(Loan.final_ecl) ).filter( Loan.portfolio_id == portfolio.id ).scalar() or 0
        has_calculated_ecl = total_final_ecl>0
        
        # Check if local impairment calculation exists
        total_bog_provision=db.query( func.sum(Loan.bog_provision) ).filter( Loan.portfolio_id == portfolio.id ).scalar() or 0
        has_calculated_local_impairment = total_bog_provision>0
        
        # Check quality issues
        has_issues = db.query(QualityIssue).filter(
            QualityIssue.portfolio_id == portfolio_id
        ).first() is not None

        # Only check approval status if there are issues
        has_all_issues_approved = None
        if has_issues:
            has_open_issues = db.query(QualityIssue).filter(
                QualityIssue.portfolio_id == portfolio_id,
                QualityIssue.status != "approved"
            ).first() is not None
            has_all_issues_approved = not has_open_issues
        
        #Get aggregate statistics in one query
        loan_stats = db.query(
            func.count(Loan.id).label("total_loans"),
            func.sum(Loan.ead).label("total_loan_value"),
            func.avg(Loan.ead).label("average_loan_amount")
        ).filter(Loan.portfolio_id == portfolio_id).first()
        
        loan_count=loan_stats.total_loans or 0
        total_loan_balance=loan_stats.total_loan_value or 0
        loan_average=loan_stats.average_loan_amount or 0

        # Customer statistics - use the same values as in CustomerType enum
        # CustomerType values: "individuals", "institution", "mixed"
        customer_stats = db.query(
            func.count(Client.id).label("total_customers"),
            func.sum(case((Client.client_type == "individuals", 1), else_=0)).label("individual_customers"),
            func.sum(case((Client.client_type == "institution", 1), else_=0)).label("institutions"),
            func.sum(case((Client.client_type == "mixed", 1), else_=0)).label("mixed")
        ).filter(Client.portfolio_id == portfolio_id).first()
        
        total_customers = customer_stats.total_customers or 0
        individual_customers = customer_stats.individual_customers or 0
        institutions = customer_stats.institutions or 0
        mixed = customer_stats.mixed or 0
        
        # Active customers
        active_loans = db.query(Loan.employee_id).filter(
            Loan.portfolio_id == portfolio_id,
            Loan.paid == False
        ).distinct().subquery()
        
        active_customers = db.query(Client).filter(
            Client.portfolio_id == portfolio_id,
            Client.employee_id.in_(select(active_loans))
        ).count()
        
        # Get the portfolio's customer type to distribute active customers
        portfolio_customer_type = db.query(Portfolio.customer_type).filter(
            Portfolio.id == portfolio_id
        ).scalar()
        
        # Distribute active customers based on portfolio customer type
        if portfolio_customer_type == "individuals":
            individual_customers = active_customers
        elif portfolio_customer_type == "institution":
            institutions = active_customers
        elif portfolio_customer_type == "mixed":
            mixed = active_customers
        else:
            # If no specific customer type is set, default to individual
            individual_customers = active_customers
        
        # Get quality checks
        quality_counts = create_quality_issues_if_needed(db, portfolio_id)
        
        quality_check_summary = QualityCheckSummary(
            duplicate_customer_ids=quality_counts["duplicate_customer_ids"],
            duplicate_addresses=quality_counts["duplicate_addresses"],
            duplicate_dob=quality_counts["duplicate_dob"],
            duplicate_loan_ids=quality_counts["duplicate_loan_ids"],
            unmatched_employee_ids=quality_counts["clients_without_matching_loans"],
            loan_customer_mismatches=quality_counts["loans_without_matching_clients"],
            missing_dob=quality_counts["missing_dob"],
            total_issues=quality_counts["total_issues"],
            high_severity_issues=quality_counts["high_severity_issues"],
            open_issues=quality_counts["open_issues"],
        )
        
        # Fetch report history (most recent 10)
        report_history = (
            db.query(Report)
            .filter(Report.portfolio_id == portfolio_id)
            .order_by(Report.created_at.desc())
            .limit(10)
            .all()
        )
        
        # 1. IFRS9 calculation summary (from ifrs9_stage field)
        ifrs9_stats = db.query(
            Loan.ifrs9_stage,
            func.count(Loan.id).label("num_loans"),
            func.sum(Loan.ead).label("total_exposure_at_default"),
            func.sum(Loan.final_ecl).label("provision_amount")
        ).filter(
            Loan.portfolio_id == portfolio_id
        ).group_by(
            Loan.ifrs9_stage
        ).all()

        # 2. BOG staging summary (from bog_stage field)
        bog_stats = db.query(
            Loan.bog_stage,
            func.count(Loan.id).label("num_loans"),
            func.sum(Loan.ead).label("total_loan_balance"),
            func.sum(Loan.bog_provision).label("provision_amount")
        ).filter(
            Loan.portfolio_id == portfolio_id
        ).group_by(
            Loan.bog_stage
        ).all()

        # 3. Prepare empty bands
        ifrs9_bands = ["Stage 1", "Stage 2", "Stage 3"]
        bog_bands = ["Current", "OLEM", "Substandard", "Doubtful", "Loss"]

        ecl_summary = {}
        local_impairment_summary = {}

        for band in ifrs9_bands:
            ecl_summary[band] = {
                "num_loans": 0,
                "outstanding_loan_balance": 0.0,
                "total_loan_value": 0.0,
                "provision_amount": 0.0,
                "provision_rate": 0.0
            }

        for band in bog_bands:
            local_impairment_summary[band] = {
                "num_loans": 0,
                "outstanding_loan_balance": 0.0,
                "total_loan_value": 0.0,
                "provision_amount": 0.0,
                "provision_rate": 0.0
            }


        # 4. Fill in IFRS9 results
        for row in ifrs9_stats:
            stage = (row.ifrs9_stage or "").title()  # Stage 1, Stage 2, Stage 3
            if stage in ecl_summary:
                ecl_summary[stage] = {
                    "num_loans": int(row.num_loans or 0),
                    "outstanding_loan_balance": float(row.total_exposure_at_default or 0),
                    "total_loan_value": float(row.total_exposure_at_default or 0),
                    "provision_amount": float(row.provision_amount or 0)
                }

        # 5. Fill in BOG results
        for row in bog_stats:
            stage = (row.bog_stage or "").capitalize()  # Current, OLEM, Substandard, etc
            if stage in local_impairment_summary:
                local_impairment_summary[stage] = {
                    "num_loans": int(row.num_loans or 0),
                    "outstanding_loan_balance": float(row.total_loan_balance or 0),
                    "total_loan_value": float(row.total_loan_balance or 0),
                    "provision_amount": float(row.provision_amount or 0)
                }

        # 6. Calculate totals
        total_ecl_provision = sum(band["provision_amount"] for band in ecl_summary.values())
        total_local_provision = sum(band["provision_amount"] for band in local_impairment_summary.values())

        # 7. Build final calculation_summary
        calculation_summary = {
            "ecl": {
                "Stage 1": ecl_summary["Stage 1"],
                "Stage 2": ecl_summary["Stage 2"],
                "Stage 3": ecl_summary["Stage 3"],
                "total_provision": float(total_ecl_provision),
                "calculation_date": today,
                "config": {}
            },
            "local_impairment": {
                "Current": local_impairment_summary["Current"],
                "OLEM": local_impairment_summary["OLEM"],
                "Substandard": local_impairment_summary["Substandard"],
                "Doubtful": local_impairment_summary["Doubtful"],
                "Loss": local_impairment_summary["Loss"],
                "total_provision": float(total_local_provision),
                "calculation_date": today,
                "config": {}
            }
        }

        
        # Create and return the response
        return PortfolioWithSummaryResponse(
            id=portfolio.id,
            name=portfolio.name,
            description=portfolio.description,
            asset_type=portfolio.asset_type,
            customer_type=portfolio.customer_type,
            funding_source=portfolio.funding_source,
            # data_source=portfolio.data_source,
            repayment_source=portfolio.repayment_source,
            # credit_risk_reserve=portfolio.credit_risk_reserve,
            # loan_assets=portfolio.loan_assets,
            # ecl_impairment_account=portfolio.ecl_impairment_account,
            has_ingested_data=has_ingested_data,
            has_calculated_ecl=has_calculated_ecl,
            has_calculated_local_impairment=has_calculated_local_impairment,
            has_all_issues_approved=has_all_issues_approved,
            # created_at=portfolio.created_at,
            updated_at=portfolio.updated_at,
            overview=OverviewModel(
                total_loans=total_loans,
                total_loan_value=round(float(total_loan_balance), 2),
                average_loan_amount=round(float(loan_average), 2),
                total_customers=total_customers,
            ),
            customer_summary=CustomerSummaryModel(
                individual_customers=individual_customers,
                institutions=institutions,
                mixed=mixed,
                active_customers=active_customers,
            ),
            quality_check=quality_check_summary,
            report_history=report_history,
            calculation_summary=calculation_summary
        )
        
    except Exception as e:
        logger.error(f"Error in fast_get_portfolio: {str(e)}")
        import traceback
        error_details = traceback.format_exc()
        raise HTTPException(
            status_code=500, 
            detail=f"Error retrieving portfolio: {str(e)}\n{error_details}"
        )

@router.put("/{portfolio_id}", response_model=PortfolioWithSummaryResponse)
async def update_portfolio(
    portfolio_id: int,
    portfolio_update: PortfolioUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Update a specific portfolio by ID.
    Processes staging configurations in an optimized way if provided.
    Allows storing configurations even when the portfolio has no loan data.
    Returns the complete portfolio details using the optimized get_portfolio function.
    """
    try:
        # Get only the portfolio itself
        portfolio = (
            db.query(Portfolio)
            .filter(Portfolio.id == portfolio_id)
            .with_for_update()
            .first()
        )

        if not portfolio:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
            )

        # Extract configs for processing
        ecl_staging_config = portfolio_update.ecl_staging_config 
        bog_staging_config = portfolio_update.bog_staging_config
        
        # Update basic portfolio fields
        update_data = portfolio_update.dict()

        # Convert enum values to strings
        for field in ["asset_type", "customer_type", "funding_source", "data_source"]:
            if field in update_data and update_data[field]:
                update_data[field] = update_data[field].value

        # Update fields
        for key, value in update_data.items():
            setattr(portfolio, key, value)
            
        # Commit the basic update immediately
        db.commit()
        logger.info(f"Portfolio {portfolio_id} basic fields updated successfully")
        
        # Check if the portfolio has any data
        has_data = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).limit(1).count() > 0
        
        # Process ECL staging config if provided
        if ecl_staging_config and has_data:
            try:
                await stage_loans_ecl_orm(portfolio_id, db)
                logger.info(f"ECL staging completed for portfolio {portfolio_id}")

            except Exception as e:
                logger.error(f"Error during ECL staging: {str(e)}")
                # Continue with other operations
        
        # Process local impairment staging config if provided
        if bog_staging_config and has_data:
            try:
                stage_loans_local_impairment_orm(portfolio_id,db)
                logger.info(f"Local impairment staging completed for portfolio {portfolio_id}")
                
            except Exception as e:
                logger.error(f"Error during local impairment staging: {str(e)}")
                # Continue with other operations
        
        # Use the optimized get_portfolio function to return the complete portfolio data
        return get_portfolio(
            portfolio_id=portfolio_id, 
            db=db, 
            current_user=current_user
        )

    except Exception as e:
        db.rollback()
        logger.error(f"Error updating portfolio: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update portfolio: {str(e)}"
        )

    
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
        .filter(Portfolio.id == portfolio_id)
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
    client_data: Optional[UploadFile] = File(None),
    loan_guarantee_data: Optional[UploadFile] = File(None),
    loan_collateral_data: Optional[UploadFile] = File(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Ingest Excel files containing portfolio data and automatically perform both types of staging.
    
    Accepts up to four Excel files:
    - loan_details: Primary loan information (required)
    - client_data: Customer information (required)
    - loan_guarantee_data: Information about loan guarantees (optional)
    - loan_collateral_data: Information about loan collateral (optional)
    
    The function processes the files synchronously and returns the processing result.
    """
    # Check if portfolio exists and belongs to user
    portfolio = db.query(Portfolio).filter(
        Portfolio.id == portfolio_id
    ).first()
    
    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Portfolio with ID {portfolio_id} not found or does not belong to you",
        )
    
    # Check if both required files are provided
    if not loan_details or not client_data:
        missing_files = []
        if not loan_details:
            missing_files.append("loan_details")
        if not client_data:
            missing_files.append("client_data")
            
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Missing required files: {', '.join(missing_files)}. Both loan_details and client_data files are required for portfolio ingestion.",
        )
    
    # Process files synchronously
    result = await process_portfolio_ingestion_sync(
        portfolio_id= portfolio_id,
        loan_details_content= await loan_details.read(),
        client_data_content= await client_data.read(),
        loan_guarantee_data_content=await loan_guarantee_data.read() if loan_guarantee_data else None,
        loan_collateral_data_content=await loan_collateral_data.read() if loan_collateral_data else None,
        db=db
    )
    
    # Check for errors in any component of the result
    if "details" in result:
        # Check loan details
        if "loan_details" in result["details"] and "error" in result["details"]["loan_details"]:
            error_message = result["details"]["loan_details"]["error"]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_message
            )
        
        # Check client data
        if "client_data" in result["details"] and "error" in result["details"]["client_data"]:
            error_message = result["details"]["client_data"]["error"]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_message
            )
        
        # Check loan guarantee data
        if "loan_guarantee_data" in result["details"] and "error" in result["details"]["loan_guarantee_data"]:
            error_message = result["details"]["loan_guarantee_data"]["error"]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_message
            )
        
        # Check loan collateral data
        if "loan_collateral_data" in result["details"] and "error" in result["details"]["loan_collateral_data"]:
            error_message = result["details"]["loan_collateral_data"]["error"]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_message
            )
    
    # Check for errors in quality checks
    if "quality_checks" in result and "error" in result["quality_checks"]:
        error_message = result["quality_checks"]["error"]
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error in quality checks: {error_message}"
        )
    
    # Check for errors in staging
    if "staging" in result and "error" in result["staging"]:
        error_message = result["staging"]["error"]
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error in staging: {error_message}"
        )
    
    # Check for general errors
    if "error" in result:
        error_message = result["error"]
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=error_message
        )
    
    return result

@router.get("/{portfolio_id}/calculate-ecl")
async def calculate_ecl_provision(
    portfolio_id: int,
    reporting_date: Optional[date] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):

    # Use provided reporting date or default to current date
    if not reporting_date:
        reporting_date = datetime.now().date()

    # Verify portfolio exists and belongs to current user
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id)
        .first()
    )
    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )
    

    try:
        return await process_ecl_calculation_sync(
            portfolio_id=portfolio_id,
            reporting_date=reporting_date,
            db=db
        )
    except Exception as e:
        logger.error(f"ECL calculation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/{portfolio_id}/stage-loans-ecl")
async def stage_loans_ecl(
    portfolio_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):

    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id)
        .first()
    )
    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )
    from app.utils.staging import (stage_loans_ecl_orm)
    await stage_loans_ecl_orm(portfolio.id, db)
    



@router.post("/{portfolio_id}/stage-loans-local")
async def stage_loans_local(
    portfolio_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):

    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id)
        .first()
    )
    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )
    from app.utils.staging import (stage_loans_local_impairment_orm)
    await stage_loans_local_impairment_orm(portfolio.id, db)
    


@router.get("/{portfolio_id}/calculate-local-impairment")
async def calculate_local_provision(
    portfolio_id: int,
    reporting_date: Optional[date] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Calculate local impairment provisions directly from a portfolio ID.
    This route uses the latest staging data stored in the database.
    The calculation is performed in a background task and returns a task ID for tracking progress.
    """
    # Use provided reporting date or default to current date
    if not reporting_date:
        reporting_date = datetime.now().date()

    # Verify portfolio exists and belongs to current user
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id)
        .first()
    )
    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )

    try:
        return await process_bog_impairment_calculation_sync(
            portfolio_id=portfolio_id,
            reporting_date=reporting_date,
            db=db
        )
    except Exception as e:
        logger.error(f"Local Impairment calculation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    


# Optimized local impairment staging implementation
async def stage_loans_local_impairment_optimized(portfolio_id: int, config: LocalImpairmentConfig, db: Session):
    """
    Highly optimized implementation of local impairment staging using direct SQL for large datasets.
    """
    # Parse day ranges from config
    try:
        current_range = parse_days_range(config.current.days_range)
        olem_range = parse_days_range(config.olem.days_range)
        substandard_range = parse_days_range(config.substandard.days_range)
        doubtful_range = parse_days_range(config.doubtful.days_range)
        loss_range = parse_days_range(config.loss.days_range)
    except ValueError as e:
        logger.error(f"Error parsing local impairment ranges: {str(e)}")
        return {"status": "error", "error": str(e)}
    
    # Use a direct SQL update to classify and store results
    try:
        # Get the current timestamp
        staged_at = datetime.now()
        
        # Execute a direct SQL query to count and sum by category
        result = db.execute(text("""
        WITH calculated_ndias AS (
            SELECT
                id,
                COALESCE(ndia, 
                    CASE 
                        WHEN accumulated_arrears IS NOT NULL AND monthly_installment IS NOT NULL AND monthly_installment > 0 
                        THEN CAST((accumulated_arrears / monthly_installment) * 30 AS INTEGER)
                        ELSE 0
                    END
                ) as calculated_ndia,
                outstanding_loan_balance
            FROM loans
            WHERE portfolio_id = :portfolio_id
        ),
        staged_loans AS (
            SELECT
                CASE
                    WHEN calculated_ndia >= :current_min AND calculated_ndia <= :current_max THEN 'Current'
                    WHEN calculated_ndia >= :olem_min AND calculated_ndia <= :olem_max THEN 'OLEM'
                    WHEN calculated_ndia >= :substandard_min AND calculated_ndia <= :substandard_max THEN 'Substandard'
                    WHEN calculated_ndia >= :doubtful_min AND calculated_ndia <= :doubtful_max THEN 'Doubtful'
                    WHEN calculated_ndia >= :loss_min THEN 'Loss'
                    ELSE 'Loss'
                END as stage,
                COUNT(*) as count,
                SUM(outstanding_loan_balance) as total
            FROM calculated_ndias
            GROUP BY 
                CASE
                    WHEN calculated_ndia >= :current_min AND calculated_ndia <= :current_max THEN 'Current'
                    WHEN calculated_ndia >= :olem_min AND calculated_ndia <= :olem_max THEN 'OLEM'
                    WHEN calculated_ndia >= :substandard_min AND calculated_ndia <= :substandard_max THEN 'Substandard'
                    WHEN calculated_ndia >= :doubtful_min AND calculated_ndia <= :doubtful_max THEN 'Doubtful'
                    WHEN calculated_ndia >= :loss_min THEN 'Loss'
                    ELSE 'Loss'
                END
        )
        SELECT 
            SUM(CASE WHEN stage = 'Current' THEN count ELSE 0 END) as current_count,
            SUM(CASE WHEN stage = 'Current' THEN total ELSE 0 END) as current_total,
            SUM(CASE WHEN stage = 'OLEM' THEN count ELSE 0 END) as olem_count,
            SUM(CASE WHEN stage = 'OLEM' THEN total ELSE 0 END) as olem_total,
            SUM(CASE WHEN stage = 'Substandard' THEN count ELSE 0 END) as substandard_count,
            SUM(CASE WHEN stage = 'Substandard' THEN total ELSE 0 END) as substandard_total,
            SUM(CASE WHEN stage = 'Doubtful' THEN count ELSE 0 END) as doubtful_count,
            SUM(CASE WHEN stage = 'Doubtful' THEN total ELSE 0 END) as doubtful_total,
            SUM(CASE WHEN stage = 'Loss' THEN count ELSE 0 END) as loss_count,
            SUM(CASE WHEN stage = 'Loss' THEN total ELSE 0 END) as loss_total,
            SUM(count) as total_count
        FROM staged_loans
        """), {
            "portfolio_id": portfolio_id,
            "current_min": current_range[0] if current_range[0] is not None else 0,
            "current_max": current_range[1] if current_range[1] is not None else 30,
            "olem_min": olem_range[0] if olem_range[0] is not None else 31,
            "olem_max": olem_range[1] if olem_range[1] is not None else 90,
            "substandard_min": substandard_range[0] if substandard_range[0] is not None else 91,
            "substandard_max": substandard_range[1] if substandard_range[1] is not None else 180,
            "doubtful_min": doubtful_range[0] if doubtful_range[0] is not None else 181,
            "doubtful_max": doubtful_range[1] if doubtful_range[1] is not None else 365,
            "loss_min": loss_range[0] if loss_range[0] is not None else 366
        }).fetchone()
        
        # Update the staging result - fixed the query to avoid using astext
        staging_result = (
            db.query(StagingResult)
            .filter(
                StagingResult.portfolio_id == portfolio_id,
                StagingResult.staging_type == "local_impairment",
                cast(StagingResult.result_summary['status'], String) == "processing"
            )
            .order_by(StagingResult.created_at.desc())
            .first()
        )
        
        if staging_result:
            # Update with aggregated results only (no detailed loan data)
            staging_result.result_summary = {
                "status": "completed",
                "completed_at": staged_at.isoformat(),
                "total_loans": int(result.total_count or 0),
                "Current": {
                    "num_loans": int(result.current_count or 0),
                    "outstanding_loan_balance": float(result.current_total or 0),
                    "total_loan_value": float(result.current_total or 0),
                    "provision_amount": float(result.current_total or 0) * 0.01,
                    "provision_rate": 0.01
                },
                "OLEM": {
                    "num_loans": int(result.olem_count or 0),
                    "outstanding_loan_balance": float(result.olem_total or 0),
                    "total_loan_value": float(result.olem_total or 0),
                    "provision_amount": float(result.olem_total or 0) * 0.05,
                    "provision_rate": 0.05
                },
                "Substandard": {
                    "num_loans": int(result.substandard_count or 0),
                    "outstanding_loan_balance": float(result.substandard_total or 0),
                    "total_loan_value": float(result.substandard_total or 0),
                    "provision_amount": float(result.substandard_total or 0) * 0.25,
                    "provision_rate": 0.25
                },
                "Doubtful": {
                    "num_loans": int(result.doubtful_count or 0),
                    "outstanding_loan_balance": float(result.doubtful_total or 0),
                    "total_loan_value": float(result.doubtful_total or 0),
                    "provision_amount": float(result.doubtful_total or 0) * 0.50,
                    "provision_rate": 0.50
                },
                "Loss": {
                    "num_loans": int(result.loss_count or 0),
                    "outstanding_loan_balance": float(result.loss_total or 0),
                    "total_loan_value": float(result.loss_total or 0),
                    "provision_amount": float(result.loss_total or 0) * 1.0,
                    "provision_rate": 1.0
                }
            }
            db.commit()
            
        return {
            "status": "success",
            "loans_staged": int(result.total_count or 0),
            "Current": {
                "num_loans": int(result.current_count or 0),
                "outstanding_loan_balance": float(result.current_total or 0),
                "total_loan_value": float(result.current_total or 0),
                "provision_amount": float(result.current_total or 0) * 0.01,
                "provision_rate": 0.01
            },
            "OLEM": {
                "num_loans": int(result.olem_count or 0),
                "outstanding_loan_balance": float(result.olem_total or 0),
                "total_loan_value": float(result.olem_total or 0),
                "provision_amount": float(result.olem_total or 0) * 0.05,
                "provision_rate": 0.05
            },
            "Substandard": {
                "num_loans": int(result.substandard_count or 0),
                "outstanding_loan_balance": float(result.substandard_total or 0),
                "total_loan_value": float(result.substandard_total or 0),
                "provision_amount": float(result.substandard_total or 0) * 0.25,
                "provision_rate": 0.25
            },
            "Doubtful": {
                "num_loans": int(result.doubtful_count or 0),
                "outstanding_loan_balance": float(result.doubtful_total or 0),
                "total_loan_value": float(result.doubtful_total or 0),
                "provision_amount": float(result.doubtful_total or 0) * 0.50,
                "provision_rate": 0.50
            },
            "Loss": {
                "num_loans": int(result.loss_count or 0),
                "outstanding_loan_balance": float(result.loss_total or 0),
                "total_loan_value": float(result.loss_total or 0),
                "provision_amount": float(result.loss_total or 0) * 1.0,
                "provision_rate": 1.0
            }
        }
    except Exception as e:
        logger.error(f"Error in optimized local impairment staging: {str(e)}")
        db.rollback()
        return {"status": "error", "error": str(e)}
