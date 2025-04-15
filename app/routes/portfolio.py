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
from sqlalchemy import text, func, case, cast, String
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
    start_background_ecl_calculation,
    start_background_local_impairment_calculation,
    process_ecl_calculation_sync,
    process_local_impairment_calculation_sync
)
import os

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
        
        # Check if ECL calculation exists
        has_calculated_ecl = db.query(CalculationResult).filter(
            CalculationResult.portfolio_id == portfolio.id,
            CalculationResult.calculation_type == "ecl"
        ).limit(1).count() > 0
        
        # Check if local impairment calculation exists
        has_calculated_local_impairment = db.query(CalculationResult).filter(
            CalculationResult.portfolio_id == portfolio.id,
            CalculationResult.calculation_type == "local_impairment"
        ).limit(1).count() > 0

    
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
        # Verify portfolio exists and user has access
        portfolio = db.query(Portfolio).filter(
            Portfolio.id == portfolio_id, 
            Portfolio.user_id == current_user.id
        ).first()
        
        if not portfolio:
            raise HTTPException(status_code=404, detail="Portfolio not found")
        
        # Basic loan statistics
        loan_count = db.query(func.count(Loan.id)).filter(Loan.portfolio_id == portfolio_id).scalar() or 0
        has_ingested_data = loan_count > 0
        
        # Check flags
        has_calculated_ecl = db.query(CalculationResult).filter(
            CalculationResult.portfolio_id == portfolio_id,
            CalculationResult.calculation_type == "ecl"
        ).limit(1).count() > 0
        
        has_calculated_local_impairment = db.query(CalculationResult).filter(
            CalculationResult.portfolio_id == portfolio_id,
            CalculationResult.calculation_type == "local_impairment"
        ).limit(1).count() > 0
        
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
        
        # Get aggregate statistics in one query
        loan_stats = db.query(
            func.count(Loan.id).label("total_loans"),
            func.sum(Loan.outstanding_loan_balance).label("total_loan_value"),
            func.avg(Loan.loan_amount).label("average_loan_amount")
        ).filter(Loan.portfolio_id == portfolio_id).first()
        
        total_loans = loan_stats.total_loans or 0
        total_loan_value = float(loan_stats.total_loan_value or 0)
        average_loan_amount = float(loan_stats.average_loan_amount or 0)

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
            Client.employee_id.in_(active_loans)
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
        
        # Get latest staging and calculation results
        latest_ecl_staging = (
            db.query(StagingResult)
            .filter(
                StagingResult.portfolio_id == portfolio_id,
                StagingResult.staging_type == "ecl"
            )
            .order_by(StagingResult.created_at.desc())
            .first()
        )
        
        latest_local_impairment_staging = (
            db.query(StagingResult)
            .filter(
                StagingResult.portfolio_id == portfolio_id,
                StagingResult.staging_type == "local_impairment"
            )
            .order_by(StagingResult.created_at.desc())
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
        
        latest_local_impairment_calculation = (
            db.query(CalculationResult)
            .filter(
                CalculationResult.portfolio_id == portfolio_id,
                CalculationResult.calculation_type == "local_impairment"
            )
            .order_by(CalculationResult.created_at.desc())
            .first()
        )
        
        # Process staging results
        staging_summary = None
        if latest_ecl_staging or latest_local_impairment_staging:
            staging_summary = {}
            
            # Process ECL staging
            if latest_ecl_staging and latest_ecl_staging.result_summary:
                ecl_result = latest_ecl_staging.result_summary
                ecl_config = latest_ecl_staging.config
                
                # We'll use the result summary directly if it has the right structure
                if "Stage 1" in ecl_result and isinstance(ecl_result["Stage 1"], dict):
                    stage_1_data = {
                        "num_loans": int(ecl_result["Stage 1"].get("num_loans", 0)),
                        "outstanding_loan_balance": float(ecl_result["Stage 1"].get("outstanding_loan_balance", 0)),
                        "total_loan_value": float(ecl_result["Stage 1"].get("outstanding_loan_balance", 0)),
                        "provision_amount": float(ecl_result["Stage 1"].get("provision_amount", 0)),
                        "provision_rate": float(ecl_result["Stage 1"].get("provision_rate", 0.01))
                    }
                    
                    stage_2_data = {
                        "num_loans": int(ecl_result["Stage 2"].get("num_loans", 0)),
                        "outstanding_loan_balance": float(ecl_result["Stage 2"].get("outstanding_loan_balance", 0)),
                        "total_loan_value": float(ecl_result["Stage 2"].get("outstanding_loan_balance", 0)),
                        "provision_amount": float(ecl_result["Stage 2"].get("provision_amount", 0)),
                        "provision_rate": float(ecl_result["Stage 2"].get("provision_rate", 0.05))
                    }
                    
                    stage_3_data = {
                        "num_loans": int(ecl_result["Stage 3"].get("num_loans", 0)),
                        "outstanding_loan_balance": float(ecl_result["Stage 3"].get("outstanding_loan_balance", 0)),
                        "total_loan_value": float(ecl_result["Stage 3"].get("outstanding_loan_balance", 0)),
                        "provision_amount": float(ecl_result["Stage 3"].get("provision_amount", 0)),
                        "provision_rate": float(ecl_result["Stage 3"].get("provision_rate", 0.15))
                    }
                else:
                    # Extract aggregated data from the summary
                    stage_1_data = {
                        "num_loans": int(ecl_result.get("stage1_count", 0)),
                        "outstanding_loan_balance": float(ecl_result.get("stage1_total", 0)),
                        "total_loan_value": float(ecl_result.get("stage1_total", 0)),
                        "provision_amount": float(ecl_result.get("stage1_provision", 0)),
                        "provision_rate": float(ecl_result.get("stage1_provision_rate", 0.01))
                    }
                    
                    stage_2_data = {
                        "num_loans": int(ecl_result.get("stage2_count", 0)),
                        "outstanding_loan_balance": float(ecl_result.get("stage2_total", 0)),
                        "total_loan_value": float(ecl_result.get("stage2_total", 0)),
                        "provision_amount": float(ecl_result.get("stage2_provision", 0)),
                        "provision_rate": float(ecl_result.get("stage2_provision_rate", 0.05))
                    }
                    
                    stage_3_data = {
                        "num_loans": int(ecl_result.get("stage3_count", 0)),
                        "outstanding_loan_balance": float(ecl_result.get("stage3_total", 0)),
                        "total_loan_value": float(ecl_result.get("stage3_total", 0)),
                        "provision_amount": float(ecl_result.get("stage3_provision", 0)),
                        "provision_rate": float(ecl_result.get("stage3_provision_rate", 0.15))
                    }
                
                staging_summary["ecl"] = {
                    "Stage 1": stage_1_data,
                    "Stage 2": stage_2_data,
                    "Stage 3": stage_3_data,
                    "staging_date": latest_ecl_staging.created_at,
                    "config": ecl_config
                }
            
            # Process local impairment staging
            if latest_local_impairment_staging and latest_local_impairment_staging.result_summary:
                local_result = latest_local_impairment_staging.result_summary
                local_config = latest_local_impairment_staging.config
                
                # Extract from result summary if it has the right structure
                if "Current" in local_result and isinstance(local_result["Current"], dict):
                    current_data = {
                        "num_loans": int(local_result["Current"].get("num_loans", 0)),
                        "outstanding_loan_balance": float(local_result["Current"].get("total_loan_value", local_result["Current"].get("outstanding_balance", 0))),
                        "total_loan_value": float(local_result["Current"].get("total_loan_value", local_result["Current"].get("outstanding_balance", 0))),
                        "provision_amount": float(local_result["Current"].get("provision_amount", local_result["Current"].get("provision", 0))),
                        "provision_rate": 1
                    }
                    
                    olem_data = {
                        "num_loans": int(local_result["OLEM"].get("num_loans", 0)),
                        "outstanding_loan_balance": float(local_result["OLEM"].get("total_loan_value", local_result["OLEM"].get("outstanding_balance", 0))),
                        "total_loan_value": float(local_result["OLEM"].get("total_loan_value", local_result["OLEM"].get("outstanding_balance", 0))),
                        "provision_amount": float(local_result["OLEM"].get("provision_amount", local_result["OLEM"].get("provision", 0))),
                        "provision_rate": 5
                    }
                    
                    substandard_data = {
                        "num_loans": int(local_result["Substandard"].get("num_loans", 0)),
                        "outstanding_loan_balance": float(local_result["Substandard"].get("total_loan_value", local_result["Substandard"].get("outstanding_balance", 0))),
                        "total_loan_value": float(local_result["Substandard"].get("total_loan_value", local_result["Substandard"].get("outstanding_balance", 0))),
                        "provision_amount": float(local_result["Substandard"].get("provision_amount", local_result["Substandard"].get("provision", 0))),
                        "provision_rate": 25
                    }
                    
                    doubtful_data = {
                        "num_loans": int(local_result["Doubtful"].get("num_loans", 0)),
                        "outstanding_loan_balance": float(local_result["Doubtful"].get("total_loan_value", local_result["Doubtful"].get("outstanding_balance", 0))),
                        "total_loan_value": float(local_result["Doubtful"].get("total_loan_value", local_result["Doubtful"].get("outstanding_balance", 0))),
                        "provision_amount": float(local_result["Doubtful"].get("provision_amount", local_result["Doubtful"].get("provision", 0))),
                        "provision_rate": 50
                    }
                    
                    loss_data = {
                        "num_loans": int(local_result["Loss"].get("num_loans", 0)),
                        "outstanding_loan_balance": float(local_result["Loss"].get("total_loan_value", local_result["Loss"].get("outstanding_balance", 0))),
                        "total_loan_value": float(local_result["Loss"].get("total_loan_value", local_result["Loss"].get("outstanding_balance", 0))),
                        "provision_amount": float(local_result["Loss"].get("provision_amount", local_result["Loss"].get("provision", 0))),
                        "provision_rate": 100
                    }
                else:
                    # Extract aggregated data from the summary
                    current_data = {
                        "num_loans": int(local_result.get("current_count", 0)),
                        "outstanding_loan_balance": float(local_result.get("current_balance", 0)),
                        "total_loan_value": float(local_result.get("current_balance", 0)),
                        "provision_amount": float(local_result.get("current_provision", 0)),
                        "provision_rate": 1
                    }
                    
                    olem_data = {
                        "num_loans": int(local_result.get("olem_count", 0)),
                        "outstanding_loan_balance": float(local_result.get("olem_balance", 0)),
                        "total_loan_value": float(local_result.get("olem_balance", 0)),
                        "provision_amount": float(local_result.get("olem_provision", 0)),
                        "provision_rate": 5
                    }
                    
                    substandard_data = {
                        "num_loans": int(local_result.get("substandard_count", 0)),
                        "outstanding_loan_balance": float(local_result.get("substandard_balance", 0)),
                        "total_loan_value": float(local_result.get("substandard_balance", 0)),
                        "provision_amount": float(local_result.get("substandard_provision", 0)),
                        "provision_rate": 25
                    }
                    
                    doubtful_data = {
                        "num_loans": int(local_result.get("doubtful_count", 0)),
                        "outstanding_loan_balance": float(local_result.get("doubtful_balance", 0)),
                        "total_loan_value": float(local_result.get("doubtful_balance", 0)),
                        "provision_amount": float(local_result.get("doubtful_provision", 0)),
                        "provision_rate": 50
                    }
                    
                    loss_data = {
                        "num_loans": int(local_result.get("loss_count", 0)),
                        "outstanding_loan_balance": float(local_result.get("loss_balance", 0)),
                        "total_loan_value": float(local_result.get("loss_balance", 0)),
                        "provision_amount": float(local_result.get("loss_provision", 0)),
                        "provision_rate": 100
                    }
                
                staging_summary["local_impairment"] = {
                    "Current": current_data,
                    "OLEM": olem_data,
                    "Substandard": substandard_data,
                    "Doubtful": doubtful_data,
                    "Loss": loss_data,
                    "staging_date": latest_local_impairment_staging.created_at,
                    "config": local_config
                }
        
        # Process calculation results
        calculation_summary = None
        if latest_ecl_calculation or latest_local_impairment_calculation:
            calculation_summary = {
                "total_loan_value": round(float(total_loan_value), 2),
            }
            
            # Add ECL data if available
            if latest_ecl_calculation and latest_ecl_calculation.result_summary:
                ecl_summary = latest_ecl_calculation.result_summary
                ecl_config = latest_ecl_calculation.config
                
                # First try to get values from the nested format, then fall back to flattened format
                stage_1_data = {
                    "num_loans": int(ecl_summary.get("Stage 1", {}).get("num_loans", ecl_summary.get("stage1_count", 0))),
                    "outstanding_loan_balance": float(ecl_summary.get("Stage 1", {}).get("outstanding_loan_balance", ecl_summary.get("stage1_total", 0))),
                    "total_loan_value": float(ecl_summary.get("Stage 1", {}).get("outstanding_loan_balance", ecl_summary.get("stage1_total", 0))),
                    "provision_amount": float(ecl_summary.get("Stage 1", {}).get("provision_amount", ecl_summary.get("stage1_provision", 0))),
                    "provision_rate": float(ecl_summary.get("Stage 1", {}).get("provision_rate", ecl_summary.get("stage1_provision_rate", 0.01)))
                }
                
                stage_2_data = {
                    "num_loans": int(ecl_summary.get("Stage 2", {}).get("num_loans", ecl_summary.get("stage2_count", 0))),
                    "outstanding_loan_balance": float(ecl_summary.get("Stage 2", {}).get("outstanding_loan_balance", ecl_summary.get("stage2_total", 0))),
                    "total_loan_value": float(ecl_summary.get("Stage 2", {}).get("outstanding_loan_balance", ecl_summary.get("stage2_total", 0))),
                    "provision_amount": float(ecl_summary.get("Stage 2", {}).get("provision_amount", ecl_summary.get("stage2_provision", 0))),
                    "provision_rate": float(ecl_summary.get("Stage 2", {}).get("provision_rate", ecl_summary.get("stage2_provision_rate", 0.05)))
                }
                
                stage_3_data = {
                    "num_loans": int(ecl_summary.get("Stage 3", {}).get("num_loans", ecl_summary.get("stage3_count", 0))),
                    "outstanding_loan_balance": float(ecl_summary.get("Stage 3", {}).get("outstanding_loan_balance", ecl_summary.get("stage3_total", 0))),
                    "total_loan_value": float(ecl_summary.get("Stage 3", {}).get("outstanding_loan_balance", ecl_summary.get("stage3_total", 0))),
                    "provision_amount": float(ecl_summary.get("Stage 3", {}).get("provision_amount", ecl_summary.get("stage3_provision", 0))),
                    "provision_rate": float(ecl_summary.get("Stage 3", {}).get("provision_rate", ecl_summary.get("stage3_provision_rate", 0.15)))
                }
                
                calculation_summary["ecl"] = {
                    "Stage 1": stage_1_data,
                    "Stage 2": stage_2_data,
                    "Stage 3": stage_3_data,
                    "total_provision": float(latest_ecl_calculation.total_provision or 0),
                    "provision_percentage": float(latest_ecl_calculation.provision_percentage or 0),
                    "calculation_date": latest_ecl_calculation.created_at,
                    "config": ecl_config
                }
            
            # Add local impairment data if available
            if latest_local_impairment_calculation and latest_local_impairment_calculation.result_summary:
                local_summary = latest_local_impairment_calculation.result_summary
                local_config = latest_local_impairment_calculation.config
                
                # First try to get values from the nested format, then fall back to flattened format
                current_data = {
                    "num_loans": int(local_summary.get("Current", {}).get("num_loans", local_summary.get("current_count", 0))),
                    "outstanding_loan_balance": float(local_summary.get("Current", {}).get("total_loan_value", local_summary.get("Current", {}).get("outstanding_balance", local_summary.get("current_balance", 0)))),
                    "total_loan_value": float(local_summary.get("Current", {}).get("total_loan_value", local_summary.get("Current", {}).get("outstanding_balance", local_summary.get("current_balance", 0)))),
                    "provision_amount": float(local_summary.get("Current", {}).get("provision_amount", local_summary.get("Current", {}).get("provision", local_summary.get("current_provision", 0)))),
                    "provision_rate": 1
                }
                
                olem_data = {
                    "num_loans": int(local_summary.get("OLEM", {}).get("num_loans", local_summary.get("olem_count", 0))),
                    "outstanding_loan_balance": float(local_summary.get("OLEM", {}).get("total_loan_value", local_summary.get("OLEM", {}).get("outstanding_balance", local_summary.get("olem_balance", 0)))),
                    "total_loan_value": float(local_summary.get("OLEM", {}).get("total_loan_value", local_summary.get("OLEM", {}).get("outstanding_balance", local_summary.get("olem_balance", 0)))),
                    "provision_amount": float(local_summary.get("OLEM", {}).get("provision_amount", local_summary.get("OLEM", {}).get("provision", local_summary.get("olem_provision", 0)))),
                    "provision_rate": 5
                }
                
                substandard_data = {
                    "num_loans": int(local_summary.get("Substandard", {}).get("num_loans", local_summary.get("substandard_count", 0))),
                    "outstanding_loan_balance": float(local_summary.get("Substandard", {}).get("total_loan_value", local_summary.get("Substandard", {}).get("outstanding_balance", local_summary.get("substandard_balance", 0)))),
                    "total_loan_value": float(local_summary.get("Substandard", {}).get("total_loan_value", local_summary.get("Substandard", {}).get("outstanding_balance", local_summary.get("substandard_balance", 0)))),
                    "provision_amount": float(local_summary.get("Substandard", {}).get("provision_amount", local_summary.get("Substandard", {}).get("provision", local_summary.get("substandard_provision", 0)))),
                    "provision_rate": 25
                }
                
                doubtful_data = {
                    "num_loans": int(local_summary.get("Doubtful", {}).get("num_loans", local_summary.get("doubtful_count", 0))),
                    "outstanding_loan_balance": float(local_summary.get("Doubtful", {}).get("total_loan_value", local_summary.get("Doubtful", {}).get("outstanding_balance", local_summary.get("doubtful_balance", 0)))),
                    "total_loan_value": float(local_summary.get("Doubtful", {}).get("total_loan_value", local_summary.get("Doubtful", {}).get("outstanding_balance", local_summary.get("doubtful_balance", 0)))),
                    "provision_amount": float(local_summary.get("Doubtful", {}).get("provision_amount", local_summary.get("Doubtful", {}).get("provision", local_summary.get("doubtful_provision", 0)))),
                    "provision_rate": 50
                }
                
                loss_data = {
                    "num_loans": int(local_summary.get("Loss", {}).get("num_loans", local_summary.get("loss_count", 0))),
                    "outstanding_loan_balance": float(local_summary.get("Loss", {}).get("total_loan_value", local_summary.get("Loss", {}).get("outstanding_balance", local_summary.get("loss_balance", 0)))),
                    "total_loan_value": float(local_summary.get("Loss", {}).get("total_loan_value", local_summary.get("Loss", {}).get("outstanding_balance", local_summary.get("loss_balance", 0)))),
                    "provision_amount": float(local_summary.get("Loss", {}).get("provision_amount", local_summary.get("Loss", {}).get("provision", local_summary.get("loss_provision", 0)))),
                    "provision_rate": 100
                }
                
                calculation_summary["local_impairment"] = {
                    "Current": current_data,
                    "OLEM": olem_data,
                    "Substandard": substandard_data,
                    "Doubtful": doubtful_data,
                    "Loss": loss_data,
                    "total_provision": float(latest_local_impairment_calculation.total_provision or 0),
                    "provision_percentage": float(latest_local_impairment_calculation.provision_percentage or 0),
                    "calculation_date": latest_local_impairment_calculation.created_at,
                    "config": local_config
                }
        
        # Create and return the response
        return PortfolioWithSummaryResponse(
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
            has_calculated_ecl=has_calculated_ecl,
            has_calculated_local_impairment=has_calculated_local_impairment,
            has_all_issues_approved=has_all_issues_approved,
            created_at=portfolio.created_at,
            updated_at=portfolio.updated_at,
            overview=OverviewModel(
                total_loans=total_loans,
                total_loan_value=round(float(total_loan_value), 2),
                average_loan_amount=round(float(average_loan_amount), 2),
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
            calculation_summary=calculation_summary,
            staging_summary=staging_summary,
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
def update_portfolio(
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
            .filter(Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id)
            .with_for_update()
            .first()
        )

        if not portfolio:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
            )

        # Extract configs for processing
        ecl_staging_config = portfolio_update.ecl_staging_config 
        local_impairment_config = portfolio_update.local_impairment_config
        
        # Update basic portfolio fields
        update_data = portfolio_update.dict(
            exclude={"ecl_staging_config", "local_impairment_config"}, 
            exclude_unset=True
        )

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
        if ecl_staging_config:
            try:
                # Create a new staging result entry regardless of whether there's loan data
                ecl_staging_result = StagingResult(
                    portfolio_id=portfolio_id,
                    staging_type="ecl",
                    config=ecl_staging_config.dict(),
                    result_summary={
                        "status": "stored_config" if not has_data else "processing",
                        "timestamp": datetime.now().isoformat(),
                        "has_loan_data": has_data
                    }
                )
                db.add(ecl_staging_result)
                db.commit()
                
                # Only run the staging calculation if there's loan data
                if has_data:
                    ecl_staging = stage_loans_ecl(
                        portfolio_id=portfolio_id,
                        config=ecl_staging_config,
                        db=db,
                        current_user=current_user
                    )
                    logger.info(f"ECL staging completed for portfolio {portfolio_id}")
                else:
                    logger.info(f"ECL config stored for portfolio {portfolio_id} (no loan data to process)")
            except Exception as e:
                logger.error(f"Error during ECL staging: {str(e)}")
                # Continue with other operations
        
        # Process local impairment staging config if provided
        if local_impairment_config:
            try:
                # Create a new staging result entry regardless of whether there's loan data
                local_staging_result = StagingResult(
                    portfolio_id=portfolio_id,
                    staging_type="local_impairment",
                    config=local_impairment_config.dict(),
                    result_summary={
                        "status": "stored_config" if not has_data else "processing",
                        "timestamp": datetime.now().isoformat(),
                        "has_loan_data": has_data
                    }
                )
                db.add(local_staging_result)
                db.commit()
                
                # Only run the staging calculation if there's loan data
                if has_data:
                    local_staging = stage_loans_local_impairment(
                        portfolio_id=portfolio_id,
                        config=local_impairment_config,
                        db=db,
                        current_user=current_user
                    )
                    logger.info(f"Local impairment staging completed for portfolio {portfolio_id}")
                else:
                    logger.info(f"Local impairment config stored for portfolio {portfolio_id} (no loan data to process)")
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
def ingest_portfolio_data(
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
        Portfolio.id == portfolio_id, Portfolio.user_id == current_user.id
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
    result = process_portfolio_ingestion_sync(
        portfolio_id=portfolio_id,
        loan_details_content=loan_details.file.read(),
        client_data_content=client_data.file.read(),
        loan_guarantee_data_content=loan_guarantee_data.file.read() if loan_guarantee_data else None,
        loan_collateral_data_content=loan_collateral_data.file.read() if loan_collateral_data else None,
        db=db
    )
    
    return result

@router.get("/{portfolio_id}/calculate-ecl")
def calculate_ecl_provision(
    portfolio_id: int,
    reporting_date: Optional[date] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Calculate ECL provisions directly from a portfolio ID.
    This route uses the latest staging data stored in the database.
    The calculation is performed in a background task and returns a task ID for tracking progress.
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
    
    # Start the background task for ECL calculation
    try:
        return process_ecl_calculation_sync(
            portfolio_id=portfolio_id,
            reporting_date=reporting_date,
            staging_result=latest_staging,
            db=db
        )
    except Exception as e:
        logger.error(f"ECL calculation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/{portfolio_id}/stage-loans-ecl", response_model=StagingResponse)
def stage_loans_ecl(
    portfolio_id: int,
    config: ECLStagingConfig = Body(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Classify loans in the portfolio according to ECL staging criteria (Stage 1, 2, 3).
    Optimized for large datasets with 100K+ records.
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

    # OPTIMIZATION 1: Use SQL for initial classification
    # This performs the classification at the database level rather than in Python
    staged_loans_query = text("""
    WITH staged_loans AS (
        SELECT 
            id, 
            employee_id, 
            ndia, 
            outstanding_loan_balance,
            loan_issue_date,
            loan_amount,
            monthly_installment,
            loan_term,
            accumulated_arrears,
            CASE
                WHEN ndia IS NULL THEN 'Stage 3'
                WHEN ndia >= :stage_1_min AND ndia <= :stage_1_max THEN 'Stage 1'
                WHEN ndia >= :stage_2_min AND ndia <= :stage_2_max THEN 'Stage 2'
                WHEN ndia >= :stage_3_min THEN 'Stage 3'
                ELSE 'Stage 3'
            END as stage
        FROM loans
        WHERE portfolio_id = :portfolio_id
    )
    SELECT * FROM staged_loans
    """)
    
    # Prepare parameters for the query
    params = {
        "portfolio_id": portfolio_id,
        "stage_1_min": stage_1_range[0] if stage_1_range[0] is not None else 0,
        "stage_1_max": stage_1_range[1] if stage_1_range[1] is not None else 120,
        "stage_2_min": stage_2_range[0] if stage_2_range[0] is not None else 121,
        "stage_2_max": stage_2_range[1] if stage_2_range[1] is not None else 240,
        "stage_3_min": stage_3_range[0] if stage_3_range[0] is not None else 241
    }
    
    # Execute the query
    result = db.execute(staged_loans_query, params).fetchall()
    
    # Convert results to model instances (without loading all fields from DB)
    staged_loans = []
    
    # OPTIMIZATION 2: Count stages in the same pass
    stage1_count = 0
    stage2_count = 0 
    stage3_count = 0
    
    # OPTIMIZATION 3: Pre-calculate totals during iteration
    stage1_total = 0
    stage2_total = 0
    stage3_total = 0
    
    serialized_loans = []
    
    for row in result:
        # Count stages
        if row.stage == "Stage 1":
            stage1_count += 1
            if row.outstanding_loan_balance:
                stage1_total += float(row.outstanding_loan_balance)
        elif row.stage == "Stage 2":
            stage2_count += 1
            if row.outstanding_loan_balance:
                stage2_total += float(row.outstanding_loan_balance)
        else:  # Stage 3 or any other
            stage3_count += 1
            if row.outstanding_loan_balance:
                stage3_total += float(row.outstanding_loan_balance)
        
        # Create loan stage info
        loan_stage = LoanStageInfo(
            loan_id=row.id,
            employee_id=row.employee_id,
            stage=row.stage,
            outstanding_loan_balance=row.outstanding_loan_balance,
            ndia=int(row.ndia) if row.ndia is not None else 0,
            loan_issue_date=row.loan_issue_date,
            loan_amount=row.loan_amount,
            monthly_installment=row.monthly_installment,
            loan_term=row.loan_term,
            accumulated_arrears=row.accumulated_arrears,
        )
        staged_loans.append(loan_stage)
        
        # Serialize for DB storage (we only need a subset of data)
        serialized_loan = {
            "loan_id": row.id,
            "employee_id": row.employee_id,
            "stage": row.stage,
            "outstanding_loan_balance": float(row.outstanding_loan_balance) if row.outstanding_loan_balance else 0,
        }
        serialized_loans.append(serialized_loan)
    
    # OPTIMIZATION 4: Store summary data instead of detailed loan data
    total_count = stage1_count + stage2_count + stage3_count
    staged_at = datetime.now()
    
    # Create a result summary with aggregated data
    result_summary = {
        "total_loans": total_count,
        "Stage 1": {
            "num_loans": stage1_count,
            "outstanding_loan_balance": stage1_total,
            "total_loan_value": stage1_total,
            "provision_amount": float(stage1_total) * 0.01,
            "provision_rate": 0.01
        },
        "Stage 2": {
            "num_loans": stage2_count,
            "outstanding_loan_balance": stage2_total,
            "total_loan_value": stage2_total,
            "provision_amount": float(stage2_total) * 0.05,
            "provision_rate": 0.05
        },
        "Stage 3": {
            "num_loans": stage3_count,
            "outstanding_loan_balance": stage3_total,
            "total_loan_value": stage3_total,
            "provision_amount": float(stage3_total) * 0.15,
            "provision_rate": 0.15
        },
        "staged_at": staged_at.isoformat(),
        # OPTIMIZATION 5: Only store a sample of loan data, up to 1000 records
        "loans_sample": serialized_loans[:1000] if serialized_loans else []
    }
    
    # OPTIMIZATION 6: Use a separate transaction for storing the result
    staging_result = StagingResult(
        portfolio_id=portfolio_id,
        staging_type="ecl",
        config=config.dict(),
        result_summary=result_summary
    )
    db.add(staging_result)
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
    Classify loans in the portfolio according to local impairment categories.
    Optimized for large datasets with 100K+ records.
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

    # OPTIMIZATION 1: Use SQL for initial classification
    # This performs the classification at the database level rather than in Python
    staged_loans_query = text("""
    WITH staged_loans AS (
        SELECT 
            id, 
            employee_id, 
            COALESCE(ndia, 
                CASE 
                    WHEN accumulated_arrears IS NOT NULL AND monthly_installment IS NOT NULL AND monthly_installment > 0 
                    THEN CAST((accumulated_arrears / monthly_installment) * 30 AS INTEGER)
                    ELSE 0
                END
            ) as calculated_ndia,
            outstanding_loan_balance,
            loan_issue_date,
            loan_amount,
            monthly_installment,
            loan_term,
            accumulated_arrears,
            CASE
                WHEN ndia IS NULL AND (accumulated_arrears IS NULL OR monthly_installment IS NULL OR monthly_installment = 0) 
                    THEN 'Loss'
                WHEN COALESCE(ndia, 
                      CASE 
                          WHEN accumulated_arrears IS NOT NULL AND monthly_installment IS NOT NULL AND monthly_installment > 0 
                          THEN CAST((accumulated_arrears / monthly_installment) * 30 AS INTEGER)
                          ELSE 0
                      END) >= :current_min AND 
                     COALESCE(ndia, 
                      CASE 
                          WHEN accumulated_arrears IS NOT NULL AND monthly_installment IS NOT NULL AND monthly_installment > 0 
                          THEN CAST((accumulated_arrears / monthly_installment) * 30 AS INTEGER)
                          ELSE 0
                      END) <= :current_max THEN 'Current'
                WHEN COALESCE(ndia, 
                      CASE 
                          WHEN accumulated_arrears IS NOT NULL AND monthly_installment IS NOT NULL AND monthly_installment > 0 
                          THEN CAST((accumulated_arrears / monthly_installment) * 30 AS INTEGER)
                          ELSE 0
                      END) >= :olem_min AND 
                     COALESCE(ndia, 
                      CASE 
                          WHEN accumulated_arrears IS NOT NULL AND monthly_installment IS NOT NULL AND monthly_installment > 0 
                          THEN CAST((accumulated_arrears / monthly_installment) * 30 AS INTEGER)
                          ELSE 0
                      END) <= :olem_max THEN 'OLEM'
                WHEN COALESCE(ndia, 
                      CASE 
                          WHEN accumulated_arrears IS NOT NULL AND monthly_installment IS NOT NULL AND monthly_installment > 0 
                          THEN CAST((accumulated_arrears / monthly_installment) * 30 AS INTEGER)
                          ELSE 0
                      END) >= :substandard_min AND 
                     COALESCE(ndia, 
                      CASE 
                          WHEN accumulated_arrears IS NOT NULL AND monthly_installment IS NOT NULL AND monthly_installment > 0 
                          THEN CAST((accumulated_arrears / monthly_installment) * 30 AS INTEGER)
                          ELSE 0
                      END) <= :substandard_max THEN 'Substandard'
                WHEN COALESCE(ndia, 
                      CASE 
                          WHEN accumulated_arrears IS NOT NULL AND monthly_installment IS NOT NULL AND monthly_installment > 0 
                          THEN CAST((accumulated_arrears / monthly_installment) * 30 AS INTEGER)
                          ELSE 0
                      END) >= :doubtful_min AND 
                     COALESCE(ndia, 
                      CASE 
                          WHEN accumulated_arrears IS NOT NULL AND monthly_installment IS NOT NULL AND monthly_installment > 0 
                          THEN CAST((accumulated_arrears / monthly_installment) * 30 AS INTEGER)
                          ELSE 0
                      END) <= :doubtful_max THEN 'Doubtful'
                WHEN COALESCE(ndia, 
                      CASE 
                          WHEN accumulated_arrears IS NOT NULL AND monthly_installment IS NOT NULL AND monthly_installment > 0 
                          THEN CAST((accumulated_arrears / monthly_installment) * 30 AS INTEGER)
                          ELSE 0
                      END) >= :loss_min THEN 'Loss'
                ELSE 'Loss'
            END as stage
        FROM loans
        WHERE portfolio_id = :portfolio_id
    )
    SELECT * FROM staged_loans
    """)
    
    # Prepare parameters for the query
    params = {
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
    }
    
    # Execute the query
    result = db.execute(staged_loans_query, params).fetchall()
    
    # Initialize counters and containers
    staged_loans = []
    serialized_loans = []
    
    # Count variables
    current_count = 0
    olem_count = 0
    substandard_count = 0
    doubtful_count = 0
    loss_count = 0
    
    # Total variables
    current_total = 0
    olem_total = 0
    substandard_total = 0
    doubtful_total = 0
    loss_total = 0
    
    # Process results in a single pass
    for row in result:
        # Create loan stage info
        loan_stage = LoanStageInfo(
            loan_id=row.id,
            employee_id=row.employee_id,
            stage=row.stage,
            outstanding_loan_balance=row.outstanding_loan_balance,
            ndia=row.calculated_ndia,
            loan_issue_date=row.loan_issue_date,
            loan_amount=row.loan_amount,
            monthly_installment=row.monthly_installment,
            loan_term=row.loan_term,
            accumulated_arrears=row.accumulated_arrears,
        )
        staged_loans.append(loan_stage)
        
        # Count stages and sum totals
        balance = float(row.outstanding_loan_balance) if row.outstanding_loan_balance else 0
        
        if row.stage == "Current":
            current_count += 1
            current_total += balance
        elif row.stage == "OLEM":
            olem_count += 1
            olem_total += balance
        elif row.stage == "Substandard":
            substandard_count += 1
            substandard_total += balance
        elif row.stage == "Doubtful":
            doubtful_count += 1
            doubtful_total += balance
        else:  # Loss or any other
            loss_count += 1
            loss_total += balance
        
        # Serialize for DB storage (just the essential fields)
        serialized_loan = {
            "loan_id": row.id,
            "employee_id": row.employee_id,
            "stage": row.stage,
            "outstanding_loan_balance": balance,
        }
        serialized_loans.append(serialized_loan)
    
    # Create a summary with aggregates instead of detailed loan data
    total_count = current_count + olem_count + substandard_count + doubtful_count + loss_count
    staged_at = datetime.now()
    
    result_summary = {
        "total_loans": total_count,
        "Current": {
            "num_loans": current_count,
            "outstanding_loan_balance": current_total,
            "total_loan_value": current_total,
            "provision_amount": float(current_total) * 0.01,
            "provision_rate": 0.01
        },
        "OLEM": {
            "num_loans": olem_count,
            "outstanding_loan_balance": olem_total,
            "total_loan_value": olem_total,
            "provision_amount": float(olem_total) * 0.05,
            "provision_rate": 0.05
        },
        "Substandard": {
            "num_loans": substandard_count,
            "outstanding_loan_balance": substandard_total,
            "total_loan_value": substandard_total,
            "provision_amount": float(substandard_total) * 0.25,
            "provision_rate": 0.25
        },
        "Doubtful": {
            "num_loans": doubtful_count,
            "outstanding_loan_balance": doubtful_total,
            "total_loan_value": doubtful_total,
            "provision_amount": float(doubtful_total) * 0.50,
            "provision_rate": 0.50
        },
        "Loss": {
            "num_loans": loss_count,
            "outstanding_loan_balance": loss_total,
            "total_loan_value": loss_total,
            "provision_amount": float(loss_total) * 1.0,
            "provision_rate": 1.0
        },
        "staged_at": staged_at.isoformat(),
        "loans_sample": serialized_loans[:1000] if serialized_loans else []
    }
    
    # Store the result in a separate transaction
    staging_result = StagingResult(
        portfolio_id=portfolio_id,
        staging_type="local_impairment",
        config=config.dict(),
        result_summary=result_summary
    )
    db.add(staging_result)
    db.commit()
    
    return StagingResponse(loans=staged_loans)

@router.get("/{portfolio_id}/calculate-local-impairment")
def calculate_local_provision(
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
    
    # Start the background task for local impairment calculation
    try:
        return process_local_impairment_calculation_sync(
            portfolio_id=portfolio_id,
            reporting_date=reporting_date,
            staging_result=latest_staging,
            db=db
        )
    except Exception as e:
        logger.error(f"Local impairment calculation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Fixed optimized ECL staging implementation
async def stage_loans_ecl_optimized(portfolio_id: int, config: ECLStagingConfig, db: Session):
    """
    Highly optimized implementation of ECL staging using direct SQL for large datasets.
    """
    # Parse day ranges from config
    try:
        stage_1_range = parse_days_range(config.stage_1.days_range)
        stage_2_range = parse_days_range(config.stage_2.days_range)
        stage_3_range = parse_days_range(config.stage_3.days_range)
    except ValueError as e:
        logger.error(f"Error parsing ECL staging ranges: {str(e)}")
        return {"status": "error", "error": str(e)}
    
    # Use a direct SQL update to classify and store results
    try:
        # Get the current timestamp
        staged_at = datetime.now()
        
        # Execute a direct SQL query to count and sum by stage
        result = db.execute(text("""
        WITH staged_loans AS (
            SELECT 
                CASE
                    WHEN ndia IS NULL THEN 'Stage 3'
                    WHEN ndia >= :stage_1_min AND ndia <= :stage_1_max THEN 'Stage 1'
                    WHEN ndia >= :stage_2_min AND ndia <= :stage_2_max THEN 'Stage 2'
                    WHEN ndia >= :stage_3_min THEN 'Stage 3'
                    ELSE 'Stage 3'
                END as stage,
                COUNT(*) as count,
                SUM(outstanding_loan_balance) as total
            FROM loans
            WHERE portfolio_id = :portfolio_id
            GROUP BY 
                CASE
                    WHEN ndia IS NULL THEN 'Stage 3'
                    WHEN ndia >= :stage_1_min AND ndia <= :stage_1_max THEN 'Stage 1'
                    WHEN ndia >= :stage_2_min AND ndia <= :stage_2_max THEN 'Stage 2'
                    WHEN ndia >= :stage_3_min THEN 'Stage 3'
                    ELSE 'Stage 3'
                END
        )
        SELECT 
            SUM(CASE WHEN stage = 'Stage 1' THEN count ELSE 0 END) as stage1_count,
            SUM(CASE WHEN stage = 'Stage 1' THEN total ELSE 0 END) as stage1_total,
            SUM(CASE WHEN stage = 'Stage 2' THEN count ELSE 0 END) as stage2_count,
            SUM(CASE WHEN stage = 'Stage 2' THEN total ELSE 0 END) as stage2_total,
            SUM(CASE WHEN stage = 'Stage 3' THEN count ELSE 0 END) as stage3_count,
            SUM(CASE WHEN stage = 'Stage 3' THEN total ELSE 0 END) as stage3_total,
            SUM(count) as total_count
        FROM staged_loans
        """), {
            "portfolio_id": portfolio_id,
            "stage_1_min": stage_1_range[0] if stage_1_range[0] is not None else 0,
            "stage_1_max": stage_1_range[1] if stage_1_range[1] is not None else 120,
            "stage_2_min": stage_2_range[0] if stage_2_range[0] is not None else 121,
            "stage_2_max": stage_2_range[1] if stage_2_range[1] is not None else 240,
            "stage_3_min": stage_3_range[0] if stage_3_range[0] is not None else 241
        }).fetchone()
        
        # Create and store the result summary
        stage1_count = result.stage1_count or 0
        stage2_count = result.stage2_count or 0
        stage3_count = result.stage3_count or 0
        stage1_total = float(result.stage1_total or 0)
        stage2_total = float(result.stage2_total or 0)
        stage3_total = float(result.stage3_total or 0)
        
        # Update the staging result - fixed the query to avoid using astext
        staging_result = (
            db.query(StagingResult)
            .filter(
                StagingResult.portfolio_id == portfolio_id,
                StagingResult.staging_type == "ecl",
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
                "Stage 1": {
                    "num_loans": stage1_count,
                    "outstanding_loan_balance": stage1_total,
                    "total_loan_value": stage1_total,
                    "provision_amount": float(stage1_total) * 0.01,
                    "provision_rate": 0.01
                },
                "Stage 2": {
                    "num_loans": stage2_count,
                    "outstanding_loan_balance": stage2_total,
                    "total_loan_value": stage2_total,
                    "provision_amount": float(stage2_total) * 0.05,
                    "provision_rate": 0.05
                },
                "Stage 3": {
                    "num_loans": stage3_count,
                    "outstanding_loan_balance": stage3_total,
                    "total_loan_value": stage3_total,
                    "provision_amount": float(stage3_total) * 0.15,
                    "provision_rate": 0.15
                }
            }
            db.commit()
            
        return {
            "status": "success", 
            "loans_staged": int(result.total_count or 0),
            "Stage 1": {
                "num_loans": stage1_count,
                "outstanding_loan_balance": stage1_total,
                "total_loan_value": stage1_total,
                "provision_amount": float(stage1_total) * 0.01,
                "provision_rate": 0.01
            },
            "Stage 2": {
                "num_loans": stage2_count,
                "outstanding_loan_balance": stage2_total,
                "total_loan_value": stage2_total,
                "provision_amount": float(stage2_total) * 0.05,
                "provision_rate": 0.05
            },
            "Stage 3": {
                "num_loans": stage3_count,
                "outstanding_loan_balance": stage3_total,
                "total_loan_value": stage3_total,
                "provision_amount": float(stage3_total) * 0.15,
                "provision_rate": 0.15
            }
        }
    except Exception as e:
        logger.error(f"Error in optimized ECL staging: {str(e)}")
        db.rollback()
        return {"status": "error", "error": str(e)}

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
