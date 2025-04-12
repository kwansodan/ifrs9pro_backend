from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import func, select
from typing import List, Dict, Any
from datetime import datetime

from app.database import get_db
from app.models import Portfolio, User, Loan, Client, Report, QualityIssue, CalculationResult
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
    calculate_loan_impairment,
    parse_days_range,
)

router = APIRouter(tags=["dashboard"])


@router.get("/dashboard")
def get_dashboard(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
):
    """
    Get dashboard information including:
    - Portfolio overview (total loans, ECL amount, risk reserve)
    - Customer overview (total customers by type)
    - Portfolio list
    """
    # Get all portfolios for current user
    portfolios = db.query(Portfolio).filter(Portfolio.user_id == current_user.id).all()

    if not portfolios:
        return {
            "name": current_user.first_name,
            "portfolio_overview": {
                "total_portfolios": 0,
                "total_loans": 0,
                "total_ecl_amount": 0,
                "total_local_impairment": 0,
                "total_risk_reserve": 0,
            },
            "customer_overview": {
                "total_customers": 0,
                "institutional": 0,
                "individual": 0,
                "mixed": 0,
            },
            "portfolios": [],
        }

    # Get portfolio IDs
    portfolio_ids = [p.id for p in portfolios]

    # --- OPTIMIZATION: Use a single query to get loan counts and values by portfolio ---
    loan_stats_by_portfolio = {}
    loan_stats = (
        db.query(
            Loan.portfolio_id,
            func.count(Loan.id).label("loan_count"),
            func.sum(Loan.outstanding_loan_balance).label("loan_value")
        )
        .filter(Loan.portfolio_id.in_(portfolio_ids))
        .group_by(Loan.portfolio_id)
        .all()
    )
    
    for stats in loan_stats:
        loan_stats_by_portfolio[stats.portfolio_id] = {
            "loan_count": stats.loan_count,
            "loan_value": float(stats.loan_value) if stats.loan_value else 0
        }
    
    # --- OPTIMIZATION: Use a single query to get customer counts by portfolio ---
    customer_stats_by_portfolio = {}
    customer_stats = (
        db.query(
            Client.portfolio_id,
            func.count(Client.id).label("customer_count")
        )
        .filter(Client.portfolio_id.in_(portfolio_ids))
        .group_by(Client.portfolio_id)
        .all()
    )
    
    for stats in customer_stats:
        customer_stats_by_portfolio[stats.portfolio_id] = {
            "customer_count": stats.customer_count
        }
    
    # --- OPTIMIZATION: Get all latest ECL calculations in a single query ---
    latest_ecl_calculations = {}
    ecl_subquery = (
        db.query(
            CalculationResult.portfolio_id,
            func.max(CalculationResult.created_at).label("max_date")
        )
        .filter(
            CalculationResult.portfolio_id.in_(portfolio_ids),
            CalculationResult.calculation_type == "ecl"
        )
        .group_by(CalculationResult.portfolio_id)
        .subquery()
    )
    
    ecl_results = (
        db.query(CalculationResult)
        .join(
            ecl_subquery,
            (CalculationResult.portfolio_id == ecl_subquery.c.portfolio_id) &
            (CalculationResult.created_at == ecl_subquery.c.max_date) &
            (CalculationResult.calculation_type == "ecl")
        )
        .all()
    )
    
    for result in ecl_results:
        latest_ecl_calculations[result.portfolio_id] = result
    
    # --- OPTIMIZATION: Get all latest local impairment calculations in a single query ---
    latest_local_impairments = {}
    local_subquery = (
        db.query(
            CalculationResult.portfolio_id,
            func.max(CalculationResult.created_at).label("max_date")
        )
        .filter(
            CalculationResult.portfolio_id.in_(portfolio_ids),
            CalculationResult.calculation_type == "local_impairment"
        )
        .group_by(CalculationResult.portfolio_id)
        .subquery()
    )
    
    local_results = (
        db.query(CalculationResult)
        .join(
            local_subquery,
            (CalculationResult.portfolio_id == local_subquery.c.portfolio_id) &
            (CalculationResult.created_at == local_subquery.c.max_date) &
            (CalculationResult.calculation_type == "local_impairment")
        )
        .all()
    )
    
    for result in local_results:
        latest_local_impairments[result.portfolio_id] = result

    # --- OPTIMIZATION: Get customer type counts in a single query ---
    customer_type_counts = {
        "total": 0,
        "institutional": 0,
        "individual": 0
    }
    
    customer_type_stats = (
        db.query(
            Client.client_type,
            func.count(Client.id).label("count")
        )
        .filter(Client.portfolio_id.in_(portfolio_ids))
        .group_by(Client.client_type)
        .all()
    )
    
    for stats in customer_type_stats:
        if stats.client_type == "institution":
            customer_type_counts["institutional"] = stats.count
        elif stats.client_type == "consumer":
            customer_type_counts["individual"] = stats.count
        
        customer_type_counts["total"] += stats.count
    
    # --- Process portfolio data ---
    total_ecl_amount = 0
    total_local_impairment = 0
    total_risk_reserve = 0
    total_loans = 0
    portfolio_summaries = []
    
    for portfolio in portfolios:
        # Get loan and customer stats for this portfolio
        loan_stats = loan_stats_by_portfolio.get(portfolio.id, {"loan_count": 0, "loan_value": 0})
        customer_stats = customer_stats_by_portfolio.get(portfolio.id, {"customer_count": 0})
        
        # Get latest calculation results
        latest_ecl = latest_ecl_calculations.get(portfolio.id)
        latest_local = latest_local_impairments.get(portfolio.id)
        
        # Set portfolio ECL and local impairment values
        portfolio_ecl = float(latest_ecl.total_provision) if latest_ecl else 0
        portfolio_local_impairment = float(latest_local.total_provision) if latest_local else 0
        
        # Calculate risk reserve (local impairment - ECL)
        portfolio_risk_reserve = portfolio_local_impairment - portfolio_ecl
        
        # Add to totals
        total_ecl_amount += portfolio_ecl
        total_local_impairment += portfolio_local_impairment
        total_risk_reserve += portfolio_risk_reserve
        total_loans += loan_stats["loan_count"]
        
        portfolio_summaries.append({
            "id": portfolio.id,
            "name": portfolio.name,
            "description": portfolio.description,
            "asset_type": portfolio.asset_type,
            "customer_type": portfolio.customer_type,
            "total_loans": loan_stats["loan_count"],
            "total_loan_value": loan_stats["loan_value"],
            "total_customers": customer_stats["customer_count"],
            "ecl_amount": round(portfolio_ecl, 2),
            "local_impairment_amount": round(portfolio_local_impairment, 2),
            "risk_reserve": round(portfolio_risk_reserve, 2),
            "created_at": portfolio.created_at.isoformat() if portfolio.created_at else None,
            "updated_at": portfolio.updated_at.isoformat() if portfolio.updated_at else None,
        })
    
    # Sort portfolios by total loan value (descending)
    portfolio_summaries.sort(key=lambda x: x["total_loan_value"], reverse=True)
    
    # Calculate mixed customers
    mixed_customers = customer_type_counts["total"] - customer_type_counts["institutional"] - customer_type_counts["individual"]
    
    return {
        "name": current_user.first_name,
        "portfolio_overview": {
            "total_portfolios": len(portfolios),
            "total_loans": total_loans,
            "total_ecl_amount": round(total_ecl_amount, 2),
            "total_local_impairment": round(total_local_impairment, 2),
            "total_risk_reserve": round(total_risk_reserve, 2),
        },
        "customer_overview": {
            "total_customers": customer_type_counts["total"],
            "institutional": customer_type_counts["institutional"],
            "individual": customer_type_counts["individual"],
            "mixed": mixed_customers,
        },
        "portfolios": portfolio_summaries,
    }
