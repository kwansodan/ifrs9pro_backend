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

    # --- Portfolio Overview ---

    # Count total loans
    total_loans = (
        db.query(func.count(Loan.id))
        .filter(Loan.portfolio_id.in_(portfolio_ids))
        .scalar()
        or 0
    )

    # Initialize counters for total ECL and local impairment
    total_ecl_amount = 0
    total_local_impairment = 0
    total_risk_reserve = 0

    # Get the latest calculation results for each portfolio
    portfolio_summaries = []
    current_date = datetime.now().date()

    for portfolio in portfolios:
        # Get latest ECL calculation for this portfolio
        latest_ecl_calculation = (
            db.query(CalculationResult)
            .filter(
                CalculationResult.portfolio_id == portfolio.id,
                CalculationResult.calculation_type == "ecl"
            )
            .order_by(CalculationResult.created_at.desc())
            .first()
        )
        
        # Get latest local impairment calculation for this portfolio
        latest_local_impairment = (
            db.query(CalculationResult)
            .filter(
                CalculationResult.portfolio_id == portfolio.id,
                CalculationResult.calculation_type == "local_impairment"
            )
            .order_by(CalculationResult.created_at.desc())
            .first()
        )
        
        # Set portfolio ECL and local impairment values
        portfolio_ecl = float(latest_ecl_calculation.total_provision) if latest_ecl_calculation else 0
        portfolio_local_impairment = float(latest_local_impairment.total_provision) if latest_local_impairment else 0
        
        # Calculate risk reserve (local impairment - ECL)
        portfolio_risk_reserve = portfolio_local_impairment - portfolio_ecl
        
        # Add to totals
        total_ecl_amount += portfolio_ecl
        total_local_impairment += portfolio_local_impairment
        total_risk_reserve += portfolio_risk_reserve

        # Count loans in this portfolio
        portfolio_loans_count = (
            db.query(func.count(Loan.id))
            .filter(Loan.portfolio_id == portfolio.id)
            .scalar()
            or 0
        )

        # Calculate total loan value in this portfolio
        portfolio_loan_value = (
            db.query(func.sum(Loan.outstanding_loan_balance))
            .filter(Loan.portfolio_id == portfolio.id)
            .scalar()
            or 0
        )

        # Count customers in this portfolio
        portfolio_customers_count = (
            db.query(func.count(Client.id))
            .filter(Client.portfolio_id == portfolio.id)
            .scalar()
            or 0
        )

        # If we don't have saved calculations, calculate on-the-fly
        if portfolio_ecl == 0 and portfolio_loans_count > 0:
            portfolio_loans = db.query(Loan).filter(Loan.portfolio_id == portfolio.id).all()
            
            # Calculate ECL for this portfolio
            portfolio_ecl = 0
            for loan in portfolio_loans:
                # Skip loans that are fully paid or have no outstanding balance
                if (
                    loan.paid
                    or not loan.outstanding_loan_balance
                    or loan.outstanding_loan_balance <= 0
                ):
                    continue

                # Get securities for this loan if applicable
                securities = []

                # Calculate ECL components
                try:
                    ead_percentage = calculate_exposure_at_default_percentage(
                        loan, current_date
                    )
                    pd = calculate_probability_of_default(
                        loan, db)
                    lgd = calculate_loss_given_default(loan, securities)

                    # Calculate ECL for this loan
                    loan_ecl = calculate_marginal_ecl(loan, ead_percentage, pd, lgd)
                    portfolio_ecl += loan_ecl
                except Exception as e:
                    # Skip loans that cause errors in ECL calculation
                    continue
        
        portfolio_summaries.append(
            {
                "id": portfolio.id,
                "name": portfolio.name,
                "description": portfolio.description,
                "asset_type": portfolio.asset_type,
                "customer_type": portfolio.customer_type,
                "total_loans": portfolio_loans_count,
                "total_loan_value": (
                    float(portfolio_loan_value) if portfolio_loan_value else 0
                ),
                "total_customers": portfolio_customers_count,
                "ecl_amount": round(portfolio_ecl, 2),
                "local_impairment_amount": round(portfolio_local_impairment, 2),
                "risk_reserve": round(portfolio_risk_reserve, 2),
                "created_at": (
                    portfolio.created_at.isoformat() if portfolio.created_at else None
                ),
                "updated_at": (
                    portfolio.updated_at.isoformat() if portfolio.updated_at else None
                ),
            }
        )

    # Sort portfolios by total loan value (descending)
    portfolio_summaries.sort(key=lambda x: x["total_loan_value"], reverse=True)

    # --- Customer Overview ---
    # Count total customers
    total_customers = (
        db.query(func.count(Client.id))
        .filter(Client.portfolio_id.in_(portfolio_ids))
        .scalar()
        or 0
    )

    # Count customers by type
    institutional_customers = (
        db.query(func.count(Client.id))
        .filter(
            Client.portfolio_id.in_(portfolio_ids), Client.client_type == "institution"
        )
        .scalar()
        or 0
    )

    individual_customers = (
        db.query(func.count(Client.id))
        .filter(
            Client.portfolio_id.in_(portfolio_ids), Client.client_type == "consumer"
        )
        .scalar()
        or 0
    )

    mixed_customers = total_customers - institutional_customers - individual_customers

    return {
        "portfolio_overview": {
            "total_portfolios": len(portfolios),
            "total_loans": total_loans,
            "total_ecl_amount": round(total_ecl_amount, 2),
            "total_local_impairment": round(total_local_impairment, 2),
            "total_risk_reserve": round(total_risk_reserve, 2),
        },
        "customer_overview": {
            "total_customers": total_customers,
            "institutional": institutional_customers,
            "individual": individual_customers,
            "mixed": mixed_customers,
        },
        "portfolios": portfolio_summaries,
    }
