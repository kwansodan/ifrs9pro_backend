from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional
from app.database import get_db
from app.models import Portfolio, User, AssetType, CustomerType, FundingSource, DataSource
from app.schemas import PortfolioCreate, PortfolioUpdate, PortfolioResponse, PortfolioList
from app.auth.utils import get_current_active_user

router = APIRouter(
    prefix="/portfolios",
    tags=["portfolios"]
)

@router.post("/", response_model=PortfolioResponse, status_code=status.HTTP_201_CREATED)
def create_portfolio(
    portfolio: PortfolioCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
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
        user_id=current_user.id
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
    current_user: User = Depends(get_current_active_user)
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
    
    return {
        "items": portfolios,
        "total": total
    }

@router.get("/{portfolio_id}", response_model=PortfolioResponse)
def get_portfolio(
    portfolio_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Retrieve a specific portfolio by ID.
    """
    portfolio = db.query(Portfolio).filter(
        Portfolio.id == portfolio_id,
        Portfolio.user_id == current_user.id
    ).first()
    
    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Portfolio not found"
        )
    
    return portfolio

@router.put("/{portfolio_id}", response_model=PortfolioResponse)
def update_portfolio(
    portfolio_id: int,
    portfolio_update: PortfolioUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Update a specific portfolio by ID.
    """
    portfolio = db.query(Portfolio).filter(
        Portfolio.id == portfolio_id,
        Portfolio.user_id == current_user.id
    ).first()
    
    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Portfolio not found"
        )
    
    # Update fields if provided
    update_data = portfolio_update.dict(exclude_unset=True)
    
    # Convert enum values to strings for storage
    if 'asset_type' in update_data and update_data['asset_type']:
        update_data['asset_type'] = update_data['asset_type'].value
    if 'customer_type' in update_data and update_data['customer_type']:
        update_data['customer_type'] = update_data['customer_type'].value
    if 'funding_source' in update_data and update_data['funding_source']:
        update_data['funding_source'] = update_data['funding_source'].value
    if 'data_source' in update_data and update_data['data_source']:
        update_data['data_source'] = update_data['data_source'].value
    
    for key, value in update_data.items():
        setattr(portfolio, key, value)
    
    db.commit()
    db.refresh(portfolio)
    return portfolio

@router.delete("/{portfolio_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_portfolio(
    portfolio_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Delete a specific portfolio by ID.
    """
    portfolio = db.query(Portfolio).filter(
        Portfolio.id == portfolio_id,
        Portfolio.user_id == current_user.id
    ).first()
    
    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Portfolio not found"
        )
    
    db.delete(portfolio)
    db.commit()
    
    return None

