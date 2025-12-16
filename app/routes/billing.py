from fastapi import APIRouter, HTTPException, Request, Header, status, Depends
from pydantic import BaseModel, EmailStr, Field
from typing import Optional, Dict, Any
import httpx
import hmac
import hashlib
import os
import logging
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.schemas import CustomerCreate, TransactionInitialize, SubscriptionDisable
from app.utils.billing import paystack_request, verify_paystack_signature
from app.auth.utils import is_admin, get_current_active_user
from app.database import get_db
from app.models import (
    User,
    UserSubscription,
    SubscriptionPlan,
    SubscriptionUsage,
)

router = APIRouter(prefix="/billing", 
                   tags=["billing"],
                   dependencies=[Depends(is_admin)],
)

# Configuration
PAYSTACK_SECRET_KEY = os.getenv("PAYSTACK_SECRET_KEY", "")
PAYSTACK_BASE_URL = "https://api.paystack.co"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Endpoints
@router.get("/plans", status_code=status.HTTP_200_OK)
async def list_plans(page: int = 1, per_page: int = 50):
    """List all subscription plans"""
    result = await paystack_request("GET", f"/plan?page={page}&perPage={per_page}")
    seen = {}
    for plan in result["data"]:
        plan_id = plan["id"]  # guaranteed unique
        if plan_id not in seen:
            seen[plan_id] = {
                "name": plan["name"],
                "amount": plan["amount"],
                "currency": plan["currency"],
                "interval": plan["interval"],
                "code": plan["plan_code"],         
            }
    return list(seen.values())


@router.post("/customers", status_code=status.HTTP_201_CREATED)
async def create_customer(
    customer: CustomerCreate,
    current_user: User = Depends(get_current_active_user)
):
    """Create a new customer using authenticated user's email"""
    customer_data = customer.model_dump(exclude_none=True)
    customer_data["email"] = current_user.email  # Use authenticated user's email
    result = await paystack_request("POST", "/customer", customer_data)
    return result


@router.get("/customers/me", status_code=status.HTTP_200_OK)
async def get_my_customer(current_user: User = Depends(get_current_active_user)):
    """Fetch authenticated user's customer details"""
    result = await paystack_request("GET", f"/customer/{current_user.email}")
    return result


@router.post("/transactions/initialize", status_code=status.HTTP_201_CREATED)
async def initialize_transaction(
    transaction: TransactionInitialize,
    current_user: User = Depends(get_current_active_user)
):
    """Initialize a transaction (amount in kobo) using authenticated user's email"""
    transaction_data = transaction.model_dump(exclude_none=True)
    transaction_data["email"] = current_user.email  # Use authenticated user's email
    result = await paystack_request("POST", "/transaction/initialize", transaction_data)
    return result


@router.post("/subscriptions/disable", status_code=status.HTTP_200_OK)
async def disable_subscription(subscription: SubscriptionDisable):
    """Disable a subscription using subscription code and email token"""
    data = subscription.model_dump()
    result = await paystack_request("POST", "/subscription/disable", data)
    return result


@router.get("/subscriptions", status_code=status.HTTP_200_OK)
async def get_subscription(
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Get current admin subscription details"""
    if not current_user.current_subscription_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No active subscription found for current user"
        )
    
    subscription = db.query(UserSubscription).filter(
        UserSubscription.id == current_user.current_subscription_id
    ).first()
    
    if not subscription:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Subscription not found"
        )
    
    result = await paystack_request(
        "GET", f"/subscription/{subscription.paystack_subscription_code}"
    )
    return result


@router.post("/subscriptions/manage", status_code=status.HTTP_200_OK)
async def manage_subscription(
    action: str,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Enable, disable, or cancel the current admin subscription"""
    valid_actions = ["enable", "disable", "cancel"]
    if action not in valid_actions:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid action. Must be one of: {', '.join(valid_actions)}"
        )

    if not current_user.current_subscription_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No active subscription found for current user"
        )
    
    subscription = db.query(UserSubscription).filter(
        UserSubscription.id == current_user.current_subscription_id
    ).first()
    
    if not subscription:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Subscription not found"
        )
    
    result = await paystack_request(
        "POST",
        f"/subscription/{action}",
        {"code": subscription.paystack_subscription_code, "token": subscription.paystack_subscription_code}
    )
    return result


@router.get("/transactions/verify/{reference}", status_code=status.HTTP_200_OK)
async def verify_transaction(reference: str):
    """Verify transaction by reference"""
    result = await paystack_request("GET", f"/transaction/verify/{reference}")
    return result


