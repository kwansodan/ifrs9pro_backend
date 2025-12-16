from fastapi import APIRouter, HTTPException, Request, Header, status, Depends
from typing import Optional, Dict, Any
import httpx
import hmac
import hashlib
import os

from sqlalchemy.orm import Session

from app.database import get_db
from app.auth.utils import get_current_active_user
from app.models import (
    User,
    UserSubscription,
    SubscriptionPlan,
    SubscriptionUsage,
    UserRole
)
from datetime import datetime, timezone

router = APIRouter(prefix="/billing", tags=["billing"])

# Configuration
PAYSTACK_SECRET_KEY = os.getenv("PAYSTACK_SECRET_KEY", "")
PAYSTACK_BASE_URL = "https://api.paystack.co"


# Pydantic Models


# Helper function
async def paystack_request(method: str, endpoint: str,  data: Optional[Dict] = None) -> Dict:
    """Make authenticated request to Paystack API"""
    headers = {
        "Authorization": f"Bearer {PAYSTACK_SECRET_KEY}",
        "Content-Type": "application/json"
    }
    
    url = f"{PAYSTACK_BASE_URL}{endpoint}"
    
    async with httpx.AsyncClient() as client:
        try:
            if method.upper() == "GET":
                response = await client.get(url, headers=headers)
            elif method.upper() == "POST":
                response = await client.post(url, headers=headers, json=data)
            else:
                raise HTTPException(status_code=400, detail=f"Unsupported method: {method}")
            
            response.raise_for_status()
            return response.json()
            
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=f"Paystack error: {e.response.text}")
        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail=f"Connection failed: {str(e)}")


def verify_paystack_signature(payload: bytes, signature: str) -> bool:
    """Verify Paystack webhook signature"""
    if not PAYSTACK_SECRET_KEY:
        return False
    
    computed = hmac.new(
        PAYSTACK_SECRET_KEY.encode('utf-8'),
        payload,
        hashlib.sha512
    ).hexdigest()
    
    return hmac.compare_digest(computed, signature)


async def require_active_subscription(
    db: Session = Depends(get_db),
) -> UserSubscription:
    """
    Global dependency to enforce that the admin user has an active subscription.
    Grants access to all users if admin's subscription is active.
    """
    # 🔑 Subscription authority = ADMIN
    admin_user = (
        db.query(User)
        .filter(User.role == UserRole.ADMIN)
        .order_by(User.id.asc())
        .first()
    )

    if not admin_user or not admin_user.current_subscription_id:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail="Active admin subscription required.",
        )

    subscription = db.query(UserSubscription).filter(
        UserSubscription.id == admin_user.current_subscription_id
    ).first()

    if not subscription or subscription.status != "active":
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail="Admin subscription is not active.",
        )

    from datetime import datetime, timezone
    now_utc = datetime.now(timezone.utc)

    period_end = subscription.current_period_end

    if period_end:
        # Normalize DB value to UTC-aware
        if period_end.tzinfo is None:
            period_end = period_end.replace(tzinfo=timezone.utc)

        if period_end <= now_utc:
            raise HTTPException(
                status_code=status.HTTP_402_PAYMENT_REQUIRED,
                detail="Admin subscription has expired.",
            )

    # Ensure usage row exists for downstream checks
    usage = db.query(SubscriptionUsage).filter(
        SubscriptionUsage.subscription_id == subscription.id
    ).first()

    if not usage:
        usage = SubscriptionUsage(
            subscription_id=subscription.id,
            current_loan_count=0,
            current_portfolio_count=0,
            current_team_count=0,
        )
        db.add(usage)
        db.commit()

    return subscription


