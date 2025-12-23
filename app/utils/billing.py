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
    TenantSubscription,
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
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),
) -> TenantSubscription | None:
    """
    Enforces that the User's TENANT has an active subscription,
    unless the user is a super admin.
    """

    # 0. SUPER ADMIN BYPASS (must be first)
    if current_user.role == UserRole.SUPER_ADMIN:
        return None  # Explicit bypass, no subscription enforcement

    # 1. Get the Tenant from the current user
    tenant = current_user.tenant
    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User is not associated with a valid tenant.",
        )

    # 2. Check if Tenant has a subscription linked
    if not tenant.subscriptions:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail="Organization has no active subscription.",
        )

    # 3. Fetch the subscription
    # 3. Fetch the latest subscription for this tenant
    subscription = (
        db.query(TenantSubscription)
        .filter(TenantSubscription.tenant_id == tenant.id)
        .order_by(TenantSubscription.created_at.desc())
        .first()
    )

    if not subscription or subscription.status in ("expired", "cancelled", "past_due"):
        # Note: past_due might be allowed depending on business logic, but strict check blocks it.
        # usually past_due means grace period. stricter enforcement here.
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail="Organization subscription is expired, cancelled, or past due.",
        )

    from datetime import datetime, timezone

    now_utc = datetime.now(timezone.utc)
    period_end = subscription.current_period_end

    if period_end:
        if period_end.tzinfo is None:
            period_end = period_end.replace(tzinfo=timezone.utc)

        if period_end <= now_utc:
            raise HTTPException(
                status_code=status.HTTP_402_PAYMENT_REQUIRED,
                detail="Organization subscription has expired.",
            )

    # 4. Ensure usage row exists
    usage = (
        db.query(SubscriptionUsage)
        .filter(SubscriptionUsage.subscription_id == subscription.id)
        .first()
    )

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



