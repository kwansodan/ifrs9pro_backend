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

from app.schemas import (
    CustomerCreate,
    TransactionInitialize,
    SubscriptionDisable,
    SubscriptionEnable,
    ChangeSubscriptionRequest
    )
from app.utils.billing import paystack_request, verify_paystack_signature
from app.auth.utils import is_admin, get_current_active_user
from app.database import get_db
from app.dependencies import get_tenant_db
from app.models import (
    User,
    TenantSubscription,
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
@router.get("/plans",
            status_code=status.HTTP_200_OK,
            responses={500: {"description": "Paystack failure"},
                    502: {"description": "External payment provider error"},
                    429: {"description": "Rate limit exceeded"},
                    200: {"description": "Customer details"},
                    401: {"description": "Not authenticated"}},
            )
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


@router.get(
    "/pricing",
    status_code=status.HTTP_200_OK,
    responses={
        200: {"description": "Plans with active flag"},
        401: {"description": "Not authenticated"},
        502: {"description": "Paystack error"},
    },
)
async def list_plans_with_status(
    page: int = 1,
    per_page: int = 50,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_tenant_db),
):
    """
    List all Paystack plans and mark the user's current plan as active.
    """

    # ---------------------------------------------------------
    # 1. Fetch current tenant subscription (LOCAL DB first)
    # ---------------------------------------------------------
    active_plan_code: str | None = None

    subscription = (
        db.query(TenantSubscription)
        .filter(
            TenantSubscription.status.in_(
                ["active", "past_due", "non-renewing"]
            )
        )
        .order_by(TenantSubscription.created_at.desc())
        .first()
    )

    if subscription:
        try:
            sub_result = await paystack_request(
                "GET",
                f"/subscription/{subscription.paystack_subscription_code}",
            )

            active_plan_code = (
                sub_result.get("data", {})
                .get("plan", {})
                .get("plan_code")
            )
        except Exception:
            # Fallback to local DB plan if Paystack fails
            if subscription.plan:
                active_plan_code = subscription.plan.paystack_plan_code

    # ---------------------------------------------------------
    # 2. Fetch all plans from Paystack
    # ---------------------------------------------------------
    result = await paystack_request(
        "GET",
        f"/plan?page={page}&perPage={per_page}",
    )

    plans = []
    seen_ids = set()

    for plan in result.get("data", []):
        plan_id = plan["id"]

        if plan_id in seen_ids:
            continue
        seen_ids.add(plan_id)

        plan_code = plan["plan_code"]

        plans.append(
            {
                "name": plan["name"],
                "amount": plan["amount"],
                "currency": plan["currency"],
                "interval": plan["interval"],
                "code": plan_code,
                "status": (
                    "active"
                    if active_plan_code
                    and plan_code == active_plan_code
                    else "inactive"
                ),
            }
        )

    return {
        "status": True,
        "active_plan_code": active_plan_code,
        "plans": plans,
    }


@router.post("/customers",
             status_code=status.HTTP_201_CREATED,
             responses={502: {"description": "Paystack unavailable"},
                        429: {"description": "Rate limit exceeded"},
                        200: {"description": "Customer details"},
                        401: {"description": "Not authenticated"},
                       },
)
async def create_customer(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db),  # IMPORTANT
):
    if not current_user.tenant:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User is not associated with a tenant",
        )

    customer_data = {
        "email": current_user.email,
        "first_name": current_user.tenant.name,
        "last_name": "Company",
        "phone": current_user.phone_number,
        "metadata": {
            "tenant_id": current_user.tenant.id,
            "industry": current_user.tenant.industry,
            "country": current_user.tenant.country,
            "admin_name": f"{current_user.first_name} {current_user.last_name}",
        },
    }

    result = await paystack_request("POST", "/customer", customer_data)

    if not result.get("status") or not result.get("data"):
        raise HTTPException(
            status_code=502,
            detail="Failed to create Paystack customer",
        )

    customer_code = result["data"].get("customer_code")
    if not customer_code:
        raise HTTPException(
            status_code=502,
            detail="Paystack customer code missing",
        )

    current_user.tenant.paystack_customer_code = customer_code
    db.commit()
    db.refresh(current_user.tenant)

    return result


@router.get("/customers/me",
            status_code=status.HTTP_200_OK,
            responses={
                200: {"description": "Customer details"},
                401: {"description": "Not authenticated"}
            })
async def get_my_customer(current_user: User = Depends(get_current_active_user)):
    """Fetch authenticated user's customer details"""
    result = await paystack_request("GET", f"/customer/{current_user.email}")
    return result


@router.post("/transactions/initialize",
             status_code=status.HTTP_201_CREATED,
             responses={500: {"description": "Paystack failure"},
                        422: {"description": "Paystack failure"},
                        200: {"description": "Customer details"},
                        401: {"description": "Not authenticated"},
                        400: {"description": "Invalid amount"},
                    })
async def initialize_transaction(
    transaction: TransactionInitialize,
    current_user: User = Depends(get_current_active_user)
):
    """Initialize a transaction (amount in kobo) using authenticated user's email"""
    transaction_data = transaction.model_dump(exclude_none=True)
    transaction_data["email"] = current_user.email  # Use authenticated user's email
    result = await paystack_request("POST", "/transaction/initialize", transaction_data)
    return result


@router.post("/subscriptions/disable",
             status_code=status.HTTP_200_OK,
             responses={500: {"description": "Paystack failure"},
                        502: {"description": "External payment provider error"},
                        422: {"description": "Invalid or missing parameters"},
                        400: {"description": "Missing parameters"},
                        200: {"description": "Customer details"},
                        401: {"description": "Not authenticated"}
                    },
)
async def disable_subscription(subscription: SubscriptionDisable):
    """Disable a subscription using subscription code and email token"""
    data = subscription.model_dump()
    result = await paystack_request("POST", "/subscription/disable", data)
    return result


@router.post("/subscriptions/enable",
             status_code=status.HTTP_200_OK,
             responses={500: {"description": "Paystack failure"},
                        402: {"description": "Payment required to enable subscription"},
                        404: {"description": "Subscription not found"},
                        200: {"description": "Customer details"},
                        401: {"description": "Not authenticated"}
                    },
)
async def enable_subscription(subscription: SubscriptionEnable):
    """Disable a subscription using subscription code and email token"""
    data = subscription.model_dump()
    result = await paystack_request("POST", "/subscription/enable", data)
    return result



@router.get("/subscriptions",
            status_code=status.HTTP_200_OK,
            responses={404: {"description": "No active subscription found"},
                    200: {"description": "Subscription details"},
                    401: {"description": "Not authenticated"}},)
async def get_subscription(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_tenant_db)
):
    """Get current tenant's subscription details"""
    if not current_user.tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User is not associated with a tenant"
        )

    # Query for the latest active (or past_due/non-renewing) subscription for this tenant.
    # The query is automatically filtered by tenant_id via get_tenant_db
    subscription = (
        db.query(TenantSubscription)
        .filter(TenantSubscription.status.in_(["active", "past_due", "non-renewing"]))
        .order_by(TenantSubscription.created_at.desc())
        .first()
    )

    if not subscription:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No active subscription found for tenant"
        )

    try:
        result = await paystack_request(
            "GET", f"/subscription/{subscription.paystack_subscription_code}"
        )
        return result
    except Exception as e:
        logger.error(f"Failed to fetch subscription details: {e}")
        # Fallback to local data if Paystack call fails
        return {
            "status": True,
            "message": "Local subscription retrieved",
            "data": {
                "subscription_code": subscription.paystack_subscription_code,
                "status": subscription.status,
                "amount": 0, # Cannot determine current amount without Paystack
                "next_payment_date": subscription.next_billing_date.isoformat() if subscription.next_billing_date else None,
                "plan": {
                    "plan_code": subscription.plan.paystack_plan_code if subscription.plan else None,
                    "name": subscription.plan.name if subscription.plan else "Unknown Plan"
                }
            }
        }

@router.get(
    "/overview",
    status_code=status.HTTP_200_OK,
    responses={
        200: {"description": "Billing summary"},
        401: {"description": "Not authenticated"},
        502: {"description": "Paystack error"},
    },
)
async def billing_summary(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_tenant_db),
) -> Dict[str, Any]:
    """
    Return a small billing summary for the authenticated user/tenant:
      - number of active subscriptions (from Paystack customer payload)
      - number of expired subscriptions (from Paystack customer payload)
      - current plan (name, plan_code, amount, currency)
      - billing cycle (interval)
      - billing email
    """

    # Defaults
    active_count = 0
    expired_count = 0
    current_plan = None
    billing_cycle = None
    billing_email = None

    # 1) Fetch Paystack customer (contains list of subscriptions)
    try:
        customer_res = await paystack_request("GET", f"/customer/{current_user.email}")
        customer_data = customer_res.get("data", {}) if isinstance(customer_res, dict) else {}
        subscriptions = customer_data.get("subscriptions", []) or []
        # classify statuses
        expired_statuses = {"expired", "cancelled", "non-renewing"}
        for s in subscriptions:
            status = (s.get("status") or "").lower()
            if status == "active":
                active_count += 1
            elif status in expired_statuses:
                expired_count += 1
            else:
                # anything else treat as expired-ish
                expired_count += 1
    except Exception as e:
        logger.error(f"Failed to fetch Paystack customer for {current_user.email}: {e}")
        # keep counts as 0 and continue — we'll still try to fetch subscription details from DB

    # 2) Fetch authoritative subscription details for the tenant (single subscription)
    try:
        if not current_user.tenant:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User has no tenant")

        subscription = (
            db.query(TenantSubscription)
            .filter(TenantSubscription.status.in_(["active", "past_due", "non-renewing"]))
            .order_by(TenantSubscription.created_at.desc())
            .first()
        )

        if subscription:
            # billing email fallback
            billing_email = current_user.email

            try:
                sub_res = await paystack_request(
                    "GET", f"/subscription/{subscription.paystack_subscription_code}"
                )
                sub_data = sub_res.get("data", {}) if isinstance(sub_res, dict) else {}

                # current plan info (if present)
                plan = sub_data.get("plan") or {}
                if plan:
                    current_plan = {
                        "name": plan.get("name"),
                        "plan_code": plan.get("plan_code") or plan.get("code"),
                        "amount": plan.get("amount"),
                        "currency": plan.get("currency"),
                    }
                    billing_cycle = plan.get("interval")

                # billing email from subscription customer object if present
                customer_obj = sub_data.get("customer") or {}
                billing_email = customer_obj.get("email") or billing_email

            except Exception as e:
                logger.error(f"Failed to fetch Paystack subscription {subscription.paystack_subscription_code}: {e}")
                # fallback to local denormalized plan if available
                if subscription.plan:
                    current_plan = {
                        "name": subscription.plan.name,
                        "plan_code": subscription.plan.paystack_plan_code,
                        "amount": getattr(subscription.plan, "amount", None),
                        "currency": getattr(subscription.plan, "currency", None),
                    }
                    # billing_cycle unknown from DB if not stored; leave as None
                billing_email = billing_email or current_user.email

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error while resolving tenant subscription for user {current_user.email}: {e}")

    return {
        "status": True,
        "data": {
            "active_subscriptions": active_count,
            "expired_subscriptions": expired_count,
            "current_plan": current_plan,
            "billing_cycle": billing_cycle,
            "billing_email": billing_email,
        },
    }


@router.get(
    "/subscriptions/manage-link",
    status_code=status.HTTP_200_OK,
    responses={
        404: {"description": "No active subscription found"},
        500: {"description": "Paystack failure"},
        401: {"description": "Not authenticated"},
        200: {"description": "Subscription manage link"},
    },
)
async def get_subscription_manage_link(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_tenant_db),
):
    """
    Generate a Paystack-hosted URL that allows the tenant
    to update the card on their current subscription.
    """

    if not current_user.tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User is not associated with a tenant",
        )

    # Get latest usable subscription for tenant
    subscription = (
        db.query(TenantSubscription)
        .filter(
            TenantSubscription.status.in_(
                ["active", "past_due", "non-renewing"]
            )
        )
        .order_by(TenantSubscription.created_at.desc())
        .first()
    )

    if not subscription:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No active subscription found for tenant",
        )

    try:
        result = await paystack_request(
            "GET",
            f"/subscription/{subscription.paystack_subscription_code}/manage/link",
        )
        return result

    except Exception as e:
        logger.error(f"Failed to generate subscription manage link: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unable to generate subscription manage link",
        )

@router.post(
    "/subscriptions/change",
    status_code=status.HTTP_201_CREATED,
    responses={
        404: {"description": "No active subscription found"},
        500: {"description": "Paystack failure"},
        422: {"description": "Invalid parameters"},
        401: {"description": "Not authenticated"},
        200: {"description": "Subscription change initiated"},
    },
)
async def change_subscription(
    payload: ChangeSubscriptionRequest,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_tenant_db),
):
    """
    Change tenant subscription by:
    1. Initializing a new subscription for a new plan
    2. Disabling the existing subscription (from DB)
    """

    if not current_user.tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User is not associated with a tenant",
        )

    # 1. Fetch current subscription from DB (authoritative)
    old_subscription = (
        db.query(TenantSubscription)
        .filter(
            TenantSubscription.status.in_(
                ["active", "past_due", "non-renewing"]
            )
        )
        .order_by(TenantSubscription.created_at.desc())
        .first()
    )

    if not old_subscription:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No active subscription found for tenant",
        )

    # 2. Initialize new subscription (Paystack creates it after payment)
    transaction_data = {
        "email": current_user.email,
        "plan": payload.new_plan_code,
        "amount": 3000
    }

    init_result = await paystack_request(
        "POST",
        "/transaction/initialize",
        transaction_data,
    )

    if not init_result.get("status"):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to initialize new subscription",
        )

    # 3. Disable old subscription
    disable_data = {
        "code": old_subscription.paystack_subscription_code,
        "token": payload.old_subscription_token,
    }

    await paystack_request(
        "POST",
        "/subscription/disable",
        disable_data,
    )

    return {
        "status": True,
        "message": "Subscription change initiated",
        "data": {
            "authorization_url": init_result["data"]["authorization_url"],
            "access_code": init_result["data"]["access_code"],
            "reference": init_result["data"]["reference"],
        },
    }



@router.get("/transactions/verify/{reference}",
            status_code=status.HTTP_200_OK,
            responses={500: {"description": "Transaction not found"},
                    502: {"description": "External payment provider error"},
                    400: {"description": "Invalid transaction reference"},
                    200: {"description": "Customer details"},
                    401: {"description": "Not authenticated"}},
            )
async def verify_transaction(reference: str):
    """Verify transaction by reference"""
    result = await paystack_request("GET", f"/transaction/verify/{reference}")
    return result
