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
from app.database import get_db
from app.models import (
    User,
    UserSubscription,
    SubscriptionPlan,
    SubscriptionUsage,
)

router = APIRouter(prefix="/webhooks", 
                   tags=["webhooks"],
)

# Configuration
PAYSTACK_SECRET_KEY = os.getenv("PAYSTACK_SECRET_KEY", "")
PAYSTACK_BASE_URL = "https://api.paystack.co"

logger = logging.getLogger("uvicorn.error")  # attach to Uvicorn
logger.setLevel(logging.INFO)


@router.post("/billing", 
             status_code=status.HTTP_200_OK,
             responses={400: {"description": "Bad request"},
                        401: {"description": "Unauthorized - invalid signature"},
                        200: {"description": "Webhook processed successfully"},
                        },)
async def paystack_webhook(
    request: Request,
    x_paystack_signature: Optional[str] = Header(None),
    db: Session = Depends(get_db),
):
    """Handle Paystack webhook events"""
    body = await request.body()
    
    if not x_paystack_signature:
        logger.warning("Webhook received without signature header")
        raise HTTPException(status_code=401, detail="Missing signature header")
    
    try:
        event_data = await request.json()
        event_type = event_data.get("event")
        data = event_data.get("data", {})
        
        # Verify signature
        if not verify_paystack_signature(body, x_paystack_signature):
            raise HTTPException(status_code=400, detail="Invalid signature")
        
        logger.info(f"Webhook received - Event type: {event_type}")
        
        response_details = {
            "status": "success",
            "event_type": event_type,
            "processed_at": data.get("created_at") or data.get("createdAt"),
            "details": {}
        }
        
        # Handle different event types
        if event_type == "charge.success":
            reference = data.get("reference")
            amount = data.get("amount")
            customer_email = data.get("customer", {}).get("email")
            currency = data.get("currency")
            
            logger.info(f"Payment successful - Reference: {reference}, Amount: {amount} {currency}, Customer: {customer_email}")
            
            response_details["details"] = {
                "reference": reference,
                "amount": amount,
                "currency": currency,
                "customer_email": customer_email,
                "status": data.get("status")
            }
            
        elif event_type == "subscription.create":
            subscription_code = data.get("subscription_code")
            customer = data.get("customer") or {}
            customer_email = customer.get("email")
            customer_code = customer.get("customer_code")

            plan_data = data.get("plan") or {}
            plan_code = plan_data.get("plan_code") or plan_data.get("code")
            plan_name = plan_data.get("name")

            next_payment_date_raw = data.get("next_payment_date")

            logger.info(
                f"subscription.create | sub={subscription_code} "
                f"plan={plan_code} user={customer_email}"
            )
        
            if not subscription_code or not customer_email or not plan_code:
                logger.error("Webhook payload missing required fields")
                return {"status": "ignored", "reason": "invalid payload"}

            try:
                # 1. Resolve user
                user = (
                    db.query(User)
                    .filter(User.email == customer_email)
                    .one_or_none()
                )
                if not user:
                    logger.error(f"No local user for email={customer_email}")
                    return {"status": "ignored", "reason": "user not found"}

                # 2. Resolve plan (MANDATORY)
                plan = (
                    db.query(SubscriptionPlan)
                    .filter(SubscriptionPlan.paystack_plan_code == plan_code)
                    .one_or_none()
                )
                if not plan:
                    logger.critical(
                        f"Rejecting subscription.create: unknown plan_code={plan_code}"
                    )
                    raise HTTPException(
                        status_code=500,
                        detail=f"Unmapped Paystack plan_code={plan_code}"
                    )

                # 3. Idempotent subscription upsert
                subscription = (
                    db.query(UserSubscription)
                    .filter(
                        UserSubscription.paystack_subscription_code
                        == subscription_code
                    )
                    .one_or_none()
                )

                if not subscription:
                    subscription = UserSubscription(
                        user_id=user.id,
                        plan_id=plan.id,
                        paystack_subscription_code=subscription_code,
                        paystack_customer_code=customer_code,
                        status="active",  # MUST exist in enum
                        started_at=datetime.now(timezone.utc),
                    )
                    db.add(subscription)
                    db.flush()
                else:
                    subscription.plan_id = plan.id
                    subscription.paystack_customer_code = (
                        customer_code or subscription.paystack_customer_code
                    )
                    subscription.status = "active"

                # 4. Billing dates (best effort)
                if next_payment_date_raw:
                    try:
                        subscription.next_billing_date = datetime.fromisoformat(
                            next_payment_date_raw
                        )
                    except ValueError:
                        logger.warning(
                            f"Invalid next_payment_date: {next_payment_date_raw}"
                        )

                # 5. Ensure usage row
                usage = (
                    db.query(SubscriptionUsage)
                    .filter(
                        SubscriptionUsage.subscription_id == subscription.id
                    )
                    .one_or_none()
                )
                if not usage:
                    db.add(
                        SubscriptionUsage(
                            subscription_id=subscription.id,
                            current_loan_count=0,
                            current_portfolio_count=0,
                            current_team_count=0,
                        )
                    )

                # 6. Update user linkage (single active subscription)
                user.paystack_customer_code = (
                    customer_code or user.paystack_customer_code
                )
                user.current_subscription_id = subscription.id
                user.subscription_status = "active"

                db.commit()

            except Exception as e:
                db.rollback()
                logger.exception("subscription.create webhook failed")
                raise

            response_details["details"] = {
                "status": "processed",
                "subscription_code": subscription_code,
                "plan_code": plan_code,
                "user": customer_email
            }
            logger.info(response_details)


        elif event_type in ("subscription.not_renew", "subscription.disable"):
            subscription_code = data.get("subscription_code")
            status_from_paystack = data.get("status")  # ONLY relevant for disable
            customer = data.get("customer", {}) or {}

            admin_user = db.query(User).filter(User.role == "admin").first()
            if not admin_user:
                logger.error("No admin user found")
                raise HTTPException(status_code=500, detail="Admin user not found")

            subscription = (
                db.query(UserSubscription)
                .filter(UserSubscription.paystack_subscription_code == subscription_code)
                .first()
            )

            if not subscription:
                logger.warning(f"No local subscription found for code={subscription_code}")
                return

            now = datetime.now(timezone.utc)

            # 1️⃣ User cancels renewal (still active)
            if event_type == "subscription.not_renew":
                subscription.status = "cancelled"   # non-renewing
                subscription.cancelled_at = now

                admin_user.subscription_status = "cancelled"
                # DO NOT remove access
                # DO NOT clear current_subscription_id

            # 2️⃣ Subscription actually ends or is force-disabled
            elif event_type == "subscription.disable":
                if status_from_paystack == "complete":
                    # Natural expiry after paid period
                    subscription.status = "expired"

                    admin_user.subscription_status = "expired"
                    admin_user.current_subscription_id = None

                elif status_from_paystack == "cancelled":
                    # Immediate hard cancel
                    subscription.status = "cancelled"
                    subscription.cancelled_at = now

                    admin_user.subscription_status = "cancelled"
                    admin_user.current_subscription_id = None

            admin_user.paystack_customer_code = (
                customer.get("customer_code") or admin_user.paystack_customer_code
            )

            db.add(subscription)
            db.add(admin_user)
            db.commit()

            logger.info(
                f"Subscription {subscription_code} handled: "
                f"event={event_type}, status={subscription.status}"
            )



        elif event_type == "invoice.create":
            invoice_code = data.get("invoice_code")
            customer_email = data.get("customer", {}).get("email")
            amount = data.get("amount")
            
            logger.info(f"Invoice created - Code: {invoice_code}, Amount: {amount}, Customer: {customer_email}")
            
            response_details["details"] = {
                "invoice_code": invoice_code,
                "customer_email": customer_email,
                "amount": amount,
                "status": data.get("status")
            }
            
        elif event_type == "invoice.payment_failed":
            invoice_code = data.get("invoice_code")
            customer_email = data.get("customer", {}).get("email")
            subscription_code = data.get("subscription", {}).get("subscription_code") or data.get("subscription_code")

            logger.warning(
                f"Invoice payment failed - Invoice: {invoice_code}, "
                f"Subscription: {subscription_code}, Customer: {customer_email}"
            )

            if subscription_code:
                subscription = (
                    db.query(UserSubscription)
                    .filter(UserSubscription.paystack_subscription_code == subscription_code)
                    .first()
                )
                if subscription:
                    subscription.status = "past_due"
                    db.add(subscription)

                    user = db.query(User).filter(User.id == subscription.user_id).first()
                    if user and user.current_subscription_id == subscription.id:
                        user.subscription_status = "past_due"
                        db.add(user)

                    db.commit()

            response_details["details"] = {
                "invoice_code": invoice_code,
                "customer_email": customer_email,
                "failure_reason": data.get("gateway_response"),
                "subscription_code": subscription_code,
            }
            
        elif event_type == "transfer.success":
            reference = data.get("reference")
            amount = data.get("amount")
            recipient_details = data.get("recipient", {})
            
            logger.info(f"Transfer successful - Reference: {reference}, Amount: {amount}")
            
            response_details["details"] = {
                "reference": reference,
                "amount": amount,
                "recipient_name": recipient_details.get("name"),
                "status": data.get("status")
            }
            
        elif event_type == "transfer.failed":
            reference = data.get("reference")
            amount = data.get("amount")
            
            logger.error(f"Transfer failed - Reference: {reference}, Amount: {amount}")
            
            response_details["details"] = {
                "reference": reference,
                "amount": amount,
                "failure_reason": data.get("gateway_response")
            }
            
        elif event_type == "refund.processed":
            reference = data.get("transaction_reference")
            amount = data.get("amount")
            
            logger.info(f"Refund processed - Transaction: {reference}, Amount: {amount}")
            
            response_details["details"] = {
                "transaction_reference": reference,
                "refund_amount": amount,
                "status": data.get("status")
            }
            
        else:
            logger.warning(f"Unhandled webhook event type: {event_type}")
            response_details["details"] = {
                "message": "Event type not specifically handled",
                "raw_data": data
            }
        
        return response_details
    except HTTPException:
        raise  # Re-raise HTTP exceptions
    except Exception as e:
        # Log the error but don't expose internals
        logger.error(f"Webhook error: {e}")
        raise HTTPException(status_code=400, detail="Invalid webhook payload")