from fastapi import APIRouter, HTTPException, Request, Header, status, Depends
from typing import Optional
import os
import logging
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.utils.billing import verify_paystack_signature
from app.database import get_db
from app.models import (
    User,
    Tenant,
    TenantSubscription,
    SubscriptionPlan,
    SubscriptionUsage,
)

router = APIRouter(prefix="/webhooks", tags=["webhooks"])

# Configuration
PAYSTACK_SECRET_KEY = os.getenv("PAYSTACK_SECRET_KEY", "")
PAYSTACK_BASE_URL = "https://api.paystack.co"

logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.INFO)


@router.post(
    "/billing",
    status_code=status.HTTP_200_OK,
    responses={
        400: {"description": "Bad request"},
        401: {"description": "Unauthorized - invalid signature"},
        200: {"description": "Webhook processed successfully"},
    },
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    "schema": {"type": "object", "additionalProperties": True}
                }
            },
        }
    },
)
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
            "details": {},
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

            next_payment_date_raw = data.get("next_payment_date")

            if not subscription_code or not customer_email or not plan_code:
                logger.error("subscription.create: missing required fields")
                return {"status": "ignored", "reason": "invalid payload"}

            try:
                # 1. Resolve user + tenant
                user = (
                    db.query(User)
                    .filter(User.email == customer_email)
                    .one_or_none()
                )
                if not user or not user.tenant:
                    logger.error(
                        f"subscription.create: unresolved tenant for {customer_email}"
                    )
                    return {"status": "ignored", "reason": "tenant not found"}

                tenant = user.tenant

                # 2. Resolve plan (MANDATORY)
                plan = (
                    db.query(SubscriptionPlan)
                    .filter(
                        SubscriptionPlan.paystack_plan_code == plan_code
                    )
                    .one_or_none()
                )
                if not plan:
                    logger.critical(
                        f"subscription.create: unmapped plan_code={plan_code}"
                    )
                    raise RuntimeError("Unmapped Paystack plan")

                # 3. Close existing active subscriptions (CRITICAL)
                previous_subscriptions = (
                    db.query(TenantSubscription)
                    .filter(
                        TenantSubscription.tenant_id == tenant.id,
                        TenantSubscription.status.in_(
                            ["active", "past_due", "non-renewing"]
                        ),
                    )
                    .all()
                )

                for old_sub in previous_subscriptions:
                    old_sub.status = "expired"
                    old_sub.ended_at = datetime.now(timezone.utc)

                # 4. Idempotent upsert for new subscription
                subscription = (
                    db.query(TenantSubscription)
                    .filter(
                        TenantSubscription.paystack_subscription_code
                        == subscription_code
                    )
                    .one_or_none()
                )

                if not subscription:
                    subscription = TenantSubscription(
                        tenant_id=tenant.id,
                        plan_id=plan.id,
                        paystack_subscription_code=subscription_code,
                        paystack_customer_code=customer_code,
                        status="active",
                        started_at=datetime.now(timezone.utc),
                    )
                    db.add(subscription)
                    db.flush()
                else:
                    subscription.plan_id = plan.id
                    subscription.status = "active"

                # 5. Billing date (best effort)
                if next_payment_date_raw:
                    try:
                        subscription.next_billing_date = datetime.fromisoformat(
                            next_payment_date_raw
                        )
                    except ValueError:
                        logger.warning(
                            f"Invalid next_payment_date: {next_payment_date_raw}"
                        )

                # 6. Ensure usage row
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

                # 7. Tenant denormalized state
                tenant.paystack_customer_code = (
                    customer_code or tenant.paystack_customer_code
                )
                tenant.subscription_status = "active"

                db.commit()

            except Exception:
                db.rollback()
                logger.exception("subscription.create failed")
                raise


        elif event_type in ("subscription.not_renew", "subscription.disable"):
            subscription_code = data.get("subscription_code")
            status_from_paystack = data.get("status")
            customer = data.get("customer", {}) or {}

            subscription = (
                db.query(TenantSubscription)
                .filter(
                    TenantSubscription.paystack_subscription_code == subscription_code
                )
                .first()
            )

            if not subscription:
                logger.warning(
                    f"No local subscription found for code={subscription_code}"
                )
                return response_details

            now = datetime.now(timezone.utc)
            tenant = subscription.tenant

            # User cancels renewal (still active until end of period)
            if event_type == "subscription.not_renew":
                subscription.status = "non-renewing"
                subscription.cancelled_at = now

                if tenant:
                    tenant.subscription_status = "non-renewing"

            # Subscription actually ends or is force-disabled
            elif event_type == "subscription.disable":
                if status_from_paystack == "complete":
                    subscription.status = "expired"
                    if tenant:
                        tenant.subscription_status = "expired"

                elif status_from_paystack == "cancelled":
                    subscription.status = "cancelled"
                    subscription.cancelled_at = now
                    if tenant:
                        tenant.subscription_status = "cancelled"

            # Update customer code if present
            if tenant:
                tenant.paystack_customer_code = (
                    customer.get("customer_code") or tenant.paystack_customer_code
                )
                db.add(tenant)

            db.add(subscription)
            db.commit()

            logger.info(
                f"Subscription {subscription_code} handled: "
                f"event={event_type}, status={subscription.status}"
            )

            response_details["details"] = {
                "subscription_code": subscription_code,
                "new_status": subscription.status,
            }

        elif event_type == "invoice.create":
            invoice_code = data.get("invoice_code")
            customer_email = data.get("customer", {}).get("email")
            amount = data.get("amount")

            logger.info(
                f"Invoice created - Code: {invoice_code}, Amount: {amount}, Customer: {customer_email}"
            )

            response_details["details"] = {
                "invoice_code": invoice_code,
                "customer_email": customer_email,
                "amount": amount,
                "status": data.get("status"),
            }

        elif event_type == "invoice.payment_failed":
            invoice_code = data.get("invoice_code")
            customer_email = data.get("customer", {}).get("email")
            subscription_code = (
                data.get("subscription", {}).get("subscription_code")
                or data.get("subscription_code")
            )

            logger.warning(
                f"Invoice payment failed - Invoice: {invoice_code}, "
                f"Subscription: {subscription_code}, Customer: {customer_email}"
            )

            if subscription_code:
                try:
                    subscription = (
                        db.query(TenantSubscription)
                        .filter(
                            TenantSubscription.paystack_subscription_code
                            == subscription_code
                        )
                        .first()
                    )

                    if subscription:
                        # Determine if this is initial payment or renewal failure
                        is_initial_payment = subscription.status == "pending"

                        if is_initial_payment:
                            # Initial payment failed - cancel and potentially restore previous
                            subscription.status = "cancelled"
                            subscription.ended_at = datetime.now(timezone.utc)

                            tenant = subscription.tenant

                            # Try to restore previous active subscription
                            previous = (
                                db.query(TenantSubscription)
                                .filter(
                                    TenantSubscription.tenant_id == tenant.id,
                                    TenantSubscription.status == "expired",
                                    TenantSubscription.ended_at.isnot(None),
                                )
                                .order_by(TenantSubscription.ended_at.desc())
                                .first()
                            )

                            if previous:
                                previous.status = "active"
                                previous.ended_at = None
                                tenant.subscription_status = "active"
                                logger.info(
                                    f"Restored previous subscription {previous.paystack_subscription_code}"
                                )
                            else:
                                tenant.subscription_status = "inactive"

                        else:
                            # Renewal payment failed - mark as past_due for retry
                            subscription.status = "past_due"

                            if subscription.tenant:
                                subscription.tenant.subscription_status = "past_due"

                        db.commit()
                        logger.info(
                            f"Processed invoice.payment_failed for subscription {subscription_code}, "
                            f"new status: {subscription.status}"
                        )

                except Exception:
                    db.rollback()
                    logger.exception("invoice.payment_failed handling failed")
                    raise

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

            logger.info(
                f"Transfer successful - Reference: {reference}, Amount: {amount}"
            )

            response_details["details"] = {
                "reference": reference,
                "amount": amount,
                "recipient_name": recipient_details.get("name"),
                "status": data.get("status"),
            }

        elif event_type == "transfer.failed":
            reference = data.get("reference")
            amount = data.get("amount")

            logger.error(f"Transfer failed - Reference: {reference}, Amount: {amount}")

            response_details["details"] = {
                "reference": reference,
                "amount": amount,
                "failure_reason": data.get("gateway_response"),
            }

        elif event_type == "refund.processed":
            reference = data.get("transaction_reference")
            amount = data.get("amount")

            logger.info(
                f"Refund processed - Transaction: {reference}, Amount: {amount}"
            )

            response_details["details"] = {
                "transaction_reference": reference,
                "refund_amount": amount,
                "status": data.get("status"),
            }

        else:
            logger.warning(f"Unhandled webhook event type: {event_type}")
            response_details["details"] = {
                "message": "Event type not specifically handled",
                "raw_data": data,
            }

        return response_details

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        raise HTTPException(status_code=400, detail="Invalid webhook payload")
