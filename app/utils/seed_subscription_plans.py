from sqlalchemy.orm import Session
from app.models import SubscriptionPlan

PLANS = [
    {
        "name": "CORE",
        "paystack_plan_code": "PLN_5s9ms4hgelzzscr",
        "max_loan_data": 1000,
        "max_portfolios": 5,
        "max_team_size": 5,
        "price": 12000.00,
        "currency": "GHS",
        "is_active": True,
    },
    {
        "name": "PROFESSIONAL",
        "paystack_plan_code": "PLN_7s3l587hlaouany",
        "max_loan_data": 10000,
        "max_portfolios": 20,
        "max_team_size": 20,
        "price": 20000.00,
        "currency": "GHS",
        "is_active": True,
    },
    {
        "name": "ENTERPRISE",
        "paystack_plan_code": "PLN_miyxov2l51hus5w",
        "max_loan_data": 100000,
        "max_portfolios": 100,
        "max_team_size": 100,
        "price": 30000.00,
        "currency": "GHS",
        "is_active": True,
    },
]


def seed_subscription_plans(db: Session) -> None:
    for plan in PLANS:
        exists = (
            db.query(SubscriptionPlan)
            .filter(
                SubscriptionPlan.paystack_plan_code
                == plan["paystack_plan_code"]
            )
            .first()
        )

        if not exists:
            db.add(SubscriptionPlan(**plan))

    db.commit()
