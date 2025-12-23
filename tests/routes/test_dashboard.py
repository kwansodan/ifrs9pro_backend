
from app.models import Portfolio, Loan, Client, CalculationResult
from datetime import datetime, timedelta, date, timezone
from app.models import Portfolio


def test_dashboard_endpoint(client):
    resp = client.get("/dashboard")
    assert resp.status_code == 200
    body = resp.json()
    assert "portfolio_overview" in body
    assert "customer_overview" in body

def test_dashboard_empty_state(client, db_session, regular_user, tenant):
    """
    When user has no portfolios, dashboard should return zeroed values.
    """

    resp = client.get("/dashboard")
    assert resp.status_code == 200

    body = resp.json()

    assert body["portfolio_overview"]["total_portfolios"] == 0
    assert body["portfolio_overview"]["total_loans"] == 0
    assert body["portfolio_overview"]["total_ecl_amount"] == 0
    assert body["portfolio_overview"]["total_local_impairment"] == 0
    assert body["portfolio_overview"]["total_risk_reserve"] == 0

    assert body["customer_overview"]["total_customers"] == 0
    assert body["portfolios"] == []


def test_dashboard_with_portfolio_no_data(client, db_session, regular_user, tenant):
    # Arrange: Create empty portfolio
    p = Portfolio(
        user_id=regular_user.id,
        tenant_id=tenant.id,
        name="Test Portfolio",
        description="desc",
        asset_type="loan",
        customer_type="institution"
    )
    db_session.add(p)
    db_session.commit()


    resp = client.get("/dashboard")
    assert resp.status_code == 200
    data = resp.json()

    assert data["portfolio_overview"]["total_portfolios"] == 1
    assert data["portfolio_overview"]["total_loans"] == 0

    assert len(data["portfolios"]) == 1
    pf = data["portfolios"][0]
    assert pf["id"] == p.id
    assert pf["total_loans"] == 0
    assert pf["total_customers"] == 0
    assert pf["ecl_amount"] == 0
    assert pf["local_impairment_amount"] == 0


def test_dashboard_full_data(client, db_session, regular_user, tenant):

    # Create portfolio
    p = Portfolio(
        user_id=regular_user.id,
        tenant_id=tenant.id,
        name="Test Portfolio",
        description="desc",
        asset_type="loan",
        customer_type="institution"
    )
    db_session.add(p)
    db_session.commit()

    # Add a loan
    loan = Loan(
        portfolio_id=p.id,
        tenant_id=tenant.id,
        outstanding_loan_balance=1000,
        loan_amount=2000,
    )
    db_session.add(loan)

    # Add a customer
    client_item = Client(
        portfolio_id=p.id,
        tenant_id=tenant.id,
        client_type="institution"
    )
    db_session.add(client_item)

    # Add last ECL result
    ecl = CalculationResult(
        portfolio_id=p.id,
        calculation_type="ecl",
        config={},
        result_summary={},
        total_provision=300,
        provision_percentage=0.5,
        reporting_date=date.today(),
        created_at=datetime.now(timezone.utc) # Use same tz aware/naive logic as in main code if needed
    )
    db_session.add(ecl)

    # Add last local impairment result
    local = CalculationResult(
        portfolio_id=p.id,
        calculation_type="local_impairment",
        config={},
        result_summary={},
        total_provision=500,
        provision_percentage=0.7,
        reporting_date=date.today(),
        created_at=datetime.now(timezone.utc)
    )
    db_session.add(local)

    db_session.commit()

    # Call dashboard
    resp = client.get("/dashboard")
    assert resp.status_code == 200

    body = resp.json()
    assert body["portfolio_overview"]["total_portfolios"] == 1
    assert body["portfolio_overview"]["total_loans"] == 1
    assert body["portfolio_overview"]["total_ecl_amount"] == 300
    assert body["portfolio_overview"]["total_local_impairment"] == 500