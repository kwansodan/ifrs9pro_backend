from datetime import datetime

from app.models import AccessRequest, RequestStatus, User, UserRole, Feedback, Help, TenantSubscription, SubscriptionPlan, SubscriptionUsage
from app.auth.utils import get_password_hash

import csv
import re

from io import StringIO

def test_admin_get_requests(client, db_session, admin_user, tenant):
    req = AccessRequest(email="adminreq@example.com", is_email_verified=True, tenant_id=tenant.id)
    db_session.add(req)
    db_session.commit()

    resp = client.get("/admin/requests")
    assert resp.status_code == 200
    assert len(resp.json()) >= 1


def test_admin_update_request_approves_and_sets_role(client, db_session, tenant):
    req = AccessRequest(email="adminupdate@example.com", is_email_verified=True, tenant_id=tenant.id)
    db_session.add(req)
    db_session.commit()

    resp = client.put(
        f"/admin/requests/{req.id}",
        json={"status": RequestStatus.APPROVED, "role": UserRole.USER},
    )
    assert resp.status_code == 200
    assert resp.json()["message"] == "Request updated successfully"

    db_session.refresh(req)
    assert req.status == RequestStatus.APPROVED
    assert req.role == UserRole.USER


def test_admin_delete_access_request(client, db_session, tenant):
    req = AccessRequest(email="adminupdate@example.com", is_email_verified=True, tenant_id=tenant.id)
    db_session.add(req)
    db_session.commit()

    resp = client.delete(f"/admin/requests/{req.id}")
    assert resp.status_code == 204


def test_get_users(client, db_session):
    resp = client.get("/admin/users")
    assert resp.status_code == 200
    users = resp.json()
    assert isinstance(users, list)

    for user in users:
        assert "id" in user
        assert "email" in user
        assert "first_name" in user
        assert "last_name" in user
        assert "role" in user
        assert "is_active" in user


def to_snake(name: str):
    """Converts 'First Name' → 'first_name'."""
    return re.sub(r"\W+", "_", name).lower().strip("_")


def test_export_users(client, db_session, tenant):
    from app.models import User
    from app.auth.utils import get_password_hash

    # Arrange
    user1 = User(
        email="alpha@example.com",
        first_name="Alpha",
        last_name="Tester",
        hashed_password=get_password_hash("pass123"),
        role="admin",
        is_active=True,
        tenant_id=tenant.id
    )

    user2 = User(
        email="beta@example.com",
        first_name="Beta",
        last_name="User",
        hashed_password=get_password_hash("pass456"),
        role="user",
        is_active=False,
        tenant_id=tenant.id
    )

    db_session.add_all([user1, user2])
    db_session.commit()

    # Act
    resp = client.get("/admin/users/export")

    # Assert basic response
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")

    # Parse CSV
    csv_content = resp.text
    reader = csv.DictReader(StringIO(csv_content))
    rows = list(reader)

    assert len(rows) >= 2

    # Convert headers → snake_case
    normalized_headers = {to_snake(h) for h in rows[0].keys()}

    expected = {
        "id",
        "first_name",
        "last_name",
        "email",
        "role",
        "is_active",
        "created_at",
        "last_login",
    }

    # Validate that normalized headers contain expected fields
    assert normalized_headers >= expected

    # Optional: Validate that users exist in CSV
    emails = [r["Email"] for r in rows]
    assert "alpha@example.com" in emails
    assert "beta@example.com" in emails


def test_get_user_details(client, db_session, tenant):
    # --- Arrange: create a test user in the DB ---
    from app.models import User
    from app.auth.utils import get_password_hash

    user = User(
        email="test@example.com",
        first_name="Test",
        last_name="User",
        hashed_password=get_password_hash("password123"),
        role="admin",
        is_active=True,
        tenant_id=tenant.id
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    # --- Act: call the endpoint ---
    resp = client.get(f"/admin/users/{user.id}")
    assert resp.status_code == 200

    # --- Assert: check response fields ---
    data = resp.json()

    assert data["id"] == user.id
    assert data["email"] == "test@example.com"
    assert data["first_name"] == "Test"
    assert data["last_name"] == "User"
    assert data["role"] == "admin"
    assert data["is_active"] is True



def test_admin_create_user(client, db_session, tenant):
    resp = client.post(
        "/admin/users",
        json={
            "email": "newadminuser@example.com",
            "password": "secret123",
            "first_name": "New",
            "last_name": "User",
            "role": "user",
            "recovery_email": None,
            "portfolio_id": None,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["email"] == "newadminuser@example.com"


def test_admin_cannot_exceed_team_limit_returns_402(client, db_session, tenant, admin_user):
    """
    Robust test: discover the plan & current usage at runtime, consume remaining slots,
    then assert creating one more user returns HTTP 402.
    """
    # get subscription/plan/usage for this tenant
    subscription = (
        db_session.query(TenantSubscription)
        .filter(TenantSubscription.tenant_id == tenant.id)
        .first()
    )
    assert subscription, "tenant must have an active subscription for this test"

    plan = (
        db_session.query(SubscriptionPlan)
        .filter(SubscriptionPlan.id == subscription.plan_id)
        .first()
    )
    usage = (
        db_session.query(SubscriptionUsage)
        .filter(SubscriptionUsage.subscription_id == subscription.id)
        .first()
    )

    current_count = usage.current_team_count if usage else 0
    max_team = plan.max_team_size if plan else 0

    # how many additional users are allowed right now
    remaining = max_team - current_count
    assert remaining >= 0, "subscription usage is larger than plan max (data issue)"

    payload = {
        "password": "secret123",
        "first_name": "New",
        "last_name": "User",
        "role": "user",
        "recovery_email": None,
        "portfolio_id": None,
    }

    # If no slots remain, the very first create must be rejected with 402
    if remaining == 0:
        resp = client.post(
            "/admin/users",
            json={**payload, "email": "user_extra@example.com"},
        )
        assert resp.status_code == 402
        detail = resp.json().get("detail", "").lower()
        assert "limit" in detail and ("team" in detail or "member" in detail)
        return

    # Otherwise consume the remaining slots (these should succeed)
    for i in range(remaining):
        resp = client.post(
            "/admin/users",
            json={**payload, "email": f"user_consume_{i+1}@example.com"},
        )
        assert resp.status_code == 200, resp.text

    # Attempt one more — must be rejected
    resp = client.post(
        "/admin/users",
        json={**payload, "email": "user_beyond_limit@example.com"},
    )
    assert resp.status_code == 402, resp.text

    detail = resp.json().get("detail", "").lower()
    assert "limit" in detail
    assert ("team" in detail) or ("member" in detail)



def test_admin_export_users_csv(client):
    resp = client.get("/admin/users/export")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")


def test_admin_feedback_flow(client, db_session, regular_user, tenant):
    feedback = Feedback(
        description="Test feedback",
        status="submitted",
        user_id=regular_user.id,
        tenant_id=tenant.id
    )
    db_session.add(feedback)
    db_session.commit()

    list_resp = client.get("/admin/feedback")
    assert list_resp.status_code == 200
    assert len(list_resp.json()) >= 1

    status_resp = client.put(
        f"/admin/feedback/{feedback.id}/status",
        json={"status": "closed"},
    )
    assert status_resp.status_code == 200


def test_admin_help_flow(client, db_session, regular_user, tenant):
    # --- Arrange ---
    help_item = Help(
        description="Need help with this testing ASAP. Let's get the text to more than 10 words so it does not fail",
        status="submitted",
        user_id=regular_user.id,
        tenant_id=tenant.id
    )
    db_session.add(help_item)
    db_session.commit()

    # --- Act: list first ---
    list_resp = client.get("/admin/help")
    assert list_resp.status_code == 200
    assert len(list_resp.json()) >= 1

    # --- Act: update status ---
    status_resp = client.put(
        f"/admin/help/{help_item.id}/status",
        json={"status": "in development"},
    )
    assert status_resp.status_code == 200

    # --- Act: delete help entry ---
    delete_resp = client.delete(f"/admin/help/{help_item.id}")

    # --- Assert DELETE success ---
    assert delete_resp.status_code == 204
    assert delete_resp.text in ("", None)

    # --- Assert deleted in DB ---
    deleted = db_session.query(Help).filter(Help.id == help_item.id).first()
    assert deleted is None

    # --- Assert deleting again returns 404 ---
    second_delete = client.delete(f"/admin/help/{help_item.id}")
    assert second_delete.status_code == 404

