from datetime import datetime, timedelta
from datetime import datetime, timedelta
from fastapi.testclient import TestClient

from app.auth.utils import (
    create_email_verification_token,
    create_invitation_token,
    get_password_hash,
)
from app.models import AccessRequest, RequestStatus, User


def test_request_access_creates_record(client, db_session):
    payload = {"email": "newuser@example.com"}
    resp = client.post("/request-access", json=payload)
    assert resp.status_code == 200
    assert resp.json()["message"] == "Verification email sent"

    record = db_session.query(AccessRequest).filter_by(email=payload["email"]).first()
    assert record is not None
    assert record.status == RequestStatus.PENDING


def test_verify_email_marks_request_verified(client, db_session):
    email = "verify@example.com"
    token = create_email_verification_token(email)
    req = AccessRequest(
        email=email,
        token=token,
        token_expiry=datetime.utcnow() + timedelta(hours=1),
        status=RequestStatus.PENDING,
    )
    db_session.add(req)
    db_session.commit()

    resp = client.get(f"/verify-email/{token}")
    assert resp.status_code == 200
    assert "Email successfully verified" in resp.json()["message"]

    db_session.refresh(req)
    assert req.is_email_verified is True


def test_submit_admin_request_updates_admin_email(client, db_session):
    email = "submit@example.com"
    req = AccessRequest(
        email=email,
        is_email_verified=True,
        status=RequestStatus.PENDING,
    )
    db_session.add(req)
    db_session.commit()

    resp = client.post(
        "/submit-admin-request/",
        json={"email": email, "admin_email": "admin@example.com"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["email"] == email
    assert body["admin_email"] == "admin@example.com"



def test_set_password_creates_user(client, db_session, tenant):
    email = "invitee@example.com"
    token = create_invitation_token(email, tenant.id)

    req = AccessRequest(
        email=email,
        status=RequestStatus.APPROVED,
        token=token,
        token_expiry=datetime.utcnow() + timedelta(hours=2),
        tenant_id=tenant.id,
    )
    db_session.add(req)
    db_session.commit()

    resp = client.post(
        f"/set-password/{token}",
        json={"password": "newpassword", "confirm_password": "newpassword"},
    )

    assert resp.status_code == 200
    assert resp.json()["token_type"] == "bearer"

    user = db_session.query(User).filter_by(email=email).first()
    assert user is not None
    assert user.tenant_id == tenant.id


def test_login_returns_token(client, db_session, regular_user):
    regular_user.hashed_password = get_password_hash("userpass")
    db_session.commit()

    resp = client.post(
        "/login",
        json={"email": regular_user.email, "password": "userpass"},
    )
    assert resp.status_code == 200
    assert resp.json()["token_type"] == "bearer"

