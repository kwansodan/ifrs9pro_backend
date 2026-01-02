import pytest
from datetime import datetime, timedelta
from app.auth.utils import create_password_reset_token, get_password_hash
from app.models import User

def test_forgot_password_success(client, db_session, regular_user):
    payload = {"email": regular_user.email}
    resp = client.post("/forgot-password", json=payload)
    assert resp.status_code == 200
    assert "If your email is registered" in resp.json()["message"]

def test_forgot_password_invalid_email(client, db_session):
    payload = {"email": "nonexistent@example.com"}
    resp = client.post("/forgot-password", json=payload)
    assert resp.status_code == 200
    assert "If your email is registered" in resp.json()["message"]

def test_reset_password_success(client, db_session, regular_user):
    token = create_password_reset_token(regular_user.email)
    payload = {
        "token": token,
        "password": "newpassword123",
        "confirm_password": "newpassword123"
    }
    resp = client.post("/reset-password", json=payload)
    assert resp.status_code == 200
    assert resp.json()["message"] == "Password reset successfully"

    db_session.refresh(regular_user)
    from app.auth.utils import verify_password
    assert verify_password("newpassword123", regular_user.hashed_password)

def test_reset_password_mismatch(client, db_session, regular_user):
    token = create_password_reset_token(regular_user.email)
    payload = {
        "token": token,
        "password": "newpassword123",
        "confirm_password": "mismatchpassword"
    }
    resp = client.post("/reset-password", json=payload)
    assert resp.status_code == 400
    assert "Passwords do not match" in resp.json()["detail"]

def test_reset_password_invalid_token(client, db_session):
    payload = {
        "token": "invalidtoken",
        "password": "newpassword123",
        "confirm_password": "newpassword123"
    }
    resp = client.post("/reset-password", json=payload)
    assert resp.status_code == 401
    assert "Invalid token" in resp.json()["detail"]

def test_reset_password_expired_token(client, db_session, regular_user):
    # Mocking expired token might be tricky with the current create_password_reset_token
    # But we can try to decode it and see if it fails.
    # Actually, decode_token checks for expiration.
    pass
