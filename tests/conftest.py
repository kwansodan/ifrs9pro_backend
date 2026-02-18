import os
import tempfile

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta, timezone
from app.models import (
    SubscriptionPlan,
    TenantSubscription,
    SubscriptionUsage,
    Tenant,
    Portfolio
)
from app.routes import reports



# Ensure the app uses the in-memory SQLite URL before any app imports
TEST_DB_PATH = os.path.join(tempfile.gettempdir(), "ifrs9pro_test.db")
TEST_DATABASE_URL = f"sqlite:///{TEST_DB_PATH}"
os.environ.setdefault("SQLALCHEMY_DATABASE_URL", TEST_DATABASE_URL)

# --- MOCK CELERY BEFORE APP IMPORT ---
import sys
from unittest.mock import MagicMock

# Create a mock for the celery module
mock_celery = MagicMock()
sys.modules["celery"] = mock_celery

# Mock the Celery class and its .task decorator
mock_celery_app = MagicMock()
mock_celery.Celery.return_value = mock_celery_app

# The tricky part: @celery_app.task must return a wrapper that has .delay
def mock_task_decorator(*args, **kwargs):
    def decorator(func):
        # Attach a .delay method to the decorated function
        func.delay = MagicMock(return_value=MagicMock(id="test-task-id"))
        return func
    return decorator

# If .task is called as @app.task(bind=True), it's a decorator factory
mock_celery_app.task.side_effect = mock_task_decorator
# -------------------------------------

from app.database import Base, get_db
from app.dependencies import get_tenant_db
from app.models import User, UserRole
from app.auth.utils import get_password_hash
from main import app

# Provide defaults for secrets so token creation works during tests
os.environ.setdefault("SECRET_KEY", "test-secret-key")
os.environ.setdefault("ACCESS_TOKEN_EXPIRE_MINUTES", "60")
os.environ.setdefault("INVITATION_EXPIRE_HOURS", "24")

# Use a writable SQLite database for tests (tmpfs avoids permission issues)
engine = create_engine(TEST_DATABASE_URL, connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture(scope="function")
def db_session():
    # drop & recreate schema per test to avoid unique/email collisions
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

    session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def tenant(db_session):
    tenant = Tenant(
        name="Test Corp",
        slug="test-corp",
        industry="Tech",
        country="Ghana",
        is_active=True
    )
    db_session.add(tenant)
    db_session.commit()
    db_session.refresh(tenant)
    return tenant

@pytest.fixture
def admin_user(db_session, tenant):
    user = User(
        email="admin@example.com",
        hashed_password=get_password_hash("adminpass"),
        role=UserRole.ADMIN,
        is_active=True,
        tenant_id=tenant.id, 
        first_name="Admin",
        last_name="User"
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


@pytest.fixture
def regular_user(db_session, tenant):
    user = User(
        email="user@example.com",
        hashed_password=get_password_hash("userpass"),
        role=UserRole.USER,
        is_active=True,
        tenant_id=tenant.id,
        first_name="Test",
        last_name="User"
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


@pytest.fixture
def admin_with_active_subscription(db_session, admin_user, tenant):
    # Seed plan (idempotent per test DB)
    plan = SubscriptionPlan(
        name="CORE",
        paystack_plan_code="TEST_PLAN_CORE",
        max_loan_data=1000,
        max_portfolios=5,
        max_team_size=2,
        price=12000,
        currency="GHS",
        is_active=True,
    )
    db_session.add(plan)
    db_session.flush()

    # Seed subscription linked to TENANT
    subscription = TenantSubscription(
        tenant_id=tenant.id,
        plan_id=plan.id,
        paystack_subscription_code="SUB_TEST_ACTIVE",
        paystack_customer_code="CUS_TEST",
        status="active",
        started_at=datetime.now(timezone.utc),
        current_period_end=datetime.now(timezone.utc) + timedelta(days=365),
    )
    db_session.add(subscription)
    db_session.flush()

    # Seed usage (required by enforcement)
    db_session.add(
        SubscriptionUsage(
            subscription_id=subscription.id,
            current_loan_count=0,
            current_portfolio_count=0,
            current_team_count=0,
        )
    )

    # Link to tenant (in case we need direct relationship access or validation)
    tenant.subscription_id = subscription.id
    tenant.subscription_status = "active"
    tenant.paystack_customer_code = "CUS_TEST"

    db_session.commit()
    return admin_user


@pytest.fixture
def client(db_session, admin_user, regular_user, admin_with_active_subscription, monkeypatch):
    """
    Shared TestClient with dependency overrides and patched external side effects.
    """

    def override_get_db():
        try:
            yield db_session
        finally:
            pass

    # Dependency overrides to bypass auth in tests
    from app.auth import utils as auth_utils

    def override_current_user():
        return regular_user

    def override_admin_user():
        return admin_user

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_tenant_db] = override_get_db
    app.dependency_overrides[auth_utils.get_current_active_user] = override_current_user
    app.dependency_overrides[auth_utils.is_admin] = override_admin_user
    

    # Patch email/minio/background side effects to no-ops
    monkeypatch.setattr("app.auth.email.send_verification_email", lambda *a, **k: None)
    monkeypatch.setattr("app.auth.email.send_admin_notification", lambda *a, **k: None)
    monkeypatch.setattr("app.auth.email.send_invitation_email", lambda *a, **k: None)
    monkeypatch.setattr("app.auth.email.send_password_setup_email", lambda *a, **k: None)

    monkeypatch.setattr("app.utils.background_ingestion.fetch_excel_from_minio", lambda *a, **k: {})
    monkeypatch.setattr("app.utils.background_ingestion.process_portfolio_ingestion_sync", lambda *a, **k: {})
    monkeypatch.setattr("app.utils.background_processors.process_loan_details_with_progress", lambda *a, **k: {})
    monkeypatch.setattr("app.utils.background_processors.process_client_data_with_progress", lambda *a, **k: {})
    monkeypatch.setattr("app.utils.background_ingestion.start_background_ingestion", lambda *a, **k: {"task": "ok"})
    monkeypatch.setattr("app.utils.minio_reports_factory.upload_multiple_files_to_minio", lambda *a, **k: [])
    monkeypatch.setattr("app.utils.background_calculations.process_ecl_calculation_sync", lambda *a, **k: {"status": "ok"})
    monkeypatch.setattr("app.utils.background_calculations.process_bog_impairment_calculation_sync", lambda *a, **k: {"status": "ok"})
    monkeypatch.setattr("app.utils.minio_reports_factory.run_and_save_report_task", lambda *a, **k: None)
    monkeypatch.setattr("app.utils.minio_reports_factory.generate_presigned_url_for_download", lambda *a, **k: "http://example.com")
    monkeypatch.setattr("app.utils.minio_reports_factory.download_report", lambda *a, **k: b"data")
    monkeypatch.setattr(
        reports,
        "run_and_save_report_task",
        lambda *a, **k: None
    )

    return TestClient(app)

@pytest.fixture
def portfolio(db_session, regular_user, tenant):
    """Create a test portfolio"""
    portfolio = Portfolio(
        name="Test Portfolio",
        user_id=regular_user.id,
        tenant_id=tenant.id,
        description="Test description",
    )
    db_session.add(portfolio)
    db_session.commit()
    db_session.refresh(portfolio)
    return portfolio
