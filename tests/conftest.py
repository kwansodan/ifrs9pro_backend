import os
import tempfile

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Ensure the app uses the in-memory SQLite URL before any app imports
TEST_DB_PATH = os.path.join(tempfile.gettempdir(), "ifrs9pro_test.db")
TEST_DATABASE_URL = f"sqlite:///{TEST_DB_PATH}"
os.environ.setdefault("SQLALCHEMY_DATABASE_URL", TEST_DATABASE_URL)

from app.database import Base, get_db
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
def admin_user(db_session):
    user = User(
        email="admin@example.com",
        hashed_password=get_password_hash("adminpass"),
        role=UserRole.ADMIN,
        is_active=True,
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


@pytest.fixture
def regular_user(db_session):
    user = User(
        email="user@example.com",
        hashed_password=get_password_hash("userpass"),
        role=UserRole.USER,
        is_active=True,
        first_name="Test",
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)
    return user


@pytest.fixture
def client(db_session, admin_user, regular_user, monkeypatch):
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

    return TestClient(app)

