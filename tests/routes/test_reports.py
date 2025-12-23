from datetime import date, datetime
from app.models import Portfolio, Report
import pytest
from unittest.mock import patch


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


@pytest.fixture
def report(db_session, portfolio, regular_user, tenant):
    """Create a test report"""
    report = Report(
        portfolio_id=portfolio.id,
        tenant_id=tenant.id,
        report_name="test_report_20241210.xlsx",
        report_type="ecl_detailed_report",
        report_date=datetime.now().date(),
        report_data={"summary": "test data"},
        status="completed",
        created_by=regular_user.id,
    )
    db_session.add(report)
    db_session.commit()
    db_session.refresh(report)
    return report


def test_get_report(client, portfolio, report):
    """Test getting a specific report"""
    response = client.get(f"/reports/{portfolio.id}/report/{report.id}")
    assert response.status_code == 200
    assert response.json()["id"] == report.id


def test_delete_report(client, portfolio, report):
    """Test deleting a specific report"""
    response = client.delete(f"/reports/{portfolio.id}/report/{report.id}")
    assert response.status_code == 204


def test_download_report_excel(client, portfolio, report, monkeypatch):
    """Test downloading a report as Excel"""
    # Mock the download_report function to return fake Excel data
    from io import BytesIO
    
    def mock_download_report(bucket_name, object_name):
        # Return a BytesIO object with fake Excel data
        fake_excel_data = BytesIO(b"fake excel file content")
        return fake_excel_data
    
    # Patch the download_report function
    monkeypatch.setattr(
        "app.routes.reports.download_report",
        mock_download_report
    )
    
    response = client.get(f"/reports/{portfolio.id}/report/{report.id}/download")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def test_get_report_status(client, report):
    """Test getting report status"""
    response = client.get(f"/reports/status/{report.id}")
    assert response.status_code == 200
    assert response.json() == "completed"


def test_get_report_not_found(client, portfolio):
    """Test getting a non-existent report"""
    response = client.get(f"/reports/{portfolio.id}/report/99999")
    assert response.status_code == 404


def test_delete_report_not_found(client, portfolio):
    """Test deleting a non-existent report"""
    response = client.delete(f"/reports/{portfolio.id}/report/99999")
    assert response.status_code == 404


def test_download_report_not_found(client, portfolio):
    """Test downloading a non-existent report"""
    response = client.get(f"/reports/{portfolio.id}/report/99999/download")
    assert response.status_code == 404


def test_get_report_status_not_found(client):
    """Test getting status for a non-existent report"""
    response = client.get("/reports/status/99999")
    assert response.status_code == 404


# tests/routes/test_reports.py
from app.database import current_tenant_id

def test_generate_report(client, db_session, regular_user, tenant):
    # Set tenant context
    token = current_tenant_id.set(tenant.id)
    try:
        portfolio = Portfolio(
            user_id=regular_user.id,
            tenant_id=tenant.id,
            name="Report Portfolio",
            asset_type="equity",
            customer_type="individuals",
            funding_source="pension fund",
            data_source="upload data",
        )
        db_session.add(portfolio)
        db_session.commit()
        
        # Mock any external services used in report generation
        with patch("app.routes.reports.run_and_save_report_task") as mock_gen:
            mock_gen.return_value = {"status": "success", "report_id": 1}
            
            resp = client.post(
                f"/reports/{portfolio.id}/generate",
                json={
                    "report_type": "ecl_detailed_report",
                    "report_date": str(date.today())
                },
            )
            
            # Debug: print error if not 200
            if resp.status_code != 200:
                print(f"Error: {resp.json()}")
            
            assert resp.status_code == 200
    finally:
        current_tenant_id.reset(token)


def test_report_history_and_download(client, db_session, regular_user, tenant):
    portfolio = db_session.query(Portfolio).first()
    if not portfolio:
        portfolio = Portfolio(
            user_id=regular_user.id,
            tenant_id=tenant.id,
            name="History Portfolio",
            asset_type="equity",
            customer_type="individuals",
            funding_source="pension fund",
            data_source="upload data",
        )
        db_session.add(portfolio)
        db_session.commit()

    history = client.get(f"/reports/{portfolio.id}/history")
    assert history.status_code == 200

    # If a report exists, try retrieving and downloading it
    items = history.json()["items"]
    if items:
        report_id = items[0]["id"]
        get_resp = client.get(f"/reports/{portfolio.id}/report/{report_id}")
        assert get_resp.status_code == 200

        dl_resp = client.get(
            f"/reports/{portfolio.id}/report/{report_id}/download",
        )
        # Might be 200 with stubbed download
        assert dl_resp.status_code in (200, 404)

def test_generate_report_error_wrapping(client, portfolio, monkeypatch):
    """Test standard error wrapping in report generation"""
    def mock_report_init(*args, **kwargs):
        raise Exception("Database fail")
    
    # Patch Report class to fail on instantiation
    monkeypatch.setattr("app.routes.reports.Report", mock_report_init)
    
    resp = client.post(
        f"/reports/{portfolio.id}/generate",
        json={"report_type": "ecl_detailed_report", "report_date": str(date.today())},
    )
    assert resp.status_code == 500
    assert "Error generating report: Database fail" in resp.json()["detail"]

