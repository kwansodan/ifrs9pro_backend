
import io
import pytest
from unittest.mock import patch, AsyncMock
from fastapi.testclient import TestClient 
from fastapi import status
from unittest.mock import patch
from io import BytesIO

from datetime import date
from app.models import Portfolio
from app.models import User
import pandas as pd
from unittest.mock import patch
from app.schemas import IngestPayload

def test_create_portfolio(client, tenant):
    resp = client.post(
        "/portfolios/",
        json={
            "name": "Test Portfolio",
            "description": "demo",
            "asset_type": "equity",
            "customer_type": "individuals",
            "funding_source": "pension fund",
            "data_source": "upload data",
            "repayment_source": True,
        },
    )
    assert resp.status_code == 201
    assert resp.json()["name"] == "Test Portfolio"

def test_user_cannot_exceed_portfolio_limit_returns_402(client, tenant):
    payload = {
        "name": "Portfolio",
        "description": "demo",
        "asset_type": "equity",
        "customer_type": "individuals",
        "funding_source": "pension fund",
        "data_source": "upload data",
        "repayment_source": True,
    }

    # Create the maximum allowed portfolios (CORE plan = 5)
    for i in range(5):
        resp = client.post(
            "/portfolios/",
            json={**payload, "name": f"Portfolio {i + 1}"},
        )
        assert resp.status_code == 201, resp.text

    # Sixth portfolio must fail with 402 Payment Required
    resp = client.post(
        "/portfolios/",
        json={**payload, "name": "Portfolio 6"},
    )

    assert resp.status_code == 402

    body = resp.json()
    assert "portfolio" in body["detail"].lower()
    assert "limit" in body["detail"].lower()


def test_list_portfolios(client):
    resp = client.get("/portfolios/")
    assert resp.status_code == 200
    assert "items" in resp.json()


def test_update_and_delete_portfolio(client, db_session, regular_user, tenant):
    portfolio = Portfolio(
        user_id=regular_user.id,
        tenant_id=tenant.id,
        name="To Update",
        asset_type="equity",
        customer_type="individuals",
        funding_source="pension fund",
        data_source="upload data",
    )
    db_session.add(portfolio)
    db_session.commit()

    update_resp = client.put(
        f"/portfolios/{portfolio.id}",
        json={"name": "Updated Name"},
    )
    assert update_resp.status_code == 200
    assert update_resp.json()["name"] == "Updated Name"

    delete_resp = client.delete(f"/portfolios/{portfolio.id}")
    assert delete_resp.status_code == 200
    assert delete_resp.json()["detail"] == "Portfolio deleted successfully"


@pytest.mark.asyncio
async def test_accept_portfolio_data(client, db_session, regular_user, tenant):
    portfolio = Portfolio(
        user_id=regular_user.id,
        tenant_id=tenant.id,
        name="Test P",
        description="desc",
    )
    db_session.add(portfolio)
    db_session.commit()

    # Create REAL in-memory Excel file
    buffer = BytesIO()
    df = pd.DataFrame({"A": [1], "B": [2]})
    df.to_excel(buffer, index=False)
    excel_bytes = buffer.getvalue()

    files = {
        "loan_details": (
            "loan.xlsx",
            excel_bytes,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
        "client_data": (
            "client.xlsx",
            excel_bytes,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
    }

    fake_uploaded = {
        "loan_details": {
            "file_id": "1",
            "file_url": "url1",
            "object_name": "loan.xlsx",
            "excel_columns": ["A", "B"],
            "model_columns": {"modelA", "modelB"},
        },
        "client_data": {
            "file_id": "2",
            "file_url": "url2",
            "object_name": "client.xlsx",
            "excel_columns": ["A", "B"],
            "model_columns": {"modelA", "modelB"},
        },
        "loan_guarantee_data": None,
        "loan_collateral_data": None,
    }

    with patch(
        "app.routes.portfolio.upload_multiple_files_to_minio",
        return_value=fake_uploaded,
    ) as mock_upload:

        resp = client.post(
            f"/portfolios/{portfolio.id}/ingest/save",
            files=files,
        )

        assert resp.status_code == 200

        data = resp.json()
        assert data["portfolio_id"] == portfolio.id
        assert int(data["uploaded_files"]["loan_details"]["file_id"]) == 1

        mock_upload.assert_called_once()
        
@pytest.mark.asyncio
async def test_accept_portfolio_data_exceeds_loan_limit_returns_402(
    client,
    db_session,
    regular_user,
    tenant,
):
    # Create portfolio
    portfolio = Portfolio(
        user_id=regular_user.id,
        tenant_id=tenant.id,
        name="Test P",
        description="desc",
    )
    db_session.add(portfolio)
    db_session.commit()
    db_session.refresh(portfolio)

    # CORE plan max_loan_data = 1000
    # Create Excel with MORE than allowed rows
    rows = 1001
    buffer = BytesIO()
    df = pd.DataFrame({"loan_amount": range(rows)})
    df.to_excel(buffer, index=False)
    excel_bytes = buffer.getvalue()

    files = {
        "loan_details": (
            "loan.xlsx",
            excel_bytes,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
        "client_data": (
            "client.xlsx",
            excel_bytes,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
    }

    with patch(
        "app.routes.portfolio.upload_multiple_files_to_minio"
    ) as mock_upload:

        resp = client.post(
            f"/portfolios/{portfolio.id}/ingest/save",
            files=files,
        )

        # Must fail due to subscription limit
        assert resp.status_code == 402

        body = resp.json()
        assert "loan" in body["detail"].lower()
        assert "limit" in body["detail"].lower()

        # Absolutely nothing should be uploaded
        mock_upload.assert_not_called()


@pytest.mark.asyncio
async def test_ingest_portfolio_data(client, db_session, regular_user, tenant):
    # Create portfolio
    portfolio = Portfolio(
        user_id=regular_user.id,
        tenant_id=tenant.id,
        name="My Portfolio"
    )
    db_session.add(portfolio)
    db_session.commit()

    payload = {
        "files": [
            {
                "type": "loan_details",
                "object_name": "loan_file.xlsx",
                "mapping": {"Loan Amount": "loan_amount"}
            },
            {
                "type": "client_data",
                "object_name": "client_file.xlsx",
                "mapping": {"Name": "client_name"}
            }
        ]
    }


    # Fake DataFrame results
    fake_dataframes = {
        "loan_details": pd.DataFrame({"loan_amount": [1000]}),
        "client_data": pd.DataFrame({"client_name": ["John"]}),
        "loan_guarantee_data": pd.DataFrame(),
        "loan_collateral_data": pd.DataFrame(),
    }

    with patch(
        "app.routes.portfolio.fetch_excel_from_minio",
        return_value=fake_dataframes
    ) as mock_fetch:

        with patch(
            "app.routes.portfolio.start_background_ingestion",
            return_value={"rows_ingested": 10}
        ) as mock_ingest:

            resp = client.post(
                f"/portfolios/{portfolio.id}/ingest",
                json=payload
            )

            assert resp.status_code == 200
            data = resp.json()

            assert data["status"] == "success"
            assert data["result"]["rows_ingested"] == 10

            mock_fetch.assert_called_once()
            mock_ingest.assert_called_once()



def test_calculation_endpoints_are_stubbed(client, db_session, regular_user, tenant):
    portfolio = Portfolio(
        user_id=regular_user.id,
        tenant_id=tenant.id,
        name="Calc",
        asset_type="equity",
        customer_type="individuals",
        funding_source="pension fund",
        data_source="upload data",
    )
    db_session.add(portfolio)
    db_session.commit()

    ecl_resp = client.get(f"/portfolios/{portfolio.id}/calculate-ecl")
    assert ecl_resp.status_code == 200
    assert ecl_resp.json()["status"] == "ok"

    local_resp = client.get(f"/portfolios/{portfolio.id}/calculate-local-impairment")
    assert local_resp.status_code == 200
    assert local_resp.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_stage_loans_ecl_success(client, db_session, regular_user, tenant):
    portfolio = Portfolio(user_id=regular_user.id, tenant_id=tenant.id, name="Test P", description="desc")
    db_session.add(portfolio)
    db_session.commit()

    # Patch the function in its actual module
    with patch("app.utils.staging.stage_loans_ecl_orm", new_callable=AsyncMock) as mock_stage:
        response = client.post(f"/portfolios/{portfolio.id}/stage-loans-ecl")
        assert response.status_code == status.HTTP_200_OK
        mock_stage.assert_awaited_once_with(
            portfolio.id,
            db_session,
            user_email=regular_user.email,
            first_name=regular_user.first_name
        )



@pytest.mark.asyncio
async def test_stage_loans_ecl_portfolio_not_found(client, db_session, regular_user):
    response = client.post("/portfolios/999/stage-loans-ecl")  # non-existent portfolio
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()["detail"] == "Portfolio not found"


@pytest.mark.asyncio
async def test_stage_loans_local_success(client, db_session, regular_user, tenant):
    portfolio = Portfolio(user_id=regular_user.id, tenant_id=tenant.id, name="Test P", description="desc")
    db_session.add(portfolio)
    db_session.commit()

    with patch("app.utils.staging.stage_loans_local_impairment_orm", new_callable=AsyncMock) as mock_stage:
        response = client.post(f"/portfolios/{portfolio.id}/stage-loans-local")
        assert response.status_code == status.HTTP_200_OK
        mock_stage.assert_awaited_once_with(
            portfolio.id,
            db_session,
            user_email=regular_user.email,
            first_name=regular_user.first_name
        )
        

@pytest.mark.asyncio
async def test_stage_loans_local_portfolio_not_found(client, db_session, regular_user):
    response = client.post("/portfolios/999/stage-loans-local")  # non-existent portfolio
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()["detail"] == "Portfolio not found"