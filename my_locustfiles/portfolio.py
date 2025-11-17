# my_locustfiles/portfolio.py
import os
import time
from datetime import date


# === CREATE PORTFOLIO ===
def create_portfolio(client, headers):
    """Create a new test portfolio."""
    url = "/portfolios/"
    payload = {
        "name": f"Test Portfolio {int(time.time())}",
        "description": "Created during load testing",
        "asset_type": "equity",
        "customer_type": "individuals",
        "funding_source": "private investors",
        "data_source": "connect to external application",
        "repayment_source": False,
    }

    start = time.time()
    with client.post(url, json=payload, headers=headers, catch_response=True, name="/portfolios/") as response:
        elapsed = int((time.time() - start) * 1000)
        if response.status_code == 201:
            try:
                portfolio_id = response.json().get("id")
                response.success()
                print(f"✅ Created portfolio {portfolio_id} in {elapsed}ms")
                return portfolio_id
            except Exception as e:
                response.failure(f"Invalid JSON: {e}")
        else:
            response.failure(f"HTTP {response.status_code}: {response.text}")
    return None


# === INGEST DATA ===
def ingest_portfolio(client, portfolio_id, loan_file_path, client_file_path, headers):
    """Upload loan and client CSVs to a portfolio."""
    url = f"/portfolios/{portfolio_id}/ingest"
    start = time.time()

    try:
        with open(loan_file_path, "rb") as loan_file, open(client_file_path, "rb") as client_file:
            files = {
                "loan_details": (os.path.basename(loan_file_path), loan_file, "text/csv"),
                "client_data": (os.path.basename(client_file_path), client_file, "text/csv"),
            }
            with client.post(url, files=files, headers=headers, catch_response=True, name="/ingest") as response:
                elapsed = int((time.time() - start) * 1000)
                if response.status_code == 200:
                    try:
                        result = response.json()
                        if isinstance(result, str) and len(result) == 36:
                            response.success()
                            print(f"✅ Ingest successful in {elapsed}ms")
                            return result
                        else:
                            response.failure(f"Ingestion failed: {result}")
                    except Exception as e:
                        response.failure(f"Invalid JSON: {e}")
                else:
                    response.failure(f"HTTP {response.status_code}: {response.text}")
    except Exception as e:
        print(f"❌ Unexpected error during ingestion: {e}")


# === CALCULATE ECL ===
def calculate_ecl(client, portfolio_id, headers):
    """Calculate Expected Credit Loss (ECL)."""
    url = f"/portfolios/{portfolio_id}/calculate-ecl"
    params = {"reporting_date": date.today().isoformat()}
    start = time.time()

    with client.get(url, params=params, headers=headers, catch_response=True, name="/calculate-ecl") as response:
        elapsed = int((time.time() - start) * 1000)
        if response.status_code == 200:
            try:
                result = response.json()
                expected_keys = [
                    "calculation_id", "portfolio_id", "grand_total_ecl", "provision_percentage", "loan_count"
                ]
                if all(k in result for k in expected_keys):
                    response.success()
                    print(f"✅ ECL calculation succeeded in {elapsed}ms")
                    return result
                else:
                    response.failure(f"Unexpected ECL response: {result}")
            except Exception as e:
                response.failure(f"Invalid JSON: {e}")
        else:
            response.failure(f"/calculate-ecl failed ({response.status_code}): {response.text}")


# === STAGE LOANS ===
def stage_loans(client, portfolio_id, headers,mode="ecl"):
    """Trigger loan staging (ECL or local)."""
    endpoint = "stage-loans-ecl" if mode == "ecl" else "stage-loans-local"
    url = f"/portfolios/{portfolio_id}/{endpoint}"
    payload = {"portfolio_id": portfolio_id}
    params = {"reporting_date": date.today().isoformat()}

    start = time.time()
    with client.post(url, json=payload, params=params, headers=headers, catch_response=True, name=f"/{endpoint}") as response:
        elapsed = int((time.time() - start) * 1000)
        if response.status_code == 200:
            try:
                result = None
                if response.text and response.text.strip().lower() != "null":
                    result = response.json()

                if result is None:
                    response.success()
                    print(f"✅ Stage loans successful (null response) in {elapsed}ms")
                elif isinstance(result, str):
                    response.success()
                    print(f"✅ Stage loans returned ID: {result} in {elapsed}ms")
                else:
                    response.failure(f"Unexpected response: {result}")
            except Exception as e:
                response.failure(f"Invalid JSON: {e}")
        else:
            response.failure(f"/{endpoint} failed ({response.status_code}): {response.text}")


# === LOCAL IMPAIRMENT ===
def calculate_local_impairment(client, portfolio_id, headers):
    """Trigger local impairment calculation."""
    url = f"/portfolios/{portfolio_id}/calculate-local-impairment"
    params = {"reporting_date": date.today().isoformat()}
    start = time.time()

    with client.get(url, params=params, headers=headers, catch_response=True, name="/calculate-local-impairment") as response:
        elapsed = int((time.time() - start) * 1000)
        if response.status_code == 200:
            try:
                if not response.text or response.text.strip().lower() == "null":
                    response.failure("Empty or null response body")
                    return

                result = response.json()
                expected_keys = ["calculation_id", "portfolio_id", "loan_count", "total_provision"]

                if all(k in result for k in expected_keys):
                    response.success()
                    print(f"✅ Local impairment calculation successful in {elapsed}ms")
                    return result
                else:
                    missing = [k for k in expected_keys if k not in result]
                    response.failure(f"Missing keys in response: {missing}")
            except Exception as e:
                response.failure(f"Invalid JSON: {e}")
        elif response.status_code == 404:
            response.failure("Portfolio not found (404)")
        elif response.status_code == 401:
            response.failure("Unauthorized - invalid JWT")
        else:
            response.failure(f"Failed ({response.status_code}): {response.text}")
