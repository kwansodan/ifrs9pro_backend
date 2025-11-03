# locustfile.py
import os
import time
from datetime import date
from locust import task, between, events
from locust.contrib.fasthttp import FastHttpUser
from dotenv import load_dotenv, find_dotenv

docker_env = "docker/.env.docker"
if os.path.exists(docker_env):
    load_dotenv(dotenv_path=docker_env, override=False)
else:
    # if you still want to fallback to a default .env in repo root
    load_dotenv(find_dotenv())

# === CONFIGURATION ===
HOST = os.getenv("HOST", "https://do-site-staging.service4gh.com")
PORTFOLIO_ID = int(os.getenv("PORTFOLIO_ID", 1))  # Must exist in your system
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")

# File paths (ensure these exist)
LOAN_FILE = "data/loan_details_complete.xlsx"
CLIENT_FILE = "data/customer_data.xlsx"
LOAN_GUARANTEE_FILE = "data/Loan_guarantee.xlsx"
LOAN_COLLATERAL_FILE = "data/loan collateral data.xlsx"

# === Validate Files Exist ===
for f in [LOAN_FILE, CLIENT_FILE]:
    if not os.path.exists(f):
        raise FileNotFoundError(f"Missing test file: {f}")

# === EVENT HOOKS ===
@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    print(f"🚀 Starting load test on {HOST}")
    print(f"Using portfolio_id: {PORTFOLIO_ID}")
    print(f"Files: {LOAN_FILE}, {CLIENT_FILE}")


# === LOCUST USER ===
class PortfolioUser(FastHttpUser):
    wait_time = between(50, 60)
    host = HOST

    def on_start(self):
        """Authenticate before running any tasks"""
        payload = {"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
        print(f"🔐 Logging in as {ADMIN_EMAIL} ...")

        response = self.client.post("/login", json=payload)
        if response.status_code == 200:
            json_data = response.json()
            token = json_data.get("access_token")
            if not token:
                print("❌ Login response missing 'access_token':", response.text)
                self.environment.runner.quit()
                return
            self.auth_headers = {"Authorization": f"Bearer {token}"}
            print("✅ Authenticated successfully!")
        else:
            print(f"❌ Login failed ({response.status_code}): {response.text}")
            self.environment.runner.quit()


    # === TASK 1: Create Portfolio ===
    @task(1)
    def create_portfolio(self):
        url = "/portfolios/"
        payload = {
            "name": f"Test Portfolio {int(time.time())}",
            "description": "Created during load testing",
            "asset_type": "equity",
            "customer_type": "individuals",
            "funding_source": "private investors",
            "data_source": "connect to external application",
            "repayment_source": False
        }

        start = time.time()
        with self.client.post(url, json=payload, headers=self.auth_headers, catch_response=True, name="/portfolios/") as response:
            elapsed = int((time.time() - start) * 1000)
            if response.status_code == 201:
                try:
                    portfolio_id = response.json().get("id")
                    response.success()
                    print(f"✅ Created portfolio {portfolio_id} in {elapsed}ms")
                except Exception as e:
                    response.failure(f"Invalid JSON response: {e}")
            else:
                response.failure(f"HTTP {response.status_code}: {response.text}")


    # === TASK 2: Ingest Portfolio Data ===
    @task(2)
    def ingest_portfolio(self):
        url = f"/portfolios/{PORTFOLIO_ID}/ingest"
        files = {
            "loan_details": ("loan_details.xlsx", open(LOAN_FILE, "rb"), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            "client_data": ("client_data.xlsx", open(CLIENT_FILE, "rb"), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            "loan_guarantee_data": ("loan_guarantee.xlsx", open(LOAN_GUARANTEE_FILE, "rb"), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            "loan_collateral_data": ("loan_collateral.xlsx", open(LOAN_COLLATERAL_FILE, "rb"), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        }

        start = time.time()
        try:
            with self.client.post(url, files=files, headers=self.auth_headers, catch_response=True, name="/ingest") as response:
                elapsed = int((time.time() - start) * 1000)
                if response.status_code == 200:
                    try:
                        result = response.json()
                        if isinstance(result, str) and len(result) == 36:
                            response.success()
                            print(f"✅ Ingest successful in {elapsed}ms")
                        else:
                            response.failure(f"Ingestion failed: {result}")
                    except Exception as e:
                        response.failure(f"Invalid JSON: {e}")
                else:
                    response.failure(f"HTTP {response.status_code}: {response.text}")
        finally:
            for f in files.values():
                f[1].close()


    # === TASK 3: Calculate ECL Provision ===
    @task(3)
    def calculate_ecl_provision(self):
        url = f"/portfolios/{PORTFOLIO_ID}/calculate-ecl"
        params = {"reporting_date": date.today().isoformat()}

        start = time.time()
        with self.client.get(url, params=params, headers=self.auth_headers, catch_response=True, name="/calculate-ecl") as response:
            elapsed = int((time.time() - start) * 1000)

            if response.status_code == 200:
                try:
                    result = response.json()
                    expected_keys = ["calculation_id", "portfolio_id", "grand_total_ecl", "provision_percentage", "loan_count"]
                    if all(key in result for key in expected_keys):
                        response.success()
                        print(f"✅ ECL calculation succeeded in {elapsed}ms")
                    else:
                        response.failure(f"Unexpected ECL response: {result}")
                except Exception as e:
                    response.failure(f"Invalid JSON: {e}")
            else:
                response.failure(f"/calculate-ecl failed ({response.status_code}): {response.text}")


    # === TASK 4: Stage Loans ECL ===
    @task(4)
    def stage_loans_ecl(self):
        """Trigger the ecl stage loans for an existing portfolio"""
        url = f"/portfolios/{PORTFOLIO_ID}/stage-loans-ecl"
        payload = {"portfolio_id": PORTFOLIO_ID}
        params = {"reporting_date": date.today().isoformat()}  # optional

        start = time.time()
        with self.client.post(url, json=payload, params=params, headers=self.auth_headers, catch_response=True, name="/stage-loans-ecl") as response:
            elapsed = int((time.time() - start) * 1000)

            if response.status_code == 200:
                try:
                    # Handle empty or 'null' responses
                    result = None
                    if response.text and response.text.strip().lower() != "null":
                        result = response.json()

                    # Success case — API returns "null" or UUID string
                    if result is None:
                        response.success()
                        print(f"✅ Stage loans successful (null response) in {elapsed}ms")
                    elif isinstance(result, str):
                        response.success()
                        print(f"✅ Stage loans returned ID: {result} in {elapsed}ms")
                    else:
                        response.failure(f"Unexpected response body: {result}")
                except Exception as e:
                    response.failure(f"Invalid JSON: {e}")
            else:
                response.failure(f"/stage-loans-ecl failed ({response.status_code}): {response.text}")

    @task(5)
    def stage_loans_loacl(self):
        """Trigger the local stage loans for an existing portfolio"""
        url = f"/portfolios/{PORTFOLIO_ID}/stage-loans-local"
        payload = {"portfolio_id": PORTFOLIO_ID}
        params = {"reporting_date": date.today().isoformat()}  # optional

        start = time.time()
        with self.client.post(url, json=payload, params=params, headers=self.auth_headers, catch_response=True, name="/stage-loans-ecl") as response:
            elapsed = int((time.time() - start) * 1000)

            if response.status_code == 200:
                try:
                    # Handle empty or 'null' responses
                    result = None
                    if response.text and response.text.strip().lower() != "null":
                        result = response.json()

                    # Success case — API returns "null" or UUID string
                    if result is None:
                        response.success()
                        print(f"✅ Stage loans successful (null response) in {elapsed}ms")
                    elif isinstance(result, str):
                        response.success()
                        print(f"✅ Stage loans returned ID: {result} in {elapsed}ms")
                    else:
                        response.failure(f"Unexpected response body: {result}")
                except Exception as e:
                    response.failure(f"Invalid JSON: {e}")
            else:
                response.failure(f"/stage-loans-local failed ({response.status_code}): {response.text}")
                
'''
    @task(6)
    def calculate_local_impairment(self):
        """Trigger local impairment calculation for a portfolio"""
        url = f"/portfolios/{PORTFOLIO_ID}/calculate-local-impairment"
        params = {"reporting_date": date.today().isoformat()}

        start = time.time()
        with self.client.get(url, params=params, headers=self.auth_headers, catch_response=True, name="/calculate-local-impairment") as response:
            elapsed = int((time.time() - start) * 1000)

            if response.status_code == 200:
                try:
                    # Parse only if response is not "null"
                    if not response.text or response.text.strip().lower() == "null":
                        response.failure("Empty or null response body")
                        return

                    result = response.json()

                    # Expected keys that indicate success
                    expected_keys = ["calculation_id", "portfolio_id", "loan_count", "total_provision"]

                    if all(key in result for key in expected_keys):
                        response.success()
                        print(f"✅ Local impairment calculation successful in {elapsed}ms — contains all expected keys.")
                    else:
                        missing = [key for key in expected_keys if key not in result]
                        response.failure(f"Missing keys in response: {missing}")
                except Exception as e:
                    response.failure(f"Invalid JSON: {e}")
            elif response.status_code == 404:
                response.failure("Portfolio not found (404)")
            elif response.status_code == 401:
                response.failure("Unauthorized - check JWT token")
            else:
                response.failure(f"/calculate-local-impairment failed ({response.status_code}): {response.text}")
'''