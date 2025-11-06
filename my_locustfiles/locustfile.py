from locust import HttpUser, task, between, events
import os
import sys
from dotenv import load_dotenv, find_dotenv

# Import all reusable test functions
from auth import login_user
from dashboard import  get_dashboard
from portfolio import (
    create_portfolio,
    ingest_portfolio,
    calculate_ecl,
    stage_loans,
    calculate_local_impairment
)
from reports import (
    authenticate as reports_auth,
    generate_report,
    get_report_history,
    get_specific_report
)

# --- Load environment variables ---
docker_env = "docker/.env.docker"
if os.path.exists(docker_env):
    load_dotenv(dotenv_path=docker_env, override=False)
else:
    load_dotenv(find_dotenv())

# --- Config ---
HOST = os.getenv("HOST", "https://do-site-staging.service4gh.com")
PORTFOLIO_ID = int(os.getenv("PORTFOLIO_ID", 1))
REPORT_ID = int(os.getenv("REPORT_ID", 1))
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")
LOAN_FILE = "/my_locustfiles/loan_data_70k.csv"
CLIENT_FILE = "/my_locustfiles/client_data_70k.csv"


# --- Event Hooks ---
@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    print(f"🚀 Starting IFRS9 load test on {HOST}")
    print(f"Portfolio ID: {PORTFOLIO_ID}")


# --- Locust User Definition ---
class TestUser(HttpUser):
    host = HOST
    wait_time = between(3, 5)

    def on_start(self):
        """Login once before starting tasks"""
        try:
            print(f"🔐 Logging in with {ADMIN_EMAIL}")
            self.token = login_user(self.client, ADMIN_EMAIL, ADMIN_PASSWORD)
            if not self.token:
                print("❌ Failed to get token — stopping test")
                self.environment.runner.quit()
            else:
                self.headers = {"Authorization": f"Bearer {self.token}"}
                print("✅ Login successful — token stored")
        except Exception as e:
            print(f"❌ Error during login: {e}")
            self.environment.runner.quit()

    # === AUTH TEST ===
    @task(1)
    def test_auth(self):
        print("🔍 Running authentication test...")
        token = login_user(self.client, ADMIN_EMAIL, ADMIN_PASSWORD)
        if token:
            print("✅ Auth test passed")
        else:
            print("❌ Auth test failed")

    # === DASHBOARD TEST ===
    @task(2)
    def test_dashboard(self):
        print("📊 Fetching dashboard data...")
        get_dashboard(self.client, self.headers)

    # === PORTFOLIO TESTS ===
    @task(3)
    def test_portfolio_create(self):
        print("📦 Creating new portfolio...")
        create_portfolio(self.client, self.headers)

    @task(4)
    def test_portfolio_ingest(self):
        print("📤 Ingesting portfolio data...")
        ingest_portfolio(self.client, PORTFOLIO_ID, LOAN_FILE, CLIENT_FILE, self.headers)

    @task(5)
    def test_portfolio_ecl(self):
        print("📈 Calculating ECL...")
        calculate_ecl(self.client, PORTFOLIO_ID, self.headers)

    @task(6)
    def test_stage_loans(self):
        print("🏦 Staging loans...")
        stage_loans(self.client, PORTFOLIO_ID, self.headers)

    @task(7)
    def test_local_impairment(self):
        print("💡 Calculating local impairment...")
        calculate_local_impairment(self.client, PORTFOLIO_ID, self.headers)

    # === REPORT TESTS ===
    @task(8)
    def test_generate_report(self):
        print("🧾 Generating report...")
        generate_report(self.client, PORTFOLIO_ID, self.headers)

    @task(9)
    def test_report_history(self):
        print("📚 Fetching report history...")
        get_report_history(self.client, PORTFOLIO_ID, self.headers)

    @task(10)
    def test_specific_report(self):
        print("📄 Fetching specific report...")
        get_specific_report(self.client, PORTFOLIO_ID, REPORT_ID, self.headers)
