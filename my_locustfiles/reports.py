import os
import time
from datetime import date
from locust import task, between, events
from locust.contrib.fasthttp import FastHttpUser
from dotenv import load_dotenv, find_dotenv

# Load environment variables from docker/.env.docker first (if it exists),
# then fall back to any other .env or process environment variables.
docker_env = "docker/.env.docker"
if os.path.exists(docker_env):
    load_dotenv(dotenv_path=docker_env, override=False)
else:
    # if you still want to fallback to a default .env in repo root
    load_dotenv(find_dotenv())


HOST = os.getenv("HOST", "https://do-site-staging.service4gh.com")
PORTFOLIO_ID = int(os.getenv("PORTFOLIO_ID", 1))  # Must exist in your system
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")

class UserReports(FastHttpUser):
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

    @task(1)
    def generate_report(self):
        """Trigger report generation for a given portfolio"""
        url = f"/reports/{PORTFOLIO_ID}/generate"

        payload = {
            "report_date": date.today().isoformat(),
            "report_type": "ecl_detailed_report"
        }

        start = time.time()
        with self.client.post(url, json=payload, headers=self.auth_headers, catch_response=True, name="/reports/{portfolio_id}/generate") as response:
            elapsed = int((time.time() - start) * 1000)

            if response.status_code == 200:
                try:
                    result = response.json()
                    message = result.get("message")
                    report_id = result.get("report_id")

                    if message == "Report generation started" and report_id:
                        response.success()
                        print(f"✅ Report generation started successfully — Report ID: {report_id} ({elapsed}ms)")
                    else:
                        response.failure(f"❌ Unexpected response format: {result}")

                except Exception as e:
                    response.failure(f"Invalid JSON: {e}")

            elif response.status_code == 404:
                response.failure(f"Portfolio not found (404): {response.text}")
            elif response.status_code == 401:
                response.failure(f"Unauthorized (401): {response.text}")
            else:
                response.failure(f"Report generation failed ({response.status_code}): {response.text}")

    @task(2)
    def get_report_history(self):
        """Fetch report history for a given portfolio"""
        url = f"/reports/{PORTFOLIO_ID}/history"

        start = time.time()
        with self.client.get(url, headers=self.auth_headers, catch_response=True, name="/reports/{portfolio_id}/history") as response:
            elapsed = int((time.time() - start) * 1000)

            if response.status_code == 200:
                try:
                    result = response.json()

                    # Validate expected keys
                    if "items" in result and "total" in result:
                        items = result.get("items", [])
                        total = result.get("total", 0)

                        # Check consistency between items and total
                        if isinstance(items, list) and total == len(items):
                            response.success()
                            print(f"✅ Retrieved {total} reports successfully ({elapsed}ms)")
                        else:
                            response.failure(f"❌ Mismatch: total={total}, items_count={len(items)} — {result}")
                    else:
                        response.failure(f"❌ Unexpected response structure: {result}")

                except Exception as e:
                    response.failure(f"Invalid JSON: {e}")

            elif response.status_code == 404:
                response.failure(f"Portfolio not found (404): {response.text}")
            elif response.status_code == 401:
                response.failure(f"Unauthorized (401): {response.text}")
            else:
                response.failure(f"Failed to fetch report history ({response.status_code}): {response.text}")

    @task(3)
    def get_specific_report(self):
        """Fetch a specific report by portfolio_id and report_id"""
        # You can set a known valid report_id here or get it dynamically from history
        REPORT_ID = 3 
        url = f"/reports/{PORTFOLIO_ID}/report/{REPORT_ID}"

        start = time.time()
        with self.client.get(url, headers=self.auth_headers, catch_response=True, name="/reports/{portfolio_id}/report/{report_id}") as response:
            elapsed = int((time.time() - start) * 1000)

            if response.status_code == 200:
                try:
                    result = response.json()

                    expected_keys = [
                        "report_type",
                        "report_date",
                        "report_name",
                        "id",
                        "portfolio_id",
                        "created_at",
                        "created_by",
                        "report_data"
                    ]

                    if all(key in result for key in expected_keys):
                        response.success()
                        print(f"✅ Successfully fetched report ID {result.get('id')} for portfolio {result.get('portfolio_id')} ({elapsed}ms)")
                    else:
                        response.failure(f"❌ Missing keys in report response: {result}")

                except Exception as e:
                    response.failure(f"Invalid JSON: {e}")

            elif response.status_code == 404:
                response.failure(f"Report or portfolio not found (404): {response.text}")
            elif response.status_code == 401:
                response.failure(f"Unauthorized (401): {response.text}")
            else:
                response.failure(f"Unexpected error ({response.status_code}): {response.text}")
                        