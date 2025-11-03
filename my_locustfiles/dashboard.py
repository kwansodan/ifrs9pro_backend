from locust import HttpUser, task, between
import os
import json
from dotenv import load_dotenv, find_dotenv

docker_env = "docker/.env.docker"
if os.path.exists(docker_env):
    load_dotenv(dotenv_path=docker_env, override=False)
else:
    # if you still want to fallback to a default .env in repo root
    load_dotenv(find_dotenv())
class UserDashboard(HttpUser):
    """
    Load test user that logs in and repeatedly hits the /dashboard endpoint.
    It uses JWT authentication from the FastAPI backend.
    """

    # Simulate user wait time between requests (adjust as needed)
    wait_time = between(50, 60)

    def on_start(self):
        """
        Called when a simulated user starts.
        It logs in once to obtain a valid access token.
        """

        # --- CONFIGURATION ---
        self.admin_email = os.getenv("ADMIN_EMAIL")
        self.admin_password = os.getenv("ADMIN_PASSWORD")
        self.login_url = "/login"  # adjust if your auth endpoint differs
        self.dashboard_url = "/dashboard"

        # --- LOGIN ---
        with self.client.post(
            self.login_url,
            json={"email": self.admin_email, "password": self.admin_password},
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                data = response.json()
                # Example: {"access_token": "xxx", "token_type": "bearer"}
                self.access_token = data.get("access_token")
                if not self.access_token:
                    response.failure("No access token in response")
                else:
                    response.success()
            else:
                response.failure(f"Login failed: {response.status_code}")

    @task
    def get_dashboard(self):
        """
        Fetch dashboard data with authentication.
        This simulates a logged-in user checking the main dashboard.
        """
        headers = {"Authorization": f"Bearer {self.access_token}"}

        with self.client.get(
            self.dashboard_url,
            headers=headers,
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                # Optional sanity check: ensure expected keys exist
                try:
                    data = response.json()
                    if "portfolio_overview" not in data:
                        response.failure("Missing portfolio_overview in response")
                    else:
                        response.success()
                except json.JSONDecodeError:
                    response.failure("Invalid JSON in response")
            else:
                response.failure(f"Dashboard request failed: {response.status_code}")
