from locust import HttpUser, task, between
import os
import sys
from dotenv import load_dotenv, find_dotenv

docker_env = "docker/.env.docker"
if os.path.exists(docker_env):
    load_dotenv(dotenv_path=docker_env, override=False)
else:
    # if you still want to fallback to a default .env in repo root
    load_dotenv(find_dotenv())
    
class LoginUser(HttpUser):
    host = os.getenv("BASE_URL")  # default for local testing
    wait_time = between(50, 60)

    def on_start(self):
        # Validate env vars once at start
        self.admin_email = os.getenv("ADMIN_EMAIL")
        self.admin_password = os.getenv("ADMIN_PASSWORD")
        if not self.admin_email or not self.admin_password:
            print("ERROR: ADMIN_EMAIL and ADMIN_PASSWORD must be set in environment", file=sys.stderr)
            # Stop Locust by raising SystemExit in worker/main
            raise SystemExit(1)

    @task
    def login(self):
        payload = {"email": self.admin_email, "password": self.admin_password}
        with self.client.post("/login", json=payload, catch_response=True) as response:
            if response.status_code == 200:
                try:
                    data = response.json()
                    self.token = data.get("access_token")
                    if not self.token:
                        response.failure("No access token returned in response")
                        return
                    # Save headers for later use
                    self.headers = {"Authorization": f"Bearer {self.token}"}
                    response.success()
                except Exception as e:
                    response.failure(f"Invalid response format: {e}")
            else:
                response.failure(f"Login failed: {response.text}")
