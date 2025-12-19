from locust import HttpUser, task, between
import os

ADMIN_TOKEN = os.getenv("ADMIN_JWT")

class BillingUser(HttpUser):
    wait_time = between(0.5, 2.0)

    def on_start(self):
        assert ADMIN_TOKEN, "ADMIN_JWT not set"
        self.client.headers.update({
            "Authorization": f"Bearer {ADMIN_TOKEN}"
        })

    @task(5)
    def list_plans(self):
        self.client.get("/billing/plans")

    @task(3)
    def get_customer(self):
        self.client.get("/billing/customers/me")

    @task(2)
    def get_subscription(self):
        self.client.get("/billing/subscriptions")

    @task(1)
    def verify_transaction(self):
        # Use a known test reference
        self.client.get("/billing/transactions/verify/test_ref_123")
