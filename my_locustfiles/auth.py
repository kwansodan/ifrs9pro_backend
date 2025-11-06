# my_locustfiles/auth.py
from locust import HttpUser
import os

def login_user(client, email, password):
    """Login user and return Bearer token"""
    with client.post("/login", json={"email": email, "password": password}, catch_response=True) as response:
        if response.status_code == 200:
            data = response.json()
            token = data.get("access_token")
            if token:
                response.success()
                return token
            else:
                response.failure("No access_token returned")
        else:
            response.failure(f"Login failed: {response.text}")
    return None
