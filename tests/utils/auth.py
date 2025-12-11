# Helpers for authentication in tests
import requests
import os
from dotenv import load_dotenv
import schemathesis


def get_auth_headers_for_user(client, email: str, password: str = 'password'):
    """Call the /login endpoint and return Authorization header dict."""
    resp = client.post('/login', json={'email': email, 'password': password})
    assert resp.status_code == 200, f"Login failed: {resp.text}"
    token = resp.json().get('access_token')
    return {'Authorization': f'Bearer {token}'}

load_dotenv()

# tests/utils/auth.py

@schemathesis.auth()
class TokenAuth:
    def get(self, case, ctx):
        # Adjust URL/fields to match your /token endpoint precisely
        r = requests.post(
            "http://localhost:8000/token",
            data={"username": os.getenv("ADMIN_EMAIL"),
                    "password": os.getenv("ADMIN_PASSWORD"),},
        )
        r.raise_for_status()
        return r.json()["access_token"]

    def set(self, case, data, ctx):
        case.headers = case.headers or {}
        case.headers["Authorization"] = f"Bearer {data}"


