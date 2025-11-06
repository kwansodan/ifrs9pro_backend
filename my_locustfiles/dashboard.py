# my_locustfiles/dashboard.py
import json

def get_dashboard(client, headers):
    """
    Fetch the main dashboard data using a valid Bearer token.
    """
    with client.get("/dashboard", headers=headers, catch_response=True, name="/dashboard") as response:
        if response.status_code == 200:
            try:
                data = response.json()
                # sanity check for expected keys
                if "portfolio_overview" not in data:
                    response.failure("Missing 'portfolio_overview' in response")
                else:
                    response.success()
            except json.JSONDecodeError:
                response.failure("Invalid JSON in dashboard response")
        else:
            response.failure(f"Dashboard request failed: {response.text}")
