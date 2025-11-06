import time
import json
from datetime import date

# ==============================
# AUTHENTICATION HELPER
# ==============================
def authenticate(client, email, password):
    """
    Logs into the API and returns auth headers.
    Raises RuntimeError if authentication fails.
    """
    print(f"🔐 Logging in as {email} ...")
    response = client.post("/login", json={"email": email, "password": password})

    if response.status_code == 200:
        data = response.json()
        token = data.get("access_token")
        if not token:
            raise RuntimeError("❌ Login response missing 'access_token'")
        print("✅ Authenticated successfully!")
        return {"Authorization": f"Bearer {token}"}
    else:
        raise RuntimeError(f"❌ Login failed ({response.status_code}): {response.text}")


# ==============================
# REPORT GENERATION
# ==============================
def generate_report(client, portfolio_id, headers):
    """Trigger report generation for a given portfolio"""
    url = f"/reports/{portfolio_id}/generate"
    payload = {
        "report_date": date.today().isoformat(),
        "report_type": "ecl_detailed_report"
    }

    start = time.time()
    with client.post(url, json=payload, headers=headers, catch_response=True, name="/reports/{portfolio_id}/generate") as response:
        elapsed = int((time.time() - start) * 1000)
        if response.status_code == 200:
            try:
                result = response.json()
                message = result.get("message")
                report_id = result.get("report_id")

                if message == "Report generation started" and report_id:
                    response.success()
                    print(f"✅ Report generation started — Report ID: {report_id} ({elapsed}ms)")
                else:
                    response.failure(f"Unexpected response format: {result}")
            except Exception as e:
                response.failure(f"Invalid JSON: {e}")
        elif response.status_code == 404:
            response.failure(f"Portfolio not found (404): {response.text}")
        elif response.status_code == 401:
            response.failure(f"Unauthorized (401): {response.text}")
        else:
            response.failure(f"Report generation failed ({response.status_code}): {response.text}")


# ==============================
# REPORT HISTORY
# ==============================
def get_report_history(client, portfolio_id, headers):
    """Fetch report history for a given portfolio"""
    url = f"/reports/{portfolio_id}/history"

    start = time.time()
    with client.get(url, headers=headers, catch_response=True, name="/reports/{portfolio_id}/history") as response:
        elapsed = int((time.time() - start) * 1000)
        if response.status_code == 200:
            try:
                result = response.json()
                if "items" in result and "total" in result:
                    items = result.get("items", [])
                    total = result.get("total", 0)

                    if isinstance(items, list) and total == len(items):
                        response.success()
                        print(f"✅ Retrieved {total} reports successfully ({elapsed}ms)")
                    else:
                        response.failure(f"❌ Mismatch: total={total}, items={len(items)}")
                else:
                    response.failure(f"❌ Unexpected structure: {result}")
            except Exception as e:
                response.failure(f"Invalid JSON: {e}")
        elif response.status_code == 404:
            response.failure(f"Portfolio not found (404): {response.text}")
        elif response.status_code == 401:
            response.failure(f"Unauthorized (401): {response.text}")
        else:
            response.failure(f"Failed to fetch history ({response.status_code}): {response.text}")


# ==============================
# SPECIFIC REPORT FETCH
# ==============================
def get_specific_report(client, portfolio_id, report_id, headers):
    """Fetch a specific report by portfolio_id and report_id"""
    url = f"/reports/{portfolio_id}/report/{report_id}"

    start = time.time()
    with client.get(url, headers=headers, catch_response=True, name="/reports/{portfolio_id}/report/{report_id}") as response:
        elapsed = int((time.time() - start) * 1000)
        if response.status_code == 200:
            try:
                result = response.json()
                expected_keys = [
                    "report_type", "report_date", "report_name", "id",
                    "portfolio_id", "created_at", "created_by", "report_data"
                ]
                if all(k in result for k in expected_keys):
                    response.success()
                    print(f"✅ Report {result.get('id')} fetched successfully ({elapsed}ms)")
                else:
                    response.failure(f"Missing keys: {result}")
            except Exception as e:
                response.failure(f"Invalid JSON: {e}")
        elif response.status_code == 404:
            response.failure(f"Report/portfolio not found (404): {response.text}")
        elif response.status_code == 401:
            response.failure(f"Unauthorized (401): {response.text}")
        else:
            response.failure(f"Unexpected error ({response.status_code}): {response.text}")
