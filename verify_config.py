import sys
import os

# Add the project root to the python path
sys.path.append(os.getcwd())

from app.utils.seed_subscription_plans import PLANS
from app.config import settings

print("--- Verifying Configuration ---")
print(f"PAYSTACK_PLAN_CORE from settings: {settings.PAYSTACK_PLAN_CORE}")
print(f"PAYSTACK_PLAN_PROFESSIONAL from settings: {settings.PAYSTACK_PLAN_PROFESSIONAL}")
print(f"PAYSTACK_PLAN_ENTERPRISE from settings: {settings.PAYSTACK_PLAN_ENTERPRISE}")

print("\n--- Verifying PLANS structure ---")
for plan in PLANS:
    print(f"Plan: {plan['name']}, Code: {plan['paystack_plan_code']}")

# Expected values from .env
EXPECTED_CORE = "PLN_5s9ms4hgelzzscr"
EXPECTED_PROF = "PLN_7s3l587hlaouany"
EXPECTED_ENT = "PLN_miyxov2l51hus5w"

if settings.PAYSTACK_PLAN_CORE == EXPECTED_CORE and \
   settings.PAYSTACK_PLAN_PROFESSIONAL == EXPECTED_PROF and \
   settings.PAYSTACK_PLAN_ENTERPRISE == EXPECTED_ENT:
    print("\nSUCCESS: Configuration loaded correctly from .env")
else:
    print("\nFAILURE: Configuration mismatch!")
    print(f"Expected Core: {EXPECTED_CORE}, Got: {settings.PAYSTACK_PLAN_CORE}")
