import os
from typing import List
from app.config import settings

BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")

async def send_email(to_email: str, subject: str, body: str):
    """
    Mock email sending functionality.
    In a production environment, this would use aiosmtplib to send actual emails.
    """
    print(f"\n----- MOCK EMAIL -----")
    print(f"To: {to_email}")
    print(f"Subject: {subject}")
    print(f"Body:\n{body}")
    print(f"----- END EMAIL -----\n")
    return True

async def send_verification_email(email: str, token: str):
    verification_url = f"{BASE_URL}/verify-email/{token}"
    subject = "Verify your email"
    body = f"""
Hello,

Please verify your email address by clicking the link below:

{verification_url}

This link will expire in 24 hours.

Thank you,
IFRS9Pro Team
    """
    return await send_email(email, subject, body)

async def send_admin_notification(admin_email: str, requester_email: str):
    subject = "New Access Request"
    body = f"""
Hello Admin,

A new access request has been submitted:

Requester Email: {requester_email}

Please login to the admin dashboard to review this request.

Thank you,
IFRS9Pro Team
    """
    return await send_email(admin_email, subject, body)

async def send_invitation_email(email: str, token: str):
    invitation_url = f"{BASE_URL}/set-password/{token}"
    subject = "Account Invitation"
    body = f"""
Hello,

Your access request has been approved! Please set up your password by clicking the link below:

{invitation_url}

This link will expire in {settings.INVITATION_EXPIRE_HOURS} hours.

Thank you,
IFRS9Pro Team
    """
    return await send_email(email, subject, body)

