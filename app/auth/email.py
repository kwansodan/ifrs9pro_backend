import os
from typing import List, Optional
from email.message import EmailMessage
from urllib.parse import urlencode
from mailjet_rest import Client
from app.config import settings

FRONTEND_BASE_URL = os.getenv("FRONTEND_BASE_URL")

# Mailjet configuration
MAILJET_API_KEY = os.getenv("MAILJET_API_KEY")
MAILJET_API_SECRET = os.getenv("MAILJET_API_SECRET")
MAILJET_SENDER_EMAIL = os.getenv("MAILJET_SENDER_EMAIL")

async def send_email(
    to_email: str,
    subject: str,
    body: str,
    html_body: Optional[str] = None,
) -> bool:
    """
    Send email using Mailjet API.
    """
    try:
        # Initialize Mailjet client
        mailjet = Client(auth=(MAILJET_API_KEY, MAILJET_API_SECRET), version='v3.1')

        # Prepare email data
        email_data = {
            "Messages": [
                {
                    "From": {
                        "Email": MAILJET_SENDER_EMAIL,
                        "Name": "IFRS9Pro Team"
                    },
                    "To": [
                        {
                            "Email": to_email
                        }
                    ],
                    "Subject": subject,
                    "TextPart": body,
                    "HTMLPart": html_body if html_body else body.replace("\n", "<br>")
                }
            ]
        }

        # Send email via Mailjet API
        result = mailjet.send.create(data=email_data)

        # Check if the email was sent successfully
        if result.status_code == 200:
            return True
        else:
            print(f"Failed to send email via Mailjet: {result.json()}")
            if getattr(settings, "DEBUG", False):
                print(f"\n----- MOCK EMAIL (Mailjet send failed) -----")
                print(f"To: {to_email}")
                print(f"Subject: {subject}")
                print(f"Body:\n{body}")
                print(f"----- END EMAIL -----\n")
            return False

    except Exception as e:
        print(f"Failed to send email via Mailjet: {str(e)}")
        if getattr(settings, "DEBUG", False):
            print(f"\n----- MOCK EMAIL (Mailjet send failed) -----")
            print(f"To: {to_email}")
            print(f"Subject: {subject}")
            print(f"Body:\n{body}")
            print(f"----- END EMAIL -----\n")
        return False


async def send_verification_email(email: str, token: str):
    params = {"email": email, "token": token}
    verification_url = f"{FRONTEND_BASE_URL}/admin-request?{urlencode(params)}"
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
    invitation_url = f"{FRONTEND_BASE_URL}/password-reset/{token}"
    subject = "Account Invitation"
    body = f"""
Hello,
Your access request has been approved! Please set your password by clicking the link below:
{invitation_url}
This link will expire in {settings.INVITATION_EXPIRE_HOURS} hours.

Thank you,
IFRS9Pro Team
    """
    return await send_email(email, subject, body)


async def send_password_setup_email(email: str, token: str):
    invitation_url = f"{FRONTEND_BASE_URL}/password-reset/{token}"
    subject = "Set your password"
    body = f"""
Hello,
An account has been created for you! Please set your password by clicking the link below:
{invitation_url}
This link will expire in {settings.INVITATION_EXPIRE_HOURS} hours.

Thank you,
IFRS9Pro Team
    """
    return await send_email(email, subject, body)