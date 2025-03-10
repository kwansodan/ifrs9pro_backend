import os
from typing import List
import asyncio
from azure.communication.email import EmailClient
from azure.communication.email.aio import EmailClient as AsyncEmailClient
from azure.core.credentials import AzureKeyCredential
from app.config import settings
from urllib.parse import urlencode

FRONTEND_BASE_URL = os.getenv("FRONTEND_BASE_URL")

# Azure Communication Services configuration
AZURE_COMMUNICATION_CONNECTION_STRING = os.getenv(
    "AZURE_COMMUNICATION_CONNECTION_STRING"
)
AZURE_SENDER_EMAIL = os.getenv("AZURE_SENDER_EMAIL")


async def send_email(to_email: str, subject: str, body: str):
    """
    Send email using Azure Communication Services.
    """
    try:
        # Create the email client
        email_client = AsyncEmailClient.from_connection_string(
            AZURE_COMMUNICATION_CONNECTION_STRING
        )

        # Create the email message
        message = {
            "senderAddress": AZURE_SENDER_EMAIL,
            "recipients": {"to": [{"address": to_email}]},
            "content": {
                "subject": subject,
                "plainText": body,
                "html": body.replace("\n", "<br>"),
            },
        }

        # Send the email
        poller = await email_client.begin_send(message)
        result = await poller.result()

        # Close the client
        await email_client.close()

        return True
    except Exception as e:
        print(f"Failed to send email: {str(e)}")
        # For development/testing, you may want to fall back to mock implementation
        if settings.DEBUG:
            print(f"\n----- MOCK EMAIL (Azure send failed) -----")
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
Your access request has been approved! Please set up your password by clicking the link below:
{invitation_url}
This link will expire in {settings.INVITATION_EXPIRE_HOURS} hours.

Thank you,
IFRS9Pro Team
    """
    return await send_email(email, subject, body)


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
