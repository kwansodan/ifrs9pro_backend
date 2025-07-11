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


async def send_email(to_email: str, subject: str, body: str, cc_emails: str):
    """
    Send email using Azure Communication Services.
    """
    try:
        # Create the email client
        email_client = AsyncEmailClient.from_connection_string(
            AZURE_COMMUNICATION_CONNECTION_STRING
        )

        recipients = {"to": [{"address": to_email}]}
        if cc_emails:
            recipients["cc"] = [{"address": email} for email in cc_emails]

        # Create the email message
        message = {
            "senderAddress": AZURE_SENDER_EMAIL,
            "recipients":recipients,
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


async def send_ingestion_success_email(user_email: str, first_name: str, portfolio_id: int, uploaded_filenames: str, cc_emails: str):
    """
    Notify user that their ingestion process has completed successfully.
    """
    user_email=user_email
    summary_url = f"{FRONTEND_BASE_URL}/dashboard/portfolio-details/{portfolio_id}"
    subject = "Ingestion Completed Successfully"
    body = f"""
Hello {first_name},

Your data ingestion process (including data quality review and automatic loan staging for the following imports'{uploaded_filenames}' has been completed successfully.

You can view the results here:
{summary_url}

Thank you,
IFRS9Pro Team
support@service4gh.com
    """
    return await send_email(user_email, subject, body, cc_emails)


async def send_ingestion_failed_email(user_email: str, first_name: str, portfolio_id: int, uploaded_filenames: str, cc_emails: str):
    """
    Notify user that their ingestion process has completed successfully.
    """
    user_email=user_email
    summary_url = f"{FRONTEND_BASE_URL}/dashboard/portfolio-details/{portfolio_id}"
    subject = "Ingestion Completed Successfully"
    body = f"""
Hello {first_name},

Your data ingestion process (including data quality review and automatic loan staging for the following imports'{uploaded_filenames}' has not been completed successfully.

The process failure has also been logged with our support team. Rest assured they will look into it and revert to you ASAP!

Thank you,
IFRS9Pro Team
support@service4gh.com
    """
    return await send_email(user_email, subject, body, cc_emails)
