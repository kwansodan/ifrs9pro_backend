import os
from typing import List
import asyncio
from maijet_rest import Client
from app.config import settings
from urllib.parse import urlencode

FRONTEND_BASE_URL = os.getenv("FRONTEND_BASE_URL")

# Mailjet configuration
MAILJET_API_KEY = os.getenv("MAILJET_API_KEY")
MAILJET_API_SECRET = os.getenv("MAILJET_API_SECRET")
MAILJET_SENDER_EMAIL = os.getenv("MAILJET_SENDER_EMAIL")


async def send_email(to_email: str, subject: str, body: str, cc_emails: str):
    """
    Send email using Mailjet Communication Services.
    """
    try:
        # Create the email client
        email_client = Client(auth=(MAILJET_API_KEY, MAILJET_API_SECRET), version='v3.1')

        recipients = {"to": [{"address": to_email}]}
        if cc_emails:
            recipients["cc"] = [{"address": email} for email in cc_emails]

        # Create the email message
        message = {
            "senderAddress": MAILJET_SENDER_EMAIL,
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
            print(f"\n----- MOCK EMAIL (Mailjet send failed) -----")
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
