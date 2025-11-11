import os
from typing import List, Optional
from mailjet_rest import Client
from app.config import settings
import html

FRONTEND_BASE_URL = os.getenv("FRONTEND_BASE_URL")
MAILJET_API_KEY = os.getenv("MAILJET_API_KEY")
MAILJET_API_SECRET = os.getenv("MAILJET_API_SECRET")
MAILJET_SENDER_EMAIL = os.getenv("MAILJET_SENDER_EMAIL")


async def send_email(to_email: str, subject: str, body: str, cc_emails: Optional[List[str]] = None):
    """
    Send email using Mailjet Communication Services.
    """
    try:
        email_client = Client(auth=(MAILJET_API_KEY, MAILJET_API_SECRET), version="v3.1")

        data = {
            "Messages": [
                {
                    "From": {"Email": MAILJET_SENDER_EMAIL, "Name": "IFRS9Pro Team"},
                    "To": [{"Email": to_email}],
                    "Cc": [{"Email": email} for email in cc_emails] if cc_emails else [],
                    "Subject": subject,
                    "TextPart": body,
                    "HTMLPart": html.escape(body).replace("\n", "<br>"),
                }
            ]
        }

        result = email_client.send.create(data=data)
        return True

    except Exception as e:
        print(f"Failed to send email: {str(e)}")
        if settings.DEBUG:
            print(f"\n----- MOCK EMAIL (Mailjet send failed) -----")
            print(f"To: {to_email}")
            print(f"Subject: {subject}")
            print(f"Body:\n{body}")
            print(f"----- END EMAIL -----\n")
        return False


async def send_ingestion_success_email(user_email: str, first_name: str, portfolio_id: int, uploaded_filenames: str, cc_emails: Optional[List[str]] = None):
    summary_url = f"{FRONTEND_BASE_URL}/dashboard/portfolio-details/{portfolio_id}"
    subject = "Ingestion Completed Successfully"
    body = f"""
Hello {first_name},

Your data ingestion process (including data quality review and automatic loan staging) for the following imports '{uploaded_filenames}' has been completed successfully.

You can view the results here:
{summary_url}

Thank you,
IFRS9Pro Team
support@service4gh.com
    """
    return await send_email(user_email, subject, body, cc_emails)


async def send_ingestion_failed_email(user_email: str, first_name: str, portfolio_id: int, uploaded_filenames: str, cc_emails: Optional[List[str]] = None):
    summary_url = f"{FRONTEND_BASE_URL}/dashboard/portfolio-details/{portfolio_id}"
    subject = "Ingestion Failed"
    body = f"""
Hello {first_name},

Your data ingestion process (including data quality review and automatic loan staging) for the following imports '{uploaded_filenames}' has NOT been completed successfully.

The process failure has been logged with our support team, who will investigate and revert to you ASAP.

Thank you,
IFRS9Pro Team
support@service4gh.com
    """
    return await send_email(user_email, subject, body, cc_emails)


async def send_ingestion_began_email(
    user_email: str,
    first_name: str,
    portfolio_id: int,
    uploaded_filenames: str,
    cc_emails: Optional[List[str]] = None,
):
    """
    Notify user that the ingestion process has started and provide an ETA.
    """
    summary_url = f"{FRONTEND_BASE_URL}/dashboard/portfolio-details/{portfolio_id}"
    subject = "Ingestion Process Started"
    body = f"""
Hello {first_name},

Your data ingestion process (including data quality review and automatic loan staging) for the following imports '{uploaded_filenames}' has **begun**.

This process typically completes within about **one hour**, depending on data size and system load.

You can monitor progress and later view results here:
{summary_url}

Thank you for your patience,
IFRS9Pro Team
support@service4gh.com
    """
    return await send_email(user_email, subject, body, cc_emails)

