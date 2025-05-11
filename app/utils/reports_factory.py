from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from app.database import SessionLocal 
from sqlalchemy import text, func, case, cast, String, and_, select, Numeric, literal_column, union_all
from app.auth.utils import get_current_active_user
import pandas as pd
from io import BytesIO
import logging
import requests
from app.database import get_db
from app.models import Loan, User, Report, Portfolio
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from app.config import settings
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta, date
from urllib.parse import urlparse
from sqlalchemy.orm import Session



logger = logging.getLogger(__name__)


def upload_file_to_blob(file_path: str, blob_name: str) -> str:
    blob_service_client = BlobServiceClient.from_connection_string(settings.AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(settings.CONTAINER_NAME)
    blob_client = container_client.get_blob_client(blob_name)

    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    return blob_client.url

def run_and_save_report_task(report_id: int, report_type: str, file_path: str, portfolio_id: int):
    db = SessionLocal()
    try:
        import xlsxwriter

        logger.info(f"[TASK START] Running report task: report_id={report_id}, report_type={report_type}")

        report = db.query(Report).filter(Report.id == report_id).first()
        if not report:
            logger.error(f"Report {report_id} not found.")
            return

        portfolio_id = report.portfolio_id
        relevant_portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()

        workbook = xlsxwriter.Workbook(file_path)
        worksheet = workbook.add_worksheet(report_type)


        match report_type:
            case "ecl_detailed_report":
                bold_format = workbook.add_format({'bold': True})
                left_format = workbook.add_format({'align': 'left'})
                bold_left_format = workbook.add_format({'bold': True, 'align': 'left'})
                worksheet.write('A1', f"Dalex Finance", bold_format)
                worksheet.write('A2', f"Detailed IFRS9 ECL report", bold_format)
                worksheet.write('A3', f"Portfolio: {relevant_portfolio.name}", bold_format)
                worksheet.write('A4', f"Report date: {date.today().strftime('%Y-%m-%d')}", bold_format)
                worksheet.write('A5', f"Report extraction date: {date.today().strftime('%Y-%m-%d')}", bold_format)

                headers = [
                    "Loan No", "Employee ID", "Employee Name", "Loan Amount", "Theoretical Balance",
                    "Accumulated Arrears", "NDIA", "Stage", "EAD", "LGD", "EIR", "PD", "ECL"
                ]
                start_row=7
                for col, h in enumerate(headers):
                    worksheet.write(7, col, h, workbook.add_format({'bold': True, 'align': 'left'}))

                query = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).yield_per(1000)
                row_idx = 1

                for row in query:
                    worksheet.write(row_idx, 0, row.loan_no)
                    worksheet.write(row_idx, 1, row.employee_id)
                    worksheet.write(row_idx, 2, row.employee_name)
                    worksheet.write(row_idx, 3, float(row.loan_amount or 0))
                    worksheet.write(row_idx, 4, row.theoretical_balance)
                    worksheet.write(row_idx, 5, str(row.accumulated_arrears))
                    worksheet.write(row_idx, 6, str(row.ndia))
                    worksheet.write(row_idx, 7, str(row.ifrs9_stage))
                    worksheet.write(row_idx, 8, str(row.ead))
                    worksheet.write(row_idx, 9, str(row.lgd))
                    worksheet.write(row_idx, 10, str(row.eir))
                    worksheet.write(row_idx, 11, str(row.pd))
                    worksheet.write(row_idx, 12, str(row.final_ecl))
                    row_idx += 1

            case "local_impairment_detailed_report":
                bold_format = workbook.add_format({'bold': True})
                left_format = workbook.add_format({'align': 'left'})
                bold_left_format = workbook.add_format({'bold': True, 'align': 'left'})
                worksheet.write('A1', f"Dalex Finance", bold_format)
                worksheet.write('A2', f"Detailed BOG impairment report", bold_format)
                worksheet.write('A3', f"Portfolio: {relevant_portfolio.name}", bold_format)
                worksheet.write('A4', f"Report date: {date.today().strftime('%Y-%m-%d')}", bold_format)
                worksheet.write('A5', f"Report extraction date: {date.today().strftime('%Y-%m-%d')}", bold_format)

                headers = [
                    "Loan No", "Employee ID", "Employee Name", "Loan Amount", "Theoretical Balance",
                    "Accumulated Arrears", "NDIA", "Stage", "Provision rate %", "Provision"
                ]
                start_row=7
                for col, h in enumerate(headers):
                    worksheet.write(start_row, col, h, workbook.add_format({'bold': True, 'align': 'center'}))

                query = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).yield_per(1000)
                row_idx = 1

                for row in query:
                    worksheet.write(row_idx, 0, row.loan_no)
                    worksheet.write(row_idx, 1, row.employee_id)
                    worksheet.write(row_idx, 2, row.employee_name)
                    worksheet.write(row_idx, 3, float(row.loan_amount or 0))
                    worksheet.write(row_idx, 4, row.theoretical_balance)
                    worksheet.write(row_idx, 5, str(row.accumulated_arrears))
                    worksheet.write(row_idx, 6, str(row.ndia))
                    worksheet.write(row_idx, 7, str(row.bog_stage))
                    worksheet.write(row_idx, 8, str(row.bog_prov_rate))
                    worksheet.write(row_idx, 9, str(row.bog_provision))
                    row_idx += 1


            case "ecl_report_summarised_by_stages":
                bold_format = workbook.add_format({'bold': True})
                left_format = workbook.add_format({'align': 'left'})
                bold_left_format = workbook.add_format({'bold': True, 'align': 'left'})
                worksheet.write('A1', f"Dalex Finance", bold_format)
                worksheet.write('A2', f"Summary IFRS 9 ECL report", bold_format)
                worksheet.write('A3', f"Portfolio: {relevant_portfolio.name}", bold_format)
                worksheet.write('A4', f"Report date: {date.today().strftime('%Y-%m-%d')}", bold_format)
                worksheet.write('A5', f"Report extraction date: {date.today().strftime('%Y-%m-%d')}", bold_format)

                headers = [
                    "Stages", "Loan value", "Outstanding loan balance", "ECL", "Recovery rate %"
                ]
                start_row=7
                for col, h in enumerate(headers):
                    worksheet.write(start_row, col, h, workbook.add_format({'bold': True, 'align': 'center'}))

                query = db.query( Loan.ifrs9_stage.label("stage"), func.sum(Loan.loan_amount).label("loan_value"), func.sum(Loan.ead).label("outstanding_loan_balance"), func.sum(Loan.final_ecl).label("ecl"), cast(0.20, Numeric(5, 2)).label("recovery_rate")  # Fixed 20% for all
                 ) .filter(Loan.portfolio_id == portfolio_id) .group_by(Loan.ifrs9_stage) .order_by(Loan.ifrs9_stage) .all() 
                row_idx = 1

                for row in query:
                    worksheet.write(row_idx, 0, row.stage)
                    worksheet.write(row_idx, 1, round(float(row.loan_value),2))
                    worksheet.write(row_idx, 2, round(float(row.outstanding_loan_balance),2))
                    worksheet.write(row_idx, 3, round(float(row.ecl),2))
                    worksheet.write(row_idx, 4, round(float(row.outstanding_loan_balance)/float(row.loan_value) * 100,2))
                    row_idx+= 1

            case "local_impairment_report_summarised_by_stages":
                bold_format = workbook.add_format({'bold': True})
                left_format = workbook.add_format({'align': 'left'})
                bold_left_format = workbook.add_format({'bold': True, 'align': 'left'})
                worksheet.write('A1', f"Dalex Finance", bold_format)
                worksheet.write('A2', f"Summary BOG impairment report ", bold_format)
                worksheet.write('A3', f"Portfolio: {relevant_portfolio.name}", bold_format)
                worksheet.write('A4', f"Report date: {date.today().strftime('%Y-%m-%d')}", bold_format)
                worksheet.write('A5', f"Report extraction date: {date.today().strftime('%Y-%m-%d')}", bold_format)

                headers = [
                    "Stages", "Loan value", "Outstanding loan balance", "Provision", "Recovery rate %"
                ]
                start_row = 7
                for col, h in enumerate(headers):
                    worksheet.write(start_row, col, h, workbook.add_format({'bold': True}))

                query = db.query( Loan.bog_stage.label("stage"), func.sum(Loan.loan_amount).label("loan_value"), func.sum(Loan.ead).label("outstanding_loan_balance"), func.sum(Loan.bog_provision).label("provision"), cast(0.20, Numeric(5, 2)).label("recovery_rate")  # Fixed 20% for all
                 ) .filter(Loan.portfolio_id == portfolio_id) .group_by(Loan.ifrs9_stage) .order_by(Loan.ifrs9_stage) .all() 
                row_idx = 1

                for row in query:
                    worksheet.write(row_idx, 0, row.stage)
                    worksheet.write(row_idx, 1, round(float(row.loan_value),2))
                    worksheet.write(row_idx, 2, round(float(row.outstanding_loan_balance),2))
                    worksheet.write(row_idx, 3, round(float(row.provision),2))
                    worksheet.write(row_idx, 4, round(float(row.outstanding_loan_balance)/float(row.loan_value) * 100,2))
                    row_idx+= 1

            case "journals_report":
                
                bold_format = workbook.add_format({'bold': True})
                left_format = workbook.add_format({'align': 'left'})
                bold_left_format = workbook.add_format({'bold': True, 'align': 'left'})
                worksheet.write('A1', f"Dalex Finance", bold_format)
                worksheet.write('A2', f"Journals report", bold_format)
                worksheet.write('A3', f"Portfolio: {relevant_portfolio.name}", bold_format)
                worksheet.write('A4', f"Report date: {date.today().strftime('%Y-%m-%d')}", bold_format)
                worksheet.write('A5', f"Report extraction date: {date.today().strftime('%Y-%m-%d')}", bold_format)

                headers = [
                    "GL Account code", "Journal description", "Journal amount"
                ]
                start_row = 7
                for col, h in enumerate(headers):
                    worksheet.write(start_row, col, h, bold_left_format)
                    col_width = max(len(h), 30)  # Set a minimum width
                    worksheet.set_column(col, col, col_width, left_format)

                # Step 1: Get Portfolio object and values
                portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
                if not portfolio:
                    raise HTTPException(status_code=404, detail="Portfolio not found")

                # Step 2: Calculate monetary values
                ecl_total = db.query(func.sum(Loan.final_ecl)).filter(Loan.portfolio_id == portfolio_id).scalar() or 0
                bog_total = db.query(func.sum(Loan.bog_provision)).filter(Loan.portfolio_id == portfolio_id).scalar() or 0
                bog_topup = float(bog_total) - float(ecl_total)

                # Step 3: Compose individual selects using dynamic Portfolio values
                stmt1 = select(
                    literal_column(f"'{portfolio.ecl_impairment_account}'").label("account_code"),
                    literal_column("'IFRS9 Impairment - P&L charge'").label("description"),
                    literal_column(f"{ecl_total}").label("ghs")
                )

                stmt2 = select(
                    literal_column(f"'{portfolio.loan_assets}'").label("account_code"),
                    literal_column("'IFRS9 Impairment - impact on loans'").label("description"),
                    literal_column(f"{-ecl_total}").label("ghs")
                )

                stmt3 = select(
                    literal_column(f"'{portfolio.ecl_impairment_account}'").label("account_code"),
                    literal_column("'Top up for BOG Impairment - P&L charge'").label("description"),
                    literal_column(f"{bog_topup}").label("ghs")
                )

                stmt4 = select(
                    literal_column(f"'{portfolio.credit_risk_reserve}'").label("account_code"),
                    literal_column("'Credit risk reserve'").label("description"),
                    literal_column(f"{-bog_topup}").label("ghs")
                )


                # Step 4: Combine all queries
                query = union_all(stmt1, stmt2, stmt3, stmt4)

                # Step 5: Execute
                results = db.execute(query).fetchall()               

                row_idx =start_row + 1
                for row in results:
                    worksheet.write(row_idx, 0, row.account_code)
                    worksheet.write(row_idx, 1, row.description)
                    worksheet.write(row_idx, 2, row.ghs)
                    row_idx+= 1


        workbook.close()
        logger.info(f"[TASK] Excel workbook completed for report_id={report_id}, rows={row_idx}")

        # ... upload to blob, update status, etc. ...
        import os
        from azure.storage.blob import BlobServiceClient
        
        blob_url = upload_file_to_blob(file_path, f"reports/{os.path.basename(file_path)}")


        db.query(Report).filter(Report.id == report_id).update({
            "status": "success",
            "file_path": blob_url
        })
        db.commit()
        logger.info(f"[TASK COMPLETE] Report {report_id} successfully uploaded and status updated.")

    except Exception as e:
        logger.error(f"[TASK ERROR] Report task failed for report_id={report_id}: {e}", exc_info=True)
        db.query(Report).filter(Report.id == report_id).update({"status": "failed"})
        db.commit()

    finally:
        db.close()


# def download_report(report_id: int, current_user: User = Depends(get_current_active_user)):
#     report = db.query(Report).filter_by(id=report_id, user_id=user.id).first()
#     return FileResponse(path=report.filepath, filename=report.filename)



blob_service_client = BlobServiceClient.from_connection_string(
    conn_str=settings.AZURE_STORAGE_CONNECTION_STRING
)

async def generate_sas_url(blob_url: str, expiry_minutes: int = 10) -> str:
    parsed = urlparse(blob_url)
    container_client = blob_service_client.get_container_client(settings.CONTAINER_NAME)
    blob_name = parsed.path.lstrip('/').split('/', 1)[1]
    # blob_name = parsed.path.lstrip(f"/{container_client.container_name}/")

    # Extract the account key from connection string manually
    conn_dict = dict(item.split("=", 1) for item in settings.AZURE_STORAGE_CONNECTION_STRING.split(";") if "=" in item)
    account_key = conn_dict.get("AccountKey")

    sas = generate_blob_sas(
        account_name=settings.AZURE_STORAGE_ACCOUNT_NAME,
        container_name=settings.CONTAINER_NAME,
        blob_name=blob_name,
        account_key=settings.AZURE_STORAGE_ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(minutes=expiry_minutes)
    )

    return f"{blob_url}?{sas}"

async def download_report(report_id: int, db: Session, current_user: User):
    report = db.query(Report).filter_by(id=report_id, created_by=current_user.id).first()
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    if report.status != "success":
        raise HTTPException(status_code=400, detail=f"Report is {report.status}")

    # # Generate signed download URL
    # signed_url = generate_sas_url(report.file_path)
    # return {"download_url": signed_url}

        # Get the SAS URL for the report file
    signed_url = await generate_sas_url(report.file_path)

    # Use the SAS URL to download the file
    response = requests.get(signed_url, stream=True)

    return response
