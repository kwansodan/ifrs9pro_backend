import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import time
from datetime import datetime, timedelta, date
from app.config import settings
import io
import uuid
import pandas as pd
import logging
from io import BytesIO
from sqlalchemy import text, func, case, cast, String, and_, select, Numeric, literal_column, union_all
from app.database import SessionLocal
from app.models import Loan, User, Report, Portfolio, Client
from app.schemas import LoanGuaranteeColumns, CollateralColumns
from sqlalchemy.orm import Session
from fastapi import HTTPException, UploadFile
import os
import shutil
from typing import Optional, Dict
from app.utils.mapping_utils import get_model_columns



logger = logging.getLogger(__name__)


# Initialize the MinIO (S3-compatible) client
s3_client = boto3.client(
    "s3",
    endpoint_url=settings.MINIO_ENDPOINT,  # e.g. "http://localhost:9000"
    aws_access_key_id=settings.MINIO_ACCESS_KEY,
    aws_secret_access_key=settings.MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"  # arbitrary for MinIO
)
MINIO_PUBLIC_ENDPOINT = getattr(settings, "MINIO_PUBLIC_ENDPOINT")

public_s3_client = boto3.client(
    "s3",
    endpoint_url=settings.MINIO_PUBLIC_ENDPOINT,  # e.g. "http://localhost:9000"
    aws_access_key_id=settings.MINIO_ACCESS_KEY,
    aws_secret_access_key=settings.MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"  # arbitrary for MinIO
)


def upload_file_to_minio(file_path: str, object_name: str) -> str:
    """
    Upload a local file to MinIO and return its accessible URL.
    """
    bucket_name = settings.MINIO_BUCKET_NAME

    # Ensure bucket exists
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception:
        s3_client.create_bucket(Bucket=bucket_name)

    try:
        # Upload the file
        s3_client.upload_file(file_path, bucket_name, object_name)
    except boto3.exceptions.S3UploadFailedError as e:
        logger.error(f"Upload failed: {e}")
        raise HTTPException(status_code=500, detail="File upload failed")

    # Return URL to access file
    file_url = f"{MINIO_PUBLIC_ENDPOINT}/{bucket_name}/{object_name}"
    return file_url

def upload_and_extract_columns(
    portfolio_id: int,
    uploadfile: UploadFile,
    key: str,
    expected_columns: list[str],
) -> dict:
    """
    Uploads file to MinIO, extracts Excel column headers,
    and returns metadata including file_id and expected columns.
    """

    if uploadfile is None:
        return None

    file_id = str(uuid.uuid4())
    ext_filename = f"{file_id}_{uploadfile.filename}"
    object_name = f"portfolio/{portfolio_id}/{key}/{ext_filename}"
    tmp_path = f"/tmp/{ext_filename}"

    # Save file locally
    with open(tmp_path, "wb") as buffer:
        shutil.copyfileobj(uploadfile.file, buffer)

    # Upload to MinIO
    file_url = upload_file_to_minio(tmp_path, object_name)

    # Extract Excel metadata
    try:
        df = pd.read_excel(tmp_path)
        excel_columns = df.columns.tolist()
        row_count = len(df.index)
    except Exception as e:
        os.remove(tmp_path)
        raise HTTPException(
            status_code=400,
            detail=f"Invalid Excel file for '{key}': {e}",
        )

    os.remove(tmp_path)

    return {
        "file_id": file_id,
        "file_url": file_url,
        "object_name": object_name,
        "excel_columns": excel_columns,
        "expected_columns": expected_columns,
        "row_count": row_count,
    }


from typing import Optional, Dict
from fastapi import UploadFile

from typing import Optional, Dict
from fastapi import UploadFile


def upload_multiple_files_to_minio(
    portfolio_id: int,
    loan_details: Optional[UploadFile],
    client_data: Optional[UploadFile],
    loan_guarantee_data: Optional[UploadFile] = None,
    loan_collateral_data: Optional[UploadFile] = None,
) -> Dict[str, Optional[dict]]:

    EXPECTED_COLUMNS = {
        "loan_details": [
            "Loan No.",
            "Loan Issue Date",
            "Deduction Start Period",
            "Submission Period",
            "Loan Amount",
            "Loan Term",
            "Administrative Fees",
            "Total Interest",
            "Outstanding Loan Balance",
            "Accumulated Arrears",
            "NDIA",
            "Monthly Installment",
        ],
        "client_data": [
            "Employee Id",
            "Marital Status",
            "Gender",
            "Date of Birth",
            "Employment Date",
        ],
        "loan_guarantee_data": [
            "id",
            "Guarantor name",
            "pledged amount",
        ],
        "loan_collateral_data": [
            "Client Id",
            "Collateral_value",
            "Collateral description",
            "forced_sale_value",
            "Method_of_evaluation",
            "Marital Status",
            "Social Security No",
            "Next of kin",
        ],
    }

    file_map = {
        "loan_details": loan_details,
        "client_data": client_data,
        "loan_guarantee_data": loan_guarantee_data,
        "loan_collateral_data": loan_collateral_data,
    }

    results: Dict[str, Optional[dict]] = {}

    for key, uploadfile in file_map.items():
        if uploadfile is None:
            results[key] = None
            continue

        results[key] = upload_and_extract_columns(
            portfolio_id=portfolio_id,
            uploadfile=uploadfile,
            key=key,
            expected_columns=EXPECTED_COLUMNS[key],
        )

    return results



def generate_presigned_url(object_name: str, expiry_minutes: int = 10):
    print("DEBUG PRESIGNED USING:", public_s3_client.meta.endpoint_url)
    print("DEBUG INTERNAL:", s3_client.meta.endpoint_url)
    bucket = settings.MINIO_BUCKET_NAME
    url = public_s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": object_name},
        ExpiresIn=expiry_minutes * 60,
    )
    return url


def download_report(bucket_name: str, object_name: str) -> BytesIO:
    """
    Download a file from a MinIO bucket using boto3 and return it as BytesIO.
    Raises an exception if bucket or object not found.
    """
    try:
        # Check bucket existence
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception:
        raise FileNotFoundError(f"Bucket '{bucket_name}' not found")

    start_time = time.time()
    last_error = None

    # Retry for up to 10 seconds
    while time.time() - start_time < 10:
        try:
            file_obj = BytesIO()
            s3_client.download_fileobj(bucket_name, object_name, file_obj)
            file_obj.seek(0)
            return file_obj
        except s3_client.exceptions.NoSuchKey:
            last_error = FileNotFoundError(f"Object '{object_name}' not found in bucket '{bucket_name}'")
            time.sleep(1)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "404" or error_code == "NotFound":
                 last_error = FileNotFoundError(f"Object '{object_name}' not found in bucket '{bucket_name}' - {str(e)}")
                 time.sleep(1)
            else:
                 # If it's another client error, raise immediately
                 raise RuntimeError(f"Error downloading '{object_name}' from '{bucket_name}': {str(e)}")
        except Exception as e:
            # If it's a different kind of error, we might want to retry or fail.
            # For network blips, retrying might be good.
            last_error = RuntimeError(f"Error downloading '{object_name}' from '{bucket_name}': {str(e)}")
            time.sleep(1)
            
    # If we fall through here, we ran out of time
    if last_error:
        raise last_error
    else:
        # Should not happen if loop logic is correct
        raise TimeoutError(f"Timed out trying to download '{object_name}'")
    

def run_and_save_report_task(report_id: int, report_type: str, file_path: str, portfolio_id: int):
    """
    Background task to generate Excel reports and upload to MinIO.
    Mirrors run_and_save_report_task from reports_factory.py but uses MinIO instead of Azure.
    """
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
                italic_format = workbook.add_format({'italic': True})
                worksheet.write('A1', f"Dalex Finance", bold_format)
                worksheet.write('A2', f"Detailed IFRS9 ECL report", bold_format)
                worksheet.write('A3', f"Portfolio: {relevant_portfolio.name}", bold_format)
                worksheet.write('A4', f"Report date: {date.today().strftime('%Y-%m-%d')}", bold_format)
                worksheet.write('A5', f"Report extraction date: {date.today().strftime('%Y-%m-%d')}", bold_format)

                worksheet.write('A7', f"Note that ECL calculation results are as at the report run date. ECLs are discounted at the effective interest rate to the calculation run date", italic_format)

                headers = [
                    "Loan No", "Loan Issue Date", "Deduction Start Period", "Submission Period", "Maturity Period", "Outsanding Loan Balance","Deduction Status", "Employee ID", "Employee Name", "Loan Amount", "Theoretical Balance",
                    "Accumulated Arrears", "NDIA", "Stage", "EAD", "LGD", "EIR", "PD", "ECL"
                ]
                start_row=8
                for col, h in enumerate(headers):
                    worksheet.write(8, col, h, workbook.add_format({'bold': True, 'align': 'left'}))

                query = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).yield_per(1000)
                row_idx = start_row+1

                for row in query:
                    worksheet.write(row_idx, 0, row.loan_no)
                    worksheet.write(row_idx, 1, str(row.loan_issue_date))
                    worksheet.write(row_idx, 2, str(row.deduction_start_period))
                    worksheet.write(row_idx, 3, str(row.submission_period))
                    worksheet.write(row_idx, 4, str(row.maturity_period))
                    worksheet.write(row_idx, 5, float(row.outstanding_loan_balance or 0))
                    worksheet.write(row_idx, 6, str(row.deduction_status))
                    worksheet.write(row_idx, 7, row.employee_id)
                    worksheet.write(row_idx, 8, row.employee_name)
                    worksheet.write(row_idx, 9, float(row.loan_amount or 0))
                    worksheet.write(row_idx, 10, row.theoretical_balance)
                    worksheet.write(row_idx, 11, str(row.accumulated_arrears))
                    worksheet.write(row_idx, 12, str(row.ndia))
                    worksheet.write(row_idx, 13, str(row.ifrs9_stage))
                    worksheet.write(row_idx, 14, str(row.ead))
                    worksheet.write(row_idx, 15, str(row.lgd))
                    worksheet.write(row_idx, 16, str(row.eir))
                    worksheet.write(row_idx, 17, str(row.pd))
                    worksheet.write(row_idx, 18, str(row.final_ecl))
                    row_idx += 1

            case "BOG_impairment_detailed_report":
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
                row_idx = start_row+1

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

                query = db.query( Loan.ifrs9_stage.label("stage"), func.sum(Loan.loan_amount).label("loan_value"), func.sum(Loan.ead).label("outstanding_loan_balance"), func.sum(Loan.final_ecl).label("ecl"), cast(0.20, Numeric(5, 2)).label("recovery_rate")
                 ) .filter(Loan.portfolio_id == portfolio_id) .group_by(Loan.ifrs9_stage) .order_by(Loan.ifrs9_stage) .all() 
                row_idx = start_row+1

                for row in query:
                    worksheet.write(row_idx, 0, row.stage)
                    worksheet.write(row_idx, 1, round(float(row.loan_value),2))
                    worksheet.write(row_idx, 2, round(float(row.outstanding_loan_balance),2))
                    worksheet.write(row_idx, 3, round(float(row.ecl),2))
                    worksheet.write(row_idx, 4, round(float(row.outstanding_loan_balance)/float(row.loan_value) * 100,2))
                    row_idx+= 1

            case "BOG_impairmnt_summary_by_stages":
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

                query = db.query(Loan.bog_stage.label("stage"), func.sum(Loan.loan_amount).label("loan_value"), func.sum(Loan.ead).label("outstanding_loan_balance"), func.sum(Loan.bog_provision).label("provision"), cast(0.20, Numeric(5, 2)).label("recovery_rate")
                 ) .filter(Loan.portfolio_id == portfolio_id) .group_by(Loan.bog_stage) .order_by(Loan.bog_stage) .all() 
                row_idx = start_row+1

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
                    col_width = max(len(h), 30)
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

        # Upload to MinIO
        object_name = f"reports/{os.path.basename(file_path)}"
        minio_url = upload_file_to_minio(file_path, object_name)

        # Update report status
        db.query(Report).filter(Report.id == report_id).update({
            "status": "success",
            "file_path": minio_url
        })
        db.commit()
        logger.info(f"[TASK COMPLETE] Report {report_id} successfully uploaded to MinIO and status updated.")
        
        # Clean up local file after successful upload
        try:
            os.remove(file_path)
            logger.info(f"[CLEANUP] Local file {file_path} removed successfully.")
        except Exception as cleanup_error:
            logger.warning(f"[CLEANUP] Failed to remove local file {file_path}: {cleanup_error}")

    except Exception as e:
        logger.error(f"[TASK ERROR] Report task failed for report_id={report_id}: {e}", exc_info=True)
        db.query(Report).filter(Report.id == report_id).update({"status": "failed"})
        db.commit()
        
        # Clean up local file if it exists
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"[CLEANUP] Local file {file_path} removed after error.")
        except Exception as cleanup_error:
            logger.warning(f"[CLEANUP] Failed to remove local file {file_path}: {cleanup_error}")

    finally:
        db.close()


async def generate_presigned_url_for_download(file_url: str, expiry_minutes: int = 10) -> str:
    """
    Generate a pre-signed URL from a MinIO file URL.
    This is the MinIO equivalent of generate_sas_url from Azure version.
    """
    # Parse the MinIO URL to extract bucket and object name
    # Format: http://minio:9000/bucket-name/object-key
    # Handle both internal and public endpoints gracefully
    internal_prefix = settings.MINIO_ENDPOINT.rstrip("/") + "/"
    public_prefix = MINIO_PUBLIC_ENDPOINT.rstrip("/") + "/"
    if file_url.startswith(internal_prefix):
        file_url = file_url.replace(internal_prefix, "")
    elif file_url.startswith(public_prefix):
        file_url = file_url.replace(public_prefix, "")

    url_parts = file_url.split("/", 1)

    
    if len(url_parts) != 2:
        raise ValueError(f"Invalid MinIO URL format: {file_url}")
    
    bucket_name = url_parts[0]
    object_name = url_parts[1]
    
    if bucket_name != settings.MINIO_BUCKET_NAME:
        raise ValueError(f"Invalid bucket name: {bucket_name}")
    
    # Generate pre-signed URL
    presigned_url = s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket_name, "Key": object_name},
        ExpiresIn=int(timedelta(minutes=expiry_minutes).total_seconds())
    )
    
    return presigned_url

