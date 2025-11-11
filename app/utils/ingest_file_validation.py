import pandas as pd
from fastapi import HTTPException, status, UploadFile
from sqlalchemy.inspection import inspect

from app.models import Loan, Client

# Map file types to model classes
MODEL_MAP = {
    "loan_details": Loan,
    "client_data": Client,
    #"loan_guarantee_data": LoanGuarantee,
    #"loan_collateral_data": LoanCollateral,
}

ALLOWED_EXTENSIONS = {".xlsx", ".xls"}

def get_model_columns(model):
    """Return a list of column names from a SQLAlchemy model."""
    mapper = inspect(model)
    return [column.key for column in mapper.columns]

async def validate_uploaded_file(file: UploadFile, file_type: str):
    """
    Validates that an uploaded file is an Excel file and contains the required columns
    based on the SQLAlchemy model schema.
    """
    if file is None:
        return  # skip optional files

    # --- Check file extension ---
    filename = file.filename.lower()
    if not any(filename.endswith(ext) for ext in ALLOWED_EXTENSIONS):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{file_type} must be an Excel file (.xlsx or .xls). Provided: {filename}",
        )

    # --- Try to read the Excel file ---
    try:
        df = pd.read_excel(file.file, nrows=5)  # only read a few rows to get headers
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to read {file_type} as an Excel file. Error: {str(e)}",
        )

    # --- Get required model columns ---
    model_class = MODEL_MAP.get(file_type)
    if not model_class:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown file type: {file_type}",
        )

    model_columns = get_model_columns(model_class)

    # Exclude columns that are auto-generated or handled internally
    excluded = {"id", "portfolio_id", "created_at", "updated_at"}
    required_columns = [col for col in model_columns if col not in excluded]

    # --- Check if all required columns are present ---
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{file_type} is missing required columns: {', '.join(missing_columns)}",
        )

    # Reset file pointer for downstream reading
    file.file.seek(0)

async def validate_all_uploaded_files(
    loan_details: UploadFile,
    client_data: UploadFile,
    #loan_guarantee_data: UploadFile = None,
    #loan_collateral_data: UploadFile = None,
):
    """Validates all uploaded Excel files for required columns."""
    await validate_uploaded_file(loan_details, "loan_details")
    await validate_uploaded_file(client_data, "client_data")
    '''
    if loan_guarantee_data:
        await validate_uploaded_file(loan_guarantee_data, "loan_guarantee_data")

    if loan_collateral_data:
        await validate_uploaded_file(loan_collateral_data, "loan_collateral_data")
    '''