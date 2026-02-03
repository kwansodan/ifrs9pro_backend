import pickle
import pandas as pd
from datetime import datetime
from decimal import Decimal
from typing import Optional, Tuple, List, Union, Dict, Any
from app.models import Client
import numpy as np
import math
import logging

logger = logging.getLogger(__name__)

# Global cache for the PD model to avoid reloading for every loan
_PD_MODEL_CACHE = None
_PD_MODEL_PATH = "app/ml_models/logistic_model.pkl"

def get_pd_model():
    """Lazy-load and cache the PD model."""
    global _PD_MODEL_CACHE
    if _PD_MODEL_CACHE is None:
        try:
            import warnings
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning)
                with open(_PD_MODEL_PATH, "rb") as file:
                    _PD_MODEL_CACHE = pickle.load(file)
            logger.info("Successfully loaded and cached PD model from %s", _PD_MODEL_PATH)
        except Exception as e:
            logger.error("Failed to load PD model from %s: %s", _PD_MODEL_PATH, e)
            return None
    return _PD_MODEL_CACHE

def calculate_effective_interest_rate_lender(loan_amount, administrative_fees, loan_term, monthly_payment):
    """
    Calculates the effective annual interest rate of a loan from the lender's perspective,
    considering administrative fees as income.

    Args:
        loan_amount (float): The original loan amount.
        administrative_fees (float): The one-time administrative fees (lender's income).
        loan_term (int): The loan term in months.
        monthly_payment (float): The monthly payment amount.

    Returns:
        float: The effective annual interest rate as a percentage, or None if calculation fails.
    """
    try:
        total_income = loan_amount + administrative_fees #From the lender's perspective the admin fees are income.

        cash_flows = [-loan_amount] + [monthly_payment] * loan_term

        def irr(values, guess=0.1):
            """Internal rate of return calculation."""
            rate = guess
            for _ in range(100):  # Maximum iterations
                npv = sum(v / (1 + rate)**i for i, v in enumerate(values))
                derivative = sum(-i * v / (1 + rate)**(i + 1) for i, v in enumerate(values))
                rate -= npv / derivative
                if abs(npv) < 1e-6:
                    return rate
            return None #failed to converge

        if not all(isinstance(val, (int, float)) for val in cash_flows):
          return None

        if any(math.isnan(val) or math.isinf(val) for val in cash_flows):
            return None

        monthly_rate = irr(cash_flows)

        if monthly_rate is None:
            return None

        annual_rate = monthly_rate * 12
        return annual_rate * 100  # Return as percentage

    except (TypeError, ValueError, ZeroDivisionError):
        return None  # Handle potential errors




def calculate_exposure_at_default_percentage(loan, reporting_date):
    """
    Calculate Exposure at Default as a percentage

    Formula:
    Bt = P * ((1+r)^n - (1+r)^t)/((1+r)^n - 1)

    Where:
    Bt = Loan Balance at month t
    P = Original loan amount (Principal)
    r = Monthly interest rate (Annual rate/12)
    n = Total number of months in the loan term
    t = number of months from loan start to specified date

    EAD% = (Bt + Accumulated Arrears) / P * 100
    """
    if not loan.loan_amount or loan.loan_amount <= 0:
        return 0  # If no original amount, assume 0% exposure

    original_amount = loan.loan_amount

    # Get effective interest rate (annual) and convert to monthly
    annual_rate = calculate_effective_interest_rate_lender(
        loan_amount=loan.loan_amount,
        administrative_fees=loan.administrative_fees,
        loan_term=loan.loan_term,
        monthly_payment=loan.monthly_installment,
    )
    
    # Handle case when annual_rate is None
    if annual_rate is None:
        annual_rate = 0  # Default to 0 if calculation fails
    
    monthly_rate = annual_rate / 12

    # Get loan term in months
    loan_term_months = loan.loan_term

    # Calculate months elapsed from loan issue date to reporting date
    issue_date = loan.loan_issue_date
    if not issue_date or not hasattr(issue_date, 'year') or not hasattr(issue_date, 'month') or not reporting_date or not hasattr(reporting_date, 'year') or not hasattr(reporting_date, 'month'):
        return 0
    months_elapsed = (reporting_date.year - issue_date.year) * 12 + (
        reporting_date.month - issue_date.month
    )

    # Ensure months_elapsed is not negative or greater than loan term
    months_elapsed = max(0, min(months_elapsed, loan_term_months))

    theoretical_balance = 0
    if monthly_rate > 0:
        numerator = (1 + monthly_rate) ** loan_term_months - (
            1 + monthly_rate
        ) ** months_elapsed
        denominator = (1 + monthly_rate) ** loan_term_months - 1
        theoretical_balance = original_amount * (numerator / denominator)

    if hasattr(loan, "accumulated_arrears") and loan.accumulated_arrears:
        theoretical_balance += loan.accumulated_arrears

    ead = theoretical_balance

    return ead


def calculate_marginal_ecl(loan, ead_percentage, pd, lgd):
    """
    Calculate the marginal Expected Credit Loss (ECL) for a loan.

    Marginal ECL = EAD * PD * LGD

    Args:
        loan: The loan object
        pd: Probability of Default as a percentage (0-100)
        lgd: Loss Given Default as a percentage (0-100)

    Returns:
        Decimal: The calculated marginal ECL amount
    """

    ead_value = Decimal(loan.outstanding_loan_balance) * Decimal(ead_percentage / 100)

    # Convert percentage values to decimals
    pd_decimal = Decimal(str(pd / 100.0))
    lgd_decimal = Decimal(str(lgd / 100.0))

    # Convert eir to Decimal

    # Calculate marginal ECL
    mecl = ead_value * pd_decimal * lgd_decimal

    return mecl


def is_in_range(value: int, range_tuple: Tuple[int, Optional[int]]) -> bool:
    """
    Check if a value is within the specified range.
    """
    min_val, max_val = range_tuple
    if max_val is None:
        return value >= min_val
    else:
        return min_val <= value <= max_val



def calculate_probability_of_default(employee_id, outstanding_loan_balance, start_date, selected_dt, end_date, arrears, db=None, client_yob=None):
    """
    Calculate Probability of Default.
    Accepts either a DB session to fetch client data (legacy) or directly the client_yob (optimized).
    """
    try:
        # Use provided YOB if available (optimized path)
        year_of_birth = client_yob

        # Fallback to DB query if YOB not provided (legacy path)
        if year_of_birth is None and db is not None:
            client = db.query(Client).filter(Client.employee_id == employee_id).first()
            if client and client.date_of_birth:
                year_of_birth = client.date_of_birth.year

        if year_of_birth is None:
            return 0.05  # Default PD if no data found

        # FIX: Ensure selected_dt is a date object for comparison
        if isinstance(selected_dt, str):
            try:
                selected_dt = pd.to_datetime(selected_dt, errors="coerce").date()
            except Exception:
                pass # Will likely fail comparison later if still string
        elif isinstance(selected_dt, datetime):
            selected_dt = selected_dt.date()
        
        # Ensure start_date/end_date are dates (handle datetime or None)
        if hasattr(start_date, 'date'): start_date = start_date.date()
        if hasattr(end_date, 'date'): end_date = end_date.date()

        # Main logic
        result = 0.00
        if outstanding_loan_balance >= 0:
            # Safe comparisons processing
            if not start_date or not end_date or pd.isna(start_date) or pd.isna(end_date):
                 # Fallback if dates are missing but we need to return something
                 model = get_pd_model()
                 if model:
                    return calculate_pd_from_yob(year_of_birth, model)
                 return 0.05

            if start_date <= selected_dt and end_date > selected_dt:
                model = get_pd_model()
                if model:
                    result = calculate_pd_from_yob(year_of_birth, model)
                else:
                    result = 0.05  # Default PD if model fails to load
            elif start_date < selected_dt and end_date < selected_dt:
                result = 1.0 if arrears else None
        elif outstanding_loan_balance < 0:
            result = 1.0 if arrears > 0 and end_date <= selected_dt else 0.0

        return result

    except Exception as e:
        logger.error(f"Error calculating probability of default: {str(e)}")
        return 0.05


def calculate_pd_from_yob(year_of_birth: Optional[int], model: Any) -> float:
    
    DEFAULT_PD_RATE = 0.05 # 5% default rate

    if model is None:
        # print("Warning: PD Model not loaded, returning default PD.") # Reduce log noise
        return DEFAULT_PD_RATE # Default 5% probability if model failed to load

    if year_of_birth is None or not isinstance(year_of_birth, int):
        # print("Warning: Invalid year_of_birth, returning default PD.") # Reduce log noise
        return DEFAULT_PD_RATE # Default 5% if no valid YOB

    try:
        # Create DataFrame with the correct feature name (determined during model load)
        # No need for pandas if only one feature, numpy array is faster
        # X_new = pd.DataFrame({PD_MODEL_FEATURE_NAME: [year_of_birth]})
        X_new = np.array([[year_of_birth]]) # Create 2D numpy array for predict/predict_proba

        # Get probability from model
        # predict_proba returns [[prob_class_0, prob_class_1]]
        probability_class_1 = model.predict_proba(X_new)[0][1] # Probability of class 1 (default)

        # Return as a rate (0.0 to 1.0)
        return float(probability_class_1)

    except Exception as e:
        # Handle prediction exceptions
        print(f"Error during PD prediction for YOB {year_of_birth}: {str(e)}")
        return DEFAULT_PD_RATE # Default 5% probability on prediction error
