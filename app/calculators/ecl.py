import pickle
import pandas as pd
from datetime import datetime
from decimal import Decimal
from typing import Optional, Tuple, List, Union, Dict, Any
from app.models import Client
import numpy as np
import math


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



def calculate_probability_of_default(employee_id,outstanding_loan_balance, start_date, selected_dt, end_date, arrears, db):
    try:
        # Import here to avoid circular imports
        import numpy as np
        import pandas as pd
        import warnings
        

        if outstanding_loan_balance >= 0:
            if start_date <=reporting_date and end_date>reporting_date:
                run_pd(employee_id, db)
            if start_date<reporting_date and end_date<reporting_date:
                if accumulated_arrears:
                    return 1.0
                else:
                    return "N/A"


        elif outstanding_loan_balance < 0:
            if arrears>0:
                if end_date<= reporting_date:
                    return 1.0
            else:
                return 0.0


        def run_pd (employee_id, db):

            #suppressing errors
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning, 
                                      message="X does not have valid feature names")
                warnings.filterwarnings("ignore", category=UserWarning, 
                                      message="Trying to unpickle estimator")
            
                # Load the pre-trained logistic regression model
                with open("app/ml_models/logistic_model.pkl", "rb") as file:
                    model = pickle.load(file)
                    
                # Get client associated with this loan's employee_id
                client = db.query(Client).filter(
                    Client.employee_id == employee_id
                ).first()
                
                if not client or not client.date_of_birth or not hasattr(client.date_of_birth, 'year'):
                    return 0  # Return 0 if client or DOB not found or invalid
                
                # Get year of birth from date of birth
                year_of_birth = client.date_of_birth.year
                
                # Get feature name from the model if available
                if hasattr(model, 'feature_names_in_'):
                    feature_name = model.feature_names_in_[0]  # Assuming only one feature
                else:
                    feature_name = 'year_of_birth'  # Default name if not found
                    
                # Create DataFrame with proper feature name
                X_new = pd.DataFrame({feature_name: [year_of_birth]})
                
                # Get prediction and probability from model
                prediction = model.predict(X_new)[0]
                probability = model.predict_proba(X_new)[0][1]  # Probability of default
                
                
                pd_dec = round(float(probability),2) #not percentage
                
                return pd_dec

    except Exception as e:
        # Handle exceptions but maintain return type as float
        print(f"Error calculating probability of default: {str(e)}")
        


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
