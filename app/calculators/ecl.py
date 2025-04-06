import pickle
import pandas as pd
from datetime import datetime
from decimal import Decimal
from typing import Optional, Tuple, List, Union, Dict, Any
from app.models import Client


def calculate_effective_interest_rate(loan_amount, monthly_installment, loan_term):
    """Calculate the effective interest rate using IRR (Internal Rate of Return)"""
    if not loan_amount or not monthly_installment or not loan_term or loan_term <= 0:
        return 0

    try:
        # Set up cash flows: initial loan amount (negative) followed by monthly payments
        cash_flows = [-loan_amount] + [monthly_installment] * loan_term
        # Calculate monthly EIR
        monthly_eir = np.irr(cash_flows)
        # Convert to annual rate
        annual_eir = (1 + monthly_eir) ** 12 - 1
        return annual_eir * 100  # Convert to percentage
    except:
        # Fallback calculation if IRR fails to converge
        return 0


def calculate_loss_given_default(
    loan: Union[Dict[str, Any], Any],
    client_securities: List[Union[Dict[str, Any], Any]],
) -> float:
    """
    Calculate the Loss Given Default (LGD) for a loan based on client's securities.
    Uses different calculations for cash and non-cash securities.

    For cash securities: LGD = ((Outstanding loan balance - collateral value) / Outstanding loan balance) × 100
    For non-cash securities: LGD = ((Outstanding loan balance - forced sale value) / Outstanding loan balance) × 100

    Args:
        loan: The loan object or dictionary
        client_securities: List of security objects linked to the client

    Returns:
        float: LGD value as a percentage (0-100)
    """
    # Default LGD if no securities or loan data is missing
    default_lgd = 65.0  # Industry average for unsecured loans

    # Extract outstanding loan balance
    if isinstance(loan, dict):
        outstanding_amount = float(loan.get("outstanding_loan_balance", 0))
    else:
        if (
            not loan
            or not hasattr(loan, "outstanding_loan_balance")
            or not loan.outstanding_loan_balance
        ):
            return default_lgd
        outstanding_amount = float(loan.outstanding_loan_balance)

    if outstanding_amount <= 0:
        return 0.0  # No loss if no outstanding amount

    # Total values for calculation
    total_cash_collateral = 0.0
    total_non_cash_forced_sale = 0.0

    if client_securities:
        for security in client_securities:
            # Extract security type and values
            if isinstance(security, dict):
                cash_or_non_cash = security.get("cash_or_non_cash", "non-cash")
                collateral_value = security.get("collateral_value", 0)
                forced_sale_value = security.get("forced_sale_value", 0)
            else:
                cash_or_non_cash = getattr(security, "cash_or_non_cash", "non-cash")
                collateral_value = getattr(security, "collateral_value", 0)
                forced_sale_value = getattr(security, "forced_sale_value", 0)

            # Convert to float if not None, otherwise set to 0
            collateral_value = float(collateral_value) if collateral_value else 0.0
            forced_sale_value = float(forced_sale_value) if forced_sale_value else 0.0

            # Process differently based on cash or non-cash security
            if cash_or_non_cash and cash_or_non_cash.lower() == "cash":
                # For cash securities, use collateral value
                total_cash_collateral += collateral_value
            else:
                # For non-cash securities, use forced sale value
                total_non_cash_forced_sale += forced_sale_value

    # Calculate the remaining balance after applying cash securities
    remaining_after_cash = outstanding_amount - total_cash_collateral

    # Calculate LGD based on the outstanding amount and recoverable values
    if remaining_after_cash <= 0:
        # Fully covered by cash securities
        lgd = 0.0
    else:
        # Apply non-cash securities to remaining balance
        final_remaining = remaining_after_cash - total_non_cash_forced_sale

        if final_remaining <= 0:
            # Fully covered by combined securities
            lgd = 0.0
        else:
            # Calculate LGD based on remaining balance
            lgd = (final_remaining / outstanding_amount) * 100.0

    # Apply floor and cap to LGD
    lgd = max(0.0, min(100.0, lgd))

    return lgd


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
        return 100  # If no original amount, assume 100% exposure

    original_amount = loan.loan_amount

    # Get effective interest rate (annual) and convert to monthly
    annual_rate = calculate_effective_interest_rate(
        loan_amount=loan.loan_amount,
        monthly_installment=loan.monthly_installment,
        loan_term=loan.loan_term,
    )
    monthly_rate = annual_rate / 12

    # Get loan term in months
    loan_term_months = loan.loan_term

    # Calculate months elapsed from loan issue date to reporting date
    issue_date = loan.loan_issue_date
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

    ead_percentage = (theoretical_balance / original_amount) * 100

    # Ensure EAD% is between 0 and 100
    return max(0, min(100, ead_percentage))


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



def calculate_probability_of_default(loan, db):
    """
    Calculate Probability of Default using the machine learning model based on customer age
    
    Parameters:
    - loan: Loan object from the database
    - db: SQLAlchemy database session
    
    Returns:
    - float: Probability of default as a percentage (0-100)
    """
    try:
        # Import here to avoid circular imports
        import numpy as np
        import pandas as pd
        import warnings
        
        # Suppress specific warnings
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
                Client.employee_id == loan.employee_id
            ).first()
            
            if not client or not client.date_of_birth:
                return 5.0  # Default 5% probability if client or DOB not found
            
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
            
            # Convert to percentage
            percentage = probability * 100
            
            return percentage
    except Exception as e:
        # Handle exceptions but maintain return type as float
        print(f"Error calculating probability of default: {str(e)}")
        return 5.0  # Default 5% probability on error
