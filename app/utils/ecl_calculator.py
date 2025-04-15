import numpy as np
import pickle
from datetime import datetime
from decimal import Decimal
from typing import Optional, Tuple, List, Union, Dict, Any
from dateutil.relativedelta import relativedelta
from calendar import monthrange
import pickle 
import logging

logger = logging.getLogger(__name__)

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

        import math

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
    Calculate Exposure at Default as a value (theoretical balance)

    Formula:
    Bt = P * ((1+r)^n - (1+r)^t)/((1+r)^n - 1)

    Where:
    Bt = Loan Balance at month t
    P = Original loan amount (Principal)
    r = Monthly interest rate (Annual rate/12)
    n = Total number of months in the loan term
    t = number of months from loan start to specified date

    EAD = Bt + Accumulated Arrears
    """
    if not loan.loan_amount or loan.loan_amount <= 0:
        return Decimal(0)  # If no original amount, assume 0 exposure

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


def calculate_marginal_ecl(loan, ead_value, pd, lgd):
    """
    Calculate the marginal Expected Credit Loss (ECL) for a loan.

    Marginal ECL = EAD * PD * LGD

    Args:
        loan: The loan object
        ead_value: Exposure at Default as a value (theoretical balance)
        pd: Probability of Default as a percentage (0-100)
        lgd: Loss Given Default as a percentage (0-100)

    Returns:
        Decimal: The calculated marginal ECL amount
    """
    # Convert percentage values to decimals
    pd_decimal = Decimal(str(pd / 100.0))
    lgd_decimal = Decimal(str(lgd / 100.0))

    # Calculate marginal ECL
    mecl = Decimal(str(ead_value)) * pd_decimal * lgd_decimal

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
            try:
                with open("app/ml_models/logistic_model.pkl", "rb") as file:
                    model = pickle.load(file)
            except FileNotFoundError:
                # Return a default value if model file is not found
                return 5.0
                
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



def get_amortization_schedule(
    loan_amount: float,
    loan_term: int,
    annual_interest_rate: float,
    monthly_installment: float,
    start_date: str,
    reporting_date: str,
    pd: float = None,
    db = None,
    loan = None
) -> Tuple[List[List], float, float]:
    """Generates amortization schedule and returns schedule, 12-month ECL and lifetime ECL.
    
    Args:
        loan_amount: The principal amount of the loan
        loan_term: The loan term in months
        annual_interest_rate: Annual interest rate as a percentage
        monthly_installment: Monthly payment amount
        start_date: Loan start date in format DD/MM/YYYY
        reporting_date: Reporting date in format DD/MM/YYYY
        pd: Probability of default as a percentage (0-100). If None, will be calculated using the loan object
        db: Database session (required if pd is None)
        loan: Loan object (required if pd is None)
        
    Returns:
        Tuple containing:
        - Amortization schedule as a list of lists
        - 12-month ECL
        - Lifetime ECL
    """

    def pv_at_month(pmt, rate, total_months, present_month):
        remaining_months = total_months - present_month
        return pmt / ((1 + rate) ** remaining_months)

    # If PD is not provided, calculate it using the loan object
    if pd is None:
        if loan is None or db is None:
            raise ValueError("If pd is not provided, both loan and db must be provided")
        pd = calculate_probability_of_default(loan, db)

    current_date = datetime.strptime(start_date, "%d/%m/%Y")
    monthly_rate = annual_interest_rate / 12 / 100
    balance = loan_amount
    schedule: List[List] = []

    # Use the actual PD instead of the placeholder
    ecl = balance * pd / 100  # Convert percentage to decimal
    pv_ecl = pv_at_month(ecl, monthly_rate, loan_term, 0)

    schedule.append(
        ["Month", "Date", "Closing Balance", "Principal", "Interest", "Gross Carrying Amount", "Exposure", "ECL", "PV of ECL"]
    )
    schedule.append(
        [0, current_date.strftime("%d/%m/%Y"), round(balance, 2), 0.0, 0.0, round(balance, 2), round(balance, 2), round(ecl, 2), round(pv_ecl, 2)]
    )

    for month in range(1, loan_term + 1):
        interest = balance * monthly_rate
        principal = max(0, min(monthly_installment, balance + interest) - interest)
        balance = max(0, balance - principal)

        ecl = balance * pd / 100  # Convert percentage to decimal
        pv_ecl = pv_at_month(ecl, monthly_rate, loan_term, month)

        current_date += relativedelta(months=1)
        schedule.append(
            [month, current_date.strftime("%d/%m/%Y"), round(balance, 2), round(principal, 2), round(interest, 2),
             round(balance, 2), round(balance, 2), round(ecl, 2), round(pv_ecl, 2)]
        )

    # --- Determine start index for ECL calculation based on reporting date ---
    def get_start_index(reporting_date_str: str) -> int:
        logger.info(f"get_start_index: Input reporting_date_str: {reporting_date_str}, type: {type(reporting_date_str)}")
        
        try:
            reporting_dt = datetime.strptime(reporting_date_str, "%d/%m/%Y")
            logger.info(f"Successfully parsed reporting date: {reporting_dt}")
        except Exception as e:
            logger.error(f"Error parsing reporting date: {str(e)}")
            try:
                # Try alternative format
                if isinstance(reporting_date_str, str) and "-" in reporting_date_str:
                    reporting_dt = datetime.strptime(reporting_date_str, "%Y-%m-%d")
                    logger.info(f"Successfully parsed reporting date using alternative format: {reporting_dt}")
                elif hasattr(reporting_date_str, 'strftime'):
                    # It's already a date/datetime object
                    reporting_dt = reporting_date_str
                    logger.info(f"reporting_date_str is already a date object: {reporting_dt}")
                else:
                    logger.error(f"Could not parse reporting date in any format: {reporting_date_str}")
                    raise ValueError(f"Invalid reporting date format: {reporting_date_str}")
            except Exception as e2:
                logger.error(f"Error in fallback date parsing: {str(e2)}")
                raise ValueError(f"Invalid reporting date format: {reporting_date_str}")
        
        last_day = monthrange(reporting_dt.year, reporting_dt.month)[1]
        logger.info(f"Last day of month: {last_day}, current day: {reporting_dt.day}")

        if reporting_dt.day != last_day:
            adjusted_date = (reporting_dt - relativedelta(months=1)).replace(day=1)
            logger.info(f"Adjusted date (not last day): {adjusted_date}")
        else:
            adjusted_date = reporting_dt.replace(day=1)
            logger.info(f"Adjusted date (last day): {adjusted_date}")

        reporting_month_str = adjusted_date.strftime("%m/%Y")
        logger.info(f"Looking for reporting month: {reporting_month_str} in schedule")
        
        # Log the first few rows of the schedule to see what we're working with
        logger.info(f"Schedule has {len(schedule)} rows")
        for i, row in enumerate(schedule[:min(5, len(schedule))]):
            logger.info(f"Schedule row {i}: {row}")
        
        for idx, row in enumerate(schedule[1:], start=1):  # Skip header
            try:
                logger.info(f"Checking row {idx}: date part is {row[1]}")
                if reporting_month_str in row[1]:
                    logger.info(f"Found matching month at index {idx}")
                    return idx
            except Exception as e:
                logger.error(f"Error checking row {idx}: {str(e)}")
                
        logger.error(f"Reporting month {reporting_month_str} not found in schedule")
        raise ValueError("Reporting month not found in schedule.")
    
    # --- Calculate ECLs ---
    def compute_pv(schedule, start_index, rate, months: int = None):
        total_pv = 0.0
        monthly_rate = rate / 12 / 100
        data_rows = schedule[start_index + 1:]  # future only

        if months:
            data_rows = data_rows[:months]

        for i, row in enumerate(data_rows, start=1):
            ecl = row[7]
            pv = ecl / ((1 + monthly_rate) ** i)
            total_pv += pv

        return round(total_pv, 2)

    start_index = get_start_index(reporting_date)
    ecl_12_month = compute_pv(schedule, start_index, annual_interest_rate, months=12)
    ecl_lifetime = compute_pv(schedule, start_index, annual_interest_rate)

    return schedule, ecl_12_month, ecl_lifetime


def get_ecl_by_stage(schedule, ecl_12_month, ecl_lifetime, stage):
    """
    Select the appropriate ECL value based on the loan's stage.
    
    Args:
        schedule: The amortization schedule from get_amortization_schedule
        ecl_12_month: The 12-month ECL value from get_amortization_schedule
        ecl_lifetime: The lifetime ECL value from get_amortization_schedule
        stage: The loan stage (1, 2, or 3)
        
    Returns:
        float: The appropriate ECL value based on stage
    """
    if stage == 1:
        return ecl_12_month
    else:  # Stage 2 or 3
        return ecl_lifetime
