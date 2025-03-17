import re
from decimal import Decimal
from typing import List, Dict, Optional, Union, Tuple
from app.models import Loan
from app.schemas import (
    ImpairmentConfig,
    ImpairmentCategory,
    ImpairmentCategoryData,
    ImpairmentSummaryMetrics,
    LocalImpairmentSummary,
)
from datetime import date


def parse_days_range(days_range: str) -> Tuple[int, Optional[int]]:
    """
    Parse a days range string like "0-30" or "360+"
    Returns (min_days, max_days) where max_days can be None for ranges like "360+"
    """
    # Check for "X+" format (no upper limit)
    plus_match = re.match(r"^(\d+)\+$", days_range)
    if plus_match:
        return int(plus_match.group(1)), None

    # Check for "X-Y" format (range)
    range_match = re.match(r"^(\d+)-(\d+)$", days_range)
    if range_match:
        return int(range_match.group(1)), int(range_match.group(2))

    # Invalid format
    raise ValueError(
        f"Invalid days range format: {days_range}. Expected '0-30' or '360+' format."
    )


def calculate_days_past_due(loan) -> int:
    """Calculate days past due for a loan"""
    if loan.ndia is not None:
        return loan.ndia
    elif (
        loan.accumulated_arrears is not None
        and loan.monthly_installment is not None
        and loan.monthly_installment > 0
    ):
        # Estimate days past due from accumulated_arrears
        months_past_due = loan.accumulated_arrears / loan.monthly_installment
        return int(months_past_due * 30)  # Approximate 30 days per month
    else:
        # Default to 0 if no data is available
        return 0


def calculate_loan_impairment(
    loans: List[Loan], config: ImpairmentConfig
) -> Tuple[List[Loan], List[Loan], List[Loan], List[Loan], List[Loan]]:
    """
    Categorize loans based on days past due according to the provided configuration
    Returns categorized loan lists: (current, olem, substandard, doubtful, loss)
    """
    # Parse day ranges from the configuration
    current_min, current_max = parse_days_range(config.current.days_range)
    olem_min, olem_max = parse_days_range(config.olem.days_range)
    substandard_min, substandard_max = parse_days_range(config.substandard.days_range)
    doubtful_min, doubtful_max = parse_days_range(config.doubtful.days_range)
    loss_min, loss_max = parse_days_range(config.loss.days_range)

    # Initialize category lists
    current_loans = []
    olem_loans = []
    substandard_loans = []
    doubtful_loans = []
    loss_loans = []

    # Categorize loans based on days past due
    for loan in loans:
        days_past_due = calculate_days_past_due(loan)

        # Categorize the loan based on frontend-provided config
        if current_min <= days_past_due <= (current_max or float("inf")):
            current_loans.append(loan)
        elif olem_min <= days_past_due <= (olem_max or float("inf")):
            olem_loans.append(loan)
        elif substandard_min <= days_past_due <= (substandard_max or float("inf")):
            substandard_loans.append(loan)
        elif doubtful_min <= days_past_due <= (doubtful_max or float("inf")):
            doubtful_loans.append(loan)
        elif loss_min <= days_past_due:
            loss_loans.append(loan)

    return current_loans, olem_loans, substandard_loans, doubtful_loans, loss_loans


def calculate_category_data(
    loans: List[Loan], category_config: ImpairmentCategory
) -> ImpairmentCategoryData:
    """Calculate impairment data for a loan category"""
    total_value = sum(loan.outstanding_loan_balance or 0 for loan in loans)
    provision = Decimal(total_value) * Decimal(category_config.rate / 100)

    return ImpairmentCategoryData(
        days_range=category_config.days_range,
        rate=category_config.rate,
        total_loan_value=total_value,
        provision_amount=provision,
    )


def calculate_impairment_summary(
    portfolio_id: int, loans: List[Loan], config: ImpairmentConfig, reporting_date: date
) -> LocalImpairmentSummary:
    """
    Calculate the complete impairment summary for a portfolio

    Args:
        portfolio_id: ID of the portfolio
        loans: List of loans in the portfolio
        config: Impairment configuration with day ranges and rates
        reporting_date: Date for the impairment calculation

    Returns:
        Complete impairment summary with category data and totals
    """
    # Categorize loans
    current_loans, olem_loans, substandard_loans, doubtful_loans, loss_loans = (
        calculate_loan_impairment(loans, config)
    )

    # Calculate data for each category
    current_data = calculate_category_data(current_loans, config.current)
    olem_data = calculate_category_data(olem_loans, config.olem)
    substandard_data = calculate_category_data(substandard_loans, config.substandard)
    doubtful_data = calculate_category_data(doubtful_loans, config.doubtful)
    loss_data = calculate_category_data(loss_loans, config.loss)

    # Calculate summary metrics
    total_loan_value = (
        current_data.total_loan_value
        + olem_data.total_loan_value
        + substandard_data.total_loan_value
        + doubtful_data.total_loan_value
        + loss_data.total_loan_value
    )
    total_loan_value = round(total_loan_value, 2)

    total_provision = (
        current_data.provision_amount
        + olem_data.provision_amount
        + substandard_data.provision_amount
        + doubtful_data.provision_amount
        + loss_data.provision_amount
    )
    total_provision = round(total_provision, 2)
    # Construct response
    return LocalImpairmentSummary(
        portfolio_id=portfolio_id,
        calculation_date=reporting_date.strftime("%Y-%m-%d"),
        current=current_data,
        olem=olem_data,
        substandard=substandard_data,
        doubtful=doubtful_data,
        loss=loss_data,
        summary_metrics=ImpairmentSummaryMetrics(
            total_loans=total_loan_value, total_provision=total_provision
        ),
    )
