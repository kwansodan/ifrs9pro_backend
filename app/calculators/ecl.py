from decimal import Decimal


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


def calculate_loss_given_default(loan, client_securities):
    """
    Calculate the Loss Given Default (LGD) for a loan based on client's securities.

    LGD represents the percentage of exposure that would be lost in case of default
    after all recovery efforts and liquidation of collateral.

    Args:
        loan: The loan object
        client_securities: List of security objects linked to the client

    Returns:
        float: LGD value as a percentage (0-100)
    """
    # Default LGD if no securities or loan data is missing
    default_lgd = 65.0  # Industry average for unsecured loans

    if (
        not loan
        or not hasattr(loan, "outstanding_loan_balance")
        or not loan.outstanding_loan_balance
    ):
        return default_lgd

    # Total outstanding loan amount
    outstanding_amount = float(loan.outstanding_loan_balance)

    if outstanding_amount <= 0:
        return 0.0  # No loss if no outstanding amount

    # Calculate total recoverable value from securities
    total_recoverable = 0.0

    if client_securities:
        for security in client_securities:
            # Use forced sale value if available, otherwise apply a haircut to collateral value
            if security.forced_sale_value:
                recoverable = float(security.forced_sale_value)
            elif security.collateral_value:
                # Apply a standard haircut of 30% to the collateral value
                haircut = 0.7  # 30% reduction
                recoverable = float(security.collateral_value) * haircut
            else:
                recoverable = 0.0

            # Add to total recoverable value
            total_recoverable += recoverable

    # Calculate LGD based on the outstanding amount and recoverable value
    if total_recoverable >= outstanding_amount:
        # Full recovery possible
        lgd = 0.0
    else:
        # Partial recovery
        loss_amount = outstanding_amount - total_recoverable
        lgd = (loss_amount / outstanding_amount) * 100.0

    # Apply floor and cap to LGD
    lgd = max(0.0, min(100.0, lgd))

    return lgd


def calculate_probability_of_default(loan, ndia):
    """
    Calculate Probability of Default based on NDIA (Number of Days in Arrears)
    """
    # IFRS 9 staging
    if ndia < 120:  # Stage 1
        # Low risk
        pd = 5.0  # 5% (base rate for performing loans)
    elif ndia < 240:  # Stage 2
        # Significant increase in credit risk
        pd = 30.0  # 30%
    else:  # Stage 3
        # Credit impaired
        pd = 75.0  # 75%

    return pd


def calculate_exposure_at_default_percentage(loan, reporting_date):
    """
    Calculate Exposure at Default as a percentage
    EAD% = Outstanding Balance / Original Loan Amount
    """
    if not loan.loan_amount or loan.loan_amount == 0:
        return 100  # If no original amount, assume 100% exposure

    return (loan.outstanding_loan_balance / loan.loan_amount) * 100


def calculate_marginal_ecl(loan, pd, lgd, eir, reporting_date):
    """
    Calculate the marginal Expected Credit Loss (ECL) for a loan.

    Marginal ECL = EAD * PD * LGD * Discount Factor

    Args:
        loan: The loan object
        pd: Probability of Default as a percentage (0-100)
        lgd: Loss Given Default as a percentage (0-100)
        eir: Effective Interest Rate as a decimal (e.g., 0.12 for 12%)
        reporting_date: The date for which ECL is being calculated

    Returns:
        Decimal: The calculated marginal ECL amount
    """
    # Default to zero if loan balance is missing
    if (
        not loan
        or not hasattr(loan, "outstanding_loan_balance")
        or loan.outstanding_loan_balance is None
    ):
        return Decimal("0.0")

    # Get the outstanding balance as Exposure At Default (EAD)
    ead = loan.outstanding_loan_balance

    # Convert percentage values to decimals
    pd_decimal = Decimal(str(pd / 100.0))
    lgd_decimal = Decimal(str(lgd / 100.0))

    # Calculate time period for discount factor
    # Default to 1 year if maturity_period is missing
    if hasattr(loan, "maturity_period") and loan.maturity_period:
        maturity_date = loan.maturity_period
        days_to_maturity = (maturity_date - reporting_date).days
        years_to_maturity = max(0, days_to_maturity / 365.0)
    else:
        years_to_maturity = 1.0

    # Convert eir to Decimal
    eir_decimal = Decimal(str(eir))

    # Calculate present value factor
    # Use (1 + EIR)^-t formula
    if eir_decimal > Decimal("0"):
        pv_factor = Decimal("1.0") / (
            (Decimal("1.0") + eir_decimal) ** Decimal(str(years_to_maturity))
        )
    else:
        pv_factor = Decimal("1.0")

    # Calculate marginal ECL
    mecl = ead * pd_decimal * lgd_decimal * pv_factor

    return mecl
