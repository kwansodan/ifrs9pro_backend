from decimal import Decimal, ROUND_HALF_UP


def calculate_ecl(outstanding_balance: Decimal, pd: Decimal, lgd: Decimal) -> Decimal:
    """
    Calculate the Expected Credit Loss (ECL) for a loan.

    Args:
        outstanding_balance (Decimal): The outstanding loan balance (EAD).
        pd (Decimal): Probability of Default (0 to 1).
        lgd (Decimal): Loss Given Default (0 to 1).

    Returns:
        Decimal: Expected Credit Loss amount.
    """
    if not (Decimal("0") <= pd <= Decimal("1")):
        raise ValueError("PD must be between 0 and 1")
    if not (Decimal("0") <= lgd <= Decimal("1")):
        raise ValueError("LGD must be between 0 and 1")

    # Exposure at Default (EAD) is the outstanding balance
    ead = outstanding_balance

    # Calculate ECL
    ecl = pd * lgd * ead

    # Round to 2 decimal places
    return ecl.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
