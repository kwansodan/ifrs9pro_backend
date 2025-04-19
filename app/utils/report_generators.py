from sqlalchemy.orm import Session
from sqlalchemy import func, and_
import numpy as np
import pandas as pd
from decimal import Decimal
from datetime import date, datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from datetime import date
import time
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor
from app.models import (
    Portfolio,
    Loan,
    Client,
    Security,
    Guarantee,
    Report,
    CalculationResult,
    StagingResult
)
from app.utils.pdf_generator import create_report_pdf
from app.utils.excel_generator import create_report_excel as create_excel_file

from app.calculators.ecl import (
    calculate_effective_interest_rate_lender,
    calculate_exposure_at_default_percentage,
    calculate_probability_of_default,
    calculate_loss_given_default,
)

import psutil
import logging

def generate_collateral_summary(
    db: Session, portfolio_id: int, report_date: date
) -> Dict[str, Any]:
    """
    Generate a summary of collateral data for a portfolio.
    """
    # Get portfolio loans
    loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()

    # Get client IDs for these loans
    employee_ids = [loan.employee_id for loan in loans if loan.employee_id]

    # Get clients
    clients = (
        db.query(Client)
        .filter(Client.portfolio_id == portfolio_id)
        .filter(Client.employee_id.in_(employee_ids))
        .all()
    )

    # Map client IDs to their database IDs
    client_id_map = {client.employee_id: client.id for client in clients}

    # Get securities for these clients
    securities = (
        db.query(Security)
        .filter(Security.client_id.in_(list(client_id_map.values())))
        .all()
    )

    # Calculate security statistics
    total_security_value = sum(security.security_value or Decimal(0) for security in securities)
    average_security_value = total_security_value / len(securities) if securities else Decimal(0)

    # Count security types
    security_types = {}
    for security in securities:
        if security.security_type:
            security_types[security.security_type] = (
                security_types.get(security.security_type, Decimal(0)) + Decimal(1)
            )

    # Get top 10 most valuable securities
    top_securities = sorted(
        securities, key=lambda x: x.security_value or Decimal(0), reverse=True
    )[:10]

    top_securities_data = [
        {
            "id": security.id,
            "client_id": security.client_id,
            "security_type": security.security_type,
            "security_value": security.security_value,
            "description": security.description,
        }
        for security in top_securities
    ]

    # Calculate collateral coverage ratio
    total_loan_value = sum(loan.outstanding_loan_balance or Decimal(0) for loan in loans)
    collateral_coverage_ratio = (
        total_security_value / total_loan_value if total_loan_value > Decimal(0) else Decimal(0)
    )

    # Count clients with and without collateral
    clients_with_collateral = len(set(security.client_id for security in securities))
    clients_without_collateral = len(client_id_map) - clients_with_collateral

    return {
        "total_security_value": total_security_value,
        "average_security_value": average_security_value,
        "security_types": security_types,
        "top_securities": top_securities_data,
        "collateral_coverage_ratio": round(collateral_coverage_ratio, 2),
        "total_securities": len(securities),
        "clients_with_collateral": clients_with_collateral,
        "clients_without_collateral": clients_without_collateral,
        "reporting_date": report_date.isoformat(),
    }


def generate_guarantee_summary(
    db: Session, portfolio_id: int, report_date: date
) -> Dict[str, Any]:
    """
    Generate a summary of guarantee data for a portfolio.
    """
    # Get guarantees for the portfolio
    guarantees = (
        db.query(Guarantee).filter(Guarantee.portfolio_id == portfolio_id).all()
    )

    # Calculate guarantee statistics
    total_guarantee_value = sum(
        guarantee.pledged_amount or Decimal(0) for guarantee in guarantees
    )
    average_guarantee_value = (
        total_guarantee_value / len(guarantees) if guarantees else Decimal(0)
    )

    # Get loans for the portfolio
    loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
    total_loan_value = sum(loan.outstanding_loan_balance or Decimal(0) for loan in loans)

    # Calculate guarantee coverage ratio
    guarantee_coverage_ratio = (
        total_guarantee_value / total_loan_value if total_loan_value > Decimal(0) else Decimal(0)
    )

    # Get top guarantors by pledged amount
    top_guarantors = sorted(
        guarantees, key=lambda x: x.pledged_amount or Decimal(0), reverse=True
    )[:10]

    top_guarantors_data = [
        {
            "id": guarantee.id,
            "guarantor": guarantee.guarantor,
            "pledged_amount": guarantee.pledged_amount,
        }
        for guarantee in top_guarantors
    ]

    # Count guarantors by type if available
    guarantor_types = {}
    for guarantee in guarantees:
        if hasattr(guarantee, "guarantor_type") and guarantee.guarantor_type:
            guarantor_types[guarantee.guarantor_type] = (
                guarantor_types.get(guarantee.guarantor_type, Decimal(0)) + Decimal(1)
            )

    return {
        "total_guarantee_value": total_guarantee_value,
        "average_guarantee_value": average_guarantee_value,
        "guarantee_coverage_ratio": round(guarantee_coverage_ratio, 2),
        "total_guarantees": len(guarantees),
        "top_guarantors": top_guarantors_data,
        "guarantor_types": guarantor_types,
        "reporting_date": report_date.isoformat(),
    }


def generate_interest_rate_summary(
    db: Session, portfolio_id: int, report_date: date
) -> Dict[str, Any]:
    """
    Generate a summary of interest rates for a portfolio.
    """
    # Get loans for the portfolio
    loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()

    # Calculate Effective Interest Rates (EIR) for each loan
    loan_eirs = []
    for loan in loans:
        if loan.loan_amount and loan.monthly_installment and loan.loan_term:
            eir = calculate_effective_interest_rate_lender(
                loan_amount=Decimal(loan.loan_amount),
                administrative_fees=Decimal(loan.administrative_fees) if loan.administrative_fees else Decimal(0),
                loan_term=loan.loan_term,
                monthly_payment=Decimal(loan.monthly_installment),
            )
            loan_eirs.append((loan, eir))

    # Calculate EIR statistics
    all_eirs = [eir for _, eir in loan_eirs]
    if all_eirs:
        average_eir = sum(all_eirs) / len(all_eirs)
        min_eir = min(all_eirs)
        max_eir = max(all_eirs)
        median_eir = sorted(all_eirs)[len(all_eirs) // 2]
    else:
        average_eir = min_eir = max_eir = median_eir = Decimal(0)

    # Group loans by EIR ranges
    eir_ranges = {
        "0-5%": Decimal(0),
        "5-10%": Decimal(0),
        "10-15%": Decimal(0),
        "15-20%": Decimal(0),
        "20-25%": Decimal(0),
        "25-30%": Decimal(0),
        "30%+": Decimal(0),
    }

    for _, eir in loan_eirs:
        if eir < Decimal(0.05):
            eir_ranges["0-5%"] += Decimal(1)
        elif eir < Decimal(0.10):
            eir_ranges["5-10%"] += Decimal(1)
        elif eir < Decimal(0.15):
            eir_ranges["10-15%"] += Decimal(1)
        elif eir < Decimal(0.20):
            eir_ranges["15-20%"] += Decimal(1)
        elif eir < Decimal(0.25):
            eir_ranges["20-25%"] += Decimal(1)
        elif eir < Decimal(0.30):
            eir_ranges["25-30%"] += Decimal(1)
        else:
            eir_ranges["30%+"] += Decimal(1)

    # Group by loan type if available
    loan_type_eirs = {}
    for loan, eir in loan_eirs:
        if loan.loan_type:
            if loan.loan_type not in loan_type_eirs:
                loan_type_eirs[loan.loan_type] = []
            loan_type_eirs[loan.loan_type].append(eir)

    loan_type_avg_eirs = {
        loan_type: sum(eirs) / len(eirs) for loan_type, eirs in loan_type_eirs.items()
    }

    # Get top 10 highest EIR loans
    top_eir_loans = sorted(loan_eirs, key=lambda x: x[1], reverse=True)[:10]
    top_eir_loans_data = [
        {
            "loan_id": loan.id,
            "loan_no": loan.loan_no,
            "employee_id": loan.employee_id,
            "loan_amount": loan.loan_amount,
            "loan_term": loan.loan_term,
            "monthly_installment": loan.monthly_installment,
            "effective_interest_rate": round(eir * Decimal(100), 2),  # as percentage
        }
        for loan, eir in top_eir_loans
    ]

    return {
        "average_eir": round(average_eir * Decimal(100), 2),  # as percentage
        "min_eir": round(min_eir * Decimal(100), 2),
        "max_eir": round(max_eir * Decimal(100), 2),
        "median_eir": round(median_eir * Decimal(100), 2),
        "eir_distribution": eir_ranges,
        "loan_type_avg_eirs": {
            k: round(v * Decimal(100), 2) for k, v in loan_type_avg_eirs.items()
        },
        "top_eir_loans": top_eir_loans_data,
        "total_loans_analyzed": len(loan_eirs),
        "reporting_date": report_date.isoformat(),
    }


def generate_repayment_summary(
    db: Session, portfolio_id: int, report_date: date
) -> Dict[str, Any]:
    """
    Generate a summary of repayment data for a portfolio.
    """
    # Get loans for the portfolio
    loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()

    # Calculate repayment statistics
    total_principal_due = sum(loan.principal_due or Decimal(0) for loan in loans)
    total_interest_due = sum(loan.interest_due or Decimal(0) for loan in loans)
    total_due = sum(loan.total_due or Decimal(0) for loan in loans)

    total_principal_paid = sum(loan.principal_paid or Decimal(0) for loan in loans)
    total_interest_paid = sum(loan.interest_paid or Decimal(0) for loan in loans)
    total_paid = sum(loan.total_paid or Decimal(0) for loan in loans)

    # Calculate repayment ratios
    principal_repayment_ratio = (
        total_principal_paid / total_principal_due if total_principal_due > Decimal(0) else Decimal(0)
    )
    interest_repayment_ratio = (
        total_interest_paid / total_interest_due if total_interest_due > Decimal(0) else Decimal(0)
    )
    overall_repayment_ratio = total_paid / total_due if total_due > Decimal(0) else Decimal(0)

    # Group loans by status
    paid_loans = sum(1 for loan in loans if loan.paid is True)
    unpaid_loans = sum(1 for loan in loans if loan.paid is False)

    # Calculate delinquency statistics
    delinquent_loans = sum(1 for loan in loans if loan.ndia and loan.ndia > 0)
    delinquency_rate = delinquent_loans / len(loans) if loans else Decimal(0)

    # Group loans by NDIA ranges
    ndia_ranges = {
        "Current (0)": Decimal(0),
        "1-30 days": Decimal(0),
        "31-90 days": Decimal(0),
        "91-180 days": Decimal(0),
        "181-360 days": Decimal(0),
        "360+ days": Decimal(0),
    }

    for loan in loans:
        ndia = loan.ndia or Decimal(0)
        if ndia == Decimal(0):
            ndia_ranges["Current (0)"] += Decimal(1)
        elif ndia <= Decimal(30):
            ndia_ranges["1-30 days"] += Decimal(1)
        elif ndia <= Decimal(90):
            ndia_ranges["31-90 days"] += Decimal(1)
        elif ndia <= Decimal(180):
            ndia_ranges["91-180 days"] += Decimal(1)
        elif ndia <= Decimal(360):
            ndia_ranges["181-360 days"] += Decimal(1)
        else:
            ndia_ranges["360+ days"] += Decimal(1)

    # Top 10 loans with highest accumulated arrears
    top_arrears_loans = sorted(
        loans, key=lambda x: x.accumulated_arrears or Decimal(0), reverse=True
    )[:10]

    top_arrears_loans_data = [
        {
            "loan_id": loan.id,
            "loan_no": loan.loan_no,
            "employee_id": loan.employee_id,
            "accumulated_arrears": loan.accumulated_arrears,
            "ndia": loan.ndia,
            "outstanding_loan_balance": loan.outstanding_loan_balance,
        }
        for loan in top_arrears_loans
    ]

    return {
        "total_principal_due": total_principal_due,
        "total_interest_due": total_interest_due,
        "total_due": total_due,
        "total_principal_paid": total_principal_paid,
        "total_interest_paid": total_interest_paid,
        "total_paid": total_paid,
        "principal_repayment_ratio": round(principal_repayment_ratio, 2),
        "interest_repayment_ratio": round(interest_repayment_ratio, 2),
        "overall_repayment_ratio": round(overall_repayment_ratio, 2),
        "paid_loans": paid_loans,
        "unpaid_loans": unpaid_loans,
        "delinquent_loans": delinquent_loans,
        "delinquency_rate": round(delinquency_rate, 2),
        "ndia_distribution": ndia_ranges,
        "top_arrears_loans": top_arrears_loans_data,
        "total_loans": len(loans),
        "reporting_date": report_date.isoformat(),
    }


def generate_assumptions_summary(
    db: Session, portfolio_id: int, report_date: date
) -> Dict[str, Any]:
    """
    Generate a summary of assumptions used in calculations for a portfolio.
    """
    # Get portfolio
    portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()

    # Get all loans in the portfolio
    loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()

    # Calculate PD, LGD, and EAD assumptions
    avg_pd = Decimal(0)
    avg_lgd = Decimal(0)
    avg_ead = Decimal(0)

    # Get client IDs for these loans
    employee_ids = [loan.employee_id for loan in loans if loan.employee_id]

    # Get clients
    clients = (
        db.query(Client)
        .filter(Client.portfolio_id == portfolio_id)
        .filter(Client.employee_id.in_(employee_ids))
        .all()
    )

    # Map client IDs to their database IDs
    client_id_map = {client.employee_id: client.id for client in clients}

    client_securities = {}
    for employee_id in employee_ids:
        if employee_id in client_id_map:
            client_id = client_id_map[employee_id]
            securities = (
                db.query(Security).filter(Security.client_id == client_id).all()
            )
            client_securities[employee_id] = securities

    # Calculate PD, LGD, and EAD for each loan
    pd_values = []
    lgd_values = []
    ead_values = []

    for loan in loans:
        # Calculate PD
        ndia = loan.ndia or Decimal(0)
        pd = calculate_probability_of_default(loan, db)
        pd_values.append(pd)

        # Calculate LGD
        securities = client_securities.get(loan.employee_id, [])
        lgd = calculate_loss_given_default(loan, securities)
        lgd_values.append(lgd)

        # Calculate EAD
        ead = calculate_exposure_at_default_percentage(loan, report_date)
        ead_values.append(ead)

    # Calculate averages
    if pd_values:
        avg_pd = sum(pd_values) / len(pd_values)

    if lgd_values:
        avg_lgd = sum(lgd_values) / len(lgd_values)

    if ead_values:
        avg_ead = sum(ead_values) / len(ead_values)

    # Other assumptions
    macro_economic_factor = Decimal(1.0)  # Example value, could be adjusted based on economic conditions
    recovery_rate = Decimal(0.4)  # Example value, could be adjusted based on historical data

    # Based on portfolio type
    if portfolio:
        if portfolio.asset_type == "mortgage":
            recovery_rate = Decimal(0.7)  # Higher recovery rate for mortgage loans
        elif portfolio.asset_type == "unsecured":
            recovery_rate = Decimal(0.3)  # Lower recovery rate for unsecured loans

    # Group loans by NDIA ranges for PD curve
    ndia_pd_curve = {
        "0 days": Decimal(0.02),
        "1-30 days": Decimal(0.05),
        "31-90 days": Decimal(0.20),
        "91-180 days": Decimal(0.40),
        "181-360 days": Decimal(0.75),
        "360+ days": Decimal(0.99),
    }

    return {
        "average_pd": round(avg_pd, 2),
        "average_lgd": round(avg_lgd, 2),
        "average_ead": round(avg_ead, 2),
        "macro_economic_factor": macro_economic_factor,
        "recovery_rate": recovery_rate,
        "pd_curve": ndia_pd_curve,
        "asset_type": portfolio.asset_type if portfolio else "unknown",
        "customer_type": portfolio.customer_type if portfolio else "unknown",
        "total_loans_analyzed": len(loans),
        "reporting_date": report_date.isoformat(),
    }


def generate_amortised_loan_balances(
    db: Session, portfolio_id: int, report_date: date
) -> Dict[str, Any]:
    """
    Generate a report of amortised loan balances.
    Note: This report does not consider the BOG non-accrual rule.
    """
    # Get all loans in the portfolio
    loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()

    # Create summary statistics
    total_original_loan_amount = sum(loan.loan_amount or Decimal(0) for loan in loans)
    total_current_loan_balance = sum(
        loan.outstanding_loan_balance or Decimal(0) for loan in loans
    )
    total_amortisation = total_original_loan_amount - total_current_loan_balance

    # Calculate percentage amortised
    percent_amortised = (
        (total_amortisation / total_original_loan_amount * Decimal(100))
        if total_original_loan_amount > Decimal(0)
        else Decimal(0)
    )

    # Group loans by amortisation percentage
    amortisation_ranges = {
        "0-20%": Decimal(0),
        "21-40%": Decimal(0),
        "41-60%": Decimal(0),
        "61-80%": Decimal(0),
        "81-100%": Decimal(0),
    }

    for loan in loans:
        if (
            loan.loan_amount
            and loan.loan_amount > Decimal(0)
            and loan.outstanding_loan_balance is not None
        ):
            amortised_amount = loan.loan_amount - loan.outstanding_loan_balance
            amortised_percent = (amortised_amount / loan.loan_amount) * Decimal(100)

            if amortised_percent <= Decimal(20):
                amortisation_ranges["0-20%"] += Decimal(1)
            elif amortised_percent <= Decimal(40):
                amortisation_ranges["21-40%"] += Decimal(1)
            elif amortised_percent <= Decimal(60):
                amortisation_ranges["41-60%"] += Decimal(1)
            elif amortised_percent <= Decimal(80):
                amortisation_ranges["61-80%"] += Decimal(1)
            else:
                amortisation_ranges["81-100%"] += Decimal(1)

    # Calculate expected final amortisation dates
    loan_status = []
    for loan in loans:
        if loan.loan_term and loan.loan_issue_date and loan.outstanding_loan_balance:
            # Calculate expected end date
            expected_end_date = loan.loan_issue_date + timedelta(
                days=Decimal(30) * loan.loan_term
            )

            # Calculate days remaining
            if expected_end_date > report_date:
                days_remaining = (expected_end_date - report_date).days
            else:
                days_remaining = Decimal(0)

            # Calculate expected monthly amortisation
            monthly_amortisation = (
                loan.principal_due
                if loan.principal_due
                else (loan.loan_amount / loan.loan_term if loan.loan_term > Decimal(0) else Decimal(0))
            )

            loan_status.append(
                {
                    "loan_id": loan.id,
                    "loan_no": loan.loan_no,
                    "original_amount": loan.loan_amount,
                    "current_balance": loan.outstanding_loan_balance,
                    "amortised_amount": (
                        loan.loan_amount - loan.outstanding_loan_balance
                        if loan.loan_amount
                        else Decimal(0)
                    ),
                    "amortised_percent": round(
                        (
                            (
                                (loan.loan_amount - loan.outstanding_loan_balance)
                                / loan.loan_amount
                                * Decimal(100)
                            )
                            if loan.loan_amount and loan.loan_amount > Decimal(0)
                            else Decimal(0)
                        ),
                        2,
                    ),
                    "expected_end_date": expected_end_date.isoformat(),
                    "days_remaining": days_remaining,
                    "monthly_amortisation": monthly_amortisation,
                }
            )

    # Sort by amortised percentage (descending)
    loan_status = sorted(
        loan_status, key=lambda x: x["amortised_percent"], reverse=True
    )

    return {
        "total_original_loan_amount": total_original_loan_amount,
        "total_current_loan_balance": total_current_loan_balance,
        "total_amortisation": total_amortisation,
        "percent_amortised": round(percent_amortised, 2),
        "amortisation_distribution": amortisation_ranges,
        "loan_status": loan_status[:50],  # Limit to top 50 loans
        "total_loans_analyzed": len(loans),
        "reporting_date": report_date.isoformat(),
        "note": "This report does not consider the BOG non-accrual rule.",
    }


def generate_probability_default_report(
    db: Session, portfolio_id: int, report_date: date
) -> Dict[str, Any]:
    """
    Generate a report on probability of default for the portfolio.
    """
    # Get all loans in the portfolio
    loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()

    # Calculate PD for each loan
    loan_pds = []
    pd_values = []

    for loan in loans:
        ndia = loan.ndia or Decimal(0)
        pd = calculate_probability_of_default(loan, db)
        pd_values.append(pd)

        loan_pds.append(
            {
                "loan_id": loan.id,
                "loan_no": loan.loan_no,
                "employee_id": loan.employee_id,
                "ndia": ndia,
                "pd": round(pd, 4),
                "outstanding_balance": loan.outstanding_loan_balance,
            }
        )

    # Calculate PD statistics
    if pd_values:
        avg_pd = sum(pd_values) / len(pd_values)
        min_pd = min(pd_values)
        max_pd = max(pd_values)
        median_pd = sorted(pd_values)[len(pd_values) // 2]
    else:
        avg_pd = min_pd = max_pd = median_pd = Decimal(0)

    # Group loans by PD ranges
    pd_ranges = {
        "0-10%": Decimal(0),
        "11-25%": Decimal(0),
        "26-50%": Decimal(0),
        "51-75%": Decimal(0),
        "76-90%": Decimal(0),
        "91-100%": Decimal(0),
    }

    weighted_pd_sum = Decimal(0)
    total_outstanding_balance = Decimal(0)

    for loan_pd in loan_pds:
        pd_percent = loan_pd["pd"] * Decimal(100)
        outstanding_balance = loan_pd["outstanding_balance"] or Decimal(0)

        # Add to weighted PD calculation
        weighted_pd_sum += loan_pd["pd"] * outstanding_balance
        total_outstanding_balance += outstanding_balance

        if pd_percent <= Decimal(10):
            pd_ranges["0-10%"] += Decimal(1)
        elif pd_percent <= Decimal(25):
            pd_ranges["11-25%"] += Decimal(1)
        elif pd_percent <= Decimal(50):
            pd_ranges["26-50%"] += Decimal(1)
        elif pd_percent <= Decimal(75):
            pd_ranges["51-75%"] += Decimal(1)
        elif pd_percent <= Decimal(90):
            pd_ranges["76-90%"] += Decimal(1)
        else:
            pd_ranges["91-100%"] += Decimal(1)

    # Calculate portfolio weighted PD
    weighted_portfolio_pd = (
        weighted_pd_sum / total_outstanding_balance
        if total_outstanding_balance > Decimal(0)
        else Decimal(0)
    )

    # Sort loans by PD (descending)
    loan_pds = sorted(loan_pds, key=lambda x: x["pd"], reverse=True)

    return {
        "average_pd": round(avg_pd, 4),
        "min_pd": round(min_pd, 4),
        "max_pd": round(max_pd, 4),
        "median_pd": round(median_pd, 4),
        "weighted_portfolio_pd": round(weighted_portfolio_pd, 4),
        "pd_distribution": pd_ranges,
        "high_risk_loans": loan_pds[:25],  # Top 25 highest PD loans
        "total_loans_analyzed": len(loans),
        "reporting_date": report_date.isoformat(),
    }


def generate_exposure_default_report(
    db: Session, portfolio_id: int, report_date: date
) -> Dict[str, Any]:
    """
    Generate a report on exposure at default for the portfolio.
    """
    # Get all loans in the portfolio
    loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()

    # Calculate EAD for each loan
    loan_eads = []
    ead_values = []

    for loan in loans:
        ead_percentage = calculate_exposure_at_default_percentage(loan, report_date)
        ead_values.append(ead_percentage)

        # Calculate actual EAD amount
        ead_amount = (
            loan.outstanding_loan_balance * ead_percentage
            if loan.outstanding_loan_balance
            else Decimal(0)
        )

        loan_eads.append(
            {
                "loan_id": loan.id,
                "loan_no": loan.loan_no,
                "employee_id": loan.employee_id,
                "outstanding_balance": loan.outstanding_loan_balance,
                "ead_percentage": round(ead_percentage, 4),
                "ead_amount": ead_amount,
            }
        )

    # Calculate EAD statistics
    if ead_values:
        avg_ead = sum(ead_values) / len(ead_values)
        min_ead = min(ead_values)
        max_ead = max(ead_values)
        median_ead = sorted(ead_values)[len(ead_values) // 2]
    else:
        avg_ead = min_ead = max_ead = median_ead = Decimal(0)

    # Calculate total EAD
    total_outstanding_balance = sum(
        loan.outstanding_loan_balance or Decimal(0) for loan in loans
    )
    total_ead = sum(
        (loan.outstanding_loan_balance or Decimal(0))
        * calculate_exposure_at_default_percentage(loan, report_date)
        for loan in loans
    )

    # Group loans by EAD percentage ranges
    ead_ranges = {
        "0-80%": Decimal(0),
        "81-90%": Decimal(0),
        "91-95%": Decimal(0),
        "96-99%": Decimal(0),
        "100%": Decimal(0),
        "100%+": Decimal(0),
    }

    for loan_ead in loan_eads:
        ead_percentage = loan_ead["ead_percentage"] * Decimal(100)

        if ead_percentage <= Decimal(80):
            ead_ranges["0-80%"] += Decimal(1)
        elif ead_percentage <= Decimal(90):
            ead_ranges["81-90%"] += Decimal(1)
        elif ead_percentage <= Decimal(95):
            ead_ranges["91-95%"] += Decimal(1)
        elif ead_percentage <= Decimal(99):
            ead_ranges["96-99%"] += Decimal(1)
        elif ead_percentage <= Decimal(100):
            ead_ranges["100%"] += Decimal(1)
        else:
            ead_ranges["100%+"] += Decimal(1)

    # Sort loans by EAD amount (descending)
    loan_eads = sorted(loan_eads, key=lambda x: x["ead_amount"], reverse=True)

    return {
        "average_ead_percentage": round(avg_ead, 4),
        "min_ead_percentage": round(min_ead, 4),
        "max_ead_percentage": round(max_ead, 4),
        "median_ead_percentage": round(median_ead, 4),
        "total_outstanding_balance": total_outstanding_balance,
        "total_ead": total_ead,
        "ead_to_outstanding_ratio": round(
            (
                total_ead / total_outstanding_balance
                if total_outstanding_balance > Decimal(0)
                else Decimal(0)
            ),
            4,
        ),
        "ead_distribution": ead_ranges,
        "highest_exposure_loans": loan_eads[:25],  # Top 25 highest exposure loans
        "total_loans_analyzed": len(loans),
        "reporting_date": report_date.isoformat(),
    }


def generate_loss_given_default_report(
    db: Session, portfolio_id: int, report_date: date
) -> Dict[str, Any]:
    """
    Generate a report on loss given default for the portfolio.
    """
    # Get all loans in the portfolio
    loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()

    # Get client IDs for these loans
    employee_ids = [loan.employee_id for loan in loans if loan.employee_id]

    # Get clients
    clients = (
        db.query(Client)
        .filter(Client.portfolio_id == portfolio_id)
        .filter(Client.employee_id.in_(employee_ids))
        .all()
    )

    # Map client IDs to their database IDs
    client_id_map = {client.employee_id: client.id for client in clients}

    # Get securities for all clients
    client_securities = {}
    for employee_id in employee_ids:
        if employee_id in client_id_map:
            client_id = client_id_map[employee_id]
            securities = (
                db.query(Security).filter(Security.client_id == client_id).all()
            )
            client_securities[employee_id] = securities

    # Calculate LGD for each loan
    loan_lgds = []
    lgd_values = []

    for loan in loans:
        securities = client_securities.get(loan.employee_id, [])
        lgd = calculate_loss_given_default(loan, securities)
        lgd_values.append(lgd)

        # Calculate expected loss amount
        expected_loss = (
            Decimal(loan.outstanding_loan_balance) * Decimal(lgd)
            if loan.outstanding_loan_balance
            else Decimal(0)
        )

        # Calculate security value
        security_value = sum(security.security_value or Decimal(0) for security in securities)

        loan_lgds.append(
            {
                "loan_id": loan.id,
                "loan_no": loan.loan_no,
                "employee_id": loan.employee_id,
                "outstanding_balance": loan.outstanding_loan_balance,
                "security_value": security_value,
                "lgd": round(lgd, 4),
                "expected_loss": expected_loss,
            }
        )

    # Calculate LGD statistics
    if lgd_values:
        avg_lgd = sum(lgd_values) / len(lgd_values)
        min_lgd = min(lgd_values)
        max_lgd = max(lgd_values)
        median_lgd = sorted(lgd_values)[len(lgd_values) // 2]
    else:
        avg_lgd = min_lgd = max_lgd = median_lgd = Decimal(0)

    # Calculate total expected loss
    total_outstanding_balance = sum(
        loan.outstanding_loan_balance or Decimal(0) for loan in loans
    )
    total_expected_loss = sum(
        Decimal(loan.outstanding_loan_balance or Decimal(0))
        * Decimal(
            calculate_loss_given_default(
                loan, client_securities.get(loan.employee_id, [])
            )
        )
        for loan in loans
    )

    # Group loans by LGD ranges
    lgd_ranges = {"0-20%": Decimal(0), "21-40%": Decimal(0), "41-60%": Decimal(0), "61-80%": Decimal(0), "81-100%": Decimal(0)}

    for loan_lgd in loan_lgds:
        lgd_percentage = loan_lgd["lgd"] * Decimal(100)

        if lgd_percentage <= Decimal(20):
            lgd_ranges["0-20%"] += Decimal(1)
        elif lgd_percentage <= Decimal(40):
            lgd_ranges["21-40%"] += Decimal(1)
        elif lgd_percentage <= Decimal(60):
            lgd_ranges["41-60%"] += Decimal(1)
        elif lgd_percentage <= Decimal(80):
            lgd_ranges["61-80%"] += Decimal(1)
        else:
            lgd_ranges["81-100%"] += Decimal(1)

    # Sort loans by expected loss (descending)
    loan_lgds = sorted(loan_lgds, key=lambda x: x["expected_loss"], reverse=True)

    return {
        "average_lgd": round(avg_lgd, 4),
        "min_lgd": round(min_lgd, 4),
        "max_lgd": round(max_lgd, 4),
        "median_lgd": round(median_lgd, 4),
        "total_outstanding_balance": total_outstanding_balance,
        "total_expected_loss": total_expected_loss,
        "loss_to_outstanding_ratio": round(
            (
                total_expected_loss / total_outstanding_balance
                if total_outstanding_balance > Decimal(0)
                else Decimal(0)
            ),
            4,
        ),
        "lgd_distribution": lgd_ranges,
        "highest_loss_loans": loan_lgds[:25],  # Top 25 highest loss loans
        "total_loans_analyzed": len(loans),
        "reporting_date": report_date.isoformat(),
    }


def generate_ecl_detailed_report(
    db: Session, portfolio_id: int, report_date: date
) -> Dict[str, Any]:
    """
    Generate a detailed ECL report for a portfolio.
    
    This populates the ECL detailed report template with:
    - B3: Report date
    - B4: Report run date (current date)
    - B6: Report description
    - B9: Total exposure at default
    - B10: Total loss given default
    - B12: Total ECL
    - Rows 15+: Loan details with ECL calculations
    
    Memory-optimized version for servers with limited memory (2GB).
    Includes all loans while minimizing memory usage.
    """
    start_time = time.time()
    process = psutil.Process()
    logging.info(f"[MEM] Start generate_ecl_detailed_report: {process.memory_info().rss / 1024 ** 2:.2f} MB")
    print(f"Starting ECL detailed report for portfolio {portfolio_id}")
    
    # Get the latest ECL calculation result
    ecl_calculation = db.query(CalculationResult).filter(
        CalculationResult.portfolio_id == portfolio_id,
        CalculationResult.calculation_type == "ecl"
    ).order_by(CalculationResult.created_at.desc()).first()
    
    # If no calculation exists, return empty structure
    if not ecl_calculation:
        return {
            "portfolio_id": portfolio_id,
            "report_date": report_date.strftime("%Y-%m-%d"),
            "report_type": "ecl_detailed_report",
            "report_run_date": datetime.now().strftime("%Y-%m-%d"),
            "description": "ECL Detailed Report - No calculations available",
            "total_ead": Decimal(0),
            "total_lgd": Decimal(0),
            "total_ecl": Decimal(0),
        }
    
    # OPTIMIZATION 1: Get total loan count without loading all loans
    total_loan_count = db.query(func.count(Loan.id)).filter(Loan.portfolio_id == portfolio_id).scalar()
    print(f"Portfolio has {total_loan_count} loans to process")
    
    # OPTIMIZATION 2: Preload all client data in a single query
    print("Preloading client data...")
    client_map = {}
    # Process in batches to avoid memory issues with very large datasets
    client_batch_size = 5000
    for offset in range(0, total_loan_count, client_batch_size):
        # Get employee IDs for this batch
        employee_ids_subq = db.query(Loan.employee_id).filter(
            Loan.portfolio_id == portfolio_id
        ).distinct().offset(offset).limit(client_batch_size).subquery()
        
        # Get clients in a single query
        clients = db.query(Client).filter(
            Client.employee_id.in_(employee_ids_subq)
        ).all()
        
        # Build client map
        for client in clients:
            if client.employee_id:
                name = f"{client.last_name or ''} {client.other_names or ''}".strip()
                client_map[client.employee_id] = name if name else "Unknown"
    logging.info(f"[MEM] After client preload: {process.memory_info().rss / 1024 ** 2:.2f} MB")
    
    # OPTIMIZATION 3: Preload staging data with O(1) lookup
    print("Preloading staging data...")
    latest_staging = (
        db.query(StagingResult)
        .filter(
            StagingResult.portfolio_id == portfolio_id,
            StagingResult.staging_type == "ecl"
        )
        .order_by(StagingResult.created_at.desc())
        .first()
    )
    
    # Create loan_id to stage mapping for O(1) lookups
    loan_stage_map = {}
    if latest_staging and latest_staging.result_summary:
        staging_data = []
        
        if "staging_data" in latest_staging.result_summary:
            staging_data = latest_staging.result_summary.get("staging_data", [])
        elif "loans" in latest_staging.result_summary:
            staging_data = latest_staging.result_summary.get("loans", [])
            
        # Convert to dictionary for O(1) lookups
        for stage_info in staging_data:
            loan_id = stage_info.get("loan_id")
            stage = stage_info.get("stage")
            if loan_id and stage:
                loan_stage_map[loan_id] = stage
    logging.info(f"[MEM] After staging preload: {process.memory_info().rss / 1024 ** 2:.2f} MB")
    
    # OPTIMIZATION 4: Preload all securities data with a join
    print("Preloading securities data...")
    security_map = {}
    # Process in batches to avoid memory issues with very large datasets
    security_batch_size = 5000
    for offset in range(0, total_loan_count, security_batch_size):
        # Get employee IDs for this batch
        employee_ids_subq = db.query(Loan.employee_id).filter(
            Loan.portfolio_id == portfolio_id
        ).distinct().offset(offset).limit(security_batch_size).subquery()
        
        # Get securities with client data in a single query
        securities_with_clients = (
            db.query(Security, Client.employee_id)
            .join(Client, Security.client_id == Client.id)
            .filter(Client.employee_id.in_(employee_ids_subq))
            .all()
        )
        
        # Group securities by employee_id for O(1) lookup
        for security, employee_id in securities_with_clients:
            if employee_id:
                if employee_id not in security_map:
                    security_map[employee_id] = []
                security_map[employee_id].append(security)
    logging.info(f"[MEM] After securities preload: {process.memory_info().rss / 1024 ** 2:.2f} MB")
    
    # OPTIMIZATION 5: Process loans in larger batches
    batch_size = 2000  # Larger batch size for better throughput
    
    # Calculate number of batches
    num_batches = (total_loan_count + batch_size - 1) // batch_size
    print(f"Processing {num_batches} batches of {batch_size} loans each")
    
    # OPTIMIZATION 6: Stream process with running totals
    # Initialize totals
    total_ead = 0.0
    total_lgd = 0.0
    total_ecl = 0.0
    
    # OPTIMIZATION 7: Use a streaming JSON writer to avoid memory issues
    import tempfile
    import json
    import os
    
    # Create a temporary file to store loan data
    temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.json')
    temp_file_path = temp_file.name
    print(f"Using temporary file for loan data: {temp_file_path}")
    
    # Write the beginning of a JSON array
    temp_file.write('[\n')
    first_loan = True
    
    # Process loans in batches
    for offset in range(0, total_loan_count, batch_size):
        batch_start_time = time.time()
        print(f"Processing batch {offset//batch_size + 1}/{num_batches}")
        
        # Fetch batch of loans directly from database
        loan_batch = db.query(Loan).filter(
            Loan.portfolio_id == portfolio_id
        ).order_by(Loan.id).offset(offset).limit(batch_size).all()
        
        # OPTIMIZATION 8: Parallel processing for independent calculations
        from concurrent.futures import ThreadPoolExecutor
        
        def process_loan(loan):
            try:
                # Convert to float early to reduce decimal overhead
                loan_amount = float(loan.loan_amount) if loan.loan_amount else 0.0
                admin_fees = float(loan.administrative_fees) if loan.administrative_fees else 0.0
                loan_term = int(loan.loan_term) if loan.loan_term else 0
                monthly_payment = float(loan.monthly_installment) if loan.monthly_installment else 0.0
                outstanding_balance = float(loan.outstanding_loan_balance) if loan.outstanding_loan_balance else 0.0
                
                # Get stage using O(1) lookup
                stage = loan_stage_map.get(loan.id, "Stage 1")  # Default to Stage 1
                
                # Get securities using O(1) lookup
                securities = security_map.get(loan.employee_id, [])
                
                # Calculate values
                pd_value = calculate_probability_of_default(loan, db)
                lgd = calculate_loss_given_default(loan, securities)
                ead = calculate_exposure_at_default_percentage(loan, report_date)
                
                # Calculate EIR
                eir = calculate_effective_interest_rate_lender(
                    loan_amount=loan_amount,
                    administrative_fees=admin_fees,
                    loan_term=loan_term,
                    monthly_payment=monthly_payment
                )
                
                # Calculate ECL
                ecl = float(ead) * float(pd_value) * float(lgd) / 100.0
                
                # Get client name using preloaded map
                client_name = client_map.get(loan.employee_id, "Unknown")
                
                # Create loan entry
                loan_entry = {
                    "loan_id": loan.id,
                    "employee_id": loan.employee_id,
                    "employee_name": client_name,
                    "loan_value": str(loan.loan_amount) if loan.loan_amount else "0",
                    "outstanding_loan_balance": str(loan.outstanding_loan_balance) if loan.outstanding_loan_balance else "0",
                    "accumulated_arrears": str(loan.accumulated_arrears or Decimal(0)),
                    "ndia": str(loan.ndia or Decimal(0)),
                    "stage": stage,
                    "ead": str(ead),
                    "lgd": str(lgd),
                    "eir": str(eir),
                    "pd": str(pd_value),
                    "ecl": str(ecl)
                }
                
                return loan_entry, {
                    "ead": float(ead),
                    "lgd": float(lgd) * outstanding_balance,
                    "ecl": ecl
                }
                
            except Exception as e:
                print(f"Error processing loan {loan.id}: {str(e)}")
                return None, None
        
        # Process loans in parallel
        max_workers = min(8, os.cpu_count() or 4)  # Use at most 8 workers or number of CPU cores
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(process_loan, loan_batch))
        
        # Write results to file and update totals
        for loan_entry, totals in results:
            if loan_entry and totals:
                # Update totals
                total_ead += totals["ead"]
                total_lgd += totals["lgd"]
                total_ecl += totals["ecl"]
                
                # Write loan entry to temporary file
                if not first_loan:
                    temp_file.write(',\n')
                first_loan = False
                json.dump(loan_entry, temp_file)
    logging.info(f"[MEM] After loan batch processing: {process.memory_info().rss / 1024 ** 2:.2f} MB")
    
    # Write the end of the JSON array
    temp_file.write('\n]')
    temp_file.close()
    
    # OPTIMIZATION 9: Create a streaming iterator for the Excel generator
    class StreamingLoanDataIterator:
        def __init__(self, file_path):
            self.file_path = file_path
            self._load_metadata()
            
        def _load_metadata(self):
            """Just load the metadata to get the count without loading all loans"""
            import json
            with open(self.file_path, 'r') as f:
                # Read first line to check if it's an empty array
                first_line = f.readline().strip()
                if first_line == '[' and f.readline().strip() == ']':
                    self.count = 0
                else:
                    # Count the number of lines that contain loan data (excluding first and last lines)
                    f.seek(0)
                    content = f.read()
                    self.count = content.count('\n') - 1
                    if self.count < 0:
                        self.count = 0
            
        def __iter__(self):
            import json
            with open(self.file_path, 'r') as f:
                # Skip the opening bracket
                f.readline()
                
                # Read each line (loan entry)
                for line in f:
                    line = line.strip()
                    if line.endswith(','):
                        line = line[:-1]  # Remove trailing comma
                    if line and line != ']':  # Skip empty lines and closing bracket
                        try:
                            yield json.loads(line)
                        except json.JSONDecodeError:
                            print(f"Error decoding JSON: {line}")
                
        def __len__(self):
            return self.count
    
    # Create the report data structure
    report_data = {
        "portfolio_id": portfolio_id,
        "report_date": report_date.strftime("%Y-%m-%d"),
        "report_type": "ecl_detailed_report",
        "report_run_date": datetime.now().strftime("%Y-%m-%d"),
        "description": "ECL Detailed Report",
        "total_ead": total_ead,
        "total_lgd": total_lgd,
        "total_ecl": total_ecl,
    }
    
    # Generate Excel file and include base64 in report data
    try:
        portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
        if portfolio:
            # Prepare a copy of report_data and add loans as a list for Excel only
            excel_report_data = dict(report_data)
            all_loans = list(StreamingLoanDataIterator(temp_file_path))
            excel_report_data["loans"] = all_loans
            
            excel_bytes = create_excel_file(
                portfolio_name=portfolio.name,
                report_type="ecl_detailed_report",
                report_date=report_date,
                report_data=excel_report_data
            )
            import base64
            excel_base64 = base64.b64encode(excel_bytes.getvalue()).decode('utf-8')
            report_data["file"] = excel_base64
            print("Added base64-encoded Excel file to report data")
    except Exception as e:
        print(f"Error generating Excel file for base64 encoding: {str(e)}")
        # Continue without the base64 file
    
    logging.info(f"[MEM] End generate_ecl_detailed_report: {process.memory_info().rss / 1024 ** 2:.2f} MB")
    print(f"ECL detailed report generated in {time.time() - start_time:.2f} seconds")
    return report_data


def generate_ecl_report_summarised(
    db: Session, portfolio_id: int, report_date: date
) -> Dict[str, Any]:
    """
    Generate a summarised ECL report for a portfolio.
    
    This report provides a summary of ECL calculations across all stages.
    """
    # Get the portfolio
    portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
    if not portfolio:
        raise ValueError(f"Portfolio with ID {portfolio_id} not found")
    
    # Get the latest ECL calculation
    latest_calculation = (
        db.query(CalculationResult)
        .filter(
            CalculationResult.portfolio_id == portfolio_id,
            CalculationResult.calculation_type == "ecl"
        )
        .order_by(CalculationResult.created_at.desc())
        .first()
    )
    
    if not latest_calculation:
        raise ValueError(f"No ECL calculation found for portfolio {portfolio_id}")
    
    # Get calculation summary data
    calculation_summary = latest_calculation.result_summary
    
    # Extract data for each stage
    stage_1_data = calculation_summary.get("Stage 1", {})
    stage_2_data = calculation_summary.get("Stage 2", {})
    stage_3_data = calculation_summary.get("Stage 3", {})
    
    # Extract loan values
    stage_1_loan_value = stage_1_data.get("total_loan_value", 0)
    stage_2_loan_value = stage_2_data.get("total_loan_value", 0)
    stage_3_loan_value = stage_3_data.get("total_loan_value", 0)
    total_loan_value = stage_1_loan_value + stage_2_loan_value + stage_3_loan_value
    
    # Extract outstanding loan balances (same as loan value in this context)
    stage_1_outstanding = stage_1_loan_value
    stage_2_outstanding = stage_2_loan_value
    stage_3_outstanding = stage_3_loan_value
    total_outstanding = stage_1_outstanding + stage_2_outstanding + stage_3_outstanding
    
    # Extract ECL amounts
    stage_1_ecl = stage_1_data.get("provision_amount", 0)
    stage_2_ecl = stage_2_data.get("provision_amount", 0)
    stage_3_ecl = stage_3_data.get("provision_amount", 0)
    total_ecl = stage_1_ecl + stage_2_ecl + stage_3_ecl
    
    # Extract loan counts
    stage_1_count = stage_1_data.get("num_loans", 0)
    stage_2_count = stage_2_data.get("num_loans", 0)
    stage_3_count = stage_3_data.get("num_loans", 0)
    total_loans = stage_1_count + stage_2_count + stage_3_count
    
    # Create the report data structure
    return {
        "portfolio_name": portfolio.name,
        "description": f"ECL Summarised Report for {portfolio.name}",
        "report_date": report_date,
        "report_run_date": datetime.now().date(),
        "stage_1": {
            "loan_value": stage_1_loan_value,
            "outstanding_balance": stage_1_outstanding,
            "ecl": stage_1_ecl,
            "num_loans": stage_1_count
        },
        "stage_2": {
            "loan_value": stage_2_loan_value,
            "outstanding_balance": stage_2_outstanding,
            "ecl": stage_2_ecl,
            "num_loans": stage_2_count
        },
        "stage_3": {
            "loan_value": stage_3_loan_value,
            "outstanding_balance": stage_3_outstanding,
            "ecl": stage_3_ecl,
            "num_loans": stage_3_count
        },
        "total": {
            "loan_value": total_loan_value,
            "outstanding_balance": total_outstanding,
            "ecl": total_ecl,
            "num_loans": total_loans
        }
    }


def generate_local_impairment_details_report(
    db: Session, portfolio_id: int, report_date: date
) -> Dict[str, Any]:
    """
    Generate a detailed report of local impairment calculations for a portfolio.
    Memory-optimized version for servers with limited memory (2GB).
    Includes all loans while minimizing memory usage.
    
    Args:
        db: Database session
        portfolio_id: ID of the portfolio
        report_date: Date of the report
        
    Returns:
        Dict containing the report data
    """
    start_time = time.time()
    process = psutil.Process()
    logging.info(f"[MEM] Start generate_local_impairment_details_report: {process.memory_info().rss / 1024 ** 2:.2f} MB")
    print(f"Starting local impairment details report for portfolio {portfolio_id}")
    
    # Get the portfolio
    portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
    if not portfolio:
        raise ValueError(f"Portfolio with ID {portfolio_id} not found")
    
    # Get the latest local impairment calculation
    latest_calculation = (
        db.query(CalculationResult)
        .filter(
            CalculationResult.portfolio_id == portfolio_id,
            CalculationResult.calculation_type == "local_impairment"
        )
        .order_by(CalculationResult.created_at.desc())
        .first()
    )
    
    if not latest_calculation:
        raise ValueError(f"No local impairment calculation found for portfolio {portfolio_id}")
    
    # Get the latest local impairment staging
    latest_staging = (
        db.query(StagingResult)
        .filter(
            StagingResult.portfolio_id == portfolio_id,
            StagingResult.staging_type == "local_impairment"
        )
        .order_by(StagingResult.created_at.desc())
        .first()
    )
    
    if not latest_staging:
        raise ValueError(f"No local impairment staging found for portfolio {portfolio_id}")
    
    # OPTIMIZATION 1: Extract staging data with O(1) lookup
    print("Preloading staging data...")
    staging_data = []
    if "staging_data" in latest_staging.result_summary:
        staging_data = latest_staging.result_summary.get("staging_data", [])
    elif "loans" in latest_staging.result_summary:
        staging_data = latest_staging.result_summary.get("loans", [])
    
    # Create a map of loan_id to impairment category for O(1) lookups
    loan_category_map = {}
    for stage_info in staging_data:
        loan_id = stage_info.get("loan_id")
        category = stage_info.get("impairment_category", stage_info.get("stage"))  # Support both formats
        if loan_id and category:
            loan_category_map[loan_id] = category
    logging.info(f"[MEM] After staging preload: {process.memory_info().rss / 1024 ** 2:.2f} MB")
    
    # OPTIMIZATION 2: Extract provision rates from calculation
    calculation_summary = latest_calculation.result_summary
    provision_rates = {
        "Current": calculation_summary.get("Current", {}).get("provision_rate", 0.01),
        "OLEM": calculation_summary.get("OLEM", {}).get("provision_rate", 0.05),
        "Substandard": calculation_summary.get("Substandard", {}).get("provision_rate", 0.25),
        "Doubtful": calculation_summary.get("Doubtful", {}).get("provision_rate", 0.50),
        "Loss": calculation_summary.get("Loss", {}).get("provision_rate", 1.0)
    }
    
    # OPTIMIZATION 3: Get total loan count without loading all loans
    total_loan_count = db.query(func.count(Loan.id)).filter(Loan.portfolio_id == portfolio_id).scalar()
    print(f"Portfolio has {total_loan_count} loans to process")
    
    # OPTIMIZATION 4: Preload all client data in a single query
    print("Preloading client data...")
    client_map = {}
    # Process in batches to avoid memory issues with very large datasets
    client_batch_size = 5000
    for offset in range(0, total_loan_count, client_batch_size):
        # Get employee IDs for this batch
        employee_ids_subq = db.query(Loan.employee_id).filter(
            Loan.portfolio_id == portfolio_id
        ).distinct().offset(offset).limit(client_batch_size).subquery()
        
        # Get clients in a single query
        clients = db.query(Client).filter(
            Client.employee_id.in_(employee_ids_subq)
        ).all()
        
        # Build client map
        for client in clients:
            if client.employee_id:
                name = f"{client.last_name or ''} {client.other_names or ''}".strip()
                client_map[client.employee_id] = name if name else "Unknown"
    logging.info(f"[MEM] After client preload: {process.memory_info().rss / 1024 ** 2:.2f} MB")
    
    # OPTIMIZATION 5: Preload all securities data with a join
    print("Preloading securities data...")
    security_map = {}
    # Process in batches to avoid memory issues with very large datasets
    security_batch_size = 5000
    for offset in range(0, total_loan_count, security_batch_size):
        # Get employee IDs for this batch
        employee_ids_subq = db.query(Loan.employee_id).filter(
            Loan.portfolio_id == portfolio_id
        ).distinct().offset(offset).limit(security_batch_size).subquery()
        
        # Get securities with client data in a single query
        securities_with_clients = (
            db.query(Security, Client.employee_id)
            .join(Client, Security.client_id == Client.id)
            .filter(Client.employee_id.in_(employee_ids_subq))
            .all()
        )
        
        # Group securities by employee_id for O(1) lookup
        for security, employee_id in securities_with_clients:
            if employee_id:
                if employee_id not in security_map:
                    security_map[employee_id] = []
                security_map[employee_id].append(security)
    logging.info(f"[MEM] After securities preload: {process.memory_info().rss / 1024 ** 2:.2f} MB")
    
    # Initialize category totals
    category_totals = {
        "Current": {"count": 0, "balance": 0.0, "provision": 0.0},
        "OLEM": {"count": 0, "balance": 0.0, "provision": 0.0},
        "Substandard": {"count": 0, "balance": 0.0, "provision": 0.0},
        "Doubtful": {"count": 0, "balance": 0.0, "provision": 0.0},
        "Loss": {"count": 0, "balance": 0.0, "provision": 0.0}
    }
    
    # OPTIMIZATION 6: Process loans in larger batches
    batch_size = 2000  # Increased batch size for better throughput
    
    # Calculate number of batches
    num_batches = (total_loan_count + batch_size - 1) // batch_size
    print(f"Processing {num_batches} batches of {batch_size} loans each")
    
    # OPTIMIZATION 7: Write loan data directly to a temporary file to avoid keeping it all in memory
    import tempfile
    import json
    import os
    
    # Create a temporary file to store loan data
    temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.json')
    temp_file_path = temp_file.name
    print(f"Using temporary file for loan data: {temp_file_path}")
    
    # Write the beginning of a JSON array
    temp_file.write('[\n')
    first_loan = True
    
    # Process loans in batches
    for offset in range(0, total_loan_count, batch_size):
        batch_start_time = time.time()
        print(f"Processing batch {offset//batch_size + 1}/{num_batches}")
        
        # Fetch batch of loans directly from database
        loan_batch = db.query(Loan).filter(
            Loan.portfolio_id == portfolio_id
        ).order_by(Loan.id).offset(offset).limit(batch_size).all()
        
        # OPTIMIZATION 8: Parallel processing for independent calculations
        from concurrent.futures import ThreadPoolExecutor
        
        def process_loan(loan):
            try:
                # Convert to float early to reduce decimal overhead
                outstanding_balance = float(loan.outstanding_loan_balance) if loan.outstanding_loan_balance else 0.0
                
                # Get category using O(1) lookup
                category = loan_category_map.get(loan.id, "Current")  # Default to Current if not found
                
                # Get provision rate using O(1) lookup
                provision_rate = provision_rates.get(category, 0.01)  # Default to 1% if category not found
                
                # Get securities using O(1) lookup
                securities = security_map.get(loan.employee_id, [])
                
                # Calculate LGD for more accurate provision
                lgd = calculate_loss_given_default(loan, securities) / 100.0  # Convert to decimal
                
                # Calculate provision amount with LGD factor
                provision_amount = outstanding_balance * provision_rate * lgd
                
                # Get client name using preloaded map
                client_name = client_map.get(loan.employee_id, "Unknown")
                
                # Create loan entry
                loan_entry = {
                    "loan_id": loan.id,
                    "employee_id": loan.employee_id,
                    "employee_name": client_name,
                    "loan_value": str(loan.loan_amount) if loan.loan_amount else "0",
                    "outstanding_balance": str(loan.outstanding_loan_balance) if loan.outstanding_loan_balance else "0",
                    "accumulated_arrears": str(loan.accumulated_arrears or Decimal(0)),
                    "ndia": str(loan.ndia or Decimal(0)),
                    "impairment_category": category,
                    "provision_rate": str(provision_rate),
                    "provision_amount": str(provision_amount)
                }
                
                # Return loan entry and category data for totals
                return loan_entry, {
                    "category": category,
                    "balance": outstanding_balance,
                    "provision": provision_amount
                }
                
            except Exception as e:
                print(f"Error processing loan {loan.id}: {str(e)}")
                # Continue processing other loans
                return None, None
        
        # Process loans in parallel
        max_workers = min(8, os.cpu_count() or 4)  # Use at most 8 workers or number of CPU cores
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(process_loan, loan_batch))
        
        # Write results to file and update totals
        for loan_entry, category_data in results:
            if loan_entry and category_data:
                # Update category totals
                category = category_data["category"]
                if category in category_totals:
                    category_totals[category]["count"] += 1
                    category_totals[category]["balance"] += category_data["balance"]
                    category_totals[category]["provision"] += category_data["provision"]
                
                # Write loan entry to temporary file
                if not first_loan:
                    temp_file.write(',\n')
                first_loan = False
                json.dump(loan_entry, temp_file)
    logging.info(f"[MEM] After loan batch processing: {process.memory_info().rss / 1024 ** 2:.2f} MB")
    
    # Write the end of the JSON array
    temp_file.write('\n]')
    temp_file.close()
    
    # OPTIMIZATION 9: Create a streaming iterator for the Excel generator
    class StreamingLoanDataIterator:
        def __init__(self, file_path):
            self.file_path = file_path
            self._load_metadata()
            
        def _load_metadata(self):
            """Just load the metadata to get the count without loading all loans"""
            import json
            with open(self.file_path, 'r') as f:
                # Read first line to check if it's an empty array
                first_line = f.readline().strip()
                if first_line == '[' and f.readline().strip() == ']':
                    self.count = 0
                else:
                    # Count the number of lines that contain loan data (excluding first and last lines)
                    f.seek(0)
                    content = f.read()
                    self.count = content.count('\n') - 1
                    if self.count < 0:
                        self.count = 0
            
        def __iter__(self):
            import json
            with open(self.file_path, 'r') as f:
                # Skip the opening bracket
                f.readline()
                
                # Read each line (loan entry)
                for line in f:
                    line = line.strip()
                    if line.endswith(','):
                        line = line[:-1]  # Remove trailing comma
                    if line and line != ']':  # Skip empty lines and closing bracket
                        try:
                            yield json.loads(line)
                        except json.JSONDecodeError:
                            print(f"Error decoding JSON: {line}")
                
        def __len__(self):
            return self.count
    
    # Calculate total provision
    total_provision = sum(category_totals[category]["provision"] for category in category_totals)
    
    # Create the final report
    result = {
        "portfolio_name": portfolio.name,
        "description": f"Local Impairment Details Report for {portfolio.name}",
        "report_date": report_date,
        "report_run_date": datetime.now().date(),
        "total_provision": total_provision,
        "category_totals": category_totals,
    }
    
    # Generate Excel file and include base64 in report data
    try:
        excel_report_data = dict(result)
        all_loans = list(StreamingLoanDataIterator(temp_file_path))
        excel_report_data["loans"] = all_loans
        
        excel_bytes = create_excel_file(
            portfolio_name=portfolio.name,
            report_type="local_impairment_detailed_report",
            report_date=report_date,
            report_data=excel_report_data
        )
        import base64
        excel_base64 = base64.b64encode(excel_bytes.getvalue()).decode('utf-8')
        result["file"] = excel_base64
        print("Added base64-encoded Excel file to report data")
    except Exception as e:
        print(f"Error generating Excel file for base64 encoding: {str(e)}")
        # Continue without the base64 file
    
    logging.info(f"[MEM] End generate_local_impairment_details_report: {process.memory_info().rss / 1024 ** 2:.2f} MB")
    elapsed_time = time.time() - start_time
    print(f"Report generation completed in {elapsed_time:.2f} seconds")
    return result


def generate_local_impairment_report_summarised(
    db: Session, portfolio_id: int, report_date: date
) -> Dict[str, Any]:
    """
    Generate a summarised local impairment report for a portfolio.
    
    This report provides a summary of local impairment calculations across all categories.
    """
    # Get the portfolio
    portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
    if not portfolio:
        raise ValueError(f"Portfolio with ID {portfolio_id} not found")
    
    # Get the latest local impairment calculation
    latest_calculation = (
        db.query(CalculationResult)
        .filter(
            CalculationResult.portfolio_id == portfolio_id,
            CalculationResult.calculation_type == "local_impairment"
        )
        .order_by(CalculationResult.created_at.desc())
        .first()
    )
    
    if not latest_calculation:
        raise ValueError(f"No local impairment calculation found for portfolio {portfolio_id}")
    
    # Get calculation summary data
    calculation_summary = latest_calculation.result_summary
    
    # Extract data for each category
    current_data = calculation_summary.get("Current", {})
    olem_data = calculation_summary.get("OLEM", {})
    substandard_data = calculation_summary.get("Substandard", {})
    doubtful_data = calculation_summary.get("Doubtful", {})
    loss_data = calculation_summary.get("Loss", {})
    
    # Extract loan values
    current_loan_value = current_data.get("total_loan_value", 0)
    olem_loan_value = olem_data.get("total_loan_value", 0)
    substandard_loan_value = substandard_data.get("total_loan_value", 0)
    doubtful_loan_value = doubtful_data.get("total_loan_value", 0)
    loss_loan_value = loss_data.get("total_loan_value", 0)
    
    # Extract provision amounts (equivalent to ECL in the local impairment context)
    current_provision = current_data.get("provision_amount", 0)
    olem_provision = olem_data.get("provision_amount", 0)
    substandard_provision = substandard_data.get("provision_amount", 0)
    doubtful_provision = doubtful_data.get("provision_amount", 0)
    loss_provision = loss_data.get("provision_amount", 0)
    
    # Create the report data structure
    return {
        "portfolio_name": portfolio.name,
        "description": f"Local Impairment Summarised Report for {portfolio.name}",
        "report_date": report_date,
        "report_run_date": datetime.now().date(),
        "current": {
            "loan_value": current_loan_value,
            "outstanding_balance": current_loan_value,  # Same as loan value in this context
            "provision": current_provision
        },
        "olem": {
            "loan_value": olem_loan_value,
            "outstanding_balance": olem_loan_value,
            "provision": olem_provision
        },
        "substandard": {
            "loan_value": substandard_loan_value,
            "outstanding_balance": substandard_loan_value,
            "provision": substandard_provision
        },
        "doubtful": {
            "loan_value": doubtful_loan_value,
            "outstanding_balance": doubtful_loan_value,
            "provision": doubtful_provision
        },
        "loss": {
            "loan_value": loss_loan_value,
            "outstanding_balance": loss_loan_value,
            "provision": loss_provision
        }
    }


def generate_journal_report(
    db: Session, portfolio_ids: List[int], report_date: date
) -> Dict[str, Any]:
    """
    Generate a journal report for all portfolios.
    
    This report provides journal entries for IFRS9 impairment and credit risk reserves.
    
    Args:
        db: Database session
        portfolio_ids: List of portfolio IDs (ignored, will get all portfolios)
        report_date: Date of the report
    
    Returns:
        Dict containing the report data
    """
    portfolios_data = []
    
    # Tracking totals for summary
    total_ecl = 0
    total_local_impairment = 0
    total_risk_reserve = 0
    
    # Get all portfolios
    all_portfolios = db.query(Portfolio).all()
    
    for portfolio in all_portfolios:
        portfolio_id = portfolio.id
        
        # Skip portfolios without required account information
        if not portfolio.ecl_impairment_account or not portfolio.loan_assets or not portfolio.credit_risk_reserve:
            continue
        
        try:
            # Get the latest ECL calculation
            ecl_calculation = (
                db.query(CalculationResult)
                .filter(
                    CalculationResult.portfolio_id == portfolio_id,
                    CalculationResult.calculation_type == "ecl"
                )
                .order_by(CalculationResult.created_at.desc())
                .first()
            )
            
            if not ecl_calculation:
                continue  # Skip portfolios without ECL calculations
            
            # Get the latest local impairment calculation
            local_calculation = (
                db.query(CalculationResult)
                .filter(
                    CalculationResult.portfolio_id == portfolio_id,
                    CalculationResult.calculation_type == "local_impairment"
                )
                .order_by(CalculationResult.created_at.desc())
                .first()
            )
            
            if not local_calculation:
                continue  # Skip portfolios without local impairment calculations
            
            # Extract total ECL from ECL calculation
            ecl_summary = ecl_calculation.result_summary
            portfolio_ecl = 0
            for stage_key in ["Stage 1", "Stage 2", "Stage 3"]:
                stage_data = ecl_summary.get(stage_key, {})
                portfolio_ecl += stage_data.get("provision_amount", 0)
            
            # Extract total local impairment from local impairment calculation
            local_summary = local_calculation.result_summary
            portfolio_local_impairment = 0
            for category in ["Current", "OLEM", "Substandard", "Doubtful", "Loss"]:
                category_data = local_summary.get(category, {})
                portfolio_local_impairment += category_data.get("provision_amount", 0)
            
            # Calculate risk reserve (difference between local impairment and ECL)
            # If local impairment is greater than ECL, we need a risk reserve
            portfolio_risk_reserve = max(0, portfolio_local_impairment - portfolio_ecl)
            
            # Update totals for summary
            total_ecl += portfolio_ecl
            total_local_impairment += portfolio_local_impairment
            total_risk_reserve += portfolio_risk_reserve
            
            # Add portfolio data to the list
            portfolios_data.append({
                "portfolio_id": portfolio_id,
                "portfolio_name": portfolio.name,
                "ecl_impairment_account": portfolio.ecl_impairment_account,
                "loan_assets": portfolio.loan_assets,
                "credit_risk_reserve": portfolio.credit_risk_reserve,
                "total_ecl": portfolio_ecl,
                "total_local_impairment": portfolio_local_impairment,
                "risk_reserve": portfolio_risk_reserve
            })
        except Exception as e:
            # Skip portfolios with errors
            print(f"Error processing portfolio {portfolio_id}: {str(e)}")
            continue
    
    # Add summary entry if we have at least one portfolio
    if portfolios_data:
        # Get the most common account numbers to use in the summary
        ecl_impairment_accounts = [p["ecl_impairment_account"] for p in portfolios_data]
        loan_assets_accounts = [p["loan_assets"] for p in portfolios_data]
        credit_risk_reserve_accounts = [p["credit_risk_reserve"] for p in portfolios_data]
        
        # Use the most common account numbers for the summary
        most_common_ecl_account = max(set(ecl_impairment_accounts), key=ecl_impairment_accounts.count)
        most_common_loan_assets = max(set(loan_assets_accounts), key=loan_assets_accounts.count)
        most_common_credit_risk = max(set(credit_risk_reserve_accounts), key=credit_risk_reserve_accounts.count)
        
        # Add summary entry
        portfolios_data.append({
            "portfolio_id": None,  # No specific portfolio ID for summary
            "portfolio_name": "Summary",  # Use "Summary" instead of a specific portfolio name
            "ecl_impairment_account": most_common_ecl_account,
            "loan_assets": most_common_loan_assets,
            "credit_risk_reserve": most_common_credit_risk,
            "total_ecl": total_ecl,
            "total_local_impairment": total_local_impairment,
            "risk_reserve": total_risk_reserve
        })
    
    # Create the report data structure
    return {
        "description": f"Journal Report for All Portfolios ({len(portfolios_data) - (1 if portfolios_data else 0)} portfolios)",
        "report_date": report_date,
        "report_run_date": datetime.now().date(),
        "portfolios": portfolios_data
    }


def get_portfolio_name(db: Session, portfolio_id: int) -> str:
    """Get the portfolio name from the database"""
    portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
    return portfolio.name if portfolio else f"Portfolio {portfolio_id}"


def generate_report_pdf(
    db: Session,
    portfolio_id: int,
    report_type: str,
    report_date: date,
    report_data: Dict[str, Any],
) -> bytes:
    """
    Generate a PDF for a report.

    Args:
        db: Database session
        portfolio_id: ID of the portfolio
        report_type: Type of the report
        report_date: Date of the report
        report_data: Data for the report

    Returns:
        bytes: PDF file as bytes
    """
    portfolio_name = get_portfolio_name(db, portfolio_id)

    # Generate the PDF
    pdf_buffer = create_report_pdf(
        portfolio_name=portfolio_name,
        report_type=report_type,
        report_date=report_date,
        report_data=report_data,
    )

    return pdf_buffer.getvalue()


def generate_report_excel(
    db: Session,
    portfolio_id: int,
    report_type: str,
    report_date: date,
    report_data: Dict[str, Any],
) -> bytes:
    """
    Generate an Excel file for a report.

    Args:
        db: Database session
        portfolio_id: ID of the portfolio
        report_type: Type of the report
        report_date: Date of the report
        report_data: Data for the report

    Returns:
        bytes: Excel file as bytes
    """
    portfolio_name = get_portfolio_name(db, portfolio_id)

    # Generate the Excel file
    excel_buffer = create_excel_file(
        portfolio_name=portfolio_name,
        report_type=report_type,
        report_date=report_date,
        report_data=report_data,
    )

    return excel_buffer.getvalue()


# Export all report generator functions
__all__ = [
    "generate_collateral_summary",
    "generate_guarantee_summary",
    "generate_interest_rate_summary",
    "generate_repayment_summary",
    "generate_assumptions_summary",
    "generate_amortised_loan_balances",
    "generate_probability_default_report",
    "generate_exposure_default_report",
    "generate_loss_given_default_report",
    "generate_ecl_detailed_report",
    "generate_ecl_report_summarised",
    "generate_local_impairment_details_report",
    "generate_local_impairment_report_summarised",
    "generate_journal_report",
    "generate_report_excel",
]
