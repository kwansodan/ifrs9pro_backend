from sqlalchemy.orm import Session
import multiprocessing
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
from app.calculators.ecl import (
    calculate_effective_interest_rate_lender,
    calculate_exposure_at_default_percentage,
    calculate_probability_of_default,
    calculate_pd_from_yob
)
from app.utils.ecl_calculator import calculate_loss_given_default
import psutil
import logging
from app.utils.excel_generator import (
    create_report_excel as create_excel_file,
    populate_ecl_detailed_report as populate_ecl_detailed_excel,
    populate_local_impairment_details_report as populate_local_impairment_detailed_excel,
    load_excel_template
)
import tempfile
import json
import os
import base64
import traceback
import pickle
import warnings

# Suppress specific warnings globally if they always occur with this model
warnings.filterwarnings("ignore", category=UserWarning, message="X does not have valid feature names")
warnings.filterwarnings("ignore", category=UserWarning, message="Trying to unpickle estimator")



class StreamingLoanDataIterator:
    def __init__(self, file_path):
        self.file_path = file_path
        self._load_metadata()
        self.estimated_count = 0 # Initialize count

    def _load_metadata(self):
        """Estimate count by reading lines, handles empty file."""
        self.estimated_count = 0
        try:
            with open(self.file_path, 'r') as f:
                # Quick check for empty array '[]' spanning two lines
                first_char = f.read(1)
                if first_char == '[':
                    second_char = f.read(1)
                    if second_char == ']':
                        self.estimated_count = 0
                        return # Empty array
                    # Check for content on the first line like '[{...'
                    if second_char != '\n':
                         f.seek(0) # Reset if content starts immediately
                    # Else, assume multi-line array
                elif not first_char:
                    self.estimated_count = 0 # Empty file
                    return

                # Count lines roughly
                f.seek(0)
                # Subtract 2 for '[' and ']' lines, minimum 0
                line_count = sum(1 for _ in f)
                self.estimated_count = max(0, line_count - 2)
                # This count is an estimate, actual yield count is definitive

        except FileNotFoundError:
            print(f"Warning: Temporary file not found during metadata load: {self.file_path}")
            self.estimated_count = 0
        except Exception as e:
            print(f"Warning: Error reading metadata from {self.file_path}: {e}")
            self.estimated_count = -1 # Indicate error

    def __iter__(self):
        try:
            with open(self.file_path, 'r') as f:
                # Skip the opening bracket line '['
                first_line = f.readline()
                if not first_line.strip() == '[':
                    print(f"Warning: Expected '[' on first line of {self.file_path}, found: {first_line.strip()}")
                    # Attempt to proceed anyway or raise error? For now, proceed cautiously.

                line_num = 1
                while True:
                    line = f.readline()
                    line_num += 1
                    if not line: # End of file
                        break

                    line_content = line.strip()

                    # Skip closing bracket ']' line
                    if line_content == ']':
                        continue
                    # Skip empty lines if any exist
                    if not line_content:
                        continue

                    # Remove trailing comma if present
                    if line_content.endswith(','):
                        line_content = line_content[:-1]

                    try:
                        yield json.loads(line_content)
                    except json.JSONDecodeError as json_e:
                        print(f"Error decoding JSON on line {line_num} of {self.file_path}: {json_e}")
                        print(f"Problematic line content: {line_content}")
                        # Optionally: raise error, or yield an error object, or just skip
                        continue # Skip this line

        except FileNotFoundError:
            print(f"Error: Temporary file not found during iteration: {self.file_path}")
            # Yield nothing if file is gone
            return
        except Exception as e:
            print(f"Error during iteration of {self.file_path}: {e}")
            # Yield nothing on other errors


    def __len__(self):
        # Return the estimated count, actual count is determined by iterating
        if self.estimated_count == -1:
            print("Warning: Returning length -1 due to metadata load error.")
        return self.estimated_count


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
        # pd = calculate_probability_of_default(loan, db)
        pd = calculate_pd_from_yob()
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
    db: Session, portfolio_id: int, report_date: date, portfolio: Portfolio
) -> Dict[str, Any]:
    try:
        start_time = time.time()
        process = psutil.Process()
        start_mem = process.memory_info().rss / 1024**2
        logging.info(f"[MEM] Start generate_ecl_detailed_report: {start_mem:.2f} MB")
        print(f"Starting ECL detailed report generation for portfolio {portfolio_id} (Streaming Mode)")

        temp_file = None
        temp_file_path = None
        total_ead_calc = db.query(func.sum(Loan.ead)).filter(Loan.portfolio_id == portfolio_id).scalar() or 0.0
        total_lgd_calc = total_lgd_weighted_calc = (
            db.query(func.sum(Loan.ead * Loan.lgd)).filter(Loan.portfolio_id == portfolio_id).scalar()
        ) or 0.0
        total_ecl_calc = db.query(func.sum(Loan.final_ecl)).filter(Loan.portfolio_id == portfolio_id).scalar() or 0.0
        total_balance_diff_calc = db.query(func.sum(Loan.balance_difference)).filter(Loan.portfolio_id == portfolio_id).scalar() or 0.0
        total_loan_count_actual = (
            db.query(func.count(Loan.id)).filter(Loan.portfolio_id == portfolio_id).scalar()
        ) or 0

        client_map = {}
        employee_yob_map = {}
        client_query = db.query(Client).filter(Client.portfolio_id == portfolio_id).all()

        for client_data in client_query:
            emp_id = client_data.employee_id
            client_map[emp_id] = f"{client_data.last_name or ''} {client_data.other_names or ''}".strip() or "Unknown"
            year_of_birth = None
            if client_data.date_of_birth and hasattr(client_data.date_of_birth, 'year'):
                try:
                    year_of_birth = int(client_data.date_of_birth.year)
                except (ValueError, TypeError):
                    year_of_birth = None
            employee_yob_map[emp_id] = year_of_birth

        print(f"Preloaded client names for {len(client_map)} clients.")
        print(f"Preloaded YOB for {len(employee_yob_map)} employees.")
        logging.info(f"[MEM] After client preload: {process.memory_info().rss / 1024**2:.2f} MB")

        temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.json', encoding='utf-8')
        temp_file_path = temp_file.name
        print(f"Using temporary file for loan data: {temp_file_path}")
        temp_file.write('[\n')
        first_loan = True

        total_loan_count_db = db.query(func.count(Loan.id)).filter(Loan.portfolio_id == portfolio_id).scalar() or 0
        print(f"Database reports {total_loan_count_db} loans to process.")

        batch_size = 500
        num_batches = (total_loan_count_db + batch_size - 1) // batch_size if total_loan_count_db > 0 else 0
        print(f"Processing {num_batches} batches of up to {batch_size} loans each.")

        for offset in range(0, total_loan_count_db, batch_size):
            batch_start_time = time.time()
            current_batch_num = offset // batch_size + 1
            print(f"Processing batch {current_batch_num}/{num_batches}")

            loan_batch = db.query(
                Loan.id, Loan.employee_id, Loan.loan_amount, Loan.theoretical_balance, Loan.ead,
                Loan.lgd, Loan.eir, Loan.pd, Loan.final_ecl,
                Loan.accumulated_arrears, Loan.ifrs9_stage, Loan.ndia
            ).filter(
                Loan.portfolio_id == portfolio_id
            ).order_by(Loan.id).offset(offset).limit(batch_size).all()

            if not loan_batch:
                print(f"No loans found in batch {current_batch_num}")
                continue

            def process_loan_ecl(loan_data):
                loan_id = loan_data.id
                try:
                    employee_id = loan_data.employee_id
                    outstanding_balance_f = float(loan_data.ead or 0.0)
                    client_name = client_map.get(employee_id, "Unknown")
                    lgd = float((loan_data.lgd or 0.0) * (loan_data.ead or 0.0))

                    ecl_amount = float(loan_data.final_ecl or 0.0)

                    loan_entry = {
                    "loan_id": loan_id,
                    "employee_id": employee_id,
                    "employee_name": client_name,
                    "loan_value": str(loan_data.loan_amount or '0'),
                    "outstanding_loan_balance": str(float(loan_data.ead or 0.0)),
                    "balance_difference": str(float(loan_data.balance_difference or 0.0)),
                    "accumulated_arrears": str(float(loan_data.accumulated_arrears or 0.0)),
                    "ndia": str(float(loan_data.ndia or 0.0)),
                    "stage": str(loan_data.ifrs9_stage or 'Unknown'),
                    "ead": str(float(loan_data.ead or 0.0)),
                    "lgd": str(float((loan_data.lgd or 0.0) * (loan_data.ead or 0.0))),
                    "eir": str(float((loan_data.eir or 0.0) * 100)),
                    "pd": str(float((loan_data.pd or 0.0) * 100)),
                    "ecl": str(float(loan_data.final_ecl or 0.0)),
                }

                    loan_totals = {
                        "ead": loan_data.ead,
                        "lgd": lgd,
                        "ecl": ecl_amount,
                        "balance_difference": float(loan_data.balance_difference or 0.0)
                    }

                    return loan_entry, loan_totals

                except Exception as e:
                    print(f"Error processing loan {loan_id}: {str(e)}")
                    return None, None

            max_workers = max(1, multiprocessing.cpu_count() - 1)
            batch_results = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                batch_results = list(executor.map(process_loan_ecl, loan_batch))

            batch_loan_count = 0
            for loan_entry, loan_totals in batch_results:
                if loan_entry and loan_totals:
                    batch_loan_count += 1
                    if not first_loan:
                        temp_file.write(',\n')
                    first_loan = False
                    json.dump(loan_entry, temp_file, ensure_ascii=False)

            print(f"Finished processing batch {current_batch_num}, {batch_loan_count} loans added. Time: {time.time() - batch_start_time:.2f}s")
            current_mem = process.memory_info().rss / 1024**2
            logging.info(f"[MEM] After batch {current_batch_num}: {current_mem:.2f} MB")

        if temp_file:
            temp_file.write('\n]')
            temp_file.close()
            print(f"Temporary JSON file {temp_file_path} created with {total_loan_count_actual} loans.")
        else:
            print("Error: Temp file was not created.")
            raise IOError("Failed to create temporary file for report generation.")

        loans_iterator = StreamingLoanDataIterator(temp_file_path)
        iterator_len = len(loans_iterator)
        print(f"Streaming iterator created. Estimated length: {iterator_len}")
        if iterator_len != total_loan_count_actual:
            print(f"Warning: Iterator length estimate ({iterator_len}) differs from actual processed count ({total_loan_count_actual}). Using actual count.")

        report_summary_data = {
            "portfolio_id": portfolio_id,
            "portfolio_name": portfolio.name,
            "report_date": report_date.strftime("%Y-%m-%d"),
            "report_type": "ecl_detailed_report",
            "report_run_date": datetime.now().strftime("%Y-%m-%d"),
            "description": f"ECL Detailed Report for {portfolio.name}",
            "total_ead": total_ead_calc,
            "total_lgd": total_lgd_calc,
            "total_ecl": total_ecl_calc,
            "total_balance_difference": total_balance_diff_calc,
            "total_loan_count": total_loan_count_actual
        }

        print(f"Generating Excel file for portfolio {portfolio.name} using iterator...")
        wb = load_excel_template("ecl_detailed_report")
        excel_bytes_io = populate_ecl_detailed_excel(
            wb=wb,
            portfolio_name=portfolio.name,
            report_date=report_date,
            report_data=report_summary_data,
            loans_iterator=loans_iterator,
            temp_file_path=temp_file_path
        )

        excel_base64 = base64.b64encode(excel_bytes_io.getvalue()).decode('utf-8')
        report_summary_data["file"] = excel_base64
        print("Added base64-encoded Excel file to report data")

        end_mem = process.memory_info().rss / 1024**2
        logging.info(f"[MEM] End generate_ecl_detailed_report: {end_mem:.2f} MB (Delta: {end_mem - start_mem:.2f} MB)")
        print(f"ECL detailed report generation finished successfully in {time.time() - start_time:.2f} seconds")
        
        return report_summary_data

    except Exception as main_e:
        print(f"FATAL ERROR during ECL report generation for portfolio {portfolio_id}: {main_e}")
        traceback.print_exc()
        if temp_file and not temp_file.closed:
            temp_file.close()
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                print(f"Attempting cleanup of temp file {temp_file_path} after main error...")
                os.remove(temp_file_path)
                print(f"Cleaned up temp file {temp_file_path} after main error.")
            except Exception as cleanup_e:
                print(f"Error cleaning up temp file {temp_file_path} after main error: {cleanup_e}")
        return {"error": f"ECL Detailed Report generation failed: {main_e}"}

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
            "balance_difference": stage_1_data.get("balance_difference", 0),
            "ecl": stage_1_ecl,
            "num_loans": stage_1_count
        },
        "stage_2": {
            "loan_value": stage_2_loan_value,
            "outstanding_balance": stage_2_outstanding,
            "balance_difference": stage_2_data.get("balance_difference", 0),
            "ecl": stage_2_ecl,
            "num_loans": stage_2_count
        },
        "stage_3": {
            "loan_value": stage_3_loan_value,
            "outstanding_balance": stage_3_outstanding,
            "balance_difference": stage_3_data.get("balance_difference", 0),
            "ecl": stage_3_ecl,
            "num_loans": stage_3_count
        },
        "total": {
            "loan_value": total_loan_value,
            "outstanding_balance": total_outstanding,
            "balance_difference": sum([
                stage_1_data.get("balance_difference", 0),
                stage_2_data.get("balance_difference", 0),
                stage_3_data.get("balance_difference", 0)
            ]),
            "ecl": total_ecl,
            "num_loans": total_loans
        }
    }

def generate_local_impairment_details_report(
    db: Session, portfolio_id: int, report_date: date
) -> Dict[str, Any]:
    """
    Generate a detailed local impairment report using memory-optimized streaming.
    """
    start_time = time.time()
    process = psutil.Process()
    start_mem = process.memory_info().rss / 1024**2
    logging.info(f"[MEM] Start generate_local_impairment_details_report: {start_mem:.2f} MB")
    print(f"Starting Local Impairment details report generation for portfolio {portfolio_id} (Streaming Mode)")

    temp_file = None
    temp_file_path = None
    category_totals_calc = {
        "Current": {"count": 0, "balance": 0.0, "provision": 0.0},
        "OLEM": {"count": 0, "balance": 0.0, "provision": 0.0},
        "Substandard": {"count": 0, "balance": 0.0, "provision": 0.0},
        "Doubtful": {"count": 0, "balance": 0.0, "provision": 0.0},
        "Loss": {"count": 0, "balance": 0.0, "provision": 0.0},
        "Unknown": {"count": 0, "balance": 0.0, "provision": 0.0} # Add unknown category catch
    }
    total_loan_count_actual = 0

    try:
        # --- 1. Preload Static Data ---
        portfolio = db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
        if not portfolio:
            print(f"Error: Portfolio with ID {portfolio_id} not found.")
            return {"error": f"Portfolio {portfolio_id} not found"}

        # Preload calculation result for provision rates
        latest_calculation = db.query(CalculationResult).filter(
            CalculationResult.portfolio_id == portfolio_id,
            CalculationResult.calculation_type == "local_impairment"
        ).order_by(CalculationResult.created_at.desc()).first()

        if not latest_calculation or not latest_calculation.result_summary:
             print(f"Warning: No local impairment calculation result found for portfolio {portfolio_id}. Using default rates.")
             provision_rates = {"Current": 0.01, "OLEM": 0.05, "Substandard": 0.25, "Doubtful": 0.50, "Loss": 1.0}
        else:
            calc_summary = latest_calculation.result_summary
            provision_rates = {
                cat: calc_summary.get(cat, {}).get("provision_rate", default_rate)
                for cat, default_rate in [("Current", 0.01), ("OLEM", 0.05), ("Substandard", 0.25), ("Doubtful", 0.50), ("Loss", 1.0)]
            }
        print(f"Using provision rates: {provision_rates}")

        # Preload staging result for categories
        print("Preloading staging data (local impairment)...")
        latest_staging = db.query(StagingResult).filter(
            StagingResult.portfolio_id == portfolio_id,
            StagingResult.staging_type == "local_impairment"
        ).order_by(StagingResult.created_at.desc()).first()
        loan_category_map = {}
        if latest_staging and latest_staging.result_summary:
            stage_list_key = "staging_data" if "staging_data" in latest_staging.result_summary else "loans"
            staging_list = latest_staging.result_summary.get(stage_list_key, [])
            loan_category_map = {
                item.get("loan_id"): item.get("impairment_category", item.get("stage", "Current")) # Default if missing
                for item in staging_list if item.get("loan_id")
            }
        print(f"Preloaded {len(loan_category_map)} local impairment category mappings.")
        logging.info(f"[MEM] After staging preload: {process.memory_info().rss / 1024**2:.2f} MB")

        # Preload clients and securities (same as ECL report)
        print("Preloading client data...")
        client_map = {
             client.employee_id: f"{client.last_name or ''} {client.other_names or ''}".strip() or "Unknown"
             for client in db.query(Client.employee_id, Client.last_name, Client.other_names)
                            .filter(Client.portfolio_id == portfolio_id, Client.employee_id != None)
                            .all()
         }
        print("Preloading securities data...")
        securities_query = (
            db.query(Security, Client.employee_id)
            .join(Client, Security.client_id == Client.id)
            .filter(Client.portfolio_id == portfolio_id, Client.employee_id != None)
            .all()
        )
        security_map = {}
        for security, employee_id in securities_query:
             if employee_id not in security_map:
                 security_map[employee_id] = []
             security_map[employee_id].append(security)
        logging.info(f"[MEM] After client/security preload: {process.memory_info().rss / 1024**2:.2f} MB")

        # --- 2. Setup Temporary File ---
        temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.json', encoding='utf-8')
        temp_file_path = temp_file.name
        print(f"Using temporary file for loan data: {temp_file_path}")
        temp_file.write('[\n')
        first_loan = True

        # --- 3. Process Loans in Batches ---
        total_loan_count_db = db.query(func.count(Loan.id)).filter(Loan.portfolio_id == portfolio_id).scalar() or 0
        print(f"Database reports {total_loan_count_db} loans to process.")

        batch_size = 500
        num_batches = (total_loan_count_db + batch_size - 1) // batch_size if total_loan_count_db > 0 else 0
        print(f"Processing {num_batches} batches of up to {batch_size} loans each.")

        for offset in range(0, total_loan_count_db, batch_size):
            batch_start_time = time.time()
            current_batch_num = offset // batch_size + 1
            print(f"Processing batch {current_batch_num}/{num_batches}")

            # Query necessary columns
            loan_batch = db.query(
                 Loan.id, Loan.employee_id, Loan.loan_amount, Loan.outstanding_loan_balance,
                 Loan.accumulated_arrears, Loan.ndia # Add others if needed by LGD calc
                 ).filter(
                 Loan.portfolio_id == portfolio_id
             ).order_by(Loan.id).offset(offset).limit(batch_size).all()

            if not loan_batch: continue

            # --- Parallel Processing within Batch ---
            def process_loan_local(loan_data):
                loan_id = loan_data.id
                try:
                    # Basic data
                    employee_id = loan_data.employee_id
                    outstanding_balance_f = float(loan_data.outstanding_loan_balance or 0.0)

                    # Preloaded data
                    category = loan_category_map.get(loan_id, "Current") # Default category
                    provision_rate = provision_rates.get(category, 0.01) # Default rate
                    client_name = client_map.get(employee_id, "Unknown")
                    # securities = security_map.get(employee_id, []) # Needed if using LGD

                    # Calculation
                    # Simple: Provision = Rate * Balance
                    provision_amount = outstanding_balance_f * provision_rate
                    # Or more complex using LGD:
                    # lgd_rate = calculate_loss_given_default(loan_data, securities)
                    # provision_amount = outstanding_balance_f * provision_rate * float(lgd_rate)

                    # Create loan entry for JSON (strings)
                    loan_entry = {
                        "loan_id": loan_id,
                        "employee_id": employee_id,
                        "employee_name": client_name,
                        "loan_value": str(loan_data.loan_amount or '0'),
                        "outstanding_balance": str(loan_data.outstanding_loan_balance or '0'),
                        "balance_difference": str(float(loan_data.balance_difference or 0.0)),
                        "accumulated_arrears": str(loan_data.accumulated_arrears or '0'),
                        "ndia": str(float(loan_data.ndia or 0.0)),
                        "impairment_category": category,
                        "provision_rate": str(provision_rate), # Store the rate (e.g., "0.25")
                        "provision_amount": str(provision_amount) # Store calculated provision
                    }

                    # Data for category totals
                    category_data_out = {
                        "category": category,
                        "balance": outstanding_balance_f,
                        "provision": provision_amount
                    }
                    return loan_entry, category_data_out

                except Exception as e:
                     print(f"Error processing loan {loan_id} for local impairment: {str(e)}")
                     return None, None

            max_workers = max(1, multiprocessing.cpu_count() - 1)
            batch_results = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                 batch_results = list(executor.map(process_loan_local, loan_batch))

            # Write results to file and update totals
            batch_loan_count = 0
            for loan_entry, category_data_res in batch_results:
                if loan_entry and category_data_res:
                    total_loan_count_actual += 1
                    batch_loan_count += 1

                    # Update category totals
                    cat = category_data_res["category"]
                    target_cat = category_totals_calc.get(cat, category_totals_calc["Unknown"]) # Use Unknown if category isn't standard
                    if cat not in category_totals_calc:
                        print(f"Warning: Loan {loan_entry['loan_id']} has unknown category '{cat}', adding to 'Unknown'.")

                    target_cat["count"] += 1
                    target_cat["balance"] += category_data_res["balance"]
                    target_cat["provision"] += category_data_res["provision"]

                    # Write loan entry to temporary file
                    if not first_loan:
                        temp_file.write(',\n')
                    first_loan = False
                    json.dump(loan_entry, temp_file, ensure_ascii=False)

            print(f"Finished processing batch {current_batch_num}, {batch_loan_count} loans added. Time: {time.time() - batch_start_time:.2f}s")
            current_mem = process.memory_info().rss / 1024**2
            logging.info(f"[MEM] After batch {current_batch_num}: {current_mem:.2f} MB")


        # --- 4. Finalize JSON File ---
        if temp_file:
            temp_file.write('\n]')
            temp_file.close()
            print(f"Temporary JSON file {temp_file_path} created with {total_loan_count_actual} loans.")
        else:
            raise IOError("Failed to create temporary file for report generation.")

        # --- 5. Prepare Summary Data and Generate Excel ---
        loans_iterator = StreamingLoanDataIterator(temp_file_path)
        iterator_len = len(loans_iterator)
        print(f"Streaming iterator created. Estimated length: {iterator_len}")
        if iterator_len != total_loan_count_actual:
            print(f"Warning: Iterator length estimate ({iterator_len}) differs from actual processed count ({total_loan_count_actual}). Using actual count.")

        # Calculate total provision from accumulated category totals
        total_provision_calc = sum(details["provision"] for details in category_totals_calc.values())

        # Prepare summary data dictionary
        report_summary_data = {
            "portfolio_id": portfolio_id,
            "portfolio_name": portfolio.name,
            "description": f"Local Impairment Details Report for {portfolio.name}",
            "report_date": report_date.strftime("%Y-%m-%d"),
            "report_run_date": datetime.now().strftime("%Y-%m-%d"),
            "total_provision": total_provision_calc,
            "category_totals": category_totals_calc, # Include calculated summaries
            "total_loan_count": total_loan_count_actual
        }

        excel_base64 = None
        excel_bytes_io = None
        print(f"Generating Excel file for portfolio {portfolio.name} using iterator...")
        wb = load_excel_template("local_impairment_detailed_report")

        # Call the MODIFIED excel generator function
        excel_bytes_io = populate_local_impairment_detailed_excel(
            wb=wb,
            portfolio_name=portfolio.name,
            report_date=report_date,
            report_data=report_summary_data, # Pass summary data
            loans_iterator=loans_iterator,    # Pass the iterator
            temp_file_path=temp_file_path     # Pass path for cleanup
        )
        # Temp file cleanup happens inside populate_local_impairment_detailed_excel

        excel_base64 = base64.b64encode(excel_bytes_io.getvalue()).decode('utf-8')
        report_summary_data["file"] = excel_base64
        print("Added base64-encoded Excel file to report data")

        end_mem = process.memory_info().rss / 1024**2
        logging.info(f"[MEM] End generate_local_impairment_details_report: {end_mem:.2f} MB (Delta: {end_mem - start_mem:.2f} MB)")
        elapsed_time = time.time() - start_time
        print(f"Local impairment detailed report generation finished successfully in {elapsed_time:.2f} seconds")

        return report_summary_data

    except Exception as main_e:
        print(f"FATAL ERROR during Local Impairment report generation for portfolio {portfolio_id}: {main_e}")
        traceback.print_exc()
        if temp_file and not temp_file.closed:
             temp_file.close()
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                print(f"Attempting cleanup of temp file {temp_file_path} after main error...")
                os.remove(temp_file_path)
                print(f"Cleaned up temp file {temp_file_path} after main error.")
            except Exception as cleanup_e:
                print(f"Error cleaning up temp file {temp_file_path} after main error: {cleanup_e}")
        return {"error": f"Local Impairment Detailed Report generation failed: {main_e}"}


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
            "outstanding_balance": current_loan_value,
            "balance_difference": current_data.get("balance_difference", 0),
            "provision": current_provision
        },
        "olem": {
            "loan_value": olem_loan_value,
            "outstanding_balance": olem_loan_value,
            "balance_difference": olem_data.get("balance_difference", 0),
            "provision": olem_provision
        },
        "substandard": {
            "loan_value": substandard_loan_value,
            "outstanding_balance": substandard_loan_value,
            "balance_difference": substandard_data.get("balance_difference", 0),
            "provision": substandard_provision
        },
        "doubtful": {
            "loan_value": doubtful_loan_value,
            "outstanding_balance": doubtful_loan_value,
            "balance_difference": doubtful_data.get("balance_difference", 0),
            "provision": doubtful_provision
        },
        "loss": {
            "loan_value": loss_loan_value,
            "outstanding_balance": loss_loan_value,
            "balance_difference": loss_data.get("balance_difference", 0),
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
