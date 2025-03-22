from sqlalchemy.orm import Session
from sqlalchemy import func, and_
import numpy as np
import pandas as pd
from decimal import Decimal
from datetime import date, datetime, timedelta
from typing import Dict, List, Any, Optional
from datetime import date
from app.models import Portfolio, Report
from app.utils.pdf_generator import create_report_pdf

from app.models import (
    Portfolio,
    Loan,
    Client,
    Security,
    Guarantee,
    Report,
)
from app.calculators.ecl import (
    calculate_effective_interest_rate,
    calculate_exposure_at_default_percentage,
    calculate_probability_of_default,
    calculate_loss_given_default,
)


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
    total_security_value = sum(security.security_value or 0 for security in securities)
    average_security_value = (
        total_security_value / len(securities) if securities else 0
    )
    
    # Count security types
    security_types = {}
    for security in securities:
        if security.security_type:
            security_types[security.security_type] = security_types.get(security.security_type, 0) + 1

    # Get top 10 most valuable securities
    top_securities = sorted(
        securities, 
        key=lambda x: x.security_value or 0, 
        reverse=True
    )[:10]
    
    top_securities_data = [
        {
            "id": security.id,
            "client_id": security.client_id,
            "security_type": security.security_type,
            "security_value": security.security_value,
            "description": security.description
        }
        for security in top_securities
    ]
    
    # Calculate collateral coverage ratio
    total_loan_value = sum(loan.outstanding_loan_balance or 0 for loan in loans)
    collateral_coverage_ratio = (
        total_security_value / total_loan_value if total_loan_value > 0 else 0
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
        "reporting_date": report_date.isoformat()
    }


def generate_guarantee_summary(
    db: Session, portfolio_id: int, report_date: date
) -> Dict[str, Any]:
    """
    Generate a summary of guarantee data for a portfolio.
    """
    # Get guarantees for the portfolio
    guarantees = db.query(Guarantee).filter(Guarantee.portfolio_id == portfolio_id).all()
    
    # Calculate guarantee statistics
    total_guarantee_value = sum(guarantee.pledged_amount or 0 for guarantee in guarantees)
    average_guarantee_value = (
        total_guarantee_value / len(guarantees) if guarantees else 0
    )
    
    # Get loans for the portfolio
    loans = db.query(Loan).filter(Loan.portfolio_id == portfolio_id).all()
    total_loan_value = sum(loan.outstanding_loan_balance or 0 for loan in loans)
    
    # Calculate guarantee coverage ratio
    guarantee_coverage_ratio = (
        total_guarantee_value / total_loan_value if total_loan_value > 0 else 0
    )
    
    # Get top guarantors by pledged amount
    top_guarantors = sorted(
        guarantees, 
        key=lambda x: x.pledged_amount or 0, 
        reverse=True
    )[:10]
    
    top_guarantors_data = [
        {
            "id": guarantee.id,
            "guarantor": guarantee.guarantor,
            "pledged_amount": guarantee.pledged_amount
        }
        for guarantee in top_guarantors
    ]
    
    # Count guarantors by type if available
    guarantor_types = {}
    for guarantee in guarantees:
        if hasattr(guarantee, 'guarantor_type') and guarantee.guarantor_type:
            guarantor_types[guarantee.guarantor_type] = guarantor_types.get(guarantee.guarantor_type, 0) + 1
    
    return {
        "total_guarantee_value": total_guarantee_value,
        "average_guarantee_value": average_guarantee_value,
        "guarantee_coverage_ratio": round(guarantee_coverage_ratio, 2),
        "total_guarantees": len(guarantees),
        "top_guarantors": top_guarantors_data,
        "guarantor_types": guarantor_types,
        "reporting_date": report_date.isoformat()
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
            eir = calculate_effective_interest_rate(
                loan_amount=loan.loan_amount,
                monthly_installment=loan.monthly_installment,
                loan_term=loan.loan_term
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
        average_eir = min_eir = max_eir = median_eir = 0
    
    # Group loans by EIR ranges
    eir_ranges = {
        "0-5%": 0,
        "5-10%": 0,
        "10-15%": 0,
        "15-20%": 0,
        "20-25%": 0,
        "25-30%": 0,
        "30%+": 0
    }
    
    for _, eir in loan_eirs:
        if eir < 0.05:
            eir_ranges["0-5%"] += 1
        elif eir < 0.10:
            eir_ranges["5-10%"] += 1
        elif eir < 0.15:
            eir_ranges["10-15%"] += 1
        elif eir < 0.20:
            eir_ranges["15-20%"] += 1
        elif eir < 0.25:
            eir_ranges["20-25%"] += 1
        elif eir < 0.30:
            eir_ranges["25-30%"] += 1
        else:
            eir_ranges["30%+"] += 1
    
    # Group by loan type if available
    loan_type_eirs = {}
    for loan, eir in loan_eirs:
        if loan.loan_type:
            if loan.loan_type not in loan_type_eirs:
                loan_type_eirs[loan.loan_type] = []
            loan_type_eirs[loan.loan_type].append(eir)
    
    loan_type_avg_eirs = {
        loan_type: sum(eirs) / len(eirs)
        for loan_type, eirs in loan_type_eirs.items()
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
            "effective_interest_rate": round(eir * 100, 2)  # as percentage
        }
        for loan, eir in top_eir_loans
    ]
    
    return {
        "average_eir": round(average_eir * 100, 2),  # as percentage
        "min_eir": round(min_eir * 100, 2),
        "max_eir": round(max_eir * 100, 2),
        "median_eir": round(median_eir * 100, 2),
        "eir_distribution": eir_ranges,
        "loan_type_avg_eirs": {k: round(v * 100, 2) for k, v in loan_type_avg_eirs.items()},
        "top_eir_loans": top_eir_loans_data,
        "total_loans_analyzed": len(loan_eirs),
        "reporting_date": report_date.isoformat()
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
    total_principal_due = sum(loan.principal_due or 0 for loan in loans)
    total_interest_due = sum(loan.interest_due or 0 for loan in loans)
    total_due = sum(loan.total_due or 0 for loan in loans)
    
    total_principal_paid = sum(loan.principal_paid or 0 for loan in loans)
    total_interest_paid = sum(loan.interest_paid or 0 for loan in loans)
    total_paid = sum(loan.total_paid or 0 for loan in loans)
    
    # Calculate repayment ratios
    principal_repayment_ratio = (
        total_principal_paid / total_principal_due if total_principal_due > 0 else 0
    )
    interest_repayment_ratio = (
        total_interest_paid / total_interest_due if total_interest_due > 0 else 0
    )
    overall_repayment_ratio = (
        total_paid / total_due if total_due > 0 else 0
    )
    
    # Group loans by status
    paid_loans = sum(1 for loan in loans if loan.paid is True)
    unpaid_loans = sum(1 for loan in loans if loan.paid is False)
    
    # Calculate delinquency statistics
    delinquent_loans = sum(1 for loan in loans if loan.ndia and loan.ndia > 0)
    delinquency_rate = delinquent_loans / len(loans) if loans else 0
    
    # Group loans by NDIA ranges
    ndia_ranges = {
        "Current (0)": 0,
        "1-30 days": 0,
        "31-90 days": 0,
        "91-180 days": 0,
        "181-360 days": 0,
        "360+ days": 0
    }
    
    for loan in loans:
        ndia = loan.ndia or 0
        if ndia == 0:
            ndia_ranges["Current (0)"] += 1
        elif ndia <= 30:
            ndia_ranges["1-30 days"] += 1
        elif ndia <= 90:
            ndia_ranges["31-90 days"] += 1
        elif ndia <= 180:
            ndia_ranges["91-180 days"] += 1
        elif ndia <= 360:
            ndia_ranges["181-360 days"] += 1
        else:
            ndia_ranges["360+ days"] += 1
    
    # Top 10 loans with highest accumulated arrears
    top_arrears_loans = sorted(
        loans, 
        key=lambda x: x.accumulated_arrears or 0, 
        reverse=True
    )[:10]
    
    top_arrears_loans_data = [
        {
            "loan_id": loan.id,
            "loan_no": loan.loan_no,
            "employee_id": loan.employee_id,
            "accumulated_arrears": loan.accumulated_arrears,
            "ndia": loan.ndia,
            "outstanding_loan_balance": loan.outstanding_loan_balance
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
        "reporting_date": report_date.isoformat()
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
    avg_pd = 0
    avg_lgd = 0
    avg_ead = 0
    
    # Get client IDs for these loans
    employee_ids = [loan.employee_id for loan in loans if loan.employee_id]
    
    # Get securities for these clients
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
                db.query(Security)
                .filter(Security.client_id == client_id)
                .all()
            )
            client_securities[employee_id] = securities
    
    # Calculate PD, LGD, and EAD for each loan
    pd_values = []
    lgd_values = []
    ead_values = []
    
    for loan in loans:
        # Calculate PD
        ndia = loan.ndia or 0
        pd = calculate_probability_of_default(loan, ndia)
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
    macro_economic_factor = 1.0  # Example value, could be adjusted based on economic conditions
    recovery_rate = 0.4  # Example value, could be adjusted based on historical data
    
    # Based on portfolio type
    if portfolio:
        if portfolio.asset_type == "mortgage":
            recovery_rate = 0.7  # Higher recovery rate for mortgage loans
        elif portfolio.asset_type == "unsecured":
            recovery_rate = 0.3  # Lower recovery rate for unsecured loans
    
    # Group loans by NDIA ranges for PD curve
    ndia_pd_curve = {
        "0 days": 0.02,
        "1-30 days": 0.05,
        "31-90 days": 0.20,
        "91-180 days": 0.40,
        "181-360 days": 0.75,
        "360+ days": 0.99
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
        "reporting_date": report_date.isoformat()
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
    total_original_loan_amount = sum(loan.loan_amount or 0 for loan in loans)
    total_current_loan_balance = sum(loan.outstanding_loan_balance or 0 for loan in loans)
    total_amortisation = total_original_loan_amount - total_current_loan_balance
    
    # Calculate percentage amortised
    percent_amortised = (
        (total_amortisation / total_original_loan_amount * 100) 
        if total_original_loan_amount > 0 else 0
    )
    
    # Group loans by amortisation percentage
    amortisation_ranges = {
        "0-20%": 0,
        "21-40%": 0,
        "41-60%": 0,
        "61-80%": 0,
        "81-100%": 0
    }
    
    for loan in loans:
        if loan.loan_amount and loan.loan_amount > 0 and loan.outstanding_loan_balance is not None:
            amortised_amount = loan.loan_amount - loan.outstanding_loan_balance
            amortised_percent = (amortised_amount / loan.loan_amount) * 100
            
            if amortised_percent <= 20:
                amortisation_ranges["0-20%"] += 1
            elif amortised_percent <= 40:
                amortisation_ranges["21-40%"] += 1
            elif amortised_percent <= 60:
                amortisation_ranges["41-60%"] += 1
            elif amortised_percent <= 80:
                amortisation_ranges["61-80%"] += 1
            else:
                amortisation_ranges["81-100%"] += 1
    
    # Calculate expected final amortisation dates
    loan_status = []
    for loan in loans:
        if loan.loan_term and loan.loan_issue_date and loan.outstanding_loan_balance:
            # Calculate expected end date
            expected_end_date = loan.loan_issue_date + timedelta(days=30 * loan.loan_term)
            
            # Calculate days remaining
            if expected_end_date > report_date:
                days_remaining = (expected_end_date - report_date).days
            else:
                days_remaining = 0
            
            # Calculate expected monthly amortisation
            monthly_amortisation = loan.principal_due if loan.principal_due else (
                loan.loan_amount / loan.loan_term if loan.loan_term > 0 else 0
            )
            
            loan_status.append({
                "loan_id": loan.id,
                "loan_no": loan.loan_no,
                "original_amount": loan.loan_amount,
                "current_balance": loan.outstanding_loan_balance,
                "amortised_amount": loan.loan_amount - loan.outstanding_loan_balance if loan.loan_amount else 0,
                "amortised_percent": round(
                    ((loan.loan_amount - loan.outstanding_loan_balance) / loan.loan_amount * 100)
                    if loan.loan_amount and loan.loan_amount > 0 else 0, 
                    2
                ),
                "expected_end_date": expected_end_date.isoformat(),
                "days_remaining": days_remaining,
                "monthly_amortisation": monthly_amortisation
            })
    
    # Sort by amortised percentage (descending)
    loan_status = sorted(loan_status, key=lambda x: x["amortised_percent"], reverse=True)
    
    return {
        "total_original_loan_amount": total_original_loan_amount,
        "total_current_loan_balance": total_current_loan_balance,
        "total_amortisation": total_amortisation,
        "percent_amortised": round(percent_amortised, 2),
        "amortisation_distribution": amortisation_ranges,
        "loan_status": loan_status[:50],  # Limit to top 50 loans
        "total_loans_analyzed": len(loans),
        "reporting_date": report_date.isoformat(),
        "note": "This report does not consider the BOG non-accrual rule."
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
        ndia = loan.ndia or 0
        pd = calculate_probability_of_default(loan, ndia)
        pd_values.append(pd)
        
        loan_pds.append({
            "loan_id": loan.id,
            "loan_no": loan.loan_no,
            "employee_id": loan.employee_id,
            "ndia": ndia,
            "pd": round(pd, 4),
            "outstanding_balance": loan.outstanding_loan_balance
        })
    
    # Calculate PD statistics
    if pd_values:
        avg_pd = sum(pd_values) / len(pd_values)
        min_pd = min(pd_values)
        max_pd = max(pd_values)
        median_pd = sorted(pd_values)[len(pd_values) // 2]
    else:
        avg_pd = min_pd = max_pd = median_pd = 0
    
    # Group loans by PD ranges
    pd_ranges = {
        "0-10%": 0,
        "11-25%": 0,
        "26-50%": 0,
        "51-75%": 0,
        "76-90%": 0,
        "91-100%": 0
    }
    
    weighted_pd_sum = 0
    total_outstanding_balance = 0
    
    for loan_pd in loan_pds:
        pd_percent = loan_pd["pd"] * 100
        outstanding_balance = loan_pd["outstanding_balance"] or 0
        
        # Add to weighted PD calculation
        weighted_pd_sum += loan_pd["pd"] * outstanding_balance
        total_outstanding_balance += outstanding_balance
        
        if pd_percent <= 10:
            pd_ranges["0-10%"] += 1
        elif pd_percent <= 25:
            pd_ranges["11-25%"] += 1
        elif pd_percent <= 50:
            pd_ranges["26-50%"] += 1
        elif pd_percent <= 75:
            pd_ranges["51-75%"] += 1
        elif pd_percent <= 90:
            pd_ranges["76-90%"] += 1
        else:
            pd_ranges["91-100%"] += 1
    
    # Calculate portfolio weighted PD
    weighted_portfolio_pd = (
        weighted_pd_sum / total_outstanding_balance if total_outstanding_balance > 0 else 0
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
        "reporting_date": report_date.isoformat()
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
            if loan.outstanding_loan_balance else 0
        )
        
        loan_eads.append({
            "loan_id": loan.id,
            "loan_no": loan.loan_no,
            "employee_id": loan.employee_id,
            "outstanding_balance": loan.outstanding_loan_balance,
            "ead_percentage": round(ead_percentage, 4),
            "ead_amount": ead_amount
        })
    
    # Calculate EAD statistics
    if ead_values:
        avg_ead = sum(ead_values) / len(ead_values)
        min_ead = min(ead_values)
        max_ead = max(ead_values)
        median_ead = sorted(ead_values)[len(ead_values) // 2]
    else:
        avg_ead = min_ead = max_ead = median_ead = 0
    
    # Calculate total EAD
    total_outstanding_balance = sum(loan.outstanding_loan_balance or 0 for loan in loans)
    total_ead = sum(
        (loan.outstanding_loan_balance or 0) * calculate_exposure_at_default_percentage(loan, report_date)
        for loan in loans
    )
    
    # Group loans by EAD percentage ranges
    ead_ranges = {
        "0-80%": 0,
        "81-90%": 0,
        "91-95%": 0,
        "96-99%": 0,
        "100%": 0,
        "100%+": 0
    }
    
    for loan_ead in loan_eads:
        ead_percentage = loan_ead["ead_percentage"] * 100
        
        if ead_percentage <= 80:
            ead_ranges["0-80%"] += 1
        elif ead_percentage <= 90:
            ead_ranges["81-90%"] += 1
        elif ead_percentage <= 95:
            ead_ranges["91-95%"] += 1
        elif ead_percentage <= 99:
            ead_ranges["96-99%"] += 1
        elif ead_percentage <= 100:
            ead_ranges["100%"] += 1
        else:
            ead_ranges["100%+"] += 1
    
    # Sort loans by EAD amount (descending)
    loan_eads = sorted(loan_eads, key=lambda x: x["ead_amount"], reverse=True)
    
    return {
        "average_ead_percentage": round(avg_ead, 4),
        "min_ead_percentage": round(min_ead, 4),
        "max_ead_percentage": round(max_ead, 4),
        "median_ead_percentage": round(median_ead, 4),
        "total_outstanding_balance": total_outstanding_balance,
        "total_ead": total_ead,
        "ead_to_outstanding_ratio": round(total_ead / total_outstanding_balance if total_outstanding_balance > 0 else 0, 4),
        "ead_distribution": ead_ranges,
        "highest_exposure_loans": loan_eads[:25],  # Top 25 highest exposure loans
        "total_loans_analyzed": len(loans),
        "reporting_date": report_date.isoformat()
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
                db.query(Security)
                .filter(Security.client_id == client_id)
                .all()
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
            if loan.outstanding_loan_balance else 0
        )
        
        # Calculate security value
        security_value = sum(security.security_value or 0 for security in securities)
        
        loan_lgds.append({
            "loan_id": loan.id,
            "loan_no": loan.loan_no,
            "employee_id": loan.employee_id,
            "outstanding_balance": loan.outstanding_loan_balance,
            "security_value": security_value,
            "lgd": round(lgd, 4),
            "expected_loss": expected_loss
        })
    
    # Calculate LGD statistics
    if lgd_values:
        avg_lgd = sum(lgd_values) / len(lgd_values)
        min_lgd = min(lgd_values)
        max_lgd = max(lgd_values)
        median_lgd = sorted(lgd_values)[len(lgd_values) // 2]
    else:
        avg_lgd = min_lgd = max_lgd = median_lgd = 0
    
    # Calculate total expected loss
    total_outstanding_balance = sum(loan.outstanding_loan_balance or 0 for loan in loans)
    total_expected_loss = sum(
        Decimal(loan.outstanding_loan_balance or 0) * Decimal(calculate_loss_given_default(
            loan, client_securities.get(loan.employee_id, [])))
        for loan in loans
    )
    
    # Group loans by LGD ranges
    lgd_ranges = {
        "0-20%": 0,
        "21-40%": 0,
        "41-60%": 0,
        "61-80%": 0,
        "81-100%": 0
    }
    
    for loan_lgd in loan_lgds:
        lgd_percentage = loan_lgd["lgd"] * 100
        
        if lgd_percentage <= 20:
            lgd_ranges["0-20%"] += 1
        elif lgd_percentage <= 40:
            lgd_ranges["21-40%"] += 1
        elif lgd_percentage <= 60:
            lgd_ranges["41-60%"] += 1
        elif lgd_percentage <= 80:
            lgd_ranges["61-80%"] += 1
        else:
            lgd_ranges["81-100%"] += 1
    
    # Sort loans by expected loss (descending)
    loan_lgds = sorted(loan_lgds, key=lambda x: x["expected_loss"], reverse=True)
    
    return {
        "average_lgd": round(avg_lgd, 4),
        "min_lgd": round(min_lgd, 4),
        "max_lgd": round(max_lgd, 4),
        "median_lgd": round(median_lgd, 4),
        "total_outstanding_balance": total_outstanding_balance,
        "total_expected_loss": total_expected_loss,
        "loss_to_outstanding_ratio": round(total_expected_loss / total_outstanding_balance if total_outstanding_balance > 0 else 0, 4),
        "lgd_distribution": lgd_ranges,
        "highest_loss_loans": loan_lgds[:25],  # Top 25 highest loss loans
        "total_loans_analyzed": len(loans),
        "reporting_date": report_date.isoformat()
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
    report_data: Dict[str, Any]
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
