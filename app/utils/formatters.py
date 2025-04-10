from decimal import Decimal
from typing import Union, Dict, Any, Optional

def format_currency(value: Union[float, Decimal, None], decimal_places: int = 2) -> Optional[float]:
    """
    Format a currency value by rounding to specified decimal places.
    Returns None if the input is None.
    """
    if value is None:
        return None
    
    # Simply round to specified decimal places
    return round(value, decimal_places)

def format_percentage(value: Union[float, Decimal, None], decimal_places: int = 2) -> Optional[float]:
    """
    Format a percentage value by multiplying by 100 and rounding to specified decimal places.
    Returns None if the input is None.
    """
    if value is None:
        return None
    
    # Multiply by 100 for percentage, and round
    return round(value * 100, decimal_places)

def format_category_data(category_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format a category data dictionary by formatting currency and percentage fields.
    """
    if not category_data:
        return {}
    
    formatted_data = category_data.copy()
    
    # Format currency fields
    if 'total_loan_value' in formatted_data:
        formatted_data['total_loan_value'] = format_currency(formatted_data['total_loan_value'])
    
    if 'provision_amount' in formatted_data:
        formatted_data['provision_amount'] = format_currency(formatted_data['provision_amount'])
    
    # Format percentage fields
    if 'provision_rate' in formatted_data:
        formatted_data['provision_rate'] = format_percentage(formatted_data['provision_rate'])
    
    return formatted_data

def format_ecl_calculation_detail(ecl_detail: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format an ECL calculation detail dictionary.
    """
    if not ecl_detail:
        return {}
    
    formatted_detail = ecl_detail.copy()
    
    # Format stage data
    for stage in ['stage_1', 'stage_2', 'stage_3']:
        if stage in formatted_detail and formatted_detail[stage]:
            formatted_detail[stage] = format_category_data(formatted_detail[stage])
    
    # Format summary fields
    if 'total_provision' in formatted_detail:
        formatted_detail['total_provision'] = format_currency(formatted_detail['total_provision'])
    
    if 'provision_percentage' in formatted_detail:
        formatted_detail['provision_percentage'] = format_percentage(formatted_detail['provision_percentage'])
    
    return formatted_detail

def format_local_impairment_detail(impairment_detail: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format a local impairment calculation detail dictionary.
    """
    if not impairment_detail:
        return {}
    
    formatted_detail = impairment_detail.copy()
    
    # Format category data
    for category in ['current', 'olem', 'substandard', 'doubtful', 'loss']:
        if category in formatted_detail and formatted_detail[category]:
            formatted_detail[category] = format_category_data(formatted_detail[category])
    
    # Format summary fields
    if 'total_provision' in formatted_detail:
        formatted_detail['total_provision'] = format_currency(formatted_detail['total_provision'])
    
    if 'provision_percentage' in formatted_detail:
        formatted_detail['provision_percentage'] = format_percentage(formatted_detail['provision_percentage'])
    
    return formatted_detail

def format_calculation_summary(calc_summary: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format the entire calculation summary dictionary.
    """
    if not calc_summary:
        return {}
    
    formatted_summary = calc_summary.copy()
    
    # Format ECL details
    if 'ecl' in formatted_summary and formatted_summary['ecl']:
        formatted_summary['ecl'] = format_ecl_calculation_detail(formatted_summary['ecl'])
    
    # Format local impairment details
    if 'local_impairment' in formatted_summary and formatted_summary['local_impairment']:
        formatted_summary['local_impairment'] = format_local_impairment_detail(formatted_summary['local_impairment'])
    
    # Format total loan value
    if 'total_loan_value' in formatted_summary:
        formatted_summary['total_loan_value'] = format_currency(formatted_summary['total_loan_value'])
    
    return formatted_summary

def format_staging_summary(staging_summary: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format the staging summary dictionary.
    """
    if not staging_summary:
        return {}
    
    formatted_summary = staging_summary.copy()
    
    # Format ECL staging
    if 'ecl' in formatted_summary and formatted_summary['ecl']:
        ecl_data = formatted_summary['ecl']
        for stage in ['stage_1', 'stage_2', 'stage_3']:
            if stage in ecl_data and ecl_data[stage]:
                if 'outstanding_loan_balance' in ecl_data[stage]:
                    ecl_data[stage]['outstanding_loan_balance'] = format_currency(ecl_data[stage]['outstanding_loan_balance'])
    
    # Format local impairment staging
    if 'local_impairment' in formatted_summary and formatted_summary['local_impairment']:
        local_data = formatted_summary['local_impairment']
        for category in ['current', 'olem', 'substandard', 'doubtful', 'loss']:
            if category in local_data and local_data[category]:
                if 'outstanding_loan_balance' in local_data[category]:
                    local_data[category]['outstanding_loan_balance'] = format_currency(local_data[category]['outstanding_loan_balance'])
    
    return formatted_summary

def format_overview_model(overview: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format the overview model dictionary.
    """
    if not overview:
        return {}
    
    formatted_overview = overview.copy()
    
    # Format currency fields
    if 'total_loan_value' in formatted_overview:
        formatted_overview['total_loan_value'] = format_currency(formatted_overview['total_loan_value'])
    
    if 'average_loan_amount' in formatted_overview:
        formatted_overview['average_loan_amount'] = format_currency(formatted_overview['average_loan_amount'])
    
    return formatted_overview
