from datetime import date
from typing import Dict, Any, List
from io import BytesIO
import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.chart import BarChart, Reference, PieChart
import os
from datetime import datetime


def create_report_excel(
    portfolio_name: str,
    report_type: str,
    report_date: date,
    report_data: Dict[str, Any],
) -> BytesIO:
    """
    Generate an Excel report from the report data.

    Args:
        portfolio_name: Name of the portfolio
        report_type: Type of the report
        report_date: Date of the report
        report_data: Data to include in the report

    Returns:
        BytesIO: Excel file as a bytes buffer
    """
    wb = load_excel_template(report_type)
    
    # Handle specialized report types
    if report_type.lower() == "ecl_detailed_report":
        return populate_ecl_detailed_report(wb, portfolio_name, report_date, report_data)
    elif report_type.lower() == "local_impairment_details_report":
        return populate_local_impairment_details_report(wb, portfolio_name, report_date, report_data)
    elif report_type.lower() == "ecl_report_summarised":
        return populate_ecl_report_summarised(wb, portfolio_name, report_date, report_data)
    elif report_type.lower() == "local_impairment_report_summarised":
        return populate_local_impairment_report_summarised(wb, portfolio_name, report_date, report_data)
    elif report_type.lower() == "journals_report":
        return populate_journal_report(wb, portfolio_name, report_date, report_data)
    
    # Default handling for other report types
    ws = wb.active
    ws.title = "Summary"
    
    # Define styles
    title_font = Font(name='Calibri', size=16, bold=True)
    subtitle_font = Font(name='Calibri', size=14, bold=True)
    header_font = Font(name='Calibri', size=12, bold=True)
    
    header_fill = PatternFill(start_color="D3D3D3", end_color="D3D3D3", fill_type="solid")
    
    thin_border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    
    # Add title
    ws['A1'] = f"{portfolio_name} - {report_type} Report"
    ws['A1'].font = title_font
    ws.merge_cells('A1:D1')
    ws['A1'].alignment = Alignment(horizontal='center')
    
    ws['A2'] = f"Generated on: {report_date.strftime('%B %d, %Y')}"
    ws['A2'].font = subtitle_font
    ws.merge_cells('A2:D2')
    ws['A2'].alignment = Alignment(horizontal='center')
    
    # Row counter
    current_row = 4
    
    # Process the summary data
    if isinstance(report_data, dict):
        ws.cell(row=current_row, column=1, value="Summary")
        ws.cell(row=current_row, column=1).font = subtitle_font
        current_row += 1
        
        summary_data = []
        chart_data = {}
        
        # First pass: extract summary-level data
        for key, value in report_data.items():
            if key == 'reporting_date' or isinstance(value, (dict, list)):
                continue
                
            # Format the key for display
            display_key = " ".join(word.capitalize() for word in key.split("_"))
            
            ws.cell(row=current_row, column=1, value=display_key)
            ws.cell(row=current_row, column=1).border = thin_border
            ws.cell(row=current_row, column=1).font = header_font
            
            # Format the value based on its type
            if isinstance(value, (int, float)):
                ws.cell(row=current_row, column=2, value=value)
                if isinstance(value, float):
                    ws.cell(row=current_row, column=2).number_format = '#,##0.00'
                else:
                    ws.cell(row=current_row, column=2).number_format = '#,##0'
            else:
                ws.cell(row=current_row, column=2, value=str(value))
                
            ws.cell(row=current_row, column=2).border = thin_border
            current_row += 1
            
        current_row += 2
        
        # Second pass: process distributions and charts
        for key, value in report_data.items():
            if isinstance(value, dict) and "_distribution" in key:
                # Create a separate sheet for this distribution
                chart_sheet = wb.create_sheet(title=key.replace('_', ' ').title())
                
                chart_sheet['A1'] = f"{key.replace('_', ' ').title()}"
                chart_sheet['A1'].font = subtitle_font
                
                # Add distribution data for charting
                chart_sheet['A3'] = "Category"
                chart_sheet['B3'] = "Count"
                chart_sheet['A3'].font = header_font
                chart_sheet['B3'].font = header_font
                
                row_idx = 4
                for cat_key, cat_value in value.items():
                    chart_sheet.cell(row=row_idx, column=1, value=cat_key)
                    chart_sheet.cell(row=row_idx, column=2, value=cat_value)
                    row_idx += 1
                
                # Create chart
                if len(value) > 0:
                    chart = None
                    if len(value) <= 6:  # Use pie chart for fewer categories
                        chart = PieChart()
                        data = Reference(chart_sheet, min_col=2, min_row=3, max_row=row_idx-1)
                        cats = Reference(chart_sheet, min_col=1, min_row=4, max_row=row_idx-1)
                        chart.add_data(data, titles_from_data=True)
                        chart.set_categories(cats)
                        chart.title = key.replace('_', ' ').title()
                    else:  # Use bar chart for more categories
                        chart = BarChart()
                        data = Reference(chart_sheet, min_col=2, min_row=3, max_row=row_idx-1)
                        cats = Reference(chart_sheet, min_col=1, min_row=4, max_row=row_idx-1)
                        chart.add_data(data, titles_from_data=True)
                        chart.set_categories(cats)
                        chart.title = key.replace('_', ' ').title()
                        
                    chart_sheet.add_chart(chart, "D3")
                
                # Add a reference to the distribution in the summary
                ws.cell(row=current_row, column=1, value=f"{key.replace('_', ' ').title()}")
                ws.cell(row=current_row, column=1).font = subtitle_font
                ws.cell(row=current_row, column=2, value=f"See '{chart_sheet.title}' sheet")
                current_row += 2
            
            # Process detailed data lists
            elif isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                # Create a separate sheet for this list
                list_sheet = wb.create_sheet(title=key.replace('_', ' ').title()[:31])  # Excel has a 31 char limit for sheet names
                
                list_sheet['A1'] = f"{key.replace('_', ' ').title()}"
                list_sheet['A1'].font = subtitle_font
                
                # Extract headers
                headers = list(value[0].keys())
                
                # Write headers
                for col_idx, header in enumerate(headers, 1):
                    display_header = " ".join(word.capitalize() for word in header.split("_"))
                    list_sheet.cell(row=3, column=col_idx, value=display_header)
                    list_sheet.cell(row=3, column=col_idx).font = header_font
                    list_sheet.cell(row=3, column=col_idx).fill = header_fill
                    
                # Write data rows
                for row_idx, item in enumerate(value, 4):
                    for col_idx, header in enumerate(headers, 1):
                        cell_value = item.get(header, "")
                        
                        if isinstance(cell_value, (int, float)):
                            list_sheet.cell(row=row_idx, column=col_idx, value=cell_value)
                            if isinstance(cell_value, float):
                                list_sheet.cell(row=row_idx, column=col_idx).number_format = '#,##0.00'
                            else:
                                list_sheet.cell(row=row_idx, column=col_idx).number_format = '#,##0'
                        elif isinstance(cell_value, date):
                            list_sheet.cell(row=row_idx, column=col_idx, value=cell_value)
                            list_sheet.cell(row=row_idx, column=col_idx).number_format = 'yyyy-mm-dd'
                        else:
                            list_sheet.cell(row=row_idx, column=col_idx, value=str(cell_value))
                
                # Auto-size columns
                for col_idx, _ in enumerate(headers, 1):
                    col_letter = get_column_letter(col_idx)
                    list_sheet.column_dimensions[col_letter].auto_size = True
                
                # Add a reference to the list in the summary
                ws.cell(row=current_row, column=1, value=f"{key.replace('_', ' ').title()}")
                ws.cell(row=current_row, column=1).font = subtitle_font
                ws.cell(row=current_row, column=2, value=f"See '{list_sheet.title}' sheet")
                current_row += 2
                
            # Process other nested dictionaries (that aren't distributions)
            elif isinstance(value, dict) and not "_distribution" in key:
                section_title = key.replace('_', ' ').title()
                ws.cell(row=current_row, column=1, value=section_title)
                ws.cell(row=current_row, column=1).font = subtitle_font
                current_row += 1
                
                for sub_key, sub_value in value.items():
                    display_sub_key = " ".join(word.capitalize() for word in sub_key.split("_"))
                    
                    ws.cell(row=current_row, column=1, value=display_sub_key)
                    ws.cell(row=current_row, column=1).border = thin_border
                    
                    if isinstance(sub_value, (int, float)):
                        ws.cell(row=current_row, column=2, value=sub_value)
                        if isinstance(sub_value, float):
                            ws.cell(row=current_row, column=2).number_format = '#,##0.00'
                        else:
                            ws.cell(row=current_row, column=2).number_format = '#,##0'
                    else:
                        ws.cell(row=current_row, column=2, value=str(sub_value))
                        
                    ws.cell(row=current_row, column=2).border = thin_border
                    current_row += 1
                
                current_row += 1
    
    # Auto-size columns in summary sheet
    for col in ['A', 'B', 'C', 'D']:
        ws.column_dimensions[col].auto_size = True
    
    # Save to BytesIO
    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return buffer


def populate_ecl_detailed_report(
    wb: Workbook, 
    portfolio_name: str, 
    report_date: date, 
    report_data: Dict[str, Any]
) -> BytesIO:
    """
    Populate the ECL detailed report template with data.
    
    Args:
        wb: Excel workbook (template)
        portfolio_name: Name of the portfolio
        report_date: Date of the report
        report_data: Data for the report
        
    Returns:
        BytesIO: Excel file as bytes buffer
    """
    # Get the main worksheet (should be the first one)
    ws = wb.active
    
    # Define number formats
    currency_format = '#,##0.00'
    percentage_format = '0.00%'
    
    # Populate header information
    ws['B3'] = report_date
    ws['B4'] = report_data.get('report_run_date', datetime.now().strftime("%Y-%m-%d"))
    ws['B6'] = report_data.get('description', f"ECL Detailed Report for {portfolio_name}")
    
    # Populate summary values
    ws['B9'] = report_data.get('total_ead', 0)
    ws['B9'].number_format = currency_format
    
    ws['B10'] = report_data.get('total_lgd', 0)
    ws['B10'].number_format = currency_format
    
    ws['B12'] = report_data.get('total_ecl', 0)
    ws['B12'].number_format = currency_format
    
    # Populate loan details starting from row 15
    loans = report_data.get('loans', [])
    start_row = 15
    
    for i, loan in enumerate(loans, start=0):
        row = start_row + i
        
        # Map loan data to columns
        ws.cell(row=row, column=1, value=loan.get('loan_id', ''))
        ws.cell(row=row, column=2, value=loan.get('employee_id', ''))
        ws.cell(row=row, column=3, value=loan.get('employee_name', ''))
        
        # Loan value with currency format
        ws.cell(row=row, column=4, value=loan.get('loan_value', 0))
        ws.cell(row=row, column=4).number_format = currency_format
        
        # Outstanding loan balance with currency format
        ws.cell(row=row, column=5, value=loan.get('outstanding_loan_balance', 0))
        ws.cell(row=row, column=5).number_format = currency_format
        
        # Accumulated arrears with currency format
        ws.cell(row=row, column=6, value=loan.get('accumulated_arrears', 0))
        ws.cell(row=row, column=6).number_format = currency_format
        
        # NDIA with currency format
        ws.cell(row=row, column=7, value=loan.get('ndia', 0))
        ws.cell(row=row, column=7).number_format = currency_format
        
        # Stage
        ws.cell(row=row, column=8, value=loan.get('stage', ''))
        
        # EAD with currency format
        ws.cell(row=row, column=9, value=loan.get('ead', 0))
        ws.cell(row=row, column=9).number_format = currency_format
        
        # LGD as percentage
        ws.cell(row=row, column=10, value=loan.get('lgd', 0))
        ws.cell(row=row, column=10).number_format = percentage_format
        
        # EIR as percentage
        ws.cell(row=row, column=11, value=loan.get('eir', 0))
        ws.cell(row=row, column=11).number_format = percentage_format
        
        # PD as percentage
        ws.cell(row=row, column=12, value=loan.get('pd', 0))
        ws.cell(row=row, column=12).number_format = percentage_format
        
        # ECL with currency format
        ws.cell(row=row, column=13, value=loan.get('ecl', 0))
        ws.cell(row=row, column=13).number_format = currency_format
    
    # Save to BytesIO
    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return buffer


def populate_ecl_report_summarised(
    wb: Workbook, 
    portfolio_name: str, 
    report_date: date, 
    report_data: Dict[str, Any]
) -> BytesIO:
    """
    Populate the ECL summarised report template with data.
    
    Args:
        wb: Excel workbook (template)
        portfolio_name: Name of the portfolio
        report_date: Date of the report
        report_data: Data for the report
        
    Returns:
        BytesIO: Excel file as bytes buffer
    """
    # Get the main worksheet (should be the first one)
    ws = wb.active
    
    # Define number formats
    currency_format = '#,##0.00'
    percentage_format = '0.00%'
    
    # Populate header information
    ws['B3'] = report_date
    ws['B4'] = report_data.get('report_run_date', datetime.now().date())
    ws['B6'] = report_data.get('description', f"ECL Summarised Report for {portfolio_name}")
    
    # Populate stage data
    # Loan values
    ws['C18'] = report_data.get('stage_1', {}).get('loan_value', 0)
    ws['D18'] = report_data.get('stage_2', {}).get('loan_value', 0)
    ws['E18'] = report_data.get('stage_3', {}).get('loan_value', 0)
    
    # Apply currency format to loan values
    for col in ['C', 'D', 'E']:
        ws[f'{col}18'].number_format = currency_format
    
    # Outstanding loan balances
    ws['C19'] = report_data.get('stage_1', {}).get('outstanding_balance', 0)
    ws['D19'] = report_data.get('stage_2', {}).get('outstanding_balance', 0)
    ws['E19'] = report_data.get('stage_3', {}).get('outstanding_balance', 0)
    
    # Apply currency format to outstanding balances
    for col in ['C', 'D', 'E']:
        ws[f'{col}19'].number_format = currency_format
    
    # ECL amounts
    ws['C20'] = report_data.get('stage_1', {}).get('ecl', 0)
    ws['D20'] = report_data.get('stage_2', {}).get('ecl', 0)
    ws['E20'] = report_data.get('stage_3', {}).get('ecl', 0)
    
    # Apply currency format to ECL amounts
    for col in ['C', 'D', 'E']:
        ws[f'{col}20'].number_format = currency_format
    
    # Save to BytesIO
    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return buffer


def populate_local_impairment_details_report(
    wb: Workbook, 
    portfolio_name: str, 
    report_date: date, 
    report_data: Dict[str, Any]
) -> BytesIO:
    """
    Populate the local impairment detailed report template with data.
    
    Args:
        wb: Excel workbook (template)
        portfolio_name: Name of the portfolio
        report_date: Date of the report
        report_data: Data for the report
        
    Returns:
        BytesIO: Excel file as bytes buffer
    """
    # Get the main worksheet (should be the first one)
    ws = wb.active
    
    # Define number formats
    currency_format = '#,##0.00'
    percentage_format = '0.00%'
    
    # Populate header information
    ws['B3'] = report_date
    ws['B4'] = report_data.get('report_run_date', datetime.now().strftime("%Y-%m-%d"))
    ws['B6'] = report_data.get('description', f"Local Impairment Details Report for {portfolio_name}")
    
    # Populate total provision
    ws['B12'] = report_data.get('total_provision', 0)
    ws['B12'].number_format = currency_format
    
    # Populate loan details starting from row 15
    loans = report_data.get('loans', [])
    start_row = 15
    
    for i, loan in enumerate(loans, start=0):
        row = start_row + i
        
        # Map loan data to columns
        ws.cell(row=row, column=1, value=loan.get('loan_id', ''))
        ws.cell(row=row, column=2, value=loan.get('employee_id', ''))
        ws.cell(row=row, column=3, value=loan.get('employee_name', ''))
        
        # Loan value with currency format
        ws.cell(row=row, column=4, value=loan.get('loan_value', 0))
        ws.cell(row=row, column=4).number_format = currency_format
        
        # Outstanding loan balance with currency format
        ws.cell(row=row, column=5, value=loan.get('outstanding_loan_balance', 0))
        ws.cell(row=row, column=5).number_format = currency_format
        
        # Accumulated arrears with currency format
        ws.cell(row=row, column=6, value=loan.get('accumulated_arrears', 0))
        ws.cell(row=row, column=6).number_format = currency_format
        
        # NDIA with currency format
        ws.cell(row=row, column=7, value=loan.get('ndia', 0))
        ws.cell(row=row, column=7).number_format = currency_format
        
        # Stage (local impairment category)
        ws.cell(row=row, column=8, value=loan.get('stage', ''))
        
        # Provision rate as percentage
        ws.cell(row=row, column=9, value=loan.get('provision_rate', 0))
        ws.cell(row=row, column=9).number_format = percentage_format
        
        # Provision amount with currency format
        ws.cell(row=row, column=10, value=loan.get('provision', 0))
        ws.cell(row=row, column=10).number_format = currency_format
    
    # Save to BytesIO
    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return buffer


def populate_local_impairment_report_summarised(
    wb: Workbook, 
    portfolio_name: str, 
    report_date: date, 
    report_data: Dict[str, Any]
) -> BytesIO:
    """
    Populate the local impairment summarised report template with data.
    
    Args:
        wb: Excel workbook (template)
        portfolio_name: Name of the portfolio
        report_date: Date of the report
        report_data: Data for the report
        
    Returns:
        BytesIO: Excel file as bytes buffer
    """
    # Get the main worksheet (should be the first one)
    ws = wb.active
    
    # Define number formats
    currency_format = '#,##0.00'
    percentage_format = '0.00%'
    
    # Populate header information
    ws['B3'] = report_date
    ws['B4'] = report_data.get('report_run_date', datetime.now().date())
    ws['B6'] = report_data.get('description', f"Local Impairment Summarised Report for {portfolio_name}")
    
    # Populate category data
    # Loan values
    ws['C18'] = report_data.get('current', {}).get('loan_value', 0)
    ws['D18'] = report_data.get('olem', {}).get('loan_value', 0)
    ws['E18'] = report_data.get('substandard', {}).get('loan_value', 0)
    ws['F18'] = report_data.get('doubtful', {}).get('loan_value', 0)
    ws['G18'] = report_data.get('loss', {}).get('loan_value', 0)
    
    # Apply currency format to loan values
    for col in ['C', 'D', 'E', 'F', 'G']:
        ws[f'{col}18'].number_format = currency_format
    
    # Outstanding loan balances
    ws['C19'] = report_data.get('current', {}).get('outstanding_balance', 0)
    ws['D19'] = report_data.get('olem', {}).get('outstanding_balance', 0)
    ws['E19'] = report_data.get('substandard', {}).get('outstanding_balance', 0)
    ws['F19'] = report_data.get('doubtful', {}).get('outstanding_balance', 0)
    ws['G19'] = report_data.get('loss', {}).get('outstanding_balance', 0)
    
    # Apply currency format to outstanding balances
    for col in ['C', 'D', 'E', 'F', 'G']:
        ws[f'{col}19'].number_format = currency_format
    
    # Provision amounts
    ws['C20'] = report_data.get('current', {}).get('provision', 0)
    ws['D20'] = report_data.get('olem', {}).get('provision', 0)
    ws['E20'] = report_data.get('substandard', {}).get('provision', 0)
    ws['F20'] = report_data.get('doubtful', {}).get('provision', 0)
    ws['G20'] = report_data.get('loss', {}).get('provision', 0)
    
    # Apply currency format to provision amounts
    for col in ['C', 'D', 'E', 'F', 'G']:
        ws[f'{col}20'].number_format = currency_format
    
    # Save to BytesIO
    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return buffer


def populate_journal_report(
    wb: Workbook, 
    portfolio_name: str, 
    report_date: date, 
    report_data: Dict[str, Any]
) -> BytesIO:
    """
    Populate the journal report template with data.
    
    Args:
        wb: Excel workbook (template)
        portfolio_name: Name of the portfolio (not used in this report as it handles multiple portfolios)
        report_date: Date of the report
        report_data: Data for the report
        
    Returns:
        BytesIO: Excel file as bytes buffer
    """
    # Get the main worksheet (should be the first one)
    ws = wb.active
    
    # Define styles
    bold_font = Font(name='Calibri', size=10, bold=True)
    
    # Define border for header cells
    thin_border = Border(
        bottom=Side(style='thin')
    )
    
    # Define number formats
    currency_format = '#,##0.00_);(#,##0.00)'
    
    # Populate header information
    ws['B3'] = report_date
    ws['B4'] = report_data.get('report_run_date', datetime.now().date())
    ws['B6'] = report_data.get('description', "Journal Report")
    
    # Get portfolios data
    portfolios = report_data.get('portfolios', [])
    
    # Start row for the first portfolio
    current_row = 8
    
    # Process each portfolio
    for index, portfolio in enumerate(portfolios, 1):
        # Portfolio header
        ws[f'A{current_row}'] = f"Portfolio {index}"
        # ws[f'A{current_row}'].font = bold_font
        current_row += 1
        
        # Column headers
        ws[f'A{current_row}'] = "Account code"
        ws[f'B{current_row}'] = "Journal description"
        ws[f'C{current_row}'] = "Amount (GHS)"
        
        # Apply bold and underline to headers
        for col in ['A', 'B', 'C']:
            ws[f'{col}{current_row}'].font = bold_font
            ws[f'{col}{current_row}'].border = thin_border
        
        current_row += 1
        
        # Journal entries
        # IFRS9 Impairment - P&L charge
        ws[f'A{current_row}'] = portfolio.get('ecl_impairment_account', '')
        ws[f'B{current_row}'] = "IFRS9 Impairment - P&l charge"
        ws[f'C{current_row}'] = portfolio.get('total_ecl', 0)
        ws[f'C{current_row}'].number_format = currency_format
        current_row += 1
        
        # IFRS9 Impairment - impact on loans
        ws[f'A{current_row}'] = portfolio.get('loan_assets', '')
        ws[f'B{current_row}'] = "IFRS9 Impairment - impact on loans"
        ws[f'C{current_row}'] = -portfolio.get('total_ecl', 0)  # Negative value
        ws[f'C{current_row}'].number_format = currency_format
        current_row += 1
        
        # Top up for BOG Impairment - P&L charge
        ws[f'A{current_row}'] = portfolio.get('ecl_impairment_account', '')
        ws[f'B{current_row}'] = "Top up for BOG Impairment - P&l charge"
        ws[f'C{current_row}'] = portfolio.get('risk_reserve', 0)
        ws[f'C{current_row}'].number_format = currency_format
        current_row += 1
        
        # Credit risk reserve
        ws[f'A{current_row}'] = portfolio.get('credit_risk_reserve', '')
        ws[f'B{current_row}'] = "Credit risk reserve"
        ws[f'C{current_row}'] = -portfolio.get('risk_reserve', 0)  # Negative value
        ws[f'C{current_row}'].number_format = currency_format
        current_row += 1
        
        # Skip a row before the next portfolio
        current_row += 1
    
    # Save to BytesIO
    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return buffer


def load_excel_template(report_type: str) -> Workbook:
    """
    Load an Excel template for a specific report type.
    
    Args:
        report_type: Type of the report
        
    Returns:
        Workbook: Excel workbook template
    """
    import os
    from openpyxl import load_workbook
    
    # Define template paths
    template_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates", "reports")
    
    # Map report types to template files
    template_map = {
        "ecl_detailed_report": "ecl_detailed_report.xlsx",
        "ecl_report_summarised": "ecl_report_summarised.xlsx",
        "local_impairment_details_report": "local_impairment_details_report.xlsx",
        "local_impairment_report_summarised": "local_impairment_report_summarised.xlsx",
        "journals_report": "journals_report.xlsx",
    }
    
    # Get the template file name
    template_file = template_map.get(report_type.lower())
    
    # If we have a template for this report type, load it
    if template_file and os.path.exists(os.path.join(template_dir, template_file)):
        return load_workbook(os.path.join(template_dir, template_file))
    
    # If no template exists, return a new workbook
    return Workbook()
