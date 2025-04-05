from datetime import date
from typing import Dict, Any, List
from io import BytesIO
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.chart import BarChart, Reference, PieChart


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
    wb = Workbook()
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
