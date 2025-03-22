from datetime import date
from typing import Dict, Any
import os
from io import BytesIO
import tempfile
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.lib.units import inch

def create_report_pdf(
    portfolio_name: str,
    report_type: str,
    report_date: date,
    report_data: Dict[str, Any],
) -> BytesIO:
    """
    Generate a PDF report from the report data.
    
    Args:
        portfolio_name: Name of the portfolio
        report_type: Type of the report
        report_date: Date of the report
        report_data: Data to include in the report
    
    Returns:
        BytesIO: PDF file as a bytes buffer
    """
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    
    # Create custom styles
    title_style = ParagraphStyle(
        'Title',
        parent=styles['Heading1'],
        fontSize=16,
        spaceAfter=12
    )
    
    subtitle_style = ParagraphStyle(
        'Subtitle',
        parent=styles['Heading2'],
        fontSize=14,
        spaceAfter=10
    )
    
    normal_style = styles["Normal"]
    
    # Build the content
    content = []
    
    # Add title
    content.append(Paragraph(f"{portfolio_name} - {report_type} Report", title_style))
    content.append(Paragraph(f"Generated on: {report_date.strftime('%B %d, %Y')}", subtitle_style))
    content.append(Spacer(1, 0.25*inch))
    
    # Process the report data based on report type
    # This is a simplified version - you'll need to adapt it based on your actual data structure
    
    if isinstance(report_data, dict):
        # Add summary section
        content.append(Paragraph("Summary", subtitle_style))
        
        # Create a table for summary data
        summary_data = []
        for key, value in report_data.items():
            if isinstance(value, dict):
                continue  # Skip nested dictionaries for the summary table
            
            # Format the key for display
            display_key = " ".join(word.capitalize() for word in key.split("_"))
            
            # Format the value based on its type
            if isinstance(value, (int, float)):
                display_value = f"{value:,.2f}" if isinstance(value, float) else f"{value:,}"
            elif isinstance(value, date):
                display_value = value.strftime("%B %d, %Y")
            else:
                display_value = str(value)
                
            summary_data.append([display_key, display_value])
        
        if summary_data:
            table = Table(summary_data, colWidths=[3*inch, 3*inch])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('PADDING', (0, 0), (-1, -1), 6),
            ]))
            content.append(table)
            content.append(Spacer(1, 0.25*inch))
        
        # Process detailed sections
        for key, value in report_data.items():
            if isinstance(value, dict):
                content.append(Paragraph(f"{key.replace('_', ' ').title()}", subtitle_style))
                
                # Create a table for the detailed section
                detail_data = []
                for sub_key, sub_value in value.items():
                    display_sub_key = " ".join(word.capitalize() for word in sub_key.split("_"))
                    
                    if isinstance(sub_value, (int, float)):
                        display_sub_value = f"{sub_value:,.2f}" if isinstance(sub_value, float) else f"{sub_value:,}"
                    elif isinstance(sub_value, date):
                        display_sub_value = sub_value.strftime("%B %d, %Y")
                    else:
                        display_sub_value = str(sub_value)
                        
                    detail_data.append([display_sub_key, display_sub_value])
                
                if detail_data:
                    table = Table(detail_data, colWidths=[3*inch, 3*inch])
                    table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
                        ('GRID', (0, 0), (-1, -1), 1, colors.black),
                        ('PADDING', (0, 0), (-1, -1), 6),
                    ]))
                    content.append(table)
                    content.append(Spacer(1, 0.25*inch))
    
    # If we have a list of items
    elif isinstance(report_data, list):
        if report_data and isinstance(report_data[0], dict):
            # Get headers from the first item
            headers = list(report_data[0].keys())
            display_headers = [" ".join(word.capitalize() for word in header.split("_")) for header in headers]
            
            # Build table data
            table_data = [display_headers]
            for item in report_data:
                row = []
                for header in headers:
                    value = item.get(header, "")
                    if isinstance(value, (int, float)):
                        display_value = f"{value:,.2f}" if isinstance(value, float) else f"{value:,}"
                    elif isinstance(value, date):
                        display_value = value.strftime("%B %d, %Y")
                    else:
                        display_value = str(value)
                    row.append(display_value)
                table_data.append(row)
            
            # Create table
            col_widths = [1.5*inch] * len(headers)
            table = Table(table_data, colWidths=col_widths)
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('PADDING', (0, 0), (-1, -1), 6),
            ]))
            content.append(table)
    
    # Add a footer
    content.append(Spacer(1, 0.5*inch))
    content.append(Paragraph("Generated by Portfolio Report System", normal_style))
    
    # Build the PDF
    doc.build(content)
    buffer.seek(0)
    return buffer

