import pytest
import pandas as pd
import polars as pl
from unittest.mock import MagicMock
from app.models import QualityIssue, Portfolio

def test_duplicate_loan_id_deduplication():
    # Simulate a Polars DataFrame with duplicate loan_nos
    data = {
        "loan_no": ["L001", "L002", "L001", "L003"],
        "loan_amount": [1000, 2000, 1500, 3000],
        "other_data": ["First", "Unique", "Duplicate", "Unique"]
    }
    df = pl.DataFrame(data)
    
    # Deduplicate by loan_no (keep first occurrence) - Logic from sync_processors.py
    initial_count = df.height
    df_dedup = df.unique(subset=["loan_no"], keep="first", maintain_order=True)
    final_count = df_dedup.height
    
    assert initial_count == 4
    assert final_count == 3
    
    # Verify we kept the first L001
    l001 = df_dedup.filter(pl.col("loan_no") == "L001")
    assert l001.select("other_data").item() == "First"

def test_quality_issue_tenant_id_population():
    # Mock DB session and Portfolio
    mock_db = MagicMock()
    mock_portfolio = MagicMock(spec=Portfolio)
    mock_portfolio.id = 1
    mock_portfolio.tenant_id = 999
    
    mock_db.query.return_value.filter.return_value.first.return_value = mock_portfolio
    
    # Simulate logic in create_and_save_quality_issues
    portfolio_id = 1
    portfolio = mock_db.query(Portfolio).filter(Portfolio.id == portfolio_id).first()
    tenant_id = portfolio.tenant_id
    
    issue = QualityIssue(
        portfolio_id=portfolio_id,
        tenant_id=tenant_id,
        issue_type="test_issue",
        description="Test description",
        affected_records=[],
        severity="low"
    )
    
    assert issue.tenant_id == 999
    assert issue.portfolio_id == 1
