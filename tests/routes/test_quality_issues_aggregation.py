import pytest
from app.models import QualityIssue

def test_affected_records_aggregation(client, portfolio, db_session, tenant):
    """
    Test that affected_records from multiple issues of the same type and severity
    are correctly aggregated and included in the summary response.
    """
    # Create two issues of the same type and severity
    issue1 = QualityIssue(
        portfolio_id=portfolio.id,
        tenant_id=tenant.id,
        issue_type="duplicate_customer_ids",
        description="Duplicate customer IDs group 1",
        severity="high",
        status="open",
        affected_records=[{"customer_id": "101", "name": "John Doe"}]
    )
    issue2 = QualityIssue(
        portfolio_id=portfolio.id,
        tenant_id=tenant.id,
        issue_type="duplicate_customer_ids",
        description="Duplicate customer IDs group 2",
        severity="high",
        status="open",
        affected_records=[{"customer_id": "102", "name": "Jane Smith"}]
    )
    
    db_session.add(issue1)
    db_session.add(issue2)
    db_session.commit()
    
    # Call the endpoint
    response = client.get(f"/portfolios/{portfolio.id}/quality-issues")
    assert response.status_code == 200
    
    data = response.json()
    # We should have one aggregated entry for duplicate_customer_ids (high)
    # Note: If quality_issue fixture was used, there might be more, but we are using fresh ones here?
    # Actually, the fixture might have already created one.
    
    target_summary = None
    for item in data:
        if item["issue_type"] == "duplicate_customer_ids" and item["severity"] == "high":
            target_summary = item
            break
            
    assert target_summary is not None
    assert "affected_records" in target_summary
    
    # Verify both records are present
    ids = [rec["customer_id"] for rec in target_summary["affected_records"]]
    assert "101" in ids
    assert "102" in ids
    assert len(target_summary["affected_records"]) >= 2
