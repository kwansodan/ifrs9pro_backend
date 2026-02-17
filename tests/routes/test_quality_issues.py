import pytest
from app.models import Portfolio, QualityIssue, QualityIssueComment


@pytest.fixture
def portfolio(db_session, regular_user, tenant):
    """Create a test portfolio"""
    portfolio = Portfolio(
        name="Test Portfolio",
        user_id=regular_user.id,
        tenant_id=tenant.id,
        description="Test description",
    )
    db_session.add(portfolio)
    db_session.commit()
    db_session.refresh(portfolio)
    return portfolio


@pytest.fixture
def quality_issue(db_session, portfolio, tenant):
    """Create a test quality issue"""
    issue = QualityIssue(
        portfolio_id=portfolio.id,
        tenant_id=tenant.id,
        issue_type="duplicate_customer_ids",
        description="Test duplicate customer IDs found",
        severity="high",
        status="open",
        affected_records=[{"customer_id": "123", "count": 2}],
    )
    db_session.add(issue)
    db_session.commit()
    db_session.refresh(issue)
    return issue


@pytest.fixture
def quality_issue_comment(db_session, quality_issue, regular_user):
    """Create a test comment"""
    comment = QualityIssueComment(
        quality_issue_id=quality_issue.id,
        user_id=regular_user.id,
        comment="Test comment",
    )
    db_session.add(comment)
    db_session.commit()
    db_session.refresh(comment)
    return comment


def test_get_quality_issues(client, portfolio, quality_issue):
    response = client.get(f"/portfolios/{portfolio.id}/quality-issues")

    assert response.status_code == 200

    data = response.json()
    assert len(data) == 1  # since only one issue exists

    issue = data[0]

    assert issue["description"] == quality_issue.description
    assert issue["occurrence_count"] == 1
    assert quality_issue.status in issue["statuses"]

def test_get_quality_issues_with_status_filter(client, portfolio, quality_issue):
    """Test getting quality issues filtered by status"""
    response = client.get(f"/portfolios/{portfolio.id}/quality-issues?status_type=open")
    assert response.status_code == 200


def test_get_quality_issues_with_type_filter(client, portfolio, quality_issue):
    """Test getting quality issues filtered by issue type"""
    response = client.get(
        f"/portfolios/{portfolio.id}/quality-issues?issue_type=duplicate_customer_ids"
    )
    assert response.status_code == 200


def test_download_all_quality_issues_excel(client, portfolio, quality_issue):
    """Test downloading all quality issues as Excel"""
    response = client.get(f"/portfolios/{portfolio.id}/quality-issues/download")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def test_download_all_quality_issues_with_comments(client, portfolio, quality_issue, quality_issue_comment):
    """Test downloading quality issues with comments included"""
    response = client.get(
        f"/portfolios/{portfolio.id}/quality-issues/download?include_comments=true"
    )
    assert response.status_code == 200


def test_download_all_quality_issues_with_filters(client, portfolio, quality_issue):
    """Test downloading quality issues with filters applied"""
    response = client.get(
        f"/portfolios/{portfolio.id}/quality-issues/download?status_type=open&issue_type=duplicate_customer_ids"
    )
    assert response.status_code == 200


def test_get_quality_issue(client, portfolio, quality_issue):
    """Test getting a specific quality issue"""
    response = client.get(
        f"/portfolios/{portfolio.id}/quality-issues/{quality_issue.id}"
    )
    assert response.status_code == 200
    assert response.json()["id"] == quality_issue.id


def test_update_quality_issue(client, portfolio, quality_issue):
    """Test updating a quality issue"""
    response = client.put(
        f"/portfolios/{portfolio.id}/quality-issues/{quality_issue.id}",
        json={"status": "approved"},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "approved"


def test_add_comment_to_quality_issue(client, portfolio, quality_issue):
    """Test adding a comment to a quality issue"""
    response = client.post(
        f"/portfolios/{portfolio.id}/quality-issues/{quality_issue.id}/comments",
        json={"comment": "This is a test comment"},
    )
    assert response.status_code == 200
    assert response.json()["comment"] == "This is a test comment"


def test_get_quality_issue_comments(client, portfolio, quality_issue, quality_issue_comment):
    """Test getting all comments for a quality issue"""
    response = client.get(
        f"/portfolios/{portfolio.id}/quality-issues/{quality_issue.id}/comments"
    )
    assert response.status_code == 200
    assert len(response.json()) > 0


def test_edit_quality_issue_comment(client, portfolio, quality_issue, quality_issue_comment):
    """Test editing a comment on a quality issue"""
    response = client.put(
        f"/portfolios/{portfolio.id}/quality-issues/{quality_issue.id}/comments/{quality_issue_comment.id}",
        json={"comment": "Updated comment text"},
    )
    assert response.status_code == 200
    assert response.json()["comment"] == "Updated comment text"


def test_approve_quality_issue(client, portfolio, quality_issue):
    """Test approving a quality issue"""
    response = client.post(
        f"/portfolios/{portfolio.id}/quality-issues/{quality_issue.id}/approve"
    )
    assert response.status_code == 200
    assert response.json()["status"] == "approved"


def test_approve_quality_issue_with_comment(client, portfolio, quality_issue):
    """Test approving a quality issue with a comment"""
    response = client.post(
        f"/portfolios/{portfolio.id}/quality-issues/{quality_issue.id}/approve?comment=Looks good"
    )
    assert response.status_code == 200
    assert response.json()["status"] == "approved"


def test_approve_all_quality_issues(client, portfolio, quality_issue):
    """Test approving all quality issues in a portfolio"""
    response = client.post(
        f"/portfolios/{portfolio.id}/approve-all-quality-issues"
    )
    assert response.status_code == 200
    assert response.json()["count"] > 0


def test_approve_all_quality_issues_with_comment(client, portfolio, quality_issue):
    """Test approving all quality issues with a comment"""
    response = client.post(
        f"/portfolios/{portfolio.id}/approve-all-quality-issues?comment=Batch approval"
    )
    assert response.status_code == 200


def test_recheck_quality_issues(client, portfolio, monkeypatch):
    """Test rechecking quality issues for a portfolio"""
    # Mock the quality check function to return sample data
    def mock_quality_checks(*args, **kwargs):
        return {
            "duplicate_customer_ids": 0,
            "duplicate_addresses": 0,
            "duplicate_dob": 0,
            "duplicate_loan_ids": 0,
            "clients_without_matching_loans": 0,
            "loans_without_matching_clients": 0,
            "missing_dob": 0,
            "total_issues": 0,
            "high_severity_issues": 0,
            "open_issues": 0,
        }
    
    monkeypatch.setattr(
        "app.routes.quality_issues.create_quality_issues_if_needed",
        mock_quality_checks,
    )
    
    response = client.post(f"/portfolios/{portfolio.id}/recheck-quality")
    assert response.status_code == 200


def test_download_quality_issue_excel(client, portfolio, quality_issue):
    """Test downloading a specific quality issue as Excel"""
    response = client.get(
        f"/portfolios/{portfolio.id}/quality-issues/{quality_issue.id}/download"
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def test_download_quality_issue_without_comments(client, portfolio, quality_issue):
    """Test downloading a quality issue without comments"""
    response = client.get(
        f"/portfolios/{portfolio.id}/quality-issues/{quality_issue.id}/download?include_comments=false"
    )
    assert response.status_code == 200