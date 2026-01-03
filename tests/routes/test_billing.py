import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException, status
from datetime import datetime, timezone

from app.routes.billing import (
    list_plans,
    create_customer,
    get_my_customer,
    initialize_transaction,
    disable_subscription,
    get_subscription,
    verify_transaction,
)
from app.models import User, TenantSubscription, Tenant


@pytest.fixture
def mock_user():
    """Create a mock authenticated user"""
    user = MagicMock(spec=User)
    user.id = 1
    user.email = "test@example.com"
    user.is_admin = True
    user.phone_number = "1234567890"
    user.first_name = "Test"
    user.last_name = "Admin"
    
    # Mock tenant
    tenant = MagicMock(spec=Tenant)
    tenant.id = 1
    tenant.name = "Test Corp"
    tenant.industry = "Tech"
    tenant.country = "Ghana"
    tenant.subscription_id = 1
    
    user.tenant = tenant
    user.tenant_id = 1
    return user


@pytest.fixture
def mock_subscription():
    """Create a mock subscription"""
    subscription = MagicMock(spec=TenantSubscription)
    subscription.id = 1
    subscription.paystack_subscription_code = "SUB_test123"
    subscription.tenant_id = 1
    return subscription


@pytest.fixture
def mock_db():
    """Create a mock database session"""
    db = MagicMock()
    return db


class TestListPlans:
    @pytest.mark.asyncio
    async def test_list_plans_success(self):
        """Test successful listing of plans"""
        mock_response = {
            "data": [
                {
                    "id": 1,
                    "name": "Basic Plan",
                    "amount": 5000,
                    "currency": "NGN",
                    "interval": "monthly",
                    "plan_code": "PLN_basic123",
                },
                {
                    "id": 2,
                    "name": "Premium Plan",
                    "amount": 10000,
                    "currency": "NGN",
                    "interval": "monthly",
                    "plan_code": "PLN_premium456",
                },
            ]
        }

        with patch("app.routes.billing.paystack_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_response

            result = await list_plans(page=1, per_page=50)

            assert len(result) == 2
            assert result[0]["name"] == "Basic Plan"
            assert result[0]["amount"] == 5000
            assert result[1]["name"] == "Premium Plan"
            mock_request.assert_called_once_with("GET", "/plan?page=1&perPage=50")

    @pytest.mark.asyncio
    async def test_list_plans_deduplication(self):
        """Test that duplicate plan IDs are deduplicated"""
        mock_response = {
            "data": [
                {
                    "id": 1,
                    "name": "Basic Plan",
                    "amount": 5000,
                    "currency": "NGN",
                    "interval": "monthly",
                    "plan_code": "PLN_basic123",
                },
                {
                    "id": 1,
                    "name": "Basic Plan Duplicate",
                    "amount": 5000,
                    "currency": "NGN",
                    "interval": "monthly",
                    "plan_code": "PLN_basic123",
                },
            ]
        }

        with patch("app.routes.billing.paystack_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_response

            result = await list_plans()

            assert len(result) == 1
            assert result[0]["name"] == "Basic Plan"

    @pytest.mark.asyncio
    async def test_list_plans_with_pagination(self):
        """Test listing plans with custom pagination"""
        with patch("app.routes.billing.paystack_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"data": [{"id": 1, "name": "Plan", "amount": 100, "currency": "GHS", "interval": "monthly", "plan_code": "PLN"}]}

            await list_plans(page=2, per_page=10)

            mock_request.assert_called_once_with("GET", "/plan?page=2&perPage=10")


class TestCreateCustomer:
    @pytest.mark.asyncio
    async def test_create_customer_success(self, mock_user):
        """Test successful customer creation using tenant details"""
        
        mock_response = {
            "status": True,
            "data": {
                "email": "test@example.com",
                "first_name": "Test Corp", # Should match tenant name
                "last_name": "Company",
                "customer_code": "CUS_test123"
            }
        }

        with patch("app.routes.billing.paystack_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_response

            # We need to mock the db session for the update part
            mock_db = MagicMock()
            
            result = await create_customer(current_user=mock_user, db=mock_db)

            assert result["status"] is True
            assert result["data"]["email"] == "test@example.com"
            
            # Verify the correct data was sent
            call_args = mock_request.call_args
            sent_data = call_args[0][2]
            assert sent_data["email"] == "test@example.com"
            assert sent_data["first_name"] == "Test Corp"
            
            # Verify tenant was updated
            assert mock_user.tenant.paystack_customer_code == "CUS_test123"
            mock_db.commit.assert_called()

    @pytest.mark.asyncio
    async def test_create_customer_uses_tenant_details(self, mock_user):
        """Test that data is pulled from tenant/user correctly"""
        
        with patch("app.routes.billing.paystack_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = {"status": True, "data": {"customer_code": "CUS_123"}}
            mock_db = MagicMock()

            await create_customer(current_user=mock_user, db=mock_db)

            call_args = mock_request.call_args
            sent_data = call_args[0][2]
            assert sent_data["email"] == mock_user.email
            assert sent_data["first_name"] == mock_user.tenant.name
            assert sent_data["metadata"]["tenant_id"] == mock_user.tenant.id


class TestGetMyCustomer:
    @pytest.mark.asyncio
    async def test_get_my_customer_success(self, mock_user):
        """Test successful retrieval of customer details"""
        mock_response = {
            "status": True,
            "data": {
                "email": "test@example.com",
                "customer_code": "CUS_test123",
                "first_name": "John",
                "last_name": "Doe"
            }
        }

        with patch("app.routes.billing.paystack_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_response

            result = await get_my_customer(mock_user)

            assert result["status"] is True
            assert result["data"]["email"] == "test@example.com"
            mock_request.assert_called_once_with("GET", f"/customer/{mock_user.email}")


class TestInitializeTransaction:
    @pytest.mark.asyncio
    async def test_initialize_transaction_success(self, mock_user):
        """Test successful transaction initialization"""
        from app.schemas import TransactionInitialize

        transaction_data = TransactionInitialize(
            amount=50000,
            reference="TRX_test123",
            callback_url="https://example.com/callback"
        )

        mock_response = {
            "status": True,
            "data": {
                "authorization_url": "https://paystack.com/pay/test123",
                "access_code": "access_test123",
                "reference": "TRX_test123"
            }
        }

        with patch("app.routes.billing.paystack_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_response

            result = await initialize_transaction(transaction_data, mock_user)

            assert result["status"] is True
            assert "authorization_url" in result["data"]
            
            call_args = mock_request.call_args
            sent_data = call_args[0][2]
            assert sent_data["email"] == mock_user.email
            assert sent_data["amount"] == 50000


class TestDisableSubscription:
    @pytest.mark.asyncio
    async def test_disable_subscription_success(self):
        """Test successful subscription disabling"""
        from app.schemas import SubscriptionDisable

        subscription_data = SubscriptionDisable(
            code="SUB_test123",
            token="token_test123"
        )

        mock_response = {
            "status": True,
            "message": "Subscription disabled successfully"
        }

        with patch("app.routes.billing.paystack_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_response

            result = await disable_subscription(subscription_data)

            assert result["status"] is True
            mock_request.assert_called_once_with(
                "POST",
                "/subscription/disable",
                {"code": "SUB_test123", "token": "token_test123"}
            )


class TestGetSubscription:
    @pytest.mark.asyncio
    async def test_get_subscription_success(self, mock_user, mock_subscription, mock_db):
        """Test successful retrieval of subscription details"""
        # Mock the chained call: db.query(...).filter(...).order_by(...).first()
        mock_query = mock_db.query.return_value
        mock_filter = mock_query.filter.return_value
        mock_order_by = mock_filter.order_by.return_value
        mock_order_by.first.return_value = mock_subscription

        mock_response = {
            "status": True,
            "data": {
                "subscription_code": "SUB_test123",
                "status": "active",
                "amount": 5000
            }
        }

        with patch("app.routes.billing.paystack_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_response

            result = await get_subscription(mock_user, mock_db)

            assert result["status"] is True
            assert result["data"]["subscription_code"] == "SUB_test123"
            mock_request.assert_called_once_with("GET", "/subscription/SUB_test123")

    @pytest.mark.asyncio
    async def test_get_subscription_no_active_subscription(self, mock_user, mock_db):
        """Test error when user has no active subscription"""
        # App logic checks if current_user.tenant exists first
        mock_user.tenant = None
        
        with pytest.raises(HTTPException) as exc_info:
            await get_subscription(mock_user, mock_db)

        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
        assert "User is not associated with a tenant" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_get_subscription_not_found_in_db(self, mock_user, mock_db):
        """Test error when subscription is not found in database"""
        mock_query = mock_db.query.return_value
        mock_filter = mock_query.filter.return_value
        mock_order_by = mock_filter.order_by.return_value
        mock_order_by.first.return_value = None

        with pytest.raises(HTTPException) as exc_info:
            await get_subscription(mock_user, mock_db)

        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
        assert "No active subscription found for tenant" in exc_info.value.detail


class TestVerifyTransaction:
    @pytest.mark.asyncio
    async def test_verify_transaction_success(self):
        """Test successful transaction verification"""
        reference = "TRX_test123"
        
        mock_response = {
            "status": True,
            "data": {
                "reference": reference,
                "amount": 50000,
                "status": "success",
                "paid_at": "2024-01-15T10:30:00.000Z"
            }
        }

        with patch("app.routes.billing.paystack_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_response

            result = await verify_transaction(reference)

            assert result["status"] is True
            assert result["data"]["reference"] == reference
            assert result["data"]["status"] == "success"
            mock_request.assert_called_once_with("GET", f"/transaction/verify/{reference}")

    @pytest.mark.asyncio
    async def test_verify_transaction_failed(self):
        """Test verification of failed transaction"""
        reference = "TRX_failed123"
        
        mock_response = {
            "status": True,
            "data": {
                "reference": reference,
                "amount": 50000,
                "status": "failed"
            }
        }

        with patch("app.routes.billing.paystack_request", new_callable=AsyncMock) as mock_request:
            mock_request.return_value = mock_response

            result = await verify_transaction(reference)

            assert result["status"] is True
            assert result["data"]["status"] == "failed"


# Integration-style tests for error handling
class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_paystack_api_error_propagation(self, mock_user):
        """Test that Paystack API errors are properly propagated"""
        
        with patch("app.routes.billing.paystack_request", new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid customer data"
            )
            mock_db = MagicMock()

            with pytest.raises(HTTPException) as exc_info:
                await create_customer(current_user=mock_user, db=mock_db)

            assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST

    @pytest.mark.asyncio
    async def test_database_query_error_handling(self, mock_user, mock_db):
        """Test handling of database query errors"""
        mock_db.query.side_effect = Exception("Database connection error")

        with pytest.raises(Exception) as exc_info:
            await get_subscription(mock_user, mock_db)

        assert "Database connection error" in str(exc_info.value)