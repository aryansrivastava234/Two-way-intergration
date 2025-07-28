"""
Test custom Stripe integration functionality.
"""
from app.integrations import integration_manager
from app.integrations.base import CustomerData

def test_stripe_integration():
    """Test Stripe integration functionality."""
    
    print("🧪 Testing Custom Stripe Integration...")
    print("=" * 50)
    
    # Get Stripe integration
    stripe_integration = integration_manager.get_integration('stripe')
    
    if not stripe_integration:
        print("❌ Stripe integration not found")
        return False
    
    print(f"📋 Using: {stripe_integration.__class__.__name__}")
    
    # Test 1: Connection test
    print("\n Testing connection...")
    connection_ok = stripe_integration.test_connection()
    print(f"   Connection: {'✅ Passed' if connection_ok else '❌ Failed'}")
    
    if not connection_ok:
        print("   Cannot proceed with further tests")
        return False
    
    # Test 2: Create customer
    print("\n Testing create customer...")
    customer_data = CustomerData(
        name="Test Customer",
        email="test.customer@example.com",
        metadata={"source": "integration_test", "test_run": "true"}
    )
    
    create_result = stripe_integration.create_customer(customer_data)
    print(f"   Create: {'✅ Success' if create_result.success else '❌ Failed'}")
    
    if create_result.success:
        customer_id = create_result.external_id
        print(f"   Customer ID: {customer_id}")
        
        # Test 3: Get customer
        print("\n Testing get customer...")
        get_result = stripe_integration.get_customer(customer_id)
        print(f"   Get: {'✅ Success' if get_result.success else '❌ Failed'}")
        
        if get_result.success:
            retrieved_data = get_result.data.get('customer_data')
            print(f"   Retrieved: {retrieved_data.name} ({retrieved_data.email})")
        
        # Test 4: Update customer
        print("\n Testing update customer...")
        updated_data = CustomerData(
            name="Updated Test Customer",
            email="test.customer@example.com",
            metadata={"source": "integration_test", "updated": "true"}
        )
        
        update_result = stripe_integration.update_customer(customer_id, updated_data)
        print(f"   Update: {'✅ Success' if update_result.success else '❌ Failed'}")
        
        # Test 5: Get customers since timestamp
        print("\n Testing get customers since...")
        from datetime import datetime, timedelta
        yesterday = (datetime.utcnow() - timedelta(days=1)).isoformat()
        
        customers_since = stripe_integration.get_customers_since(yesterday)
        print(f"   Retrieved: {len(customers_since)} customers since yesterday")
        
        # Test 6: Delete customer
        print("\n Testing delete customer...")
        delete_result = stripe_integration.delete_customer(customer_id)
        print(f"   Delete: {'✅ Success' if delete_result.success else '❌ Failed'}")
        
        # Verify deletion
        print("\n Verifying deletion...")
        get_deleted_result = stripe_integration.get_customer(customer_id)
        if not get_deleted_result.success:
            print("   ✅ Customer successfully deleted (not found)")
        else:
            print("   ⚠️ Customer still exists after deletion")
        
        print(f"\n🎉 All Stripe integration tests completed!")
        return True
        
    else:
        print(f"   Error: {create_result.error_message}")
        print("   Cannot proceed with further tests")
        return False

def test_stripe_with_dashboard_check():
    """Create a customer that stays for dashboard verification."""
    
    print("\n" + "=" * 50)
    print("🎯 Creating customer for dashboard verification...")
    
    stripe_integration = integration_manager.get_integration('stripe')
    
    if stripe_integration and hasattr(stripe_integration, 'stripe_available') and stripe_integration.stripe_available:
        customer_data = CustomerData(
            name="Dashboard Test Customer",
            email="dashboard.test@example.com",
            metadata={
                "purpose": "dashboard_verification",
                "created_by": "test_script",
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        result = stripe_integration.create_customer(customer_data)
        
        if result.success:
            print(f"✅ Dashboard customer created: {result.external_id}")
            print(f"📊 Check your Stripe dashboard: https://dashboard.stripe.com/test/customers")
            print(f"🔍 Look for: Dashboard Test Customer ({customer_data.email})")
            return result.external_id
        else:
            print(f"❌ Failed to create dashboard customer: {result.error_message}")
    else:
        print("🎭 Using mock integration - no dashboard customer will be created")
    
    return None

if __name__ == "__main__":
    print("🚀 Starting Stripe Integration Tests...")
    
    # Run main tests
    success = test_stripe_integration()
    
    # Create dashboard verification customer
    dashboard_customer_id = test_stripe_with_dashboard_check()
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 SUCCESS: All tests passed!")
        if dashboard_customer_id:
            print(f"📊 Dashboard customer created: {dashboard_customer_id}")
            print("🌐 Check your Stripe dashboard to see the customer")
    else:
        print("❌ FAILED: Some tests failed")
    
    print("=" * 50)