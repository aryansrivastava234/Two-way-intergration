"""
Test all integrations with comprehensive diagnostics.
"""
from app.integrations import integration_manager

def test_integration_manager():
    """Test integration manager functionality."""
    
    print("🔧 INTEGRATION MANAGER TEST")
    print("=" * 40)
    
    # Test 1: Check available integrations
    integrations = integration_manager.list_integrations()
    print(f"📋 Available integrations: {len(integrations)}")
    
    for name, class_name in integrations.items():
        print(f"   {name}: {class_name}")
    
    # Test 2: Connection tests
    print(f"\n🔗 Testing connections...")
    results = integration_manager.test_all_connections()
    
    all_passed = True
    for integration_name, result in results.items():
        status = "✅ Connected" if result else "❌ Failed"
        print(f"   {integration_name}: {status}")
        if not result:
            all_passed = False
    
    return all_passed, integrations

def test_stripe_specifically():
    """Test Stripe integration in detail."""
    
    print(f"\n🎯 DETAILED STRIPE TEST")
    print("=" * 30)
    
    stripe_integration = integration_manager.get_integration('stripe')
    
    if not stripe_integration:
        print("❌ No Stripe integration available")
        return False
    
    print(f"📋 Integration type: {stripe_integration.__class__.__name__}")
    
    # Check if it's real or mock
    is_real_stripe = hasattr(stripe_integration, 'stripe_available') and stripe_integration.stripe_available
    is_mock = 'Mock' in stripe_integration.__class__.__name__
    
    if is_real_stripe and not is_mock:
        print("🌐 Using REAL Stripe integration")
        print("   ✅ Will create actual customers in Stripe")
        print("   📊 Check dashboard.stripe.com to see results")
    elif is_mock:
        print("🎭 Using MOCK Stripe integration")
        print("   ✅ Safe for testing without API calls")
        print("   📝 Events will be logged to console")
    else:
        print("❓ Unknown integration type")
    
    # Test basic functionality
    print(f"\n🧪 Testing basic operations...")
    
    try:
        from app.integrations.base import CustomerData
        
        # Test create
        test_customer = CustomerData(
            name="Integration Test Customer",
            email="integration.test@example.com",
            metadata={"test_type": "integration_manager_test"}
        )
        
        print("   Creating customer...")
        create_result = stripe_integration.create_customer(test_customer)
        
        if create_result.success:
            print(f"   ✅ Customer created: {create_result.external_id}")
            
            # Test update
            print("   Updating customer...")
            updated_customer = CustomerData(
                name="Updated Integration Test Customer",
                email="integration.test@example.com",
                metadata={"test_type": "integration_manager_test", "updated": "true"}
            )
            
            update_result = stripe_integration.update_customer(
                create_result.external_id, 
                updated_customer
            )
            
            if update_result.success:
                print("   ✅ Customer updated successfully")
            else:
                print(f"   ❌ Update failed: {update_result.error_message}")
            
            # Test delete
            print("   Deleting customer...")
            delete_result = stripe_integration.delete_customer(create_result.external_id)
            
            if delete_result.success:
                print("   ✅ Customer deleted successfully")
            else:
                print(f"   ❌ Delete failed: {delete_result.error_message}")
            
            return True
        else:
            print(f"   ❌ Create failed: {create_result.error_message}")
            return False
            
    except Exception as e:
        print(f"   ❌ Test error: {e}")
        return False

def test_configuration():
    """Test configuration and environment."""
    
    print(f"\n⚙️ CONFIGURATION TEST")
    print("=" * 25)
    
    try:
        from config import settings
        
        print("✅ Settings imported successfully")
        
        # Test database config
        if hasattr(settings, 'database'):
            print(f"✅ Database config available")
            print(f"   Host: {settings.database.host}")
            print(f"   Database: {settings.database.database}")
        else:
            print("❌ Database config missing")
        
        # Test Stripe config
        if hasattr(settings, 'stripe'):
            print(f"✅ Stripe config available")
            has_key = bool(settings.stripe.secret_key)
            key_format = settings.stripe.secret_key.startswith('sk_') if has_key else False
            print(f"   Has API key: {has_key}")
            print(f"   Key format valid: {key_format}")
            if has_key:
                print(f"   Key preview: {settings.stripe.secret_key[:20]}...")
        else:
            print("❌ Stripe config missing")
        
        # Test Kafka config
        if hasattr(settings, 'kafka'):
            print(f"✅ Kafka config available")
            print(f"   Bootstrap servers: {settings.kafka.bootstrap_servers}")
            print(f"   Group ID: {settings.kafka.group_id}")
        else:
            print("❌ Kafka config missing")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False

def run_comprehensive_test():
    """Run all integration tests."""
    
    print("🚀 COMPREHENSIVE INTEGRATION TEST")
    print("=" * 50)
    
    results = {
        'manager': False,
        'stripe': False, 
        'config': False
    }
    
    # Test 1: Integration Manager
    try:
        manager_passed, integrations = test_integration_manager()
        results['manager'] = manager_passed
    except Exception as e:
        print(f"❌ Manager test failed: {e}")
    
    # Test 2: Stripe Integration
    try:
        results['stripe'] = test_stripe_specifically()
    except Exception as e:
        print(f"❌ Stripe test failed: {e}")
    
    # Test 3: Configuration
    try:
        results['config'] = test_configuration()
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
    
    # Summary
    print(f"\n📊 TEST SUMMARY")
    print("=" * 20)
    
    total_tests = len(results)
    passed_tests = sum(results.values())
    
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"   {test_name.title()}: {status}")
    
    success_rate = (passed_tests / total_tests) * 100
    print(f"\n🎯 Success Rate: {passed_tests}/{total_tests} ({success_rate:.0f}%)")
    
    if success_rate == 100:
        print("🎉 ALL TESTS PASSED! Integration system is working perfectly.")
    elif success_rate >= 66:
        print("⚠️ Most tests passed. Check failed tests above.")
    else:
        print("❌ Multiple test failures. Integration system needs attention.")
    
    return results

if __name__ == "__main__":
    results = run_comprehensive_test()
    
    # Exit with appropriate code
    import sys
    if all(results.values()):
        sys.exit(0)  # Success
    else:
        sys.exit(1)  # Failure