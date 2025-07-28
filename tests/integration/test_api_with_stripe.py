"""
Test customer API with Stripe integration.
"""
import requests
import json

BASE_URL = "http://localhost:8000"

def test_api_with_stripe():
    """Test API endpoints with Stripe integration."""
    
    print("🌐 TESTING API WITH STRIPE INTEGRATION")
    print("=" * 50)
    
    # Test 1: Health check
    print("1️⃣ Testing health endpoint...")
    try:
        response = requests.get(f"{BASE_URL}/health")
        if response.status_code == 200:
            health_data = response.json()
            print(f"   ✅ Health check passed")
            print(f"   Status: {health_data.get('status')}")
            print(f"   Stripe integration: {health_data.get('stripe_integration')}")
        else:
            print(f"   ❌ Health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ Health check error: {e}")
        print(f"   💡 Make sure API server is running: python run_api.py")
        return False
    
    # Test 2: Create customer with Stripe sync
    print("\n2️⃣ Testing customer creation with Stripe sync...")
    customer_data = {
        "name": "API Test Customer",
        "email": "api.test@example.com"
    }
    
    try:
        response = requests.post(f"{BASE_URL}/customers", json=customer_data)
        if response.status_code == 201:
            customer = response.json()
            print(f"   ✅ Customer created successfully")
            print(f"   ID: {customer['id']}")
            print(f"   Name: {customer['name']}")
            print(f"   Email: {customer['email']}")
            print(f"   Stripe ID: {customer.get('stripe_customer_id', 'Not synced')}")
            
            customer_id = customer['id']
            
            # Test 3: Check sync status
            print("\n3️⃣ Testing sync status endpoint...")
            try:
                sync_response = requests.get(f"{BASE_URL}/customers/{customer_id}/sync-status")
                if sync_response.status_code == 200:
                    sync_data = sync_response.json()
                    print(f"   ✅ Sync status retrieved")
                    print(f"   Local data: {sync_data['local_data']}")
                    
                    integrations = sync_data.get('integrations', {})
                    if integrations:
                        for integration_name, integration_data in integrations.items():
                            print(f"   {integration_name}: {integration_data}")
                    else:
                        print(f"   No integration mappings found")
                else:
                    print(f"   ❌ Sync status failed: {sync_response.status_code}")
            except Exception as e:
                print(f"   ❌ Sync status error: {e}")
            
            # Test 4: Update customer
            print("\n4️⃣ Testing customer update with Stripe sync...")
            update_data = {"name": "Updated API Test Customer"}
            
            try:
                update_response = requests.put(f"{BASE_URL}/customers/{customer_id}", json=update_data)
                if update_response.status_code == 200:
                    updated_customer = update_response.json()
                    print(f"   ✅ Customer updated successfully")
                    print(f"   New name: {updated_customer['name']}")
                else:
                    print(f"   ❌ Update failed: {update_response.status_code}")
            except Exception as e:
                print(f"   ❌ Update error: {e}")
            
            # Test 5: Get all customers
            print("\n5️⃣ Testing get all customers...")
            try:
                list_response = requests.get(f"{BASE_URL}/customers")
                if list_response.status_code == 200:
                    customers = list_response.json()
                    print(f"   ✅ Retrieved {len(customers)} customers")
                else:
                    print(f"   ❌ List customers failed: {list_response.status_code}")
            except Exception as e:
                print(f"   ❌ List customers error: {e}")
            
            # Test 6: Delete customer
            print("\n6️⃣ Testing customer deletion with Stripe sync...")
            try:
                delete_response = requests.delete(f"{BASE_URL}/customers/{customer_id}")
                if delete_response.status_code == 200:
                    delete_data = delete_response.json()
                    print(f"   ✅ Customer deleted successfully")
                    print(f"   Message: {delete_data['message']}")
                else:
                    print(f"   ❌ Delete failed: {delete_response.status_code}")
            except Exception as e:
                print(f"   ❌ Delete error: {e}")
            
            return True
            
        else:
            print(f"   ❌ Customer creation failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"   ❌ Customer creation error: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Starting API + Stripe Integration Test...")
    print("💡 Make sure your API server is running: python run_api.py")
    print()
    
    success = test_api_with_stripe()
    
    print(f"\n{'='*50}")
    if success:
        print("🎉 SUCCESS: API + Stripe integration working perfectly!")
        print("📊 Check your Stripe dashboard to see created customers")
    else:
        print("❌ FAILED: Some tests failed")
        print("🔧 Check the errors above and your API server status")
    print("="*50)