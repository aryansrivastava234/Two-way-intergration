"""Test FastAPI application."""
import requests
import json

BASE_URL = "http://localhost:8000"

def test_api():
    """Test API endpoints."""
    
    # Test health check
    response = requests.get(f"{BASE_URL}/health")
    print(f"Health check: {response.status_code} - {response.json()}")
    
    # Test root endpoint
    response = requests.get(f"{BASE_URL}/")
    print(f"Root endpoint: {response.status_code} - {response.json()}")
    
    # Test create customer
    customer_data = {
        "name": "John Doe",
        "email": "john.doe@example.com"
    }
    
    response = requests.post(f"{BASE_URL}/customers", json=customer_data)
    print(f"Create customer: {response.status_code}")
    if response.status_code == 201:
        customer = response.json()
        print(f"Created customer: {customer}")
        customer_id = customer["id"]
        
        # Test get customer
        response = requests.get(f"{BASE_URL}/customers/{customer_id}")
        print(f"Get customer: {response.status_code} - {response.json()}")
        
        # Test get all customers
        response = requests.get(f"{BASE_URL}/customers")
        print(f"Get all customers: {response.status_code} - Found {len(response.json())} customers")
        
        # Test update customer
        update_data = {"name": "John Smith"}
        response = requests.put(f"{BASE_URL}/customers/{customer_id}", json=update_data)
        print(f"Update customer: {response.status_code}")
        
        # Test delete customer
        response = requests.delete(f"{BASE_URL}/customers/{customer_id}")
        print(f"Delete customer: {response.status_code} - {response.json()}")
    else:
        print(f"Failed to create customer: {response.text}")

if __name__ == "__main__":
    test_api()