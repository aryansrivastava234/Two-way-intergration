#!/usr/bin/env python3
"""
Test script to verify Kafka connection.
"""
import sys
import time

def test_kafka_connection():
    print("🧪 Testing Kafka Connection...")
    
    try:
        # Test import
        print("1️⃣ Testing Kafka imports...")
        from kafka import KafkaProducer, KafkaConsumer
        print("   ✅ Kafka libraries imported successfully")
        
        # Test producer connection
        print("2️⃣ Testing Kafka producer...")
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: str(v).encode('utf-8'),
            request_timeout_ms=10000
        )
        print("   ✅ Kafka producer created")
        
        # Test sending a message
        print("3️⃣ Testing message send...")
        future = producer.send('test_topic', value='test_message')
        record_metadata = future.get(timeout=10)
        print(f"   ✅ Message sent to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}")
        
        producer.close()
        
        # Test our custom client
        print("4️⃣ Testing custom Kafka client...")
        from app.kafka_client import event_publisher
        
        success = event_publisher.publish_event(
            topic="test_topic",
            entity_type="test",
            entity_id=1,
            event_type="TEST",
            payload={"test": "data"}
        )
        
        if success:
            print("   ✅ Custom Kafka client working")
        else:
            print("   ❌ Custom Kafka client failed")
        
        print("\n🎉 All Kafka tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Kafka test failed: {e}")
        return False


if __name__ == "__main__":
    success = test_kafka_connection()
    sys.exit(0 if success else 1)