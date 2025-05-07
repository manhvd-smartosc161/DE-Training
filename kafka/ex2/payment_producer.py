from confluent_kafka import Producer
import json
import random
import time
from datetime import datetime

# Kafka configurations
conf = {
    'bootstrap.servers': 'localhost:9092'
}

# Initialize Producer
producer = Producer(conf)

# Define your topic
topic = "payment_events"

# Payment methods with their emojis
PAYMENT_METHODS = {
    "CASH": "💵",
    "CREDIT_CARD": "💳",
    "MOMO": "📱",
    "VNPAY": "🏦",
    "ZALOPAY": "📲"
}

# Status with their emojis
STATUS_EMOJIS = {
    "SUCCESS": "✅",
    "FAILED": "❌",
    "PENDING": "⏳"
}

def generate_payment_event():
    """Generate random payment event"""
    payment_method = random.choice(list(PAYMENT_METHODS.keys()))
    status = random.choice(["SUCCESS", "FAILED", "PENDING"])
    
    return {
        "transaction_id": f"TXN-{random.randint(1000, 9999)}",
        "timestamp": datetime.now().isoformat(),
        "amount": round(random.uniform(10000, 1000000), 2),  # Amount in VND
        "payment_method": payment_method,
        "status": status
    }

def format_amount(amount):
    """Format amount with commas and VND symbol"""
    return f"{amount:,.2f} VNĐ"

def print_payment_event(event):
    """Print payment event in a beautiful format"""
    print("\n" + "="*60)
    print(f"🔄 Transaction ID: {event['transaction_id']}")
    print(f"⏰ Timestamp: {event['timestamp']}")
    print(f"💰 Amount: {format_amount(event['amount'])}")
    print(f"💳 Payment Method: {PAYMENT_METHODS[event['payment_method']]} {event['payment_method']}")
    print(f"📊 Status: {STATUS_EMOJIS[event['status']]} {event['status']}")
    print("="*60)

def delivery_report(err, msg):
    """Callback for delivery reports"""
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"✅ Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}]\n\n\n")

def main():
    print("\n🚀 Starting Payment Event Producer")
    print("Press Ctrl+C to stop\n")
    
    try:
        while True:
            # Generate random payment event
            payment_event = generate_payment_event()
            
            # Convert to JSON string
            value = json.dumps(payment_event)
            
            # Use transaction_id as the key
            key = payment_event["transaction_id"]
            
            # Produce message
            producer.produce(
                topic=topic,
                key=key,
                value=value,
                callback=delivery_report
            )
            
            # Trigger delivery callbacks
            producer.poll(0)
            
            # Print the data being sent
            print_payment_event(payment_event)
            
            # Wait for 5 seconds before sending next data
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping producer...")
    finally:
        # Wait for any outstanding messages to be delivered
        producer.flush()

if __name__ == "__main__":
    main()