from confluent_kafka import Consumer
import json
from datetime import datetime
from collections import defaultdict

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'high_value_user_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
topic = 'ecommerce_transactions'
consumer.subscribe([topic])

# Configuration
HIGH_VALUE_THRESHOLD = 1000000  # 1 million VND

# Data storage
user_spending = defaultdict(float)         # Total spending per user
user_transactions = defaultdict(int)       # Number of transactions per user
user_last_transaction = {}                 # Last transaction timestamp per user

def format_amount(amount):
    """Format amount with comma as thousand separator"""
    return f"{amount:,.0f} VND"

def print_user_status(user_id, spending, transactions, last_transaction):
    """Display user details"""
    print("\n" + "="*60)
    print(f"👤 User ID: {user_id}")
    print(f"💰 Total Spending: {format_amount(spending)}")
    print(f"🛍️  Number of Transactions: {transactions}")
    print(f"⏰ Last Transaction: {last_transaction}")
    print("="*60)

try:
    print("\n🚀 Starting analysis for high-value users...")
    print("Press Ctrl+C to stop\n")
    
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        try:
            event = json.loads(msg.value().decode('utf-8'))
            user_id = event['user_id']
            amount = event['amount']
            timestamp = event.get('timestamp', datetime.now().isoformat())
            
            # Update user info
            user_spending[user_id] += amount
            user_transactions[user_id] += 1
            user_last_transaction[user_id] = timestamp
            
            # Print info if user reaches HIGH VALUE threshold
            if user_spending[user_id] >= HIGH_VALUE_THRESHOLD:
                print_user_status(
                    user_id,
                    user_spending[user_id],
                    user_transactions[user_id],
                    user_last_transaction[user_id]
                )

        except Exception as e:
            print(f"❌ Error processing message: {e}")

except KeyboardInterrupt:
    print("\n🛑 Stopping user analysis...")
finally:
    # Final statistics
    print("\n📊 Final statistics:")
    print("="*60)
    print(f"Total users: {len(user_spending)}")
    print(f"Number of HIGH VALUE users: {sum(1 for spending in user_spending.values() if spending >= HIGH_VALUE_THRESHOLD)}")
    print("="*60)
    consumer.close()
