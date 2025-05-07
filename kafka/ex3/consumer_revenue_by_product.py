from confluent_kafka import Consumer
import json
from datetime import datetime

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'revenue_by_product_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
topic = 'ecommerce_transactions'
consumer.subscribe([topic])

# Lưu tổng doanh thu theo từng sản phẩm
revenue_by_product = {}

try:
    print("Revenue aggregation by product (Ctrl+C to stop)...")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            event = json.loads(msg.value().decode('utf-8'))
            product_id = event['product_id']
            amount = event['amount']
            revenue_by_product[product_id] = revenue_by_product.get(product_id, 0) + amount
            # In tạm thời (có thể đổi thành in định kỳ hoặc khi sang ngày v.v)
            print(f"[{product_id}] --> Total Revenue: {revenue_by_product[product_id]:,.0f} VNĐ")

        except Exception as e:
            print(f"Error: {e}")

except KeyboardInterrupt:
    print("\nStopping revenue consumer...")
finally:
    consumer.close()
