import random
import json
import time
from datetime import datetime
from confluent_kafka import Producer


# Cấu hình Kafka Producer
conf = {
    'bootstrap.servers': 'localhost:9092',  # Nếu chạy trong container: dùng 'kafka:9092'
    'client.id': 'python-producer'
}

producer = Producer(conf)

# Callback khi gửi thành công hoặc thất bại
def delivery_report(err, msg):
    if err is not None:
        print('❌ Delivery failed:', err)
    else:
        print(f'✅ Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

print("🚀 Kafka Producer is running... Press Ctrl+C to stop.")

try:
    while True:
        # Tạo dữ liệu sự kiện giả lập
        event = {
            "user_id": random.randint(1, 100),
            "event_type": random.choice(["click", "view", "purchase"]),
            "timestamp": int(time.time()),
            "datetime": datetime.utcnow().isoformat()
        }

        # Gửi dữ liệu đến topic Kafka
        producer.produce(
            topic='clickstream-topic',
            value=json.dumps(event),
            callback=delivery_report
        )

        # Gọi poll để xử lý callback
        producer.poll(0)
        print("📤 Sent:", event)
        time.sleep(1)

except KeyboardInterrupt:
    print("\n🛑 Stopping producer...")
finally:
    # Đảm bảo gửi hết message trong hàng đợi
    producer.flush()
    print("✅ Producer shut down cleanly.")