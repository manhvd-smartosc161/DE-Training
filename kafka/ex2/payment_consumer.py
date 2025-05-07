from confluent_kafka import Consumer
import json
from datetime import datetime
from collections import defaultdict

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'payment_agg_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
topic = 'payment_events'
consumer.subscribe([topic])

# Aggregate data
curr_minute = None
sum_amount = 0.0
payment_methods = defaultdict(float)  # Tổng tiền theo phương thức thanh toán
success_count = 0
failed_count = 0
pending_count = 0

def print_summary(minute_key, total_amount, methods, success, failed, pending):
    """In thông tin tổng hợp"""
    print("\n\n\n\n" + "="*50)
    print(f"📊 Báo cáo giao dịch - {minute_key}")
    print("="*50)
    print(f"💰 Tổng tiền giao dịch: {total_amount:,.2f} VNĐ")
    print("\n💳 Chi tiết theo phương thức thanh toán:")
    for method, amount in methods.items():
        print(f"  - {method}: {amount:,.2f} VNĐ")
    
    print("\n📈 Trạng thái giao dịch:")
    print(f"  - Thành công: {success}")
    print(f"  - Thất bại: {failed}")
    print(f"  - Đang xử lý: {pending}")
    print("="*50)

try:
    print("Starting payment aggregation (Ctrl+C to stop)...")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            event = json.loads(msg.value().decode('utf-8'))
            event_time = event.get("timestamp")
            if event_time:
                dt = datetime.fromisoformat(event_time)
            else:
                dt = datetime.now()

            amount = event["amount"]
            minute_key = dt.strftime("%Y-%m-%d %H:%M")
            payment_method = event["payment_method"]
            status = event["status"]

            if curr_minute is None:
                curr_minute = minute_key

            if minute_key == curr_minute:
                # Cập nhật tổng tiền
                sum_amount += amount
                # Cập nhật theo phương thức thanh toán
                payment_methods[payment_method] += amount
                # Cập nhật trạng thái
                if status == "SUCCESS":
                    success_count += 1
                elif status == "FAILED":
                    failed_count += 1
                else:
                    pending_count += 1
            else:
                # In báo cáo cho phút trước
                print_summary(curr_minute, sum_amount, payment_methods, success_count, failed_count, pending_count)
                
                # Reset cho phút mới
                curr_minute = minute_key
                sum_amount = amount
                payment_methods = defaultdict(float)
                payment_methods[payment_method] = amount
                success_count = 1 if status == "SUCCESS" else 0
                failed_count = 1 if status == "FAILED" else 0
                pending_count = 1 if status == "PENDING" else 0

        except Exception as e:
            print(f"Error processing message: {e}")

except KeyboardInterrupt:
    print("\nStopping aggregation...")
finally:
    # In báo cáo cuối cùng nếu còn dữ liệu
    if sum_amount > 0:
        print_summary(curr_minute, sum_amount, payment_methods, success_count, failed_count, pending_count)
    consumer.close()
