import csv

def read_orders_csv(file_path):
    data = []
    with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader)  # Skip the header
        for row in csvreader:
            data.append(row)
    return data

def process_order_data(data):
    # Filter and aggregate: Calculate total_amt_usd for each account_id where total_amt_usd > 1000
    from collections import defaultdict
    account_info = defaultdict(lambda: {'total_amt_usd': 0, 'order_count': 0})
    for row in data:
        account_id = row[1]
        total_amt_usd = float(row[10])
        if total_amt_usd > 1000:
            account_info[account_id]['total_amt_usd'] += total_amt_usd
            account_info[account_id]['order_count'] += 1
    # Convert the defaultdict to a list of lists for CSV writing
    processed_data = [[account_id, info['total_amt_usd'], info['order_count']] for account_id, info in account_info.items()]
    return processed_data

def write_processed_data_to_csv(data, output_file_path):
    with open(output_file_path, mode='w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)
        # Write header
        csvwriter.writerow(["account_id", "total_amt_usd", "order_count"])
        # Write data
        csvwriter.writerows(data)

def main():
    # Example usage
    file_path = './orders.csv'
    order_data = read_orders_csv(file_path)

    # Process the order data
    processed_order_data = process_order_data(order_data)

    # Write the processed data to a new CSV file
    output_file_path = './processed_orders.csv'
    write_processed_data_to_csv(processed_order_data, output_file_path)

if __name__ == "__main__":
    main()
