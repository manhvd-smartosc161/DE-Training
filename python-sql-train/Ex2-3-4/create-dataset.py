import os
import re
from google.cloud import bigquery

def extract_table_data(sql_file_path):
    """
    Extract table creation and data insertion statements from SQL file.
    Returns a dictionary with table names as keys and their data as values.
    """
    tables = {}
    current_table = None
    
    with open(sql_file_path, 'r') as file:
        content = file.read()
        
        # Find all CREATE TABLE statements
        create_patterns = re.findall(r'CREATE TABLE (\w+) \((.*?)\);', content, re.DOTALL)
        
        for table_name, columns_text in create_patterns:
            # Parse columns
            columns = []
            for column_line in columns_text.strip().split('\n'):
                column_parts = column_line.strip().split()
                if len(column_parts) >= 2:
                    column_name = column_parts[0]
                    column_type = column_parts[1]
                    
                    # Map SQL types to BigQuery types
                    if 'integer' in column_type.lower():
                        bq_type = 'INTEGER'
                    elif 'timestamp' in column_type.lower():
                        bq_type = 'TIMESTAMP'
                    else:
                        bq_type = 'STRING'  # Default to STRING for other types
                    
                    columns.append(bigquery.SchemaField(column_name, bq_type))
            
            tables[table_name] = {
                'schema': columns,
                'rows': []
            }
        
        # Find all INSERT statements
        for table_name in tables.keys():
            insert_pattern = re.compile(r'INSERT INTO ' + table_name + r' VALUES \((.*?)\);', re.DOTALL)
            inserts = insert_pattern.findall(content)
            
            for insert in inserts:
                # Parse values, handling quoted strings properly
                values = []
                in_quote = False
                current_value = ""
                
                for char in insert:
                    if char == "'" and not in_quote:
                        in_quote = True
                        current_value += char
                    elif char == "'" and in_quote:
                        in_quote = False
                        current_value += char
                    elif char == ',' and not in_quote:
                        values.append(current_value.strip())
                        current_value = ""
                    else:
                        current_value += char
                
                if current_value:
                    values.append(current_value.strip())
                
                # Convert values to appropriate types
                row = {}
                for i, field in enumerate(tables[table_name]['schema']):
                    if i < len(values):
                        value = values[i].strip("'")
                        if field.field_type == 'INTEGER':
                            try:
                                row[field.name] = int(value)
                            except ValueError:
                                row[field.name] = None
                        elif field.field_type == 'TIMESTAMP':
                            row[field.name] = value
                        else:
                            row[field.name] = value
                
                tables[table_name]['rows'].append(row)
    
    return tables

def upload_to_bigquery(tables):
    """
    Upload extracted tables to BigQuery.
    """
    # Construct a BigQuery client object
    client = bigquery.Client()
    
    # Define the dataset
    dataset_id = 'sqltrain'
    dataset_ref = client.dataset(dataset_id)
    
    # Create the dataset if it doesn't exist
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created")
    
    # Create tables and upload data
    for table_name, table_data in tables.items():
        table_ref = dataset_ref.table(table_name)
        
        # Create table if it doesn't exist
        try:
            client.get_table(table_ref)
            print(f"Table {table_name} already exists")
        except Exception:
            table = bigquery.Table(table_ref, schema=table_data['schema'])
            table = client.create_table(table)
            print(f"Table {table_name} created")
        
        # Insert data
        if table_data['rows']:
            errors = client.insert_rows_json(table_ref, table_data['rows'])
            if errors:
                print(f"Encountered errors while inserting rows into {table_name}: {errors}")
            else:
                print(f"Inserted {len(table_data['rows'])} rows into {table_name}")

def main():
    # Path to the SQL file
    sql_file_path = './parch-and-posey.sql'
    
    # Extract table data from SQL file
    tables = extract_table_data(sql_file_path)
    
    # Upload to BigQuery
    upload_to_bigquery(tables)

if __name__ == "__main__":
    main()
