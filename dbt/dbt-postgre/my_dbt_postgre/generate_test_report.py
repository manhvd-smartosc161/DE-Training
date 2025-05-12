import json
import datetime
import os

# Path to the dbt run results
results_file = "target/run_results.json"

if not os.path.exists(results_file):
    print(f"Error: {results_file} not found. Run dbt test first.")
    exit(1)

try:
    with open(results_file, 'r') as f:
        data = json.load(f)

    # Extract test results
    test_results = [r for r in data['results'] if r['unique_id'].startswith('test.')]

    # Count results
    total = len(test_results)
    passed = sum(1 for r in test_results if r['status'] == 'pass')
    failed = sum(1 for r in test_results if r['status'] == 'fail')

    # Create a summary markdown file
    with open('test_summary.md', 'w') as f:
        f.write(f"# DBT Test Results Summary\n\n")
        f.write(f"Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"## Overview\n")
        f.write(f"- Total Tests: {total}\n")
        f.write(f"- Passed: {passed}\n")
        f.write(f"- Failed: {failed}\n\n")
        
        if failed > 0:
            f.write(f"## Failed Tests\n\n")
            for r in test_results:
                if r['status'] == 'fail':
                    test_name = r['unique_id'].split('.')[-1]
                    f.write(f"### {test_name}\n")
                    f.write(f"- Full ID: `{r['unique_id']}`\n")
                    f.write(f"- Error Message: {r.get('message', 'No message provided')}\n\n")
        
        f.write(f"## All Tests\n\n")
        f.write("| Test | Status | Execution Time |\n")
        f.write("|------|--------|---------------|\n")
        for r in test_results:
            test_name = r['unique_id'].split('.')[-1]
            status = "✅ Pass" if r['status'] == 'pass' else "❌ Fail"
            time = f"{r['execution_time']:.2f}s"
            f.write(f"| {test_name} | {status} | {time} |\n")
    
    print(f"Report generated: test_summary.md")

except Exception as e:
    print(f"Error generating report: {e}")
