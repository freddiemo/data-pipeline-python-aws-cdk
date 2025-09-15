#!/usr/bin/env python3
"""
Test script for the AWS CDK Data Pipeline
This script helps test the deployed infrastructure step by step
"""

import boto3
import json
import time

class DataPipelineTester:
    def __init__(self):
        self.lambda_client = boto3.client('lambda')
        self.s3_client = boto3.client('s3')
        self.glue_client = boto3.client('glue')
        self.athena_client = boto3.client('athena')
        
    def test_lambda_function(self, function_name, bucket_name):
        """Test the Lambda function"""
        print(f"🔍 Testing Lambda function: {function_name}")
        try:
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                Payload=json.dumps({"bucket_name": bucket_name})
            )
            
            payload = json.loads(response['Payload'].read())
            if response['StatusCode'] == 200:
                print("✅ Lambda function executed successfully")
                print(f"   Response: {payload}")
                return True
            else:
                print(f"❌ Lambda function failed: {payload}")
                return False
        except Exception as e:
            print(f"❌ Error testing Lambda: {str(e)}")
            return False

    def check_s3_data(self, bucket_name):
        """Check if data exists in S3"""
        print(f"🔍 Checking S3 bucket: {bucket_name}")
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix='raw-data/'
            )
            
            if 'Contents' in response and len(response['Contents']) > 0:
                print(f"✅ Found {len(response['Contents'])} objects in S3")
                for obj in response['Contents'][:5]:  # Show first 5 objects
                    print(f"   - {obj['Key']} ({obj['Size']} bytes)")
                return True
            else:
                print("❌ No data found in S3")
                return False
        except Exception as e:
            print(f"❌ Error checking S3: {str(e)}")
            return False

    def test_glue_crawler(self, crawler_name):
        """Start and monitor Glue crawler"""
        print(f"🔍 Testing Glue crawler: {crawler_name}")
        try:
            # Check crawler status
            response = self.glue_client.get_crawler(Name=crawler_name)
            state = response['Crawler']['State']
            print(f"   Crawler state: {state}")
            
            if state == 'READY':
                print("   Starting crawler...")
                self.glue_client.start_crawler(Name=crawler_name)
                print("✅ Crawler started successfully")
                print("   Note: Crawler may take a few minutes to complete")
                return True
            elif state == 'RUNNING':
                print("✅ Crawler is already running")
                return True
            else:
                print(f"❌ Crawler is in unexpected state: {state}")
                return False
        except Exception as e:
            print(f"❌ Error testing Glue crawler: {str(e)}")
            return False

    def check_glue_tables(self, database_name):
        """Check Glue catalog tables"""
        print(f"🔍 Checking Glue tables in database: {database_name}")
        try:
            response = self.glue_client.get_tables(DatabaseName=database_name)
            tables = response.get('TableList', [])
            
            if tables:
                print(f"✅ Found {len(tables)} table(s)")
                for table in tables:
                    print(f"   - {table['Name']} ({len(table.get('StorageDescriptor', {}).get('Columns', []))} columns)")
                return True
            else:
                print("❌ No tables found in Glue catalog")
                print("   Make sure the crawler has completed successfully")
                return False
        except Exception as e:
            print(f"❌ Error checking Glue tables: {str(e)}")
            return False

    def test_athena_query(self, workgroup, database_name, table_name, results_bucket):
        """Test multiple Athena queries with named results"""
        print("🔍 Testing Athena queries")
        
        from datetime import datetime
        timestamp = datetime.now().strftime("%H:%M")
        
        # Define queries to test
        queries = [
            {
                'name': f'count_rows_{timestamp}',
                'description': 'Total record count',
                'sql': f"SELECT COUNT(*) as record_count FROM {database_name}.{table_name}",
                'result_column': 'record_count'
            },
            {
                'name': f'users_{timestamp}',
                'description': 'User data',
                'sql': f"SELECT name, email, address_city FROM {database_name}.{table_name} LIMIT 5",
                'result_column': 'name'
            },
            {
                'name': f'users_by_city_{timestamp}',
                'description': 'Users by city',
                'sql': f"SELECT address_city, COUNT(*) as user_count FROM {database_name}.{table_name} GROUP BY address_city ORDER BY user_count DESC LIMIT 3",
                'result_column': 'address_city'
            }
        ]
        
        all_queries_passed = True
        query_executions = []  # Track execution IDs and query info
        
        for i, query_info in enumerate(queries, 1):
            print(f"\n   Query {i}: {query_info['description']} ({query_info['name']})")
            
            try:
                # Use custom result location with query name
                custom_result_location = f's3://{results_bucket}/query-results/{query_info["name"]}/'
                
                response = self.athena_client.start_query_execution(
                    QueryString=query_info['sql'],
                    WorkGroup=workgroup,
                    ResultConfiguration={
                        'OutputLocation': custom_result_location
                    }
                )
                
                query_execution_id = response['QueryExecutionId']
                print(f"   ├─ Execution ID: {query_execution_id}")
                
                # Store the mapping for later reference
                query_executions.append({
                    'execution_id': query_execution_id,
                    'query_info': query_info
                })
                
                # Wait for query to complete
                max_attempts = 30
                for attempt in range(max_attempts):
                    result = self.athena_client.get_query_execution(
                        QueryExecutionId=query_execution_id
                    )
                    status = result['QueryExecution']['Status']['State']
                    
                    if status == 'SUCCEEDED':
                        print("   ├─ Status: ✅ SUCCESS")
                        
                        # Get query results
                        results = self.athena_client.get_query_results(
                            QueryExecutionId=query_execution_id
                        )
                        
                        if 'ResultSet' in results and 'Rows' in results['ResultSet']:
                            rows = results['ResultSet']['Rows']
                            if len(rows) > 1:  # Skip header row
                                if query_info['name'].startswith('count_rows'):
                                    count = rows[1]['Data'][0]['VarCharValue']
                                    print(f"   └─ Result: {count} total records")
                                elif query_info['name'].startswith('users_by_city'):
                                    city_count = len(rows) - 1  # Exclude header
                                    print(f"   └─ Result: Found {city_count} unique cities")
                                    for j, row in enumerate(rows[1:], 1):
                                        if len(row['Data']) >= 2:
                                            city = row['Data'][0].get('VarCharValue', 'N/A')
                                            count = row['Data'][1].get('VarCharValue', 'N/A')
                                            print(f"      {j}. {city}: {count} users")
                                elif query_info['name'].startswith('users'):
                                    sample_count = len(rows) - 1  # Exclude header
                                    print(f"   └─ Result: Retrieved {sample_count} sample users")
                                    for j, row in enumerate(rows[1:4], 1):  # Show first 3
                                        if len(row['Data']) >= 3:
                                            name = row['Data'][0].get('VarCharValue', 'N/A')
                                            city = row['Data'][2].get('VarCharValue', 'N/A')
                                            print(f"      {j}. {name} from {city}")
                                else:
                                    # Generic fallback for any other query types
                                    result_count = len(rows) - 1
                                    print(f"   └─ Result: Retrieved {result_count} rows")
                        break
                    elif status == 'FAILED':
                        error = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                        print(f"   └─ Status: ❌ FAILED - {error}")
                        all_queries_passed = False
                        break
                    elif status == 'CANCELLED':
                        print("   └─ Status: ❌ CANCELLED")
                        all_queries_passed = False
                        break
                    else:
                        time.sleep(2)
                else:
                    print("   └─ Status: ❌ TIMEOUT")
                    all_queries_passed = False
                    
            except Exception as e:
                print(f"   └─ Status: ❌ ERROR - {str(e)}")
                all_queries_passed = False
        
        if all_queries_passed:
            print("\n✅ All Athena queries completed successfully")
            
            # Show the mapping between execution IDs and query types
            print("\n� Query Execution ID Mapping:")
            for exec_info in query_executions:
                exec_id = exec_info['execution_id']
                query_info = exec_info['query_info']
                print(f"   {query_info['description']}: {exec_id}")
                print(f"      └─ CSV: s3://{results_bucket}/query-results/{exec_id}.csv")
                print(f"      └─ Metadata: s3://{results_bucket}/query-results/{exec_id}.csv.metadata")
        else:
            print("\n❌ Some Athena queries failed")
            
        return all_queries_passed, query_executions

    def show_query_result_files(self, results_bucket, query_executions=None):
        """Show the query result files stored in S3"""
        print("\n📂 Query result files by type:")
        
        if query_executions:
            # Create a mapping from execution ID to query info
            exec_id_map = {exec_info['execution_id']: exec_info['query_info'] for exec_info in query_executions}
            
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=results_bucket,
                    Prefix='query-results/'
                )
                
                if 'Contents' in response:
                    # Group files by query type using execution ID
                    query_files = {}
                    
                    for obj in response['Contents']:
                        key = obj['Key']
                        if key.endswith('.csv') and not key.endswith('/'):
                            # Extract execution ID from filename: query-results/execution-id.csv
                            filename = key.split('/')[-1]  # Get just the filename
                            exec_id = filename.replace('.csv', '')  # Remove .csv extension
                            
                            if exec_id in exec_id_map:
                                query_info = exec_id_map[exec_id]
                                query_type = query_info['description']
                                
                                if query_type not in query_files:
                                    query_files[query_type] = []
                                
                                query_files[query_type].append({
                                    'csv_file': key,
                                    'metadata_file': key + '.metadata',
                                    'size': obj['Size'],
                                    'exec_id': exec_id
                                })
                    
                    # Display organized results
                    if query_files:
                        for query_type, files in query_files.items():
                            print(f"\n   📊 {query_type}:")
                            for file_info in files:
                                print(f"      ├─ CSV: {file_info['csv_file']} ({file_info['size']} bytes)")
                                print(f"      │  └─ s3://{results_bucket}/{file_info['csv_file']}")
                                print(f"      └─ Metadata: {file_info['metadata_file']}")
                                print(f"         └─ s3://{results_bucket}/{file_info['metadata_file']}")
                    else:
                        print("   No matching query result files found for recent executions")
                else:
                    print("   No files found in query-results folder")
                    
            except Exception as e:
                print(f"   ❌ Error listing result files: {str(e)}")
        else:
            print("   No query execution information available")

def get_stack_outputs():
    """Get the stack outputs dynamically from CloudFormation"""
    import boto3
    try:
        cf_client = boto3.client('cloudformation')
        response = cf_client.describe_stacks(StackName='DataPipelineStack')
        outputs = response['Stacks'][0]['Outputs']
        
        config = {
            'glue_database_name': 'data_pipeline_db',
            'glue_crawler_name': 'data-pipeline-crawler', 
            'athena_workgroup': 'data-pipeline-workgroup'
        }
        
        # Extract values from stack outputs
        for output in outputs:
            key = output['OutputKey']
            value = output['OutputValue']
            
            if key == 'LambdaFunctionName':
                config['lambda_function_name'] = value
            elif key == 'DataBucketName':
                config['data_bucket_name'] = value
            elif key == 'AthenaResultsBucketName':
                config['athena_results_bucket_name'] = value
                
        return config
        
    except Exception as e:
        print(f"❌ Could not get stack outputs: {str(e)}")
        print("   Make sure the DataPipelineStack is deployed")
        return None

def main():
    """Main test function"""
    print("🚀 Starting Data Pipeline Tests")
    print("=" * 50)
    
    # Get configuration dynamically from CloudFormation stack outputs
    config = get_stack_outputs()
    if not config:
        return
    
    # Verify all required values are present
    required_keys = ['lambda_function_name', 'data_bucket_name', 'athena_results_bucket_name']
    missing_keys = [key for key in required_keys if key not in config]
    
    if missing_keys:
        print(f"❌ Missing required configuration: {missing_keys}")
        print("   Make sure the DataPipelineStack is deployed with all outputs")
        return
    
    tester = DataPipelineTester()
    
    # Test 1: Lambda function
    print("\n1. Testing Lambda Function")
    print("-" * 30)
    lambda_success = tester.test_lambda_function(
        config['lambda_function_name'], 
        config['data_bucket_name']
    )
    
    if lambda_success:
        # Wait a bit for data to be uploaded
        print("   Waiting 10 seconds for data upload...")
        time.sleep(10)
    
    # Test 2: S3 data
    print("\n2. Checking S3 Data")
    print("-" * 30)
    s3_success = tester.check_s3_data(config['data_bucket_name'])
    
    # Test 3: Glue crawler
    print("\n3. Testing Glue Crawler")
    print("-" * 30)
    crawler_success = tester.test_glue_crawler(config['glue_crawler_name'])
    
    if crawler_success:
        print("   Waiting 60 seconds for crawler to process data...")
        time.sleep(60)
    
    # Test 4: Glue tables
    print("\n4. Checking Glue Catalog")
    print("-" * 30)
    tables_success = tester.check_glue_tables(config['glue_database_name'])
    
    # Test 5: Athena query (only if tables exist)
    if tables_success:
        print("\n5. Testing Athena Query")
        print("-" * 30)
        
        # Get the table name (assumes first table)
        try:
            response = tester.glue_client.get_tables(DatabaseName=config['glue_database_name'])
            table_name = response['TableList'][0]['Name']
            
            athena_success, query_executions = tester.test_athena_query(
                config['athena_workgroup'],
                config['glue_database_name'],
                table_name,
                config['athena_results_bucket_name']
            )
            
            # Show the query result files in S3
            if athena_success:
                tester.show_query_result_files(config['athena_results_bucket_name'], query_executions)
        except Exception as e:
            print(f"❌ Error getting table name: {str(e)}")
            athena_success = False
    else:
        print("\n5. Skipping Athena Test (no tables found)")
        athena_success = False
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 Test Summary")
    print("=" * 50)
    
    tests = [
        ("Lambda Function", lambda_success),
        ("S3 Data Storage", s3_success),
        ("Glue Crawler", crawler_success),
        ("Glue Catalog", tables_success),
        ("Athena Query", athena_success)
    ]
    
    for test_name, success in tests:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{test_name:<20} {status}")
    
    passed_tests = sum(1 for _, success in tests if success)
    total_tests = len(tests)
    
    print(f"\nResults: {passed_tests}/{total_tests} tests passed")
    
    if passed_tests == total_tests:
        print("🎉 All tests passed! Your data pipeline is working correctly.")
    else:
        print("⚠️  Some tests failed. Check the error messages above for troubleshooting.")

if __name__ == "__main__":
    main()
