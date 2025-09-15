# AWS CDK Data Pipeline

A complete **serverless data pipeline** built with AWS CDK (Python) that extracts data from JSONPlaceholder API, stores it in S3, catalogs it with AWS Glue, and makes it queryable through Amazon Athena.

## 🏗️ Architecture

```
API → Lambda → S3 → Glue Crawler → Athena Queries
```

**Components:**
- **AWS Lambda**: Data extraction from JSONPlaceholder API
- **Amazon S3**: Partitioned data storage
- **AWS Glue**: Automatic schema discovery
- **Amazon Athena**: SQL analytics engine
- **Lake Formation**: Data access control
- **EventBridge**: Daily automation

## 🚀 Quick Start

### Prerequisites
- **AWS CLI** configured
- **Python 3.10+** 
- **Node.js** (for CDK CLI)

### Deploy
```bash
# Activate virtual environment (required for CDK)
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# First deployment or after complete cleanup
npx cdk bootstrap

# Deploy the pipeline
./scripts/deploy.sh
```

This process will:
1. **Bootstrap CDK** (creates CDK assets bucket - required after cleanup)
2. Create Python virtual environment
3. Install dependencies
4. Deploy infrastructure
5. Package and update Lambda function
6. Display deployment outputs

**Note:** If you get "No bucket named 'cdk-hnb659fds-assets-*'. Is account bootstrapped?" error, run `npx cdk bootstrap` first.

### Test
```bash
# Activate virtual environment
source .venv/bin/activate

# Run the test suite
python3 test_pipeline.py
```

## 🔧 Required AWS Permissions

Add these managed policies to your AWS user for deployment:

```bash
# Core CDK permissions
aws iam attach-user-policy --user-name YOUR_USER --policy-arn arn:aws:iam::aws:policy/AWSCloudFormationFullAccess
aws iam attach-user-policy --user-name YOUR_USER --policy-arn arn:aws:iam::aws:policy/IAMFullAccess

# Service permissions  
aws iam attach-user-policy --user-name YOUR_USER --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
aws iam attach-user-policy --user-name YOUR_USER --policy-arn arn:aws:iam::aws:policy/AWSLambdaFullAccess
aws iam attach-user-policy --user-name YOUR_USER --policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess
aws iam attach-user-policy --user-name YOUR_USER --policy-arn arn:aws:iam::aws:policy/AmazonAthenaFullAccess
aws iam attach-user-policy --user-name YOUR_USER --policy-arn arn:aws:iam::aws:policy/AmazonSSMFullAccess
aws iam attach-user-policy --user-name YOUR_USER --policy-arn arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryFullAccess
```

## 📊 Testing the Pipeline

### 1. Manual Lambda Test
```bash
aws lambda invoke \
  --function-name data-pipeline-data-extractor \
  --payload '{"bucket_name": "data-pipeline-bucket-jsonplaceholder"}' \
  --cli-binary-format raw-in-base64-out \
  response.json
```

### 2. Verify S3 Data
```bash
aws s3 ls s3://data-pipeline-bucket-jsonplaceholder/raw-data/ --recursive
```

### 3. Run Glue Crawler
```bash
aws glue start-crawler --name data-pipeline-crawler
```

### 4. Query with Athena

#### Using AWS Console
Use AWS Console > Athena > Workgroup: `data-pipeline-workgroup`

#### Using AWS CLI
You can run Athena queries directly from the command line:

**1. Total Record Count Query**
```bash
# Start the query
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) as record_count FROM data_pipeline_db.raw_data;" \
  --work-group "data-pipeline-workgroup" \
  --result-configuration "OutputLocation=s3://data-pipeline-athena-results-jsonplaceholder/cli-queries/"

# Get the execution ID from the output, then check status
aws athena get-query-execution --query-execution-id "EXECUTION_ID"

# Get results once completed
aws athena get-query-results --query-execution-id "EXECUTION_ID"
```

**2. User Data Sample Query**
```bash
# Start the query
aws athena start-query-execution \
  --query-string "SELECT name, email, address_city FROM data_pipeline_db.raw_data LIMIT 10;" \
  --work-group "data-pipeline-workgroup" \
  --result-configuration "OutputLocation=s3://data-pipeline-athena-results-jsonplaceholder/cli-queries/"

# Check status and get results (same commands as above)
```

**3. Users by City Aggregation Query**
```bash
# Start the query
aws athena start-query-execution \
  --query-string "SELECT address_city, COUNT(*) as user_count FROM data_pipeline_db.raw_data GROUP BY address_city ORDER BY user_count DESC;" \
  --work-group "data-pipeline-workgroup" \
  --result-configuration "OutputLocation=s3://data-pipeline-athena-results-jsonplaceholder/cli-queries/"

# Check status and get results (same commands as above)
```

**Note:** Additional SQL query examples are available in `sql/sample_athena_queries.sql`

## 🗂️ Project Structure

```
├── app.py                      # CDK app entry point
├── test_pipeline.py           # Test suite
├── requirements.txt           # Python dependencies
├── cdk.json                  # CDK configuration
├── scripts/                  # Deployment and utility scripts
│   ├── deploy.sh            # Deploy pipeline
│   └── cleanup_aws.py       # Complete AWS cleanup
├── data_pipeline/            # CDK stack definition
│   └── data_pipeline_stack.py
├── lambda_functions/         # Lambda code
│   ├── data_extractor.py
│   └── requirements.txt
├── sql/                      # SQL query examples
│   └── sample_athena_queries.sql
└── config/                   # Configuration files
    ├── minimal-iam-permissions.json
    └── cdk-deployment-policy.json
```

## 🔄 Daily Automation

The pipeline runs automatically:
- **Lambda extraction**: Daily at 1:00 AM UTC
- **Glue crawler**: Daily at 2:00 AM UTC

## 🧹 Cleanup & Resource Deletion

### Complete AWS Resource Cleanup

**✅ Recommended: Python Cleanup Script**

```bash
# Navigate to project directory
cd /path/to/python-aws-cdk

# Activate virtual environment
source .venv/bin/activate

# Run comprehensive cleanup script
python3 scripts/cleanup_aws.py
```

This script will:
1. **Clean Athena WorkGroup** (critical for preventing CDK destroy failures)
2. **Empty S3 buckets** automatically
3. **Run CDK destroy** with proper error handling
4. **Manual resource cleanup** if CDK destroy fails
5. **Verify complete deletion** of all resources
6. **Provide detailed feedback** throughout the process

**Expected output:**
- Real-time progress updates for each cleanup step
- ✅ Success indicators for each resource type
- 🎉 Final confirmation when all resources are deleted

**✅ Successful cleanup indicators:**
- CloudFormation: Stack shows "DELETE_COMPLETE" status
- S3: No buckets found (empty output)
- Lambda: Empty array `[]`
- Glue: No output (database not found)
- Athena: No output (workgroup not found)

### ⚠️ Important Notes

- **Athena WorkGroup must be cleaned first** - it contains query execution history that blocks deletion
- **S3 buckets must be empty** before they can be deleted
- **CDK destroy is the recommended method** after manual Athena cleanup
- **Billing stops immediately** once resources are deleted
- **Data is permanently lost** - ensure you have backups if needed
- **IAM roles and policies** created by CDK are automatically cleaned up

## 🚨 Troubleshooting

**Bootstrap errors** (`No bucket named 'cdk-hnb659fds-assets-*'`): Run `npx cdk bootstrap` before deployment
**Permission errors**: Ensure all required IAM policies are attached
**Lambda import errors**: Run `./scripts/deploy.sh` to package dependencies
**Crawler failures**: Check S3 bucket permissions and data format
**Athena query errors**: Verify table exists and workgroup permissions

## 📝 Notes

- Data is partitioned by `year/month/day` for optimal query performance
- Lambda function uses `requests` library (packaged automatically)
- No Docker required - uses standard Python packaging
- All AWS resources are tagged and include lifecycle policies
