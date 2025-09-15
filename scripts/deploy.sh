#!/bin/bash

# Deployment script for AWS CDK Data Pipeline
# This script automates the deployment process

set -e  # Exit on any error

echo "🚀 AWS CDK Data Pipeline Deployment Script"
echo "=========================================="

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "📦 Creating Python virtual environment..."
    python3 -m venv .venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source .venv/bin/activate

# Install dependencies
echo "📚 Installing Python dependencies..."
pip install -r requirements.txt

# Check if AWS CDK is installed (using npx for local installation)
if ! npx cdk --version &> /dev/null; then
    echo "❌ AWS CDK not found. Please install it with: npm install aws-cdk"
    exit 1
fi

# Check if AWS CLI is configured
if ! aws sts get-caller-identity &> /dev/null; then
    echo "❌ AWS CLI not configured. Please run: aws configure"
    exit 1
fi

echo "✅ Prerequisites check completed"

# Bootstrap CDK (if needed)
echo "🏗️  Checking CDK bootstrap..."
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REGION=$(aws configure get region)

if [ -z "$REGION" ]; then
    REGION="us-east-1"
    echo "⚠️  No default region set, using us-east-1"
fi

echo "   Account: $ACCOUNT"
echo "   Region: $REGION"

# Try to bootstrap (will skip if already done)
echo "   Bootstrapping CDK..."
npx cdk bootstrap aws://$ACCOUNT/$REGION

# Synthesize the stack
echo "🔍 Synthesizing CloudFormation template..."
npx cdk synth

# Package Lambda function with dependencies
echo "📦 Packaging Lambda function..."
LAMBDA_DIR="lambda_functions"
PACKAGE_DIR="lambda_package"

# Clean up previous package
rm -rf $PACKAGE_DIR
mkdir -p $PACKAGE_DIR

# Copy Lambda function code
cp $LAMBDA_DIR/*.py $PACKAGE_DIR/

# Install dependencies in the package directory
echo "   Installing Lambda dependencies..."
pip install -r $LAMBDA_DIR/requirements.txt -t $PACKAGE_DIR/

# Create deployment package
echo "   Creating deployment package..."
cd $PACKAGE_DIR
zip -r ../lambda-package.zip . -q
cd ..

# Deploy the stack first (this creates the Lambda function with placeholder code)
echo "🚀 Deploying the stack..."
echo "   This may take 5-10 minutes..."
npx cdk deploy --require-approval never

# Get the Lambda function name from stack outputs
echo "📋 Getting Lambda function name..."
LAMBDA_FUNCTION_NAME=$(aws cloudformation describe-stacks \
    --stack-name DataPipelineStack \
    --query 'Stacks[0].Outputs[?OutputKey==`LambdaFunctionName`].OutputValue' \
    --output text)

if [ -z "$LAMBDA_FUNCTION_NAME" ]; then
    echo "❌ Could not find Lambda function name in stack outputs"
    exit 1
fi

echo "   Lambda function name: $LAMBDA_FUNCTION_NAME"

# Update Lambda function code with our package
echo "🔄 Updating Lambda function code..."
aws lambda update-function-code \
    --function-name $LAMBDA_FUNCTION_NAME \
    --zip-file fileb://lambda-package.zip

# Wait for update to complete
echo "   Waiting for function update to complete..."
aws lambda wait function-updated \
    --function-name $LAMBDA_FUNCTION_NAME

echo "✅ Lambda function updated successfully!"

# Clean up package files
echo "🧹 Cleaning up temporary files..."
rm -rf $PACKAGE_DIR lambda-package.zip

# Get stack outputs
echo "📋 Getting stack outputs..."
STACK_NAME="DataPipelineStack"

echo ""
echo "✅ Deployment completed successfully!"
echo ""
echo "📊 Stack Outputs:"
echo "=================="

# Extract and display key outputs
aws cloudformation describe-stacks \
    --stack-name $STACK_NAME \
    --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' \
    --output table

echo ""
echo "🎯 Next Steps:"
echo "=============="
echo "1. Wait a few minutes for all resources to be ready"
echo "2. Test the Lambda function manually:"
echo "   aws lambda invoke --function-name <LAMBDA_FUNCTION_NAME> --payload '{\"bucket_name\": \"<DATA_BUCKET_NAME>\"}' response.json"
echo ""
echo "3. Run the Glue crawler:"
echo "   aws glue start-crawler --name <GLUE_CRAWLER_NAME>"
echo ""
echo "4. Query data with Athena using the AWS Console"
echo ""
echo "5. Use the test script: python3 test_pipeline.py"
echo "   (Update the configuration values in the script first)"
echo ""
echo "🔗 Useful AWS Console links:"
echo "   - Lambda: https://console.aws.amazon.com/lambda"
echo "   - S3: https://console.aws.amazon.com/s3"
echo "   - Glue: https://console.aws.amazon.com/glue"
echo "   - Athena: https://console.aws.amazon.com/athena"
echo "   - Lake Formation: https://console.aws.amazon.com/lakeformation"
