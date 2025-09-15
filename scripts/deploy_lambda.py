#!/usr/bin/env python3
"""
Script to deploy only the Lambda function code without redeploying the entire CDK stack.
This is useful for quick iterations when you only change the Lambda function code.
"""

import boto3
import zipfile
import os
import sys
from pathlib import Path

def create_lambda_package():
    """Create a ZIP package of the Lambda function code with dependencies"""
    print("📦 Creating Lambda deployment package...")
    
    # Define paths
    lambda_dir = Path("lambda_functions")
    zip_path = Path("lambda_deployment.zip")
    temp_dir = Path("temp_lambda_package")
    
    if not lambda_dir.exists():
        print(f"❌ Lambda functions directory not found: {lambda_dir}")
        return None
    
    try:
        # Create temporary directory for package building
        temp_dir.mkdir(exist_ok=True)
        
        # Copy Lambda function files
        for py_file in lambda_dir.glob("*.py"):
            import shutil
            shutil.copy2(py_file, temp_dir / py_file.name)
            print(f"   Added: {py_file.name}")
        
        # Install dependencies if requirements.txt exists
        requirements_file = lambda_dir / "requirements.txt"
        if requirements_file.exists():
            print("   Installing Python dependencies...")
            import subprocess
            result = subprocess.run([
                sys.executable, "-m", "pip", "install", 
                "-r", str(requirements_file),
                "-t", str(temp_dir)
                # Removed --no-deps to include sub-dependencies
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print("   ✅ Dependencies installed")
            else:
                print(f"   ⚠️  Dependency installation had issues: {result.stderr}")
        else:
            # Install requests manually since we know it's needed
            print("   Installing requests library...")
            import subprocess
            result = subprocess.run([
                sys.executable, "-m", "pip", "install", 
                "requests",
                "-t", str(temp_dir)
                # Removed --no-deps to include sub-dependencies
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print("   ✅ Requests library installed")
            else:
                print(f"   ⚠️  Could not install requests: {result.stderr}")
        
        # Create ZIP file from temp directory
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in temp_dir.rglob("*"):
                if file_path.is_file():
                    # Use relative path from temp_dir as the archive name
                    archive_name = file_path.relative_to(temp_dir)
                    zipf.write(file_path, archive_name)
        
        # Clean up temp directory
        import shutil
        shutil.rmtree(temp_dir)
        
        # Check if file was created and has content
        if zip_path.exists() and zip_path.stat().st_size > 0:
            print(f"✅ Lambda package created: {zip_path} ({zip_path.stat().st_size} bytes)")
            return zip_path
        else:
            print("❌ Failed to create Lambda package")
            return None
            
    except Exception as e:
        print(f"❌ Error creating Lambda package: {str(e)}")
        # Clean up temp directory if it exists
        if temp_dir.exists():
            import shutil
            shutil.rmtree(temp_dir)
        return None

def get_lambda_function_name():
    """Get the Lambda function name from CloudFormation stack outputs"""
    print("🔍 Getting Lambda function name from CloudFormation...")
    
    try:
        cf_client = boto3.client('cloudformation')
        response = cf_client.describe_stacks(StackName='DataPipelineStack')
        outputs = response['Stacks'][0]['Outputs']
        
        for output in outputs:
            if output['OutputKey'] == 'LambdaFunctionName':
                function_name = output['OutputValue']
                print(f"✅ Found Lambda function: {function_name}")
                return function_name
        
        print("❌ Lambda function name not found in stack outputs")
        return None
        
    except Exception as e:
        print(f"❌ Error getting Lambda function name: {str(e)}")
        return None

def update_lambda_code(function_name, zip_path):
    """Update the Lambda function code"""
    print(f"🚀 Updating Lambda function code: {function_name}")
    
    try:
        lambda_client = boto3.client('lambda')
        
        # Read the ZIP file
        with open(zip_path, 'rb') as zip_file:
            zip_content = zip_file.read()
        
        # Update the function code
        response = lambda_client.update_function_code(
            FunctionName=function_name,
            ZipFile=zip_content
        )
        
        print("✅ Lambda function updated successfully")
        print(f"   Function ARN: {response['FunctionArn']}")
        print(f"   Last Modified: {response['LastModified']}")
        print(f"   Code Size: {response['CodeSize']} bytes")
        
        return True
        
    except Exception as e:
        print(f"❌ Error updating Lambda function: {str(e)}")
        return False

def wait_for_function_ready(function_name, max_attempts=30):
    """Wait for the Lambda function to be ready after update"""
    print("⏳ Waiting for Lambda function to be ready...")
    
    lambda_client = boto3.client('lambda')
    
    for attempt in range(max_attempts):
        try:
            response = lambda_client.get_function(FunctionName=function_name)
            state = response['Configuration']['State']
            
            if state == 'Active':
                print("✅ Lambda function is ready")
                return True
            elif state == 'Pending':
                print(f"   Status: {state} (attempt {attempt + 1}/{max_attempts})")
                import time
                time.sleep(2)
            else:
                print(f"❌ Unexpected Lambda state: {state}")
                return False
                
        except Exception as e:
            print(f"❌ Error checking Lambda status: {str(e)}")
            return False
    
    print("❌ Timeout waiting for Lambda function to be ready")
    return False

def test_updated_function(function_name):
    """Test the updated Lambda function"""
    print("🧪 Testing updated Lambda function...")
    
    try:
        lambda_client = boto3.client('lambda')
        
        # Get data bucket name for test
        cf_client = boto3.client('cloudformation')
        response = cf_client.describe_stacks(StackName='DataPipelineStack')
        outputs = response['Stacks'][0]['Outputs']
        
        data_bucket_name = None
        for output in outputs:
            if output['OutputKey'] == 'DataBucketName':
                data_bucket_name = output['OutputValue']
                break
        
        if not data_bucket_name:
            print("❌ Could not find data bucket name for testing")
            return False
        
        # Invoke the function
        import json
        response = lambda_client.invoke(
            FunctionName=function_name,
            Payload=json.dumps({"bucket_name": data_bucket_name})
        )
        
        if response['StatusCode'] == 200:
            payload = json.loads(response['Payload'].read())
            print("✅ Lambda function test successful")
            print(f"   Response: {payload}")
            return True
        else:
            print(f"❌ Lambda function test failed with status: {response['StatusCode']}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing Lambda function: {str(e)}")
        return False

def cleanup_temp_files(zip_path):
    """Clean up temporary files"""
    try:
        if zip_path and zip_path.exists():
            zip_path.unlink()
            print(f"🧹 Cleaned up temporary file: {zip_path}")
    except Exception as e:
        print(f"⚠️  Could not clean up {zip_path}: {str(e)}")

def main():
    """Main deployment function"""
    print("🚀 Lambda Function Code Deployment")
    print("=" * 50)
    
    # Change to the script directory
    script_dir = Path(__file__).parent.parent
    os.chdir(script_dir)
    print(f"📁 Working directory: {os.getcwd()}")
    
    zip_path = None
    try:
        # Step 1: Create deployment package
        zip_path = create_lambda_package()
        if not zip_path:
            return 1
        
        # Step 2: Get Lambda function name
        function_name = get_lambda_function_name()
        if not function_name:
            return 1
        
        # Step 3: Update Lambda code
        if not update_lambda_code(function_name, zip_path):
            return 1
        
        # Step 4: Wait for function to be ready
        if not wait_for_function_ready(function_name):
            return 1
        
        # Step 5: Test the updated function
        if not test_updated_function(function_name):
            print("⚠️  Lambda update completed but test failed")
            return 1
        
        print("\n🎉 Lambda function code deployed successfully!")
        print("=" * 50)
        return 0
        
    except KeyboardInterrupt:
        print("\n❌ Deployment cancelled by user")
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        return 1
    finally:
        # Clean up temporary files
        if zip_path:
            cleanup_temp_files(zip_path)

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
