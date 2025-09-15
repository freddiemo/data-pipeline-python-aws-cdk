#!/usr/bin/env python3
"""
AWS CDK Data Pipeline Cleanup Script

This script removes all AWS resources created by the data pipeline.
It handles the proper cleanup sequence and provides detailed feedback.
"""

import boto3
import subprocess
import sys
import time
from pathlib import Path


class DataPipelineCleanup:
    def __init__(self):
        """Initialize AWS clients and configuration"""
        try:
            self.athena_client = boto3.client('athena')
            self.s3_client = boto3.client('s3')
            self.lambda_client = boto3.client('lambda')
            self.glue_client = boto3.client('glue')
            self.cf_client = boto3.client('cloudformation')
            
            # Resource names from the CDK stack
            self.config = {
                'stack_name': 'DataPipelineStack',
                'workgroup_name': 'data-pipeline-workgroup',
                'database_name': 'data_pipeline_db',
                'crawler_name': 'data-pipeline-crawler',
                'lambda_function_name': 'data-pipeline-data-extractor'
            }
            
            print("🔧 AWS clients initialized successfully")
            
        except Exception as e:
            print(f"❌ Error initializing AWS clients: {str(e)}")
            print("   Make sure AWS CLI is configured correctly")
            sys.exit(1)

    def get_stack_resources(self):
        """Get resource names from CloudFormation stack outputs"""
        print("🔍 Getting resource names from CloudFormation stack...")
        
        try:
            response = self.cf_client.describe_stacks(StackName=self.config['stack_name'])
            if not response['Stacks']:
                print("   Stack not found - may already be deleted")
                return False
                
            stack = response['Stacks'][0]
            stack_status = stack['StackStatus']
            
            if 'DELETE' in stack_status:
                print(f"   Stack is already being deleted (status: {stack_status})")
                return False
            
            # Extract S3 bucket names from outputs
            for output in stack.get('Outputs', []):
                key = output['OutputKey']
                value = output['OutputValue']
                
                if key == 'DataBucketName':
                    self.config['data_bucket'] = value
                elif key == 'AthenaResultsBucketName':
                    self.config['results_bucket'] = value
                elif key == 'LambdaFunctionName':
                    self.config['lambda_function_name'] = value
            
            print("   ✅ Retrieved resource names from stack")
            return True
            
        except Exception as e:
            print(f"   ⚠️  Could not get stack info: {str(e)}")
            print("   Will use default resource names")
            return False

    def clean_athena_workgroup(self):
        """Delete Athena WorkGroup with all query executions"""
        print("\n🗂️  Step 1: Cleaning Athena WorkGroup")
        
        try:
            # Check if workgroup exists
            response = self.athena_client.list_work_groups()
            workgroups = [wg['Name'] for wg in response.get('WorkGroups', [])]
            
            if self.config['workgroup_name'] not in workgroups:
                print("   ✅ Athena WorkGroup not found (already deleted)")
                return True
                
            # Delete workgroup with recursive option
            self.athena_client.delete_work_group(
                WorkGroup=self.config['workgroup_name'],
                RecursiveDeleteOption=True
            )
            print(f"   ✅ Deleted WorkGroup: {self.config['workgroup_name']}")
            return True
            
        except Exception as e:
            print(f"   ❌ Error deleting Athena WorkGroup: {str(e)}")
            return False

    def empty_s3_buckets(self):
        """Empty S3 buckets before deletion"""
        print("\n🪣 Step 2: Emptying S3 buckets")
        
        buckets_to_empty = []
        
        # Add buckets from stack outputs if available
        for bucket_key in ['data_bucket', 'results_bucket']:
            if bucket_key in self.config:
                buckets_to_empty.append(self.config[bucket_key])
        
        # Add default bucket names as fallback
        default_buckets = [
            'data-pipeline-bucket-jsonplaceholder',
            'data-pipeline-athena-results-jsonplaceholder'
        ]
        buckets_to_empty.extend(default_buckets)
        
        # Remove duplicates
        buckets_to_empty = list(set(buckets_to_empty))
        
        for bucket_name in buckets_to_empty:
            try:
                # Check if bucket exists
                self.s3_client.head_bucket(Bucket=bucket_name)
                
                # List and delete all objects
                paginator = self.s3_client.get_paginator('list_objects_v2')
                pages = paginator.paginate(Bucket=bucket_name)
                
                delete_count = 0
                for page in pages:
                    if 'Contents' in page:
                        objects = [{'Key': obj['Key']} for obj in page['Contents']]
                        if objects:
                            self.s3_client.delete_objects(
                                Bucket=bucket_name,
                                Delete={'Objects': objects}
                            )
                            delete_count += len(objects)
                
                print(f"   ✅ Emptied bucket: {bucket_name} ({delete_count} objects)")
                
            except self.s3_client.exceptions.NoSuchBucket:
                print(f"   ✅ Bucket not found: {bucket_name} (already deleted)")
            except Exception as e:
                print(f"   ⚠️  Error with bucket {bucket_name}: {str(e)}")
        
        return True

    def run_cdk_destroy(self):
        """Run CDK destroy command"""
        print("\n🔥 Step 3: Running CDK destroy")
        
        try:
            # Change to project directory
            project_dir = Path(__file__).parent.parent
            
            # Run CDK destroy
            result = subprocess.run(
                ['npx', 'cdk', 'destroy', '--force'],
                cwd=project_dir,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                print("   ✅ CDK destroy completed successfully")
                return True
            else:
                print(f"   ❌ CDK destroy failed:")
                print(f"   Error: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            print("   ❌ CDK destroy timed out")
            return False
        except Exception as e:
            print(f"   ❌ Error running CDK destroy: {str(e)}")
            return False

    def manual_resource_cleanup(self):
        """Manual cleanup of individual resources if CDK destroy fails"""
        print("\n🧹 Step 4: Manual resource cleanup")
        
        # Delete Lambda function
        try:
            self.lambda_client.delete_function(
                FunctionName=self.config['lambda_function_name']
            )
            print(f"   ✅ Deleted Lambda function: {self.config['lambda_function_name']}")
        except self.lambda_client.exceptions.ResourceNotFoundException:
            print(f"   ✅ Lambda function not found (already deleted)")
        except Exception as e:
            print(f"   ⚠️  Error deleting Lambda function: {str(e)}")
        
        # Delete Glue resources
        try:
            self.glue_client.delete_crawler(Name=self.config['crawler_name'])
            print(f"   ✅ Deleted Glue crawler: {self.config['crawler_name']}")
        except self.glue_client.exceptions.EntityNotFoundException:
            print(f"   ✅ Glue crawler not found (already deleted)")
        except Exception as e:
            print(f"   ⚠️  Error deleting Glue crawler: {str(e)}")
        
        try:
            self.glue_client.delete_database(Name=self.config['database_name'])
            print(f"   ✅ Deleted Glue database: {self.config['database_name']}")
        except self.glue_client.exceptions.EntityNotFoundException:
            print(f"   ✅ Glue database not found (already deleted)")
        except Exception as e:
            print(f"   ⚠️  Error deleting Glue database: {str(e)}")
        
        # Delete S3 buckets
        for bucket_key in ['data_bucket', 'results_bucket']:
            if bucket_key in self.config:
                bucket_name = self.config[bucket_key]
                try:
                    self.s3_client.delete_bucket(Bucket=bucket_name)
                    print(f"   ✅ Deleted S3 bucket: {bucket_name}")
                except self.s3_client.exceptions.NoSuchBucket:
                    print(f"   ✅ S3 bucket not found: {bucket_name} (already deleted)")
                except Exception as e:
                    print(f"   ⚠️  Error deleting S3 bucket {bucket_name}: {str(e)}")

    def delete_cloudformation_stack(self):
        """Force delete CloudFormation stack if it still exists"""
        print("\n📋 Step 5: Deleting CloudFormation stack")
        
        try:
            # Check if stack exists
            response = self.cf_client.describe_stacks(StackName=self.config['stack_name'])
            if not response['Stacks']:
                print("   ✅ CloudFormation stack not found (already deleted)")
                return True
            
            # Delete stack
            self.cf_client.delete_stack(StackName=self.config['stack_name'])
            print(f"   ✅ Initiated deletion of stack: {self.config['stack_name']}")
            
            # Wait for deletion to complete
            print("   ⏳ Waiting for stack deletion to complete...")
            waiter = self.cf_client.get_waiter('stack_delete_complete')
            waiter.wait(
                StackName=self.config['stack_name'],
                WaiterConfig={'Delay': 15, 'MaxAttempts': 40}  # 10 minutes max
            )
            print("   ✅ CloudFormation stack deleted successfully")
            return True
            
        except self.cf_client.exceptions.ClientError as e:
            if 'does not exist' in str(e):
                print("   ✅ CloudFormation stack not found (already deleted)")
                return True
            else:
                print(f"   ❌ Error deleting CloudFormation stack: {str(e)}")
                return False
        except Exception as e:
            print(f"   ❌ Error deleting CloudFormation stack: {str(e)}")
            return False

    def verify_cleanup(self):
        """Verify that all resources have been deleted"""
        print("\n✅ Step 6: Verifying cleanup")
        
        verification_results = []
        
        # Check CloudFormation stack
        try:
            response = self.cf_client.describe_stacks(StackName=self.config['stack_name'])
            if response['Stacks']:
                stack_status = response['Stacks'][0]['StackStatus']
                if 'DELETE_COMPLETE' in stack_status:
                    print("   ✅ CloudFormation: Stack deleted successfully")
                    verification_results.append(True)
                else:
                    print(f"   ⚠️  CloudFormation: Stack status is {stack_status}")
                    verification_results.append(False)
            else:
                print("   ✅ CloudFormation: Stack not found")
                verification_results.append(True)
        except Exception:
            print("   ✅ CloudFormation: Stack not found")
            verification_results.append(True)
        
        # Check S3 buckets
        try:
            response = self.s3_client.list_buckets()
            data_pipeline_buckets = [
                bucket['Name'] for bucket in response['Buckets'] 
                if 'data-pipeline' in bucket['Name']
            ]
            if not data_pipeline_buckets:
                print("   ✅ S3: No data-pipeline buckets found")
                verification_results.append(True)
            else:
                print(f"   ⚠️  S3: Found buckets: {data_pipeline_buckets}")
                verification_results.append(False)
        except Exception as e:
            print(f"   ❌ S3: Error checking buckets: {str(e)}")
            verification_results.append(False)
        
        # Check Lambda functions
        try:
            response = self.lambda_client.list_functions()
            data_pipeline_functions = [
                func['FunctionName'] for func in response['Functions']
                if 'data-pipeline' in func['FunctionName']
            ]
            if not data_pipeline_functions:
                print("   ✅ Lambda: No data-pipeline functions found")
                verification_results.append(True)
            else:
                print(f"   ⚠️  Lambda: Found functions: {data_pipeline_functions}")
                verification_results.append(False)
        except Exception as e:
            print(f"   ❌ Lambda: Error checking functions: {str(e)}")
            verification_results.append(False)
        
        # Check Glue database
        try:
            response = self.glue_client.get_databases()
            data_pipeline_dbs = [
                db['Name'] for db in response['DatabaseList']
                if db['Name'] == self.config['database_name']
            ]
            if not data_pipeline_dbs:
                print("   ✅ Glue: Database not found")
                verification_results.append(True)
            else:
                print(f"   ⚠️  Glue: Found database: {data_pipeline_dbs}")
                verification_results.append(False)
        except Exception as e:
            print(f"   ❌ Glue: Error checking database: {str(e)}")
            verification_results.append(False)
        
        # Check Athena workgroup
        try:
            response = self.athena_client.list_work_groups()
            data_pipeline_wgs = [
                wg['Name'] for wg in response['WorkGroups']
                if wg['Name'] == self.config['workgroup_name']
            ]
            if not data_pipeline_wgs:
                print("   ✅ Athena: WorkGroup not found")
                verification_results.append(True)
            else:
                print(f"   ⚠️  Athena: Found workgroup: {data_pipeline_wgs}")
                verification_results.append(False)
        except Exception as e:
            print(f"   ❌ Athena: Error checking workgroup: {str(e)}")
            verification_results.append(False)
        
        return all(verification_results)

    def run_cleanup(self):
        """Run the complete cleanup process"""
        print("🧹 AWS CDK Data Pipeline Cleanup")
        print("=" * 50)
        
        # Get stack resources
        stack_exists = self.get_stack_resources()
        
        # Step 1: Clean Athena WorkGroup
        athena_success = self.clean_athena_workgroup()
        
        # Step 2: Empty S3 buckets
        s3_success = self.empty_s3_buckets()
        
        # Step 3: Run CDK destroy
        if stack_exists:
            cdk_success = self.run_cdk_destroy()
            
            # Step 4: Manual cleanup if CDK destroy failed
            if not cdk_success:
                print("\n⚠️  CDK destroy failed, attempting manual cleanup...")
                self.manual_resource_cleanup()
                
                # Step 5: Force delete CloudFormation stack
                self.delete_cloudformation_stack()
        else:
            print("\n⚠️  Stack not found, running manual cleanup...")
            self.manual_resource_cleanup()
        
        # Step 6: Verify cleanup
        cleanup_successful = self.verify_cleanup()
        
        # Final summary
        print("\n" + "=" * 50)
        print("📊 Cleanup Summary")
        print("=" * 50)
        
        if cleanup_successful:
            print("🎉 All resources have been successfully deleted!")
            print("💰 AWS billing for this pipeline has stopped.")
            print("✅ Your AWS account is clean.")
            return 0
        else:
            print("⚠️  Some resources may still exist.")
            print("💡 Check the verification results above.")
            print("🔧 You may need to delete remaining resources manually via AWS Console.")
            return 1


def main():
    """Main function"""
    try:
        cleanup = DataPipelineCleanup()
        exit_code = cleanup.run_cleanup()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n❌ Cleanup cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
