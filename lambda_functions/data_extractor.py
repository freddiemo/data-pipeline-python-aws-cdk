import json
import boto3
import requests
from datetime import datetime
import csv
import io
import logging
import os

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda function to extract data from JSONPlaceholder API and store in S3
    """
    try:
        # Get bucket name from event, environment variable, or raise error
        bucket_name = event.get('bucket_name') or os.environ.get('BUCKET_NAME')
        if not bucket_name:
            raise ValueError("Bucket name not provided in event or environment variables")
        
        logger.info(f"Using bucket: {bucket_name}")
        
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # Call JSONPlaceholder API
        logger.info("Calling JSONPlaceholder API...")
        response = requests.get('https://jsonplaceholder.typicode.com/users', timeout=30)
        response.raise_for_status()
        
        users_data = response.json()
        logger.info(f"Retrieved {len(users_data)} users from API")
        
        # Process and flatten the data for better structure
        processed_data = []
        for user in users_data:
            flat_user = {
                'id': user.get('id'),
                'name': user.get('name'),
                'username': user.get('username'),
                'email': user.get('email'),
                'phone': user.get('phone'),
                'website': user.get('website'),
                'address_street': user.get('address', {}).get('street'),
                'address_suite': user.get('address', {}).get('suite'),
                'address_city': user.get('address', {}).get('city'),
                'address_zipcode': user.get('address', {}).get('zipcode'),
                'address_lat': user.get('address', {}).get('geo', {}).get('lat'),
                'address_lng': user.get('address', {}).get('geo', {}).get('lng'),
                'company_name': user.get('company', {}).get('name'),
                'company_catchphrase': user.get('company', {}).get('catchPhrase'),
                'company_bs': user.get('company', {}).get('bs'),
                'extraction_timestamp': datetime.utcnow().isoformat()
            }
            processed_data.append(flat_user)
        
        # Convert to CSV format
        csv_buffer = io.StringIO()
        if processed_data:
            fieldnames = processed_data[0].keys()
            writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(processed_data)
        
        # Generate S3 key with timestamp for partitioning
        timestamp = datetime.utcnow()
        s3_key = f"raw-data/year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}/users_{timestamp.strftime('%Y%m%d_%H%M%S')}.csv"
        
        # Upload to S3
        logger.info(f"Uploading data to S3: {bucket_name}/{s3_key}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data extraction and upload successful',
                'records_processed': len(processed_data),
                's3_location': f's3://{bucket_name}/{s3_key}',
                'timestamp': timestamp.isoformat()
            })
        }
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'API request failed: {str(e)}'})
        }
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Unexpected error: {str(e)}'})
        }
