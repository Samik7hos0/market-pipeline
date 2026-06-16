import boto3
import os
import json
from dotenv import load_dotenv
from datetime import datetime, UTC

load_dotenv()

def get_s3_client():
    """Create and return an s3 client."""
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )

def upload_to_s3(data: list, folder: str, filename: str) -> str:
    """Upload data as JSON to s3 bucket."""
    s3 = get_s3_client()
    bucket = os.getenv('S3_BUCKET')

    key = f"{folder}/{filename}"
    body = json.dumps(data, indent=2)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType='application/json'
    )

    s3_path = f"s3://{bucket}/{key}"
    print(f"Uploaded to {s3_path}")
    return s3_path

def  download_from_s3(folder: str, filename: str) -> list:
    """Download and parse JSON from s3."""
    s3 = get_s3_client()
    bucket = os.getenv('S3_BUCKET')


    key = f"{folder}/{filename}"
    response = s3.get_object(Bucket=bucket, Key=key)
    data = json.loads(response['Body'].read().decode('utf-8'))
    
    print(f"Downloaded {len(data)} records from s3://{bucket}/{key}")
    return data

def list_s3_files(folder: str) -> list:
    """List all files in a folder."""
    s3 = get_s3_client()
    bucket = os.getenv('S3_BUCKET')
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix=f"{folder}/")
    files = [obj['Key'] for obj in response.get('Contents', [])]
    return files

if __name__ == '__main__':
    # Test: upload a sample file
    test_data = [{"test": "hello from de-grind", "timestamp": datetime.now(UTC).isoformat()}]
    filename = f"test_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.json"
    
    path = upload_to_s3(test_data, 'raw', filename)
    print(f"Uploaded to: {path}")
    
    # List files
    files = list_s3_files('raw')
    print(f"Files in raw/: {files}")