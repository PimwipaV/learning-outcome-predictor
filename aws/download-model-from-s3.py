import boto3

def lambda_handler(event, context):
    # Replace the values below with your own.
    bucket_name = 'sagemaker-studio-gm9l1avdk4m'
    object_key = 'model2_2022_frominstance.pkl'
    local_file_path = '/mnt/lambda/model2_2022_frominstance.pkl'
    
    # Create an S3 client.
    s3 = boto3.client('s3')
    
    # Download the file from S3 to a local file.
    s3.download_file(bucket_name, object_key, local_file_path)
    
    # Print a success message.
    print(f"File downloaded from S3: s3://{bucket_name}/{object_key} -> {local_file_path}")
