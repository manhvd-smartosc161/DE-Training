import boto3
import os
from botocore.exceptions import NoCredentialsError, ClientError

def upload_to_s3(local_file_path, bucket_name, s3_file_path=None):
    """
    Upload a file to an S3 bucket
    
    Parameters:
    local_file_path (str): Path to the local file
    bucket_name (str): Name of the S3 bucket
    s3_file_path (str): S3 key/path where the file will be stored. If None, uses the filename from local_file_path
    
    Returns:
    bool: True if file was uploaded, False otherwise
    """
    # Create an S3 client
    s3_client = boto3.client('s3')
    
    # If s3_file_path is not provided, use the filename from local_file_path
    if s3_file_path is None:
        s3_file_path = os.path.basename(local_file_path)
    
    try:
        # Upload the file
        s3_client.upload_file(local_file_path, bucket_name, s3_file_path)
        print(f"Successfully uploaded {local_file_path} to {bucket_name}/{s3_file_path}")
        return True
    except FileNotFoundError:
        print(f"The file {local_file_path} was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except ClientError as e:
        print(f"Error uploading file to S3: {e}")
        return False

def upload_image_to_s3(image_path, bucket_name, s3_path=None):
    """
    Upload an image file to an S3 bucket
    
    Parameters:
    image_path (str): Path to the local image file
    bucket_name (str): Name of the S3 bucket
    s3_path (str): S3 key/path where the image will be stored. If None, uses the filename from image_path
    
    Returns:
    bool: True if image was uploaded, False otherwise
    """
    # Check if file is an image by extension
    valid_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp']
    file_extension = os.path.splitext(image_path)[1].lower()
    
    if file_extension not in valid_extensions:
        print(f"File {image_path} is not a recognized image format")
        return False
    
    # Upload the image
    return upload_to_s3(image_path, bucket_name, s3_path)

if __name__ == "__main__":
    # Define the bucket name
    bucket_name = "manhvd-s3"
    
    # Example file to upload
    # Upload the image.png file directly
    file_path = "./image2.png"
    
    # Upload as image
    success = upload_image_to_s3(file_path, bucket_name)
    
    if success:
        print("Upload completed successfully!")
    else:
        print("Upload failed.")
