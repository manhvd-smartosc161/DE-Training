provider "aws" {
  region = "ap-southeast-2"
}

resource "aws_key_pair" "manhvd-keypair" {
  key_name   = "manhvd-keypair"
  public_key = file("./keypair/id_rsa.pub")
}

resource "aws_instance" "ec2-teraform" {
  ami           = "ami-0f6a1a6507c55c9a8"
  instance_type = "t2.micro"
  key_name      = aws_key_pair.manhvd-keypair.key_name
  
  tags = {
    Name = "EC2 Terraform Instance"
  }
}

resource "aws_s3_bucket" "terraform_bucket" {
  bucket = "manhvd-terraform-bucket"
  
  tags = {
    Name        = "Terraform Bucket"
    Environment = "Dev"
  }
}

# Enable versioning
resource "aws_s3_bucket_versioning" "terraform_bucket_versioning" {
  bucket = aws_s3_bucket.terraform_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Enable server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "terraform_bucket_encryption" {
  bucket = aws_s3_bucket.terraform_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Block public access
resource "aws_s3_bucket_public_access_block" "terraform_bucket_public_access" {
  bucket = aws_s3_bucket.terraform_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle rules
resource "aws_s3_bucket_lifecycle_configuration" "terraform_bucket_lifecycle" {
  bucket = aws_s3_bucket.terraform_bucket.id

  rule {
    id     = "cleanup-old-versions"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

resource "aws_s3_bucket_ownership_controls" "terraform_bucket_ownership" {
  bucket = aws_s3_bucket.terraform_bucket.id
  
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_acl" "terraform_bucket_acl" {
  depends_on = [aws_s3_bucket_ownership_controls.terraform_bucket_ownership]
  
  bucket = aws_s3_bucket.terraform_bucket.id
  acl    = "private"
}

# IAM policy for bucket access
resource "aws_iam_policy" "terraform_bucket_policy" {
  name        = "terraform-bucket-policy"
  description = "Policy for accessing the Terraform bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.terraform_bucket.arn,
          "${aws_s3_bucket.terraform_bucket.arn}/*"
        ]
      }
    ]
  })
}
