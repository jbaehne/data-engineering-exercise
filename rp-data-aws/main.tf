# Step 1: AWS Provider
provider "aws" {
  region = "us-west-2"
}

# Step 2: S3 Bucket to Store Lambda Code
resource "aws_s3_bucket" "lambda_code_bucket" {
  bucket = "rp-data-engg-rp"

  tags = {
    Name        = "Lambda Code Bucket"
    Environment = "Development"
  }
}

# Step 3: New Lambda Function with Dependency on S3 Bucket
resource "aws_lambda_function" "data_engineering_exercise_rp" {
  function_name = "data-engineering-exercise-rp"
  role          = "arn:aws:iam::884919981963:role/lambda-ex"
  handler       = "main.lambda_handler"
  runtime       = "python3.9"
  memory_size   = 128
  timeout       = 9

  # Lambda depends on the S3 bucket being created first
  depends_on = [aws_s3_bucket.lambda_code_bucket]

  # S3 bucket and key for Lambda code
  s3_bucket = aws_s3_bucket.lambda_code_bucket.bucket
  s3_key    = "lambda-code/data-engineering-exercise-rp.zip"  # Update if needed this file got reuploaded but it did not refersh

  # Layers: Using existing layers
  layers = [
    "arn:aws:lambda:us-west-2:884919981963:layer:dataenggLayer:1"
  ]

  ephemeral_storage {
    size = 512
  }

  tracing_config {
    mode = "PassThrough"
  }

  tags = {
    Name        = "Data Engineering Exercise Lambda"
    Environment = "Development"
  }
}

# Step 4: CloudWatch Log Group for the New Lambda Function
resource "aws_cloudwatch_log_group" "lambda_log_group" {
  name              = "/aws/lambda/data-engineering-exercise-rp"
  retention_in_days = 1
}
