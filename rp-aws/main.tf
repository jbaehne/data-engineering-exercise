resource "aws_lambda_function" "kai_lambda" {
  function_name = "kai_lambda"
  role          = "arn:aws:iam::884919981963:role/lambda-ex"
  handler       = "main.lambda_handler"
  runtime       = "python3.8"
  memory_size   = 128
  timeout       = 9
  layers        = [
    "arn:aws:lambda:us-west-2:898466741470:layer:psycopg2-py38:1",
    "arn:aws:lambda:us-west-2:884919981963:layer:sqlal_cockrack:1"
  ]
  environment {
    variables = {}
  }
  ephemeral_storage {
    size = 512
  }
  tracing_config {
    mode = "PassThrough"
  }
  tags = {}
}

resource "aws_cloudwatch_log_group" "lambda_log_group" {
  name              = "/aws/lambda/kai_lambda"
  retention_in_days = 14
}
