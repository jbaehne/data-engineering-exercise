#!/bin/bash

# Define variables
ZIP_FILE="data-engineering-exercise-rp.zip"
S3_BUCKET="s3://rp-data-engg-rp/lambda-code/$ZIP_FILE"

# Zip all files in the current directory (excluding directories)
find . -maxdepth 1 -type f | zip "$ZIP_FILE" -@

# Upload the zip file to S3
aws s3 cp "$ZIP_FILE" "$S3_BUCKET"

# Clean up the local zip file
rm "$ZIP_FILE"

echo "Upload complete. The ZIP file has been uploaded to $S3_BUCKET"
