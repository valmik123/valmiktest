import boto3
#s3=boto3.client("s3")
s3t=boto3.resource("s3")
#s3.create_bucket(Bucket='mybucket')

s3t.create_bucket(Bucket='valmikproject', CreateBucketConfiguration={
    'LocationConstraint': 'ap-south-1'})
#boto3 used to manage aws services programmetically
