#import boto3
import json

#s3 = boto3.client("s3")


srcArnList = []
# this method creates bucket policy on the destination bucket if it has a sourceArn 

def createBucketPolicy(s3,destinationBucket,sourceBucket,resource, sourceAccountId,destinationAccountId):
    policy_string ={
        "Version": "2012-10-17",
        "Statement": [ 
                {
                "Sid": "InventoryAndAnalyticsExamplePolicy",
                "Effect": "Allow",
                "Principal": {
                    "Service": "s3.amazonaws.com"
                },
                "Action": [
                    "s3:PutObject"
                ],
                "Resource": [
                    resource
                ],
                "Condition": {
                    "ArnLike": {
                        "aws:SourceArn": "arn:aws:s3:::"+sourceBucket
                    },
                    "StringEquals": {
                        "aws:SourceAccount": sourceAccountId,
                        "s3:x-amz-acl": "bucket-owner-full-control"
                    }
                }}]
            }
    policyJs = json.dumps(policy_string)
    s3.put_bucket_policy(Bucket=destinationBucket,Policy=policyJs,ExpectedBucketOwner=destinationAccountId)


# this method creates bucket policy (update or add) on the destination bucket if it has list of sourceArn and sourceaccounts
def createBucketSrcArnListPolicy(s3,destinationBucket,srcArnList, resource,sourceAccountLs, destinationAccountId):
    policy_srcArnList ={
        "Version": "2012-10-17",
        "Statement": [ 
                {
                "Sid": "InventoryAndAnalyticsExamplePolicy",
                "Effect": "Allow",
                "Principal": {
                    "Service": "s3.amazonaws.com"
                },
                "Action": [
                    "s3:PutObject"
                ],
                "Resource": [
                    resource
                ],
                "Condition": {
                    "ArnLike": {
                        "aws:SourceArn": srcArnList
                    },
                    "StringEquals": {
                        "aws:SourceAccount": sourceAccountLs,
                        "s3:x-amz-acl": "bucket-owner-full-control"
                    }
                }}]
            }
    policySrcArnListJs = json.dumps(policy_srcArnList)
    s3.put_bucket_policy(Bucket=destinationBucket,Policy=policySrcArnListJs,ExpectedBucketOwner=destinationAccountId)


