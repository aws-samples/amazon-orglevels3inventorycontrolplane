#import boto3
import json
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')

file_handler = logging.FileHandler('CreateDestinationBucketPolicy.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

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
    logger.info(f"sourceAccountId is {sourceAccountId}")
    logger.info(f"sourceBucket is {sourceBucket}")
    policyJs = json.dumps(policy_string)
    logger.info(f"policyJs is {policyJs}")
    #add try except block to check if policy is correctly created
    try:
        s3.put_bucket_policy(Bucket=destinationBucket,Policy=policyJs,ExpectedBucketOwner=destinationAccountId)
        logger.info(f"policy created successfully")
        return True
    except Exception as e:  
        logger.error(f"policy not created successfully {e}")
        return False


# this method creates bucket policy (update or add) on the destination bucket if it has list of sourceArn and sourceaccounts
def createBucketSrcArnListPolicy(s3,destinationBucket,srcArnList, resource,sourceAccountLs, destinationAccountId):
    logger.info(f"sourceAccountLs is {sourceAccountLs}")
    logger.info(f"srcArnList is {srcArnList}")
    
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
    logger.info(f"sourceAccountLs is {sourceAccountLs}")
    logger.info(f"srcArnList is {srcArnList}")
    policySrcArnListJs = json.dumps(policy_srcArnList)
    logger.info(f"policySRCSRNListJs is {policySrcArnListJs}")
    #add try except block to check if policy is correctly created
    try:
        s3.put_bucket_policy(Bucket=destinationBucket,Policy=policySrcArnListJs,ExpectedBucketOwner=destinationAccountId)
        logger.info(f"policy created successfully")
        return True
    except Exception as e:
        logger.error(f"policy not created successfully {e}")
        return False
    


