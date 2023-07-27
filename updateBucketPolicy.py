import logging
import json
#import sys
import createDestinationBucketPolicy
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')

file_handler = logging.FileHandler('UpdateBucketPolicy.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


#this method add or update the bucket policy of the destination bucket (where inventory data will be stored)
def addOrUpdatePolicy(s3,sourceBucket, sourceAccountId, destinationBucket, destinationAccountId):
    #to capture list the source bucket Arns.
    sourceArnLikeLs = []
    sourceAccountLs = []
    policy = {}
    resource = "arn:aws:s3:::"+destinationBucket+"/*"
    try:
        policy = s3.get_bucket_policy(Bucket=destinationBucket)
    except ClientError as err:
        if err.response['Error']['Code'] == 'NoSuchBucketPolicy':
            logger.info(f"No policy is associated with the destination bucket {destinationBucket}. Creating bucket policy ...")
            policy = None
            logger.info(f"No policy exists, creating a new policy")
            createDestinationBucketPolicy.createBucketPolicy(s3,destinationBucket,sourceBucket, resource, sourceAccountId,destinationAccountId)
        #I'm expecting the IAM user to have a permission to access bucket policy but in case not, then catching over here it
        elif "AccessDenied" in err.args[0]:
            logger.error(f"IAM user does not have getBucketPolicy permissions, nothing to update on a bucket policy")
            policy = None
        elif "Method Not Allowed" in err.args[0]:
            logger.error(f"You're not using an identity that belongs to the bucket owner's account, nothing to update on a bucket policy")
            policy = None
        else:
            logger.error(f"Unknown Error {err}, nothing to update on a bucket policy")
            policy = None
    if policy != None:
        logger.info(f"policy exists, updating it with sourceArn and Sourceaccount")
        sourceArn = json.loads(policy['Policy'])['Statement'][0]['Condition']['ArnLike']['aws:SourceArn']
        logger.info(f"Destination bucket has an existing policy with SourceArn: {sourceArn}")
        sourceAccount = json.loads(policy['Policy'])['Statement'][0]['Condition']['StringEquals']['aws:SourceAccount']
        logger.info (f"Destination Bucket source Account is {sourceAccount}")
        resource = json.loads(policy['Policy'])['Statement'][0]['Resource']
        #for the first time, it might be possible the bucket policy source Arn be a string. 
        # If it is a string, then use append method else extend.
        logger.info(f"sourceArn is {sourceArn}")
        logger.info(f"sourceArn type is {type(sourceArn)}")

        if type(sourceArn) == str:
            sourceArnLikeLs.append(sourceArn)
        else:
            sourceArnLikeLs.extend(sourceArn)
        #for the first time, it might be possible the bucket policy sourceaccount be a string. 
        # If it is a string, then use append method else extend.
        logger.info(f"sourceAccount is {sourceAccount}")
        logger.info (f"sourceAccount type is {type(sourceAccount)}")
        if type(sourceAccount) == str:
            sourceAccountLs.append(sourceAccount)
        else:
            sourceAccountLs.extend(sourceAccount)
    
        sBucket = "arn:aws:s3:::"+sourceBucket
        logger.info(f"sBucket is {sBucket}")
        if sBucket not in sourceArnLikeLs:
            sourceArnLikeLs.append("arn:aws:s3:::"+sourceBucket)
        #since bucket policy has a limit of 20 KB in size, I'm replacing list of source bucket ARN with "*"
        #https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-iam-policies.html
        #if it ever change to accept more Bytes, then comment below two lines of the code.
        logger.info(f"Since bucket policy has a limit of 20 KB in size, I'm replacing list of source bucket ARNs with '*'")
        sourceArnLikeLs = "arn:aws:s3:::*"
        #sourceArnLikeLs = "*"
        logger.info (f'sourceAccountId is {sourceAccountId}')
        logger.info(f'sourceAccountLs is {sourceAccountLs}')
        if sourceAccountId not in sourceAccountLs:
            sourceAccountLs.append(sourceAccountId)

        logger.info(f"updating destination bucket policy with source bucket arn with {sBucket} and source account with {sourceAccountId}")
        createDestinationBucketPolicy.createBucketSrcArnListPolicy(s3,destinationBucket,sourceArnLikeLs, resource,sourceAccountLs,destinationAccountId)
