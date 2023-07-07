import boto3
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')

file_handler = logging.FileHandler('CreateDestinationBucket.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

#region = 'us-east-1'
#accountId ='641065947175'
#s3 = boto3.client('s3',region_name=region)
# creating a regional destination bucket in a centralized account (destination account)
#format of the region bucket is "s3inventory-region-destinationaccountid"
def createBucket(s3,region, accountId):
    bucket = 's3inventory-'+region+'-'+accountId
    try:
        if region == 'us-east-1':
            #s3 = boto3.client("s3",region_name = region)
            s3.create_bucket(
                Bucket = bucket
            )
        else:
            #s3 = boto3.client("s3",region_name = region)
            s3.create_bucket(
                Bucket = bucket,
                CreateBucketConfiguration={
                    'LocationConstraint': region,
                },
            )
        s3.put_public_access_block(
            Bucket=bucket,
            PublicAccessBlockConfiguration={
                'BlockPublicAcls': True,
                'IgnorePublicAcls': True,
                'BlockPublicPolicy': True,
                'RestrictPublicBuckets': True
            },
        )
    except ClientError as err:
        if err.response['Error']['Code'] == 'BucketAlreadyExists':
            logger.debug(f"Bucket {bucket} already exists")
        elif err.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            logger.debug(f"Bucket {bucket} already exists and owned by you")
    return bucket



