import boto3
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')

file_handler = logging.FileHandler('createAthenaWorkgroup.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

#list workgroups
def listWorkgroups():
    athena = boto3.client('athena')
    wgList =[]
    try:
        listWg = athena.list_work_groups(
            MaxResults=50
            )
        for lwg in listWg['WorkGroups']:
            wgList.append(lwg['Name'])
        while('NextToken' in listWg):
            listWg = athena.list_work_groups(
            NextToken = listWg['NextToken'],
            MaxResults=50
            )
            for lwg in listWg['WorkGroups']:
                wgList.append(lwg['Name'])
    except athena.exceptions.InvalidRequestException:
        logger.info (f"The input provided to list workgroup is not valid.")
        #print (f"The input provided to list workgroup is not valid.")
    except athena.exceptions.InternalServerException:
        logger.error (f"Unable to list workgroup, internal service error.")
        #print (f"Unable to list workgoup, internal service error.")
    except ClientError as err:
        logger.error (f"Catch all errors in listWorkgroups {err}")
    return wgList



## Create a bucket and add to the atheba's workgroup
#creates a bucket per in the destination account for Athena's workgroup if it is not already exists.
def createAthenaWGBucket(destinationAccountId,region):
    bucket = 's3inv-athena-wgp-'+region+'-'+destinationAccountId
    s3 = boto3.client("s3", region_name = region)
    try:
        if region == 'us-east-1':
            s3.create_bucket(
                Bucket = bucket
            )
        else:
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
        name = bucket+'-wg'
        if  name not in listWorkgroups():
            createWorkgroup(bucket,region)
    except ClientError as err:
        if err.response['Error']['Code'] == 'BucketAlreadyExists':
            logger.debug(f"Bucket {bucket} already exists")    
            name = bucket+'-wg'
            if  name not in listWorkgroups():
                createWorkgroup(bucket,region)
        elif err.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            logger.debug(f"Bucket {bucket} already exists and owned by you")
            #print (f"Bucket {bucket} already exists and owned by you")
            name = "s3://"+bucket+'-wg'
            if  name not in listWorkgroups():
                createWorkgroup(bucket,region)
        else:
            logger.error(f"Error {err} while creating a bucket in the destination region {region}")
    return bucket


#create workgroup for the bucket in a region
def createWorkgroup(bucket, region):
    athena = boto3.client('athena', region_name = region )
    athenaPrimaryBucket = "s3://"+bucket
    try:
        wg = athena.create_work_group(
            Name= bucket+'-wg',
            Configuration={
                'ResultConfiguration': {
                    'OutputLocation': athenaPrimaryBucket,
                    "EncryptionConfiguration": {
                    "EncryptionOption": "SSE_S3"
                    }
                }
            }
        )
    except athena.exceptions.InvalidRequestException:
        logger.info (f"The input provided {athenaPrimaryBucket} is not valid or already existed.")
        #print (f"The input provided {athenaPrimaryBucket} is not valid or already existed.")
    except athena.exceptions.InternalServerException:
        logger.error (f"Unable to create primary workgroup {athenaPrimaryBucket}, internal service error.")
        #print (f"Unable to create primary workgroup {athenaPrimaryBucket}, internal service error.")
    except ClientError as err:
        logger.error (f"Catch all errors {err}")

