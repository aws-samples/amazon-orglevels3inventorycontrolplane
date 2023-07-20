import boto3
import sys
import logging
from tqdm import tqdm
import createDestinationBucket
import createInventoryPolicyForBucket
import updateBucketPolicy
import assumeRole
import createAthenaWorkgroup
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')

file_handler = logging.FileHandler('OrgAccountS3Inventory.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

region = ''
frequency = 'Daily'
logger.info("S3 Inventory frequency is set to : {}".format(frequency))




# method to determine accountID. I will be using this account Id as the destination account Id.
def getAccountId():
    sts = boto3.client('sts')
    account_id = sts.get_caller_identity()['Account']
    return(account_id)

# With Org account S3 Inventory structure, I will be using the destination accounts as the account
# where the user wants to create a Org-level S3 Inventory.

destinationAccountId = getAccountId()
logger.info("Destination account ID: {}".format(destinationAccountId))
#For Org accounts, will passing source account id org api list_accounts()
sourceAccountId = ''

# The next two methods are used get the list of accounts from Orgs. 
# It seems like quota to list account with a single API is 20 accounts
def listNextAccounts(nextToken):
    accountList = [] 
    org = boto3.client('organizations')
    try:
        if nextToken is None or nextToken =='':
            accounts = org.list_accounts(
                    MaxResults=20
            )
        else:
            accounts = org.list_accounts(
                    MaxResults=20, NextToken=nextToken
            )
    except org.exceptions.AccessDeniedException:
        print(f"IAM user does not have access to list accounts. Exiting ...")
        logger.error(f"IAM user does not have access to list accounts. Exiting ..")
        sys.exit()
    nextToken = accounts.get('NextToken')
    if nextToken is None:
        nextToken = '' 
    for account in accounts['Accounts']:
        if account['Status']== 'ACTIVE':
            accountList.append(account['Id'])
    return nextToken,accountList


def listAccounts():
    accList = []
    nextToken = ''
    while True:
        nextToken,accountList=listNextAccounts(nextToken)
        if accountList != []:
            accList.extend(accountList)
        if nextToken =='':
            break
    return accList


# get the location of the bucket
def getBucketLocation(s3,bucket):
    try:
        bucket['Region'] = s3.get_bucket_location(Bucket=bucket['Name'])['LocationConstraint']
        if bucket['Region'] is None:
            bucket['Region'] = 'us-east-1'
    except ClientError as e:
        if "AccessDenied" in e.args[0]:
            # If we don't have perms to call get_bucket_location(), set region to None and keep going
            logger.error("get_bucket_location(bucket='{}') AccessDenied, skipping.".format(bucket['Name']))
            bucket['Region'] = None
        else:
            raise
    return bucket['Region']


# Creating a Dict of buckets in an account with key as a region and values as bucket name
bucketDict = {}
def insertDict(region, bucketName): 
    if region not in bucketDict:
        bucketDict[region] = [bucketName]
    else:
        bucketDict[region].append(bucketName)
    return bucketDict


#Listing buckets and creating a dictonary of all buckets in an account with region as key
def listBuckets(s3):
    try:
        BucketName = s3.list_buckets() 
    except ClientError as e:
        if "AccessDenied" in e.args[0]:
            print(f"IAM User does not have access to list buckets. Exiting ...")
            logger.error(f"IAM User does not have access to list buckets. Exiting ...")
            sys.exit()
    for bucket in BucketName['Buckets']:
        location = getBucketLocation(s3,bucket)
        logger.info(f"bucket name is {bucket['Name']}")
        regionDict = insertDict(location, bucket['Name'])
    return regionDict


# using this method for S3 control plane activity like create bucket and others
def tempCredentials(temp_credentials,region):
    if region == 'us-east-1': 
        region = None
    s3_client = boto3.client(
        's3',
        region_name=region,
        aws_access_key_id=temp_credentials['AccessKeyId'],
        aws_secret_access_key=temp_credentials['SecretAccessKey'],
        aws_session_token=temp_credentials['SessionToken']
    )
    return s3_client

# for listing bucket from the source account, ignoring region_name from the s3.client 
def tempBucketCredentials(temp_credentials):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=temp_credentials['AccessKeyId'],
        aws_secret_access_key=temp_credentials['SecretAccessKey'],
        aws_session_token=temp_credentials['SessionToken']
    )
    return s3_client

# need to work on sourceAccountId and destinationaccountId issue
#creating a destination bucket and setting policy if does not exists
def createDestBucketAndSetPolicies(destinationAccountId):
    destBucket = []
    sourceAccountIds = listAccounts()
    for sourceAccountId in tqdm(sourceAccountIds):
        print(f"Analyzing account {sourceAccountId}")
        print(f"Source Account Id is {sourceAccountId}") 
        print(f"Destination Account Id is {destinationAccountId}") 
        logger.info(f"Analyzing account {sourceAccountId}")
        logger.info(f"Source Account Id is {sourceAccountId}")
        logger.info(f"Destination Account Id is {destinationAccountId}") 
        if sourceAccountId != destinationAccountId:
            temp_credentials = assumeRole.getCredentialsForRole(sourceAccountId)
            if temp_credentials != {}:
                #using tempBucketCredentials as I need to list bucket and does not need region information
                s3BucketClient = tempBucketCredentials(temp_credentials)
                accountDict = listBuckets(s3BucketClient)
            else:
                continue
        else:
            #since sourceAccountId and destinationAccountId are same, no need to assume role
            s3 = boto3.client('s3')
            accountDict = listBuckets(s3)
        for item in tqdm(accountDict.items()):
            print(f"Analyzing region {item[0]} ...")
            logger.info(f"Analyzing region {item[0]} ...")
            logger.info(f"sourcebuckets are {item[1]}")
            # Assuming role another account with the temporary credentials to work with the S3 service.
            #creating a bucket with a region item[0]
            s3c =boto3.client("s3", region_name = item[0])
            dBucket = createDestinationBucket.createBucket(s3c,item[0], destinationAccountId)
            logger.info(f"The destination Bucket {dBucket} will be created in a region {item[0]} if it does not exist")
            destBucket.append(dBucket)
            sourceBuckets = item[1]
            # if  destination bucket exists in the list of the resource bucket, remove it
            if dBucket in sourceBuckets:
                sourceBuckets.remove(dBucket)
            logger.info(f"List of source buckets in a region {item[0]} is/are {sourceBuckets}")
            if sourceAccountId != destinationAccountId:
                #Assuming role from the source account
                temp_credentials = assumeRole.getCredentialsForRole(sourceAccountId)
                s3_client = tempCredentials(temp_credentials,item[0])
                for sourceBucket in sourceBuckets:
                    createInventoryPolicyForBucket.validateInventoryPolicy(s3_client,sourceAccountId, sourceBucket, destinationAccountId,dBucket,frequency)
                    #for updating destination bucket policy use S3 client as code runs with dest account credentials.
                    s3c =boto3.client("s3", region_name = item[0])
                    updateBucketPolicy.addOrUpdatePolicy(s3c,sourceBucket,sourceAccountId,dBucket,destinationAccountId)
            else:
                for sourceBucket in sourceBuckets:
                    s3c =boto3.client("s3", region_name = item[0])
                    createInventoryPolicyForBucket.validateInventoryPolicy(s3c,sourceAccountId, sourceBucket, destinationAccountId,dBucket,frequency)
                    updateBucketPolicy.addOrUpdatePolicy(s3c,sourceBucket,sourceAccountId,dBucket,destinationAccountId)
    for key in accountDict.keys():
        logger.info((f"creating workgroup if it does not exist region is {key}"))
        createAthenaWorkgroup.createAthenaWGBucket(destinationAccountId,key)
    # clearing dictionary
    accountDict.clear()


def main():
    createDestBucketAndSetPolicies(destinationAccountId)
    

if __name__ == "__main__":
    main()




