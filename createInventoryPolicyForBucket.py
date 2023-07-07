import logging
from botocore.exceptions import ClientError

#logger = logging.getLogger("MainS3Inventory.InventoryPolicyForSourceBucket")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')

file_handler = logging.FileHandler('CreateInventorypolicyforbucket.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


#This method puts inventory configuration for the bucket
def createInventory(s3,sourceAccountId, sourceBucket, destinationAccountId,destinationBucket,frequency):
    # Id length has limitation on number of characters - whistle, I don't know the limit.
    sBucket = ''
    if len(sourceBucket) >50:
        sBucket = sourceBucket[0:50]
        id = sBucket+"-"+'Inventory'
    else:
        id = sourceBucket+"-"+'Inventory'
    try:
        s3.put_bucket_inventory_configuration(
            Bucket= sourceBucket,
            Id= id,
            InventoryConfiguration={
                'Destination': {
                    'S3BucketDestination': {
                        'AccountId': destinationAccountId,
                        'Bucket': "arn:aws:s3:::"+destinationBucket,
                        'Format': 'Parquet',
                        'Prefix': "inventory",
                        'Encryption': {
                            'SSES3': {}
                        }
                    }
                },
                'IsEnabled': True,
                'Id': id,
                'IncludedObjectVersions': 'All',
                'OptionalFields': [
                    'Size','LastModifiedDate','StorageClass','ETag','IsMultipartUploaded','ReplicationStatus','EncryptionStatus','ObjectLockRetainUntilDate','ObjectLockMode','ObjectLockLegalHoldStatus','IntelligentTieringAccessTier','BucketKeyStatus','ChecksumAlgorithm'
                ],
                'Schedule': {
                'Frequency': frequency
                }
            },
            ExpectedBucketOwner=sourceAccountId
        )
    except ClientError as err:
        if "InvalidArgument" in err.args[0]:
            logger.error (f"Invalid Argument in a sourcebucket {sourceBucket}") 
            #print (f"Invalid Argument in a sourcebucket {sourceBucket}") 
        elif "TooManyConfigurations" in err.args[0]:
            logger.error("You are attempting to create a new configuration in a source bucket {sourceBucket}but have already reached the 1,000-configuration limit.")
            #print ("You are attempting to create a new configuration in a source bucket {sourceBucket}but have already reached the 1,000-configuration limit.")
        elif "AccessDenied" in err.args[0]:
            logger.error(f"You are not the owner of the specified bucket {sourceBucket}, or you do not have the s3:PutInventoryConfiguration bucket permission to set the configuration on the bucket.")
            #print (f"You are not the owner of the specified bucket {sourceBucket}, or you do not have the s3:PutInventoryConfiguration bucket permission to set the configuration on the bucket.")
        else:
            logger.error(f"Error {err}while adding bucket Inventory policy.")
            #print (f"Error {err}while adding bucket Inventory policy.")

# For each bucket list inventory. If exists, then check if it is enabled. If not, enable it.
def listInventoryNextToken(s3,token,sourceBucket, sourceAccountId):
    invIdList = []
    try:
        if token == '':
            inventoryList = s3.list_bucket_inventory_configurations(
                Bucket=sourceBucket,
                ExpectedBucketOwner=sourceAccountId
            )
        else:
            inventoryList = s3.list_bucket_inventory_configurations(
                Bucket=sourceBucket,
                ContinuationToken = token,
                ExpectedBucketOwner=sourceAccountId
            )
        nextToken = inventoryList.get('NextContinuationToken')
        if nextToken is None:
            nextToken = '' 
        # bucket has no Inventory policy and needs to created.
        InvenConfList = inventoryList.get('InventoryConfigurationList')
        if InvenConfList is None:
            invIdList
        else:
            #for invLs in inventoryList['InventoryConfigurationList']:
            for invLs in InvenConfList:
                invEnabled = invLs['Id']+"|"+str(invLs['IsEnabled'])
                invIdList.append(invEnabled)
    except ClientError as err:
        if "AccessDenied" in err.args[0]:
            #print(f"You are not the owner of the specified bucket {sourceBucket}, or you dont'User does have a permission to list_bucket_inventory_configuration in the source bucket {sourceBucket}")
            logger.error(f"You are not the owner of the specified bucket {sourceBucket}, or you dont'User does have a permission to list_bucket_inventory_configuration in the source bucket {sourceBucket}")
            nextToken = ''
        else:
            #print(f" Error {err}while listing bucket inventoryconfiguration in the source bucket {sourceBucket}")
            logger.error(f" Error {err}while listing bucket inventoryconfiguration in the source bucket {sourceBucket}") 
    return nextToken,invIdList


def listInventory(s3,sourceBucket, sourceAccountId):
    invList = []
    nextToken = ''
    while True:
        nextToken,invIdList = listInventoryNextToken(s3,nextToken,sourceBucket, sourceAccountId)
        if invIdList != []:
            invList.extend(invIdList)
        else:
            invList
        if nextToken == '':
            break
    return invList


###Note - Inventory is created with S3 server key - 'SSES3'
# This method check if bucket exists and validates the policy exists and check its status
# If bucket exists, but status is not enabled, it create a update the Inventory policy, setting status enabled.
# If bucket exists but policy does not exists, it creates one. 
def validateInventoryPolicy(s3,sourceAccountId, sourceBucket, destinationAccountId,destinationBucket,frequency):
    id = []
    #check list of Inventory policies for the bucket if exists
    inventList = listInventory(s3,sourceBucket, sourceAccountId)
    # if Inventory does not exist, create inventory policy
    if len(inventList) == 0:
        logger.info(f"Source Bucket {sourceBucket} does not have any policy. Creating an inventory policy")
        createInventory(s3,sourceAccountId, sourceBucket, destinationAccountId,destinationBucket,frequency)
    else:
        # check if Inventory is enabled
        logger.info(f"Source Bucket {sourceBucket} has a policy. Checking the status if it is enabled")
        for invEnbList in inventList:
            invConfigId, status = invEnbList.split('|')
            #creating list of Config ID to check later if config with name bucket+'-'+'inventory'
            id.append(invConfigId)
            inventoryPolicyName = sourceBucket+'-'+'inventory'
            if (invConfigId == inventoryPolicyName and status == 'False'):
                logger.info(f"Source Bucket {sourceBucket} exists and has an inventory policy {inventoryPolicyName} with a status not enabled. Setting the status to enable")
                createInventory(s3,sourceAccountId, sourceBucket, destinationAccountId,destinationBucket,frequency)
            else:
                logger.info(f"Source Bucket {sourceBucket} exists and has an inventory policy with the name {inventoryPolicyName} and status set as enabled. No action needed.")

   