# run this account in each source account
import json 
import boto3
import sys
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')

file_handler = logging.FileHandler('orgSourceAccountRole.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


iam_client = boto3.client('iam')

role_name = "OrgS3role"

logger.info(f"Command to run this account from each source account is py.exe ./orgSourceAccountRole.py")
logger.info(f"Ensure to provide destination/centralized a valid AWS 12-digit account id")


destination_account_id = sys.argv[1]
if len(destination_account_id) != 12:
    logger.error(f"Please ensure that AWS Account Id for the destination account is 12-digit - {destination_account_id}. Exiting ....")
    sys.exit()

# method to determine accountID. I will be using this account Id as the destination account Id.
def getAccountId():
    iam = boto3.resource('iam')
    account_id = iam.CurrentUser().arn.split(':')[4]
    return(account_id)

iamUser =sys.argv[2]
logger.critical(f"Please ensure the iam user: {iamUser} belongs to the {destination_account_id} or you will unable to assume the role")

#"AWS": "arn:aws:iam::"+destination_account_id+":user/ballu"
def trustPolicy(account_id,destination_account_id):   
    if account_id != destination_account_id:
        trust_relationship_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::"+destination_account_id+":user/"+iamUser
                },
                "Action": "sts:AssumeRole"
                }
            ]
        }
    else:
        trust_relationship_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                "Effect": "Allow",
                "Principal": {
                    "Service": "ec2.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
                }
            ]
        }
    return trust_relationship_policy

def createSourceAccountRole(account_id,destination_account_id):
    logger.info(f"Analyzing source account id {account_id} ...")
    logger.info(f"Destination account id is {destination_account_id} ...")
    logger.info(f"creating a role in source account id {account_id} with role name as {role_name} ...")
    trustRelationshipPolicy = trustPolicy(account_id,destination_account_id)
    try:
        create_role_res = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trustRelationshipPolicy),
            Description='This role is used to access S3 buckets from the another account',
            Tags=[
                {
                    'Key': 'Owner',
                    'Value': account_id
                }
            ]
        )
    except ClientError as error:
        if error.response['Error']['Code'] == 'EntityAlreadyExists':
            logger.error (f"Role already exists {account_id}... hence exiting from here")
            return 'Role already exists... hence exiting from here'
        else:
            logger.error(f"Unexpected error occurred... Role could not be created {error}")
            return 'Unexpected error occurred... Role could not be created', error
    logger.info(f"Role is created: {create_role_res['Role']['Arn']} under {account_id}. Next attaching policy")
    policy_json = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": [
                    "s3:ListAllMyBuckets"
                ],
                "Effect": "Allow",
                "Resource": [
                    "arn:aws:s3:::*"
                ]
            },
            {
                "Action": [
                    "s3:ListBucket",
                    "s3:GetBucketLocation"
                ],
                "Effect": "Allow",
                "Resource": "arn:aws:s3:::*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetInventoryConfiguration",
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                    "s3:GetBucketPolicy",
                    "s3:PutInventoryConfiguration"
                ],
                "Resource": "arn:aws:s3:::*"
            }
        ]
    }
    policy_name = role_name + '_policy'
    policy_arn = ''
    logger.info(f"policy_name is {policy_name}")
    try:
        policy_res = iam_client.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_json)
        )
        policy_arn = policy_res['Policy']['Arn']
        logger.info(f"Policy arn is {policy_arn}")
    except ClientError as error:
        if error.response['Error']['Code'] == 'EntityAlreadyExists':
            logging.error('Policy already exists {policy_arn}... hence using the same policy') 
        else:
            logging.error(f"Unexpected error occurred... hence cleaning up {error}")
            iam_client.delete_role(
                RoleName= role_name
            )
            logger.error(f"Role could not be created... {error}")
            return 'Role could not be created...', error
    try:
        policy_attach_res = iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )
        logger.info(f"Policy is attached to the role {role_name} ")
    except ClientError as error:
        logging.error('Unexpected error occurred... hence cleaning up')
        iam_client.delete_role(
            RoleName= role_name
        )
        logging.error(f'Role could not be created... {error}')
        return 'Role could not be created...', error


def main():
    account_id = getAccountId()
    createSourceAccountRole(account_id, destination_account_id)

if __name__ == "__main__":
    main()
