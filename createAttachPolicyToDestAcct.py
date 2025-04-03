import boto3
import json
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')

file_handler = logging.FileHandler('createAttachPolicyToDestAcct.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
iam = boto3.client("iam")

#Creating a policy with PolicyName of 'S3InvDestAccountPolicy'
policyName='S3InvDestAccountPolicy'

# method to determine accountID. I will be using this account Id as the destination account Id.
def getUserName():
    sts = boto3.client('sts')
    user = sts.get_caller_identity()['Arn'].split("/")[-1]
    logger.info(f"IAM user name is {user}")
    return(user)


def create_iam_policy(policyName):
    # Create IAM client
    iam = boto3.client('iam')
    InLinePolicy = {}
    # Create a custom managedpolicy
    my_managed_policy = {
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
                "Effect": "Allow",
                "Action": [
                    "organizations:Describe*",
                    "organizations:List*"
                ],
                "Resource": "*"
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
                    "s3:PutBucketPublicAccessBlock",
                    "s3:GetInventoryConfiguration",
                    "s3:PutBucketPolicy",
                    "s3:CreateBucket",
                    "s3:ListBucket",
                    "iam:GetUser",
                    "s3:GetBucketLocation",
                    "s3:GetBucketPolicy",
                    "s3:PutInventoryConfiguration"
                ],
                "Resource": "arn:aws:s3:::*"
            },
            {
                "Effect": "Allow",
                "Action": "sts:AssumeRole",
                "Resource": "arn:aws:iam::*:role/OrgS3role"
            }
        ]
    }
    try:
        InLinePolicy = iam.create_policy(
            PolicyName = policyName,
            PolicyDocument=json.dumps(my_managed_policy)
        )
    except iam.exceptions.EntityAlreadyExistsException:
        logger.info(f"Policy already exists")
        


# List Policies and extract policy created using create_iam_policy()
def list_policies(policyName):
    policyArn = ''
    paginator = iam.get_paginator('list_policies')
    for response in paginator.paginate(Scope="Local"):
        for policy in response["Policies"]:
            if policy['PolicyName'] == policyName:
                policyArn = policy['Arn']
                logger.info(f"Policy Name: {policy['PolicyName']} ARN: {policyArn}")
    return policyArn    


#Attached policy to user profile
def attach_user_policy(policy_arn, userName):
    try:
        attachedUserPolicy = iam.attach_user_policy(
        UserName=userName,
        PolicyArn=policy_arn
        )
    except ClientError as e:
        logger.error(f"Error {e} while attaching {policy_arn} to the user {userName}")


def main():
    # track the user name
    userName = getUserName()
    create_iam_policy(policyName)
    policy_arn = list_policies(policyName)
    attach_user_policy(policy_arn, userName)

    
if __name__ == "__main__":
    main()


