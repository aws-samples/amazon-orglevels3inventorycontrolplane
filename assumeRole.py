import boto3
from botocore.exceptions import ClientError


import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s  %(name)s  %(levelname)s: %(message)s')

file_handler = logging.FileHandler('assumingRole.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


def getCredentialsForRole(sourceAccountId):
    assume_role_arn = "arn:aws:iam::"+sourceAccountId+":role/OrgS3role"
    #assume_role_arn ="arn:aws:iam::086413530787:role/OrgS3role"
    logger.info(f"assumed role arn is {assume_role_arn}")
    session_name = 'test'
    sts_client = boto3.client(
        'sts')
    try:
        response = sts_client.assume_role(
            RoleArn=assume_role_arn, RoleSessionName=session_name)
        temp_credentials = response['Credentials']
        logger.info(f"Temporary credentials are used to access account's S3 buckets.")
    except ClientError as err:
        temp_credentials = {}
        logger.error(f"Unable to assume role for the source account {sourceAccountId}")
        logger.error (f"Error is {err}")

    return temp_credentials

