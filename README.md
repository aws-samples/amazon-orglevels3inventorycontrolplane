**Here’s the steps for executing Python scripts:**
 
If you set a member account to collect Org-level S3 inventories in your Organization, please nominate that account as the destination account (other than Management Account) for managing inventories for all other accounts that act as source accounts.
 
**PREREQUISITE :**

• Register destination account as the delegate administrator account

• Python3.7+ installed on your machine, or on EC2 instance from your destination account, or Cloud9 

1. Clone the registry containing python scripts. Theses scripts require several Python libraries, which are listed in the requirements.txt file.

2. Run the requirement.txt file from the folder where you have downloaded all files.
**pip3 install -r requirements.txt**
 
NOTE: If the user has an administration access, you can skip step#3.

3. From the destination account, run py.exe .\createAttachPolicyToDestAcct.py to add IAM policy – “S3InvDestAccountPolicy” to IAM user’s profile to perform S3 service operational activities such as create a bucket, put bucket policy and others. 

Note: Please ensure that you have an appropriate AWS destination account credentials/role set on the machine, EC2 Instance or Cloud9 instance before executing the codes. 

4. If the destination account is not set as delegate administrator, register the destination account (other than the Management Account ) with delegated administrator permissions to create and manage stack sets with service-managed permissions for the organization. 

5. Create a CloudFormation stacksets from the delegated member account using script <<OrgS3InvSourceAccountPolicy.json>> or <<<<OrgS3InvSourceAccountPolicy.yaml>> to create IAM role, allowing the destination account to assume role and create S3 Inventories.

6. On the Specify stack details page, type a stack name in the Stack name box, Destination IAM user (the default IAM user is set to “ ”. I would recommend using the IAM user that will be performing S3 Inventory operations), and a 12-digit AWS destination account Id to assume the role from AWS source accounts. 

7. Ensure “OrgS3readonlyRole” role has the corresponding “OrgS3readonlyRole_policy” policy with a trust relationship with the destination account and IAM user (if provided) in each source account.

8. From the destination account, you can run py.exe .\ orgS3Inventory.py to create a centralized S3 inventory bucket per region in the destination account to record inventories from all source accounts and buckets. Format of the destination bucket is “s3inventory-<<region>>-<<destinationaccountId>>”

9. Finally, execute a CloudFormation stack - S3Inventory.yaml from the destination account. This stack will create two lambda functions – 
     i.	“lambda-function-inventory” which triggers when a new object is added to the destination S3 bucket. The function creates an Athena           table “Inventory” if it does not exist along with two partitions – “bucketname” and ‘dt”.
     ii.	Another lambda function is to set Event types of “s3:ObjectCreated:*” for the destination bucket.
10. Navigate to AWS Athena console/API to run analytics on the storage inventory from the table - "inventory" that is created per region.
