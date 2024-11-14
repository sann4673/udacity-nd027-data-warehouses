import boto3
import pandas as pd
import json
import configparser
import argparse
import logging
import time
from botocore.exceptions import ClientError


config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))


KEY                     = config.get('AWS', 'KEY')
SECRET                  = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE        = config.get('CLUSTER', 'DWH_CLUSTER_TYPE')
DWH_NUM_NODES           = config.get('CLUSTER', 'DWH_NUM_NODES')
DWH_NODE_TYPE           = config.get('CLUSTER', 'DWH_NODE_TYPE')
DWH_CLUSTER_IDENTIFIER  = config.get('CLUSTER', 'DWH_CLUSTER_IDENTIFIER')
DWH_IAM_ROLE_NAME       = config.get('CLUSTER', 'DWH_IAM_ROLE_NAME')
DWH_REGION              = config.get('CLUSTER', 'REGION')

DWH_DB                  = config.get('DB', 'DB_NAME')
DWH_DB_USER             = config.get('DB', 'DB_USER')
DWH_DB_PASSWORD         = config.get('DB', 'DB_PASSWORD')
DWH_PORT                = config.get('DB', 'DB_PORT')


def create_clients():
    options = dict(region_name=DWH_REGION, 
                  aws_access_key_id=KEY, 
                  aws_secret_access_key=SECRET)
    ec2 = boto3.resource('ec2', **options)
    iam = boto3.client('iam', **options)
    redshift = boto3.client('redshift', **options)
    return ec2, iam, redshift

def create_new_iam_role(iam):
    try:
        print("Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                  'Effect': 'Allow',
                  'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)

def attach_policy(iam):
    print("Attaching Policy")
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']

    
def get_role_arn(iam):
    role_Arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    print(f"Getting IAM Role Arn: {role_Arn}")
    return role_Arn

def create_redshift_cluster(redshift, roleArn):
    try:
        response = redshift.create_cluster(        
          #HW
          ClusterType=DWH_CLUSTER_TYPE,
          NodeType=DWH_NODE_TYPE,
          NumberOfNodes=int(DWH_NUM_NODES),

          #Identifiers & Credentials
          DBName=DWH_DB,
          ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
          MasterUsername=DWH_DB_USER,
          MasterUserPassword=DWH_DB_PASSWORD,
          
          #Roles (for s3 access)
          IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def delete_redshift_cluster(redshift):
    try:
        redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    except Exception as e:
        print(e)

def delete_iam_role(iam):
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)


def open_tcp(ec2, vpc_id):
    try:
        vpc = ec2.Vpc(id=vpc_id)
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except ClientError as e:
        logging.warning(e)


def main(args):
    ec2, iam, redshift = create_clients()
    if args.delete:
        delete_redshift_cluster(redshift)
        delete_iam_role(iam)
    else:
        create_new_iam_role(iam)
        attach_policy(iam)
        roleArn = get_role_arn(iam)
        create_redshift_cluster(redshift, roleArn)
        # check the status of cluster until 'available'
        timestep = 15
        for _ in range(int(600/timestep)):
            cluster_description = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            cluster_status = cluster_description['ClusterStatus']
            if cluster_status == 'available': break
            logging.info(f"Cluster status is {cluster_status}. Retrying in {timestep} seconds.")
            time.sleep(timestep)
        if cluster_description:
            logging.info(f"Cluster created at {cluster_description['Endpoint']}")
            open_tcp(ec2, cluster_description['VpcId'])
        else:
            logging.error("Could not connect to cluster")



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--delete', dest='delete', default=False, action='store_true')
    args = parser.parse_args()
    main(args)