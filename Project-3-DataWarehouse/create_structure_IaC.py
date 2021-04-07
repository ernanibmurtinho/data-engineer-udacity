#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import boto3
import json
import configparser
import time
import psycopg2


# # You need to put your AWS KEY on the dwh.cfg as you want to test this code
# 
# If you don't have one, follow the steps below:
# - Create a new IAM user in your AWS account
# - Give it `AdministratorAccess`, From `Attach existing policies directly` Tab
# - Take note of the access key and secret 
# - Edit the file `dwh.cfg` and fill
# <font color='red'>
# <BR>
# [AWS]<BR>
# KEY= YOUR_AWS_KEY<BR>
# SECRET= YOUR_AWS_SECRET<BR>
# <font/>
# 

# # Load DWH Params from a file


config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             })


# ## Create clients for EC2, S3, IAM, and Redshift


import boto3

ec2 = boto3.resource('ec2',
                        region_name='us-west-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

s3 = boto3.resource('s3',
                        region_name='us-west-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

iam = boto3.client('iam',
                        region_name='us-west-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

redshift = boto3.client('redshift',
                        region_name='us-west-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)



# ## IAM ROLE
# - Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)

def create_iam_roles():

    from botocore.exceptions import ClientError
    
    global roleArn

    try:
        print('1.1 Creating a new IAM Role')
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift Clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                  'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)

    print('1.2 Attaching Policy')
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']


    print('1.3 Get the IAM role ARN')

    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)


# - Create a RedShift Cluster

def create_redshift_cluster():
    
    try:
        response = redshift.create_cluster(        
            # Hardware
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),        

            # Identifiers & credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,        

            # Roles ( s3 access )
            IamRoles=[roleArn]

        )
    except Exception as e:
        print(e)


def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    df = pd.DataFrame(data=x, columns=["Key", "Value"])
    clusterStatus = df.Value[2]
    return pd.DataFrame(data=x, columns=["Key", "Value"]), clusterStatus


def get_endpoint_arn_redshift():
    
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    
    df, clusterStatus = prettyRedshiftProps(myClusterProps)
    
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    while clusterStatus == 'creating' :
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        df, clusterStatus = prettyRedshiftProps(myClusterProps)
        time.sleep(10)
        print('cluster redshift sendo criado!')
        print(clusterStatus)
        
    else:

        DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
        DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
        endpoint = myClusterProps['Endpoint']

        config.set( 'DWH', 'DWH_ENDPOINT', str(DWH_ENDPOINT) )
        config.set( 'DWH', 'DWH_ROLE_ARN', str(roleArn) )

        config.set( 'CLUSTER', 'host', str(DWH_ENDPOINT) )

        with open('dwh.cfg', 'w') as configfile:
            config.write(configfile)
               
        # print("DWH_ENDPOINT :: ", endpoint)
        # print("DWH_ROLE_ARN :: ", roleArn)


# ## Open an incoming  TCP port to access the cluster endpoint

        try:
            vpc = ec2.Vpc(id=myClusterProps['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]
            print(defaultSg)

            defaultSg.authorize_ingress(
                GroupName= defaultSg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(DWH_PORT),
                ToPort=int(DWH_PORT)
            )
        except Exception as e:
            print(e)
    
    
    
def clean_redshift_cluster():


    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    prettyRedshiftProps(myClusterProps)


def clean_iam_roles():

    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)


def main():
    
    create_iam_roles()
    create_redshift_cluster()
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    prettyRedshiftProps(myClusterProps)
    get_endpoint_arn_redshift()
    
    #To clean the environment, uncomment these two lines below(and comment the other, as you don't need to run it again):
    #clean_redshift_cluster()
    #clean_iam_roles()
 
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print("connecting to database")
    cur = conn.cursor()
    print("closing the connection!")
    conn.close()


if __name__ == "__main__":
    main()    
    
