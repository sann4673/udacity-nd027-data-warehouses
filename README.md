# udacity-nd027-AWS-data-warehouse-project


## 1. Introduction

This project is the submission for the Course 3 (Cloud Data Warehouses) of the Udacity Data Engineering Nanodegree. This project demonstrates how to build a scalable ETL (Extract, Transform, Load) pipeline for Sparkify, a music streaming startup, which has expanded its user base and song database. The objective is to provide Sparkify's analytics team with clean, structured data from their JSON logs and song metadata stored in S3, enabling insights into user listening patterns.

### 1.1. Project Overview
Sparkify’s data is stored in JSON format in Amazon S3, including user activity logs and metadata on songs available in the app. The goal of this project is to design a pipeline that:
* Extracts data from S3,
* Stages it in Amazon Redshift,
* Transforms it into a series of dimensional tables in a Redshift data warehouse, and
* Conduct simple analysis.
This structured data will enable Sparkify’s analytics team to easily query and analyze user activity.

### 1.2. Key Components
* AWS Redshift: A managed data warehouse solution where the data is staged and stored.
* ETL Pipeline: Built using Python and SQL, this pipeline handles the data flow from S3 to Redshift.
* Staging Tables (staging_events, staging_songs): Data from the raw logs and song metadata is loaded into staging tables in Redshift, allowing for transformation and processing independently from the raw data.
* Fact and Dimensional Tables (songplays, users, songs, artists, time): These tables follow a star schema, which optimizes query performance and organizes data for business analytics.

### 1.3. Key Skills & Technologies
* AWS (S3, Redshift, IAM): Cloud infrastructure for scalable data storage and processing.
* Data Warehousing Concepts: Applying concepts like star schema and dimensional modeling.
* SQL: For creating, loading, and transforming data into structured tables.
* Infrastructure as Code (IaC): Setting up AWS resources programmatically to ensure reliable and repeatable deployments.



## 2. How to Run
These steps assume that you have the necessary permissions and credentials set up for AWS and Redshift access. Be mindful of any AWS costs associated with Redshift usage, and remember to terminate your cluster when you're done.

### 2.1. Prerequisites
* Python 3 installed on your machine.
* Boto3 and Psycopg2 Python libraries installed for AWS and PostgreSQL connections:
```
pip install boto3 psycopg2
```
* AWS Account with access to S3 and Redshift. Temporary credentials are provided by Udacity, but you can use your own account.

### 2.2. Files Overview
* `dwh.cfg`: Configuration file for storing AWS credentials, cluster configurations, and database details (Redshift IAM role, cluster, S3 paths).
* `create_cluster.py`: Script to create a Redshift cluster on AWS.
* `sql_queries.py`: Contains all the SQL queries needed for creating the tables, loading the tables and conducting simple analysis.
* `create_tables.py`: Script to create and reset tables in Redshift.
* `etl.py`: Main ETL script that extracts data from S3, stages it in Redshift, and transforms it into the final tables. It also includes the counting queries to validate the outcome of ETL.
* `analysis.py`: Example queries to analyze the transformed data.

### 2.3. Steps to Run
1. Configure AWS Resources (`dwh.cfg`)
Open `dwh.cfg` and populate it with your AWS IAM role credentials, cluster details, and database information. This file should include:
* AWS IAM credentials (KEY, SECRET)
* Cluster configuration (DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER)
* Database and IAM role configuration (DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME)
* S3 data paths for log and song data (LOG_DATA, SONG_DATA, LOG_JSONPATH)

2. Create the Redshift Cluster (`create_cluster.py`)
Run the `create_cluster.py` script to set up the Redshift cluster on AWS. This script:
* Creates an IAM role for Redshift access to S3.
* Initializes a Redshift cluster using the configurations in `dwh.cfg`.
To execute, run:
```
python create_cluster.py
```
Wait for the cluster to be set up (can take a few minutes). Once completed, update `dwh.cfg` with the cluster's endpoint (HOST) and IAM role ARN.

3. Create Tables (`create_tables.py`)
Run `create_tables.py` to create the all the tables in Redshift, including staging tables (staging_events, staging_songs), fact table (songplays), and dimension tables (users, songs, artists, time).
This script resets any existing tables to ensure a clean environment.
To execute, run:
```
python create_tables.py
```

4. Run the ETL Pipeline (`etl.py`)
Execute the `etl.py` script to load data from S3 to Redshift staging tables, then transform and load it into the fact and dimension tables.
To execute, run:
```
python etl.py
```
This script will:
* Extract the raw JSON data from the S3 buckets.
* Load the data into the Redshift staging tables.
* Transform and load the data from the staging tables into the fact and dimension tables.

5. Analyze the Data (`analysis.py`)
Once the ETL pipeline is complete, you can run `analysis.py` to perform some basic queries on the transformed data in Redshift. This script contains a set of example queries for exploring user activity and song plays.
To execute, run:
```
python analysis.py
```

6. Delete Redshift Cluster
When you are done, delete the Redshift cluster to avoid any additional charges. Run:
```
python create_cluster.py --delete
```

### 2.4. Troubleshooting Tips
IAM Roles and Permissions: Make sure your IAM role is configured with the necessary S3 read permissions and Redshift access.
Cluster Connection: Ensure you update the dwh.cfg file with the Redshift endpoint and role ARN after creating the cluster.
Network Configuration: If you’re having trouble connecting, check that your Redshift cluster's security group allows inbound traffic on the specified port (5439 by default).

### 2.5. Important Notes
Always remember to terminate your Redshift cluster after completing your work to avoid any unexpected charges.
`dwh.cfg` contains sensitive credentials; avoid sharing it publicly or committing it to a public repository.





## 3. Expected Results

After running `etl.py`, you should expect to see outputs like the following:
```
Copying staging_events
Copying staging_songs
Inserting songplays
Inserting users
Inserting songs
Inserting artists
Inserting time
Counting staging_events: 8056
Counting staging_songs: 14896
Counting songplays: 6820
Counting users: 107
Counting songs: 14896
Counting artists: 10025
Counting time: 8023
```

After running `analysis.py`, you should expect to see outputs like the following:
```
What are the top 5 played songs?
                                               title  count
0                                     You're The One     37
1                                I CAN'T GET STARTED      9
2  Catch You Baby (Steve Pitron & Max Sanna Radio...      9
3  Nothin' On You [feat. Bruno Mars] (Album Version)      8
4                           Hey Daddy (Daddy's Home)      6


What are the highest usage hours of a day?
   hour total_usage
0    17       10050
1    15        6311
2    18        5836
3    20        4721
4    16        4690


Who are the top 5 artists with the longest played songs?
                             name total_duration
0                   Dwight Yoakam           8843
1                      Ron Carter           4473
2  Kid Cudi / Kanye West / Common           2320
3                           B.o.B           2152
4                       Metallica           1634


Who are the top 5 users with the most listening records?
  user_id  total
0      49    689
1      80    665
2      97    557
3      15    463
4      44    397
```
