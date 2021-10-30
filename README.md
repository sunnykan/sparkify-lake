# Project Summary
Create a data lake for data on songs and user activity on a music streaming app to understand what music users are listening to on the service. User activity and metadata on songs are recorded in separate JSON files which are kept in separate directories in an Amazon S3 bucket. Design a database schema to facilitate the analysis. Create an EMR cluster on AWS and connect to it. Write an ETL pipeline to read the JSON files, transform the data as needed to populate the tables and write the tables as parquet files into another S3 bucket. Each table is written to a separate folder.

# Execution  
1. Start a cluster on Amazon EMR: 
  * Option 1: Go to the EMR [website](https://us-west-2.console.aws.amazon.com/elasticmapreduce/) and click 'Create cluster'. Select the relevant software and hardware configurations. Note that a key-pair may obtained on the EC2 [page](https://us-west-2.console.aws.amazon.com/ec2/v2/). 
  * Option 2: Create a cluster using AWS cli. In your local terminal type: `aws emr create-cluster --name spark-cluster --use-default-roles --release-label emr-5.20.1 --instance-count 3 --applications Name=Spark --ec2-attributes KeyName=spark-cluster-key,SubnetId=subnet-0b81e3f7597321ecb --instance-type m4.large `

2. Create a S3 bucket on the S3 [website](https://s3.console.aws.amazon.com/s3) to hold the transformed data. Edit the copy of `etl.py` to reflect the change.

3. Open a SSH tunnel on the local terminal and copy the script file `etl.py` to the master node.

4. Connect to the master node and submit the spark job. To do so, enter `/usr/bin/spark-submit --master yarn ./etl.py`.

5. Once the job completes, go to the S3 bucket to view the files.

6. The cluster may be terminated by going to the EMR [website](https://us-west-2.console.aws.amazon.com/elasticmapreduce/) and selecting 'Terminate'.

7. The script may also be run locally and tested on the data in the `data` folder. Edit the `etl.py` file as needed and run `python etl.py` in the terminal.

# Files
- `etl.py`: Extracts data from JSON files in a S3 bucket, transforms them and populates tables based on the schema and writes them as parquet format in separate folders on a S3 bucket.
		
