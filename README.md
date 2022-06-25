# Project 4: Data Lake

Objective: Build an ETL pipeline for a music app (Sparkify) using Spark and AWS. Two datasets used are both in JSON format, and key parts are extracted from the datasets to build a star schema optimized for queries on song play analysis.

## Schema & Table Design

The project's star schema contains 1 fact table and 4 dimension tables:

**Fact Table(s)**

1. songplays - records in log data associated with song plays i.e. records with page NextSong
    * columns: songplay_id (pkey), start_time (foreign key to time), user_id (foreign key to users), level, song_id (foreign key to songs), artist_id (foreign key to artists), session_id, location, user_agent, year, month

**Dimension Table(s)**
1. users - users in the app
    * columns: user_id (pkey), first_name, last_name, gender, level
2. songs - songs in music database
    * columns: song_id (pkey), title, artist_id, year, duration
3. artists - artists in music database
    * columns: artist_id (pkey), name, location, latitude, longitude
4. time - timestamps of records in songplays broken down into specific units
    * columns: start_time (pkey), hour, day, week, month, year, weekday

`etl.py` loads data from S3, extracts key columns to model the fact and dimensional tables, and writes parquet files into S3 so that the tables can be stored as files that can be run schema-on-read

`dl.cfg` contains the AWS credentials for SparkSession initialization.

## Steps to run the program

1. Create a new EMR cluster with Spark installed
2. Migrate `dl.cfg` & `etl.py` to the master node so that it can be run on the EMR Cluster
3. Configure Spark to run on Python3 to enable library imports (e.g. ConfigParser)
4. Run `etl.py` to perform the ETL Process 
```
spark-submit etl.py
```
5. Check the Spark UI to ensure the tables are correctly written in the desired S3 bucket.

