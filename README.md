# zluri_test
**Steps to run the code**

Use python3
Install libraries:-
Pandas (pip3 install pandas)
Snowflake-sqlalchemy (pip3 install snowflake-sqlalchemy)\
Datetime (pip3 install datetime)
I have used Snowflake as the data lake whose credentials can be found below.
Raw_data file path has to be provided correctly in line #18 in the variable raw_data. I have used my local path
Command to execute the job → python3 assignment.py

**Note** 
As I found the duplicates in the raw data based on SKU, I have handled de-duping the raw data set while merging it into the final data table (merge query)

**Data lake details**
Url:- https://pz00498.ap-southeast-1.snowflakecomputing.com/console/login
Username : karanmalhotra
Password : Zluri@test1
database=ZLURI_TEST
schema=ASSIGNMENT
warehouse=ZLURI_TEST
role=ACCOUNTADMIN

Data Table:- zluri_test
select * from zluri_test.assignment.zluri_test

Aggregated table:- zluri_aggregated_data
select * from zluri_test.assignment.zluri_aggregated_data


One time script to create the data table:- this is not required until not dropped intentionally.

create or replace table ZLURI_TEST
(
NAME	VARCHAR(16777216)
, SKU	VARCHAR(16777216)
, DESCRIPTION	VARCHAR(16777216)
, ETL_SYNCED	TIMESTAMP_NTZ(9)
)


**Points to achieve**
Created data ingestion pipeline to perform the parallel processing of files into the table with minimal gab of 1 second.
This is achieved using the variable(ts) in line #22 of the code and this variable is appended in the intermediate tables to keep multiple versions of raw data in case of parallel processing.
And finally, I am merging the data from the raw dataset to the final table i.e zluri_test.
I am de-duping the records on SKU (in the raw table) while merging them into the final table.
Handled insert and update based on SKUs in Merge query. (#51)
