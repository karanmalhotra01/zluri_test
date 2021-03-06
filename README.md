# Steps to run the code

- Use python3
- Install libraries:-
- Pandas (pip3 install pandas)
- Snowflake-sqlalchemy (pip3 install snowflake-sqlalchemy)
- Datetime (pip3 install datetime)
- I have used Snowflake as the data lake whose credentials can be found below.
- Raw_data file path has to be provided correctly in line #18 in the variable raw_data. I have used my local path
- Command to execute the job → python3 assignment.py

### Note:- 
- As I found the duplicates in the raw data based on SKU, I have handled de-duping the raw data set while merging it into the final data table (merge query)

# Data lake details:-
- Url:- https://pz00498.ap-southeast-1.snowflakecomputing.com/console/login
- Username : karanmalhotra
- Password : Zluri@test1
- database=ZLURI_TEST
- schema=ASSIGNMENT
- warehouse=ZLURI_TEST
- role=ACCOUNTADMIN

- Data Table:- zluri_test
> select * from zluri_test.assignment.zluri_test

- Aggregated table:- zluri_aggregated_data
> select * from zluri_test.assignment.zluri_aggregated_data


> One time script to create the data table:- this is not required until not dropped intentionally.

> create or replace table ZLURI_TEST
> (
> NAME	VARCHAR(16777216)
> , SKU	VARCHAR(16777216)
> , DESCRIPTION	VARCHAR(16777216)
> , ETL_SYNCED	TIMESTAMP_NTZ(9)
> )


# Points to achieve:-
- Code is written in standard formatted python version 3.9.
- It supports for regular non-blocking parallel ingestion of the given file into a table.
- This is achieved using the variable(ts) in line #22 of the code and this variable is appended in the intermediate tables and snowflake stages to keep multiple versions of raw data in case of parallel processing.
- Support for updating existing products in the table based on `sku` as the primary key using the merge statement in line #51 of the code.
- I am de-duping the records on SKU (in the raw table) while merging them into the final table.
- Handled insert and update based on SKUs in Merge query. (#51)
- And finally, I am merging the data from the raw dataset to the final table i.e zluri_test.
- An aggregated table on above rows is created "zluri_aggregated_data".


# Scope of Improvement:-
- Raw file ingestion should be done using cloud technology like google drive or S3 or FTP.
- We can create a dedicated staging layer where we perform data cleaning and store the historical information.
