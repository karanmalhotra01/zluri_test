#pip3 install snowflake-sqlalchemy

import pandas as pd
import snowflake.connector
from datetime import datetime

# snowflake connection parameters
ctx = snowflake.connector.connect(
	user='KARANMALHOTRA'
    , password='Zluri@test1'
    , database='ZLURI_TEST'
    , schema='ASSIGNMENT'
    , warehouse='ZLURI_TEST'
    , role='ACCOUNTADMIN'
    , account='pz00498.ap-southeast-1')

#read raw data
raw_data = "/Users/karan/Desktop/Zluri/Zluri_Assignment_Dataset.csv"
product_ds = pd.read_csv(raw_data) 

#create variable to add timestamp in intermediate table and stage to perform the parallel processing of files into the table with minimal gab of 1 second. 
today = datetime.today()
today = pd.to_datetime(today, format='%Y%m%d %X')
ts = today.strftime('%Y%m%d%X')
ts=ts.replace(':','')


#These are intermediate stages and tables before merging the raw data into final table
sf_raw_table = 'ZLURI_TEST_RAW_'+ts


sf_stage = """create or replace stage """+sf_raw_table+""" file_format = (BINARY_FORMAT = 'BASE64' type = 'csv' field_delimiter = ',' ESCAPE = NONE   ESCAPE_UNENCLOSED_FIELD = NONE   FIELD_OPTIONALLY_ENCLOSED_BY = '"'   COMPRESSION = 'gzip'   NULL_IF = 'null-vba3aoqjpgeovgjvmzn5cklcstanclr'   SKIP_HEADER = 1)"""
ctx.cursor().execute(sf_stage)

sf_file = "PUT file://"+raw_data+" @"+sf_raw_table+" auto_compress=true"
ctx.cursor().execute(sf_file)


raw_table = """create or replace table """+sf_raw_table+"""
		 as
		 select
    	 $1 as name
    	 , $2 as sku
    	 , $3 as description
    	 from @"""+sf_raw_table+"""
    	 ; """
ctx.cursor().execute(raw_table)


# Final merge statement which includes teh deduplication of raw data on SKU
merge_query ="""merge into zluri_test t1 using
    			(select  NAME,SKU,DESCRIPTION from (
				select *
				, row_number() over(partition by sku order by sku)rn  from """+sf_raw_table+""")
				where rn=1) t2 
				on t1.sku = t2.sku
     			when matched then update set 
    			t1.name=t2.name
    			, t1.description=t2.description
    			, t1.ETL_SYNCED= to_timestamp_ntz(convert_timezone('Asia/Kolkata',current_timestamp()))
 				when not matched then insert 
 				(
 				name
				, sku
				, description
				, ETL_SYNCED
				) 
				values
				(
				t2.name
				, t2.sku
				, t2.description
				, to_timestamp_ntz(convert_timezone('Asia/Kolkata',current_timestamp()))
				);
				"""
ctx.cursor().execute(merge_query)


# dropping intermediate stages and tables.
drop_sf_stage = 'drop stage if exists '+sf_raw_table
ctx.cursor().execute(drop_sf_stage)

drop_raw_table = 'drop table if exists '+sf_raw_table
ctx.cursor().execute(drop_raw_table)


#aggregated table
agg_query ="""create or replace table zluri_aggregated_data
				as
				select name as "name",count(sku) as "no of products"
				from zluri_test
				group by 1 
			"""
ctx.cursor().execute(agg_query)

