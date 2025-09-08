%sql
create database if not exists retaildb;

%fs
mkdirs dbfs:/FileStore/raw

# load 1 orders.csv file under this

create table retaildb.orders_bronze
(
order_id int,
order_date string,
customer_id int,
order_status string,
filename string,
createdon timestamp
)
using delta
location "dbfs:/FileStore/data/orders_bronze.delta"
partitioned by (order_status)
TBLPROPERTIES(delta.enableChangeDataFeed = true);


create table retaildb.orders_silver
(
order_id int,
order_date date,
customer_id int,
order_status string,
order_year int GENERATED ALWAYS AS (YEAR(order_date)),
order_month int GENERATED ALWAYS AS (MONTH(order_date)),
createdon timestamp,
modifiedon timestamp
)
using delta
location "dbfs:/FileStore/data/orders_silver.delta"
partitioned by (order_status)
TBLPROPERTIES(delta.enableChangeDataFeed = true);

create table retaildb.orders_gold
(
customer_id int,
order_status string,
order_year int,
num_orders int
)
using delta
location "dbfs:/FileStore/data/orders_gold.delta"
TBLPROPERTIES(delta.enableChangeDataFeed = true)

%sql
describe history retaildb.orders_bronze;

# -- version	0
# -- timestamp	2025-09-08T05:59:18Z
# -- userId	142168046953687
# -- userName	gauravmishra8858@outlook.com
# -- operation	CREATE TABLE
# -- operationParameters	{"partitionBy":"[\"order_status\"]","clusterBy":"[]","description":null,"isManaged":"false","properties":"{\"delta.enableChangeDataFeed\":\"true\",\"delta.enableDeletionVectors\":\"true\"}","statsOnLoad":"false"}
# -- job	null
# -- notebook	{"notebookId":"717909296164361"}
# -- clusterId	0908-055053-ck49gn66
# -- readVersion	null
# -- isolationLevel	WriteSerializable
# -- isBlindAppend	TRUE
# -- operationMetrics	{}
# -- userMetadata	null
# -- engineInfo	Databricks-Runtime/16.4.x-scala2.12
	
%fs
ls dbfs:/FileStore/data/orders_bronze.delta

# path	                                              name	        size	modificationTime
# dbfs:/FileStore/data/orders_bronze.delta/_delta_log/	_delta_log/	  0	    1757311163000


%sql
copy into retaildb.orders_bronze from (
  select order_id::int,
  order_date::string,
  customer_id::int,
  order_status::string,
  INPUT_FILE_NAME() as filename,
  current_timestamp() as createdon
  from 'dbfs:/FileStore/raw'
)
fileformat = CSV
format_options('header'='true')

# -- num_affected_rows  num_inserted_rows num_skipped_corrupt_files
# -- 104                  104                     0
# -- if we run the same copy into commands for the same exisitn file under raw folder, it will not copy the same existing records to our tables.

dbutils.fs.ls('dbfs:/FileStore/data/orders_bronze.delta')

# [FileInfo(path='dbfs:/FileStore/data/orders_bronze.delta/_delta_log/', name='_delta_log/', size=0, modificationTime=1757311157000),
#  FileInfo(path='dbfs:/FileStore/data/orders_bronze.delta/order_status=CANCELED/', name='order_status=CANCELED/', size=0, modificationTime=1757311458000),
#  FileInfo(path='dbfs:/FileStore/data/orders_bronze.delta/order_status=CLOSED/', name='order_status=CLOSED/', size=0, modificationTime=1757311459000),
#  FileInfo(path='dbfs:/FileStore/data/orders_bronze.delta/order_status=COMPLETE/', name='order_status=COMPLETE/', size=0, modificationTime=1757311460000),
#  FileInfo(path='dbfs:/FileStore/data/orders_bronze.delta/order_status=ON_HOLD/', name='order_status=ON_HOLD/', size=0, modificationTime=1757311460000),
#  FileInfo(path='dbfs:/FileStore/data/orders_bronze.delta/order_status=PAYMENT_REVIEW/', name='order_status=PAYMENT_REVIEW/', size=0, modificationTime=1757311460000),
#  FileInfo(path='dbfs:/FileStore/data/orders_bronze.delta/order_status=PENDING/', name='order_status=PENDING/', size=0, modificationTime=1757311460000),
#  FileInfo(path='dbfs:/FileStore/data/orders_bronze.delta/order_status=PENDING_PAYMENT/', name='order_status=PENDING_PAYMENT/', size=0, modificationTime=1757311460000),
#  FileInfo(path='dbfs:/FileStore/data/orders_bronze.delta/order_status=PROCESSING/', name='order_status=PROCESSING/', size=0, modificationTime=1757311460000),
#  FileInfo(path='dbfs:/FileStore/data/orders_bronze.delta/order_status=SUSPECTED_FRAUD/', name='order_status=SUSPECTED_FRAUD/', size=0, modificationTime=1757311461000)]

%sql
select * from retaildb.orders_bronze limit 5;

# -- order_id order_date            customer_id order_status  filename  createdon
# -- 1        2013-07-25 00:00:00.0 11599       CLOSED  dbfs:/FileStore/raw/orders.csv  2025-09-05T08:57:37.799356Z
# -- 4        2013-07-25 00:00:00.0 8827        CLOSED  dbfs:/FileStore/raw/orders.csv  2025-09-05T08:57:37.799356Z
# -- 12       2013-07-25 00:00:00.0 1837        CLOSED  dbfs:/FileStore/raw/orders.csv  2025-09-05T08:57:37.799356Z
# -- 18       2013-07-25 00:00:00.0 1205        CLOSED  dbfs:/FileStore/raw/orders.csv  2025-09-05T08:57:37.799356Z
# -- 24       2013-07-25 00:00:00.0 11441       CLOSED  dbfs:/FileStore/raw/orders.csv  2025-09-05T08:57:37.799356Z


%sql
describe history retaildb.orders_bronze

# -- version  1 0 
# -- timestamp  2025-09-05T08:57:41Z  2025-09-05T08:15:38Z  
# -- userId 142168046953687 142168046953687 
# -- userName gauravmishra8858@outlook.com  gauravmishra8858@outlook.com  
# -- operation  COPY INTO CREATE TABLE  
# -- operationParameters  {"statsOnLoad":"false"} {"partitionBy":"[\"order_status\"]","clusterBy":"[]","description":null,"isManaged":"false","properties":"{\"delta.enableChangeDataFeed\":\"true\",\"delta.enableDeletionVectors\":\"true\"}","statsOnLoad":"false"}  
# -- job  null  null  
# -- notebook {"notebookId":"4053075405393512"} {"notebookId":"4053075405393513"} 
# -- clusterId  0905-080924-ahgm0w4b  0905-080924-ahgm0w4b  
# -- readVersion  0 null  
# -- isolationLevel WriteSerializable WriteSerializable 
# -- isBlindAppend  TRUE  TRUE  
# -- operationMetrics {"numFiles":"9","numOutputRows":"104","numOutputBytes":"19091","numSkippedCorruptFiles":"0"}  {}  
# -- userMetadata null  null  
# -- engineInfo Databricks-Runtime/16.4.x-scala2.12 Databricks-Runtime/16.4.x-scala2.12 

%sql
select * from table_changes('retaildb.orders_bronze', 1) limit 5;

# -- order_id order_date            customer_id order_status    filename                        createdon                   _change_type  _commit_version _commit_timestamp
# -- 2        2013-07-25 00:00:00.0 256         PENDING_PAYMENT dbfs:/FileStore/raw/orders.csv  2025-09-05T08:57:37.799356Z insert        1               2025-09-05T08:57:41Z
# -- 9        2013-07-25 00:00:00.0 5657        PENDING_PAYMENT dbfs:/FileStore/raw/orders.csv  2025-09-05T08:57:37.799356Z insert        1               2025-09-05T08:57:41Z
# -- 10       2013-07-25 00:00:00.0 5648        PENDING_PAYMENT dbfs:/FileStore/raw/orders.csv  2025-09-05T08:57:37.799356Z insert        1               2025-09-05T08:57:41Z
# -- 13       2013-07-25 00:00:00.0 9149        PENDING_PAYMENT dbfs:/FileStore/raw/orders.csv  2025-09-05T08:57:37.799356Z insert        1               2025-09-05T08:57:41Z
# -- 16       2013-07-25 00:00:00.0 7276        PENDING_PAYMENT dbfs:/FileStore/raw/orders.csv  2025-09-05T08:57:37.799356Z insert        1               2025-09-05T08:57:41Z

# changes made in the bronze table from version 1 onwards to be merged to the silver table.
# first creating a temp of bronze table, then merge this to silver table

%sql
create or replace temporary view orders_bronze_changes
AS 
select * from table_changes('retaildb.orders_bronze', 1) where order_id > 0
AND customer_id > 0 and order_status IN ("CLOSED", "PENDING_PAYMENT");
  
select * from orders_bronze_changes limit 5;

# -- order_id order_date           customer_id order_status            filename                       createdon            _change_type  _commit_version _commit_timestamp
# -- 2       2013-07-25 00:00:00.0 256         PENDING_PAYMENT dbfs:/FileStore/raw/orders.csv  2025-09-05T08:57:37.799356Z insert        1                2025-09-05T08:57:41Z
# -- 9       2013-07-25 00:00:00.0 5657        PENDING_PAYMENT dbfs:/FileStore/raw/orders.csv  2025-09-05T08:57:37.799356Z insert        1                2025-09-05T08:57:41Z
# -- 10      2013-07-25 00:00:00.0 5648        PENDING_PAYMENT dbfs:/FileStore/raw/orders.csv  2025-09-05T08:57:37.799356Z insert        1                2025-09-05T08:57:41Z
# -- 13      2013-07-25 00:00:00.0 9149        PENDING_PAYMENT dbfs:/FileStore/raw/orders.csv  2025-09-05T08:57:37.799356Z insert        1                2025-09-05T08:57:41Z
# -- 16      2013-07-25 00:00:00.0 7276        PENDING_PAYMENT dbfs:/FileStore/raw/orders.csv  2025-09-05T08:57:37.799356Z insert        1                2025-09-05T08:57:41Z

%sql
select count(*) from orders_bronze_changes; -- 45
select count(*) from retaildb.orders_bronze; -- 104 

# merging to the silver table

%sql
merge into retaildb.orders_silver tgt
using orders_bronze_changes src on tgt.order_id = src.order_id
when matched
then
update set tgt.order_status = src.order_status, tgt.customer_id = src.customer_id, tgt.modifiedon = CURRENT_TIMESTAMP()
WHEN not matched
then
insert(order_id, order_date, customer_id, order_status, createdon, modifiedon) values(order_id, order_date, customer_id, order_status, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

# -- num_affected_rows	num_updated_rows	num_deleted_rows	num_inserted_rows
# -- 45	                0	                0	                45


# -- select count(*) from orders_bronze_changes; -- 45
# -- select count(*) from retaildb.orders_silver; -- 104

#  adding complete data from fresh to gold table

%sql
insert overwrite table retaildb.orders_gold
select customer_id, order_status, order_year, count(order_id) as num_orders
from retaildb.orders_silver group by 1, 2, 3;

# -- num_affected_rows	num_inserted_rows
# -- 45	                45

%sql
select count(*) from retaildb.orders_gold; -- 45

# now uplaod 1 more new file under this folder: dbfs:/FileStore/raw, i.e orders_paste.csv, this file has total of 4 records 
# ( 2 new records + 2 for updates )

# in this file (orders_paste.csv), we have CLOSED, and PENDING for 3, and 4 order_id
# 3,2013-07-25 00:00:00.0,12111,CLOSED
# 4,2013-07-25 00:00:00.0,8827,PENDING

# 2 additional records, it has: 
# 11101,2013-07-25 00:00:00.0,12256,PROCESSING
# 11102,2013-07-25 00:00:00.0,7790,PENDING_PAYMENT

# in this file (orders.csv), we have COMPLETE, and CLOSED for 3, and 4 order_id
# 3,2013-07-25 00:00:00.0,12111,COMPLETE
# 4,2013-07-25 00:00:00.0,8827,CLOSED

# now re-run the scripts after uplaoding the above files to the raw folder: from copy into retaildb.orders_bronze
dbutils.fs.ls('/FileStore/raw')

# [FileInfo(path='dbfs:/FileStore/raw/orders.csv', name='orders.csv', size=4333, modificationTime=1757311141000),
#  FileInfo(path='dbfs:/FileStore/raw/orders_paste.csv', name='orders_paste.csv', size=212, modificationTime=1757313194000)]

%sql
copy into retaildb.orders_bronze from (
  select order_id::int,
  order_date::string,
  customer_id::int,
  order_status::string,
  INPUT_FILE_NAME() as filename,
  current_timestamp() as createdon
  from 'dbfs:/FileStore/raw'
)
fileformat = CSV
format_options('header'='true')

# -- num_affected_rows	num_inserted_rows	num_skipped_corrupt_files
# -- 4	                4	                 0

# if we run the same copy into commands for the same exisitng file under raw folder, it will not copy the same existing records to our tables.
# re run code no 223

%sql
copy into retaildb.orders_bronze from (
  select order_id::int,
  order_date::string,
  customer_id::int,
  order_status::string,
  INPUT_FILE_NAME() as filename,
  current_timestamp() as createdon
  from 'dbfs:/FileStore/raw'
)
fileformat = CSV
format_options('header'='true')

# -- num_affected_rows	num_inserted_rows	num_skipped_corrupt_files
# -- 0	                0	                0

# run the view commands of orders_bronze_changes

%sql
create or replace temporary view orders_bronze_changes
AS 
select * from table_changes('retaildb.orders_bronze', 1) where order_id > 0
AND customer_id > 0 and order_status IN ("CLOSED", "PENDING_PAYMENT");

%sql
select * from orders_bronze_changes limit 5;

# run the merges commands, any incremental changes in the bronze table has been taken gracefully because of CDC / CDF

%sql
merge into retaildb.orders_silver tgt
using orders_bronze_changes src on tgt.order_id = src.order_id
when matched
then
update set tgt.order_status = src.order_status, tgt.customer_id = src.customer_id, tgt.modifiedon = CURRENT_TIMESTAMP()
WHEN not matched
then
insert(order_id, order_date, customer_id, order_status, createdon, modifiedon) values(order_id, order_date, customer_id, order_status, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

# -- num_affected_rows	num_updated_rows	num_deleted_rows	num_inserted_rows
# -- 47	                45	                0	                2

# now run the insert command for gold tables

%sql
insert overwrite table retaildb.orders_gold
select customer_id, order_status, order_year, count(order_id) as num_orders
from retaildb.orders_silver group by 1, 2, 3;

# -- num_affected_rows	num_inserted_rows
# -- 46	                46


