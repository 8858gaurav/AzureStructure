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

-- num_affected_rows  num_inserted_rows num_skipped_corrupt_files
-- 104                  104                     0
-- if we run the same copy into commands for the same exisitn file under raw folder, it will not copy the same existing records to our tables.

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
