dbutils.fs.ls('/')
# [FileInfo(path='dbfs:/Volume/', name='Volume/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/Volumes/', name='Volumes/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/databricks-datasets/', name='databricks-datasets/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/databricks-results/', name='databricks-results/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/volume/', name='volume/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/volumes/', name='volumes/', size=0, modificationTime=0)]

# We uploaded 1 files in our azure container. from the container, we are reading this files.
# the folder will be created automatically in databricks UI after mounting the azure path.

dbutils.fs.mount(
    source = "wasbs://misgauravinputdatatsets@misgauravstorageaccount1.blob.core.windows.net/",
    mount_point = "/mnt/misgauravdb",
    extra_configs = {"fs.azure.account.key.misgauravstorageaccount1.blob.core.windows.net":"4lge2447I7Dctszhq6BRb1OvVvB0cAu+Is18VrIqSkPrDQcGMe0LQmOvtsd3nsPYQaTRfL09n/4++AStGlJLsA=="}
)

dbutils.fs.ls('/mnt/misgauravdb/')
# [FileInfo(path='dbfs:/mnt/misgauravdb/orders.csv', name='orders.csv', size=4339, modificationTime=1756565934000)]

df = spark.read.csv('/mnt/misgauravdb/orders.csv', header=True)
df.head(5)
# [Row(order_id='1', order_date='2013-07-25 00:00:00.0', order_customer_id='11599', order_status='CLOSED'),
#  Row(order_id='2', order_date='2013-07-25 00:00:00.0', order_customer_id='256', order_status='PENDING_PAYMENT'),
#  Row(order_id='3', order_date='2013-07-25 00:00:00.0', order_customer_id='12111', order_status='COMPLETE'),
#  Row(order_id='4', order_date='2013-07-25 00:00:00.0', order_customer_id='8827', order_status='CLOSED'),
#  Row(order_id='5', order_date='2013-07-25 00:00:00.0', order_customer_id='11318', order_status='COMPLETE')]

# this will create a 1 folder (parquet) under azure container (misgauravinputdatatsets) as well

# https://misgauravstorageaccount1.blob.core.windows.net/misgauravinputdatatsets/parquet/orders_partitioned/order_status=CANCELED/part-00000-bad751e9-1050-450a-b1ad-953299065460.c000.snappy.parquet


# Along with in the Data bricks UI: dbfs:/mnt/misgauravdb/parquet/orders_partitioned/order_status=CANCELED/part-00000-bad751e9-1050-450a-b1ad-953299065460.c000.snappy.parquet

# In azure we just have a 1 conatiner name: misgauravinputdatatsets
# whose storage account name is: misgauravstorageaccount1
df.write.mode('overwrite').partitionBy('order_status').save('/mnt/misgauravdb/parquet/orders_partitioned')

df.write.mode('overwrite').partitionBy('order_status').save('/mnt/misgauravdb/delta/orders_partitioned')

#dbfs:/mnt/misgauravdb/delta/orders_partitioned/order_status=CANCELED/part-00000-0e117fff-8804-4a97-baa4-1c3284d1229d.c000.snappy.parquet

# https://misgauravstorageaccount1.blob.core.windows.net/misgauravinputdatatsets/delta/orders_partitioned/order_status=CANCELED/part-00000-0e117fff-8804-4a97-baa4-1c3284d1229d.c000.snappy.parquet

%sql 
create database if not exists misgauravdb_hive

%sql
create table if not exists misgauravdb_hive.ordersparquet using parquet location "dbfs:/mnt/misgauravdb/parquet/orders_partitioned"
    
%sql
create table if not exists misgauravdb_hive.ordersdelta using delta location 'dbfs:/mnt/misgauravdb/delta/orders_partitioned/'

%sql
select * from misgauravdb_hive.ordersdelta limit 5;

-- order_id	order_date	order_customer_id	order_status
-- 102	2013-07-25 00:00:00.0	8027	COMPLETE
-- 98	2013-07-25 00:00:00.0	5243	COMPLETE
-- 95	2013-07-25 00:00:00.0	9032	COMPLETE
-- 92	2013-07-25 00:00:00.0	6932	COMPLETE
-- 91	2013-07-25 00:00:00.0	8912	COMPLETE

%sql
select * from delta.`/mnt/misgauravdb/parquet/orders_partitioned` limit 5;

-- order_id	order_date	order_customer_id	order_status
-- 97	2013-07-25 00:00:00.0	10784	PENDING
-- 96	2013-07-25 00:00:00.0	8683	PENDING
-- 85	2013-07-25 00:00:00.0	1485	PENDING
-- 68	2013-07-25 00:00:00.0	4320	PENDING
-- 55	2013-07-25 00:00:00.0	2052	PENDING

