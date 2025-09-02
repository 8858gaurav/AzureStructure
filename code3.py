dbutils.fs.mount(
    source = "wasbs://misgauravinputdatatsets@misgauravstorageaccount1.blob.core.windows.net/",
    mount_point = "/mnt/databricksfolder1",
    extra_configs = {"fs.azure.account.key.misgauravstorageaccount1.blob.core.windows.net":"9MACQkMZGgHq7Y5GVOfZQOEGd7DyRzKkcOt1MxNPpNXf43H/mh+5QY8GlK/AHsZS/OJtzCrQdapP+ASt5UdkIw=="}
)

df = spark.read.csv('/mnt/databricksfolder/orders.csv', header=True)
df.head(5)

%sql
create database if not exists misgauravdb;

# this one has a _delta_log folder, because of this it will support acid properties.
# for every operations, it will create a transaction log file after successfull job happened. 
df.write.mode("overwrite").partitionBy("order_status").format("delta").option("path", "/mnt/databricksfolder/delta_new/orders_partitioned").saveAsTable("misgauravdb.delta_orders_partitioned")

dbutils.fs.ls('mnt/databricksfolder/delta_new/orders_partitioned')

# [FileInfo(path='dbfs:/mnt/databricksfolder/delta_new/orders_partitioned/_delta_log/', name='_delta_log/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/databricksfolder/delta_new/orders_partitioned/order_status=CANCELED/', name='order_status=CANCELED/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/databricksfolder/delta_new/orders_partitioned/order_status=CLOSED/', name='order_status=CLOSED/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/databricksfolder/delta_new/orders_partitioned/order_status=COMPLETE/', name='order_status=COMPLETE/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/databricksfolder/delta_new/orders_partitioned/order_status=ON_HOLD/', name='order_status=ON_HOLD/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/databricksfolder/delta_new/orders_partitioned/order_status=PAYMENT_REVIEW/', name='order_status=PAYMENT_REVIEW/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/databricksfolder/delta_new/orders_partitioned/order_status=PENDING/', name='order_status=PENDING/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/databricksfolder/delta_new/orders_partitioned/order_status=PENDING_PAYMENT/', name='order_status=PENDING_PAYMENT/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/databricksfolder/delta_new/orders_partitioned/order_status=PROCESSING/', name='order_status=PROCESSING/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/databricksfolder/delta_new/orders_partitioned/order_status=SUSPECTED_FRAUD/', name='order_status=SUSPECTED_FRAUD/', size=0, modificationTime=0)]

# this will not have a _delta_log folder
df.write.mode("overwrite").partitionBy("order_status").format("parquet").option("path", "/mnt/databricksfolder/parquet_new/orders_partitioned").saveAsTable("misgauravdb.parquet_orders_partitioned")

dbutils.fs.ls('mnt/databricksfolder/parquet_new/orders_partitioned')

# [FileInfo(path='dbfs:/mnt/databricksfolder/parquet_new/orders_partitioned/_SUCCESS', name='_SUCCESS', size=0, modificationTime=1756796199000),
#  FileInfo(path='dbfs:/mnt/databricksfolder/parquet_new/orders_partitioned/_committed_1613162507295420061', name='_committed_1613162507295420061', size=35, modificationTime=1756796198000),
#  FileInfo(path='dbfs:/mnt/databricksfolder/parquet_new/orders_partitioned/order_status=CANCELED/', name='order_status=CANCELED/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/databricksfolder/parquet_new/orders_partitioned/order_status=CLOSED/', name='order_status=CLOSED/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/databricksfolder/parquet_new/orders_partitioned/order_status=COMPLETE/', name='order_status=COMPLETE/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/databricksfolder/parquet_new/orders_partitioned/order_status=ON_HOLD/', name='order_status=ON_HOLD/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/databricksfolder/parquet_new/orders_partitioned/order_status=PAYMENT_REVIEW/', name='order_status=PAYMENT_REVIEW/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/databricksfolder/parquet_new/orders_partitioned/order_status=PENDING/', name='order_status=PENDING/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/databricksfolder/parquet_new/orders_partitioned/order_status=PENDING_PAYMENT/', name='order_status=PENDING_PAYMENT/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/databricksfolder/parquet_new/orders_partitioned/order_status=PROCESSING/', name='order_status=PROCESSING/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/databricksfolder/parquet_new/orders_partitioned/order_status=SUSPECTED_FRAUD/', name='order_status=SUSPECTED_FRAUD/', size=0, modificationTime=0)]

# Table history is only available for Delta tables. DESCRIBE HISTORY is only available for Delta tables

%sql
insert into misgauravdb.delta_orders_partitioned (order_id, order_date, order_customer_id, order_status) values ('00000000', '2013-07-25 00:00:00.0', '22222222', 'CLOSED')

# this transaction log file has been created: dbfs:/mnt/databricksfolder/delta_new/orders_partitioned/_delta_log/00000000000000000004.json for insert operations.

dbutils.fs.head("dbfs:/mnt/databricksfolder/delta_new/orders_partitioned/_delta_log/00000000000000000004.json")

# Append operations: Uploaded 1 new file in our azure container manually
df_new = spark.read.csv('/mnt/databricksfolder/orders1.csv').toDF("order_id", "order_date", "order_customer_id", "order_status")
df_new.write.mode("append").partitionBy("order_status").format("delta").save("/mnt/databricksfolder/delta_new/orders_partitioned")

# this transaction log file has been created: dbfs:/mnt/databricksfolder/delta_new/orders_partitioned/_delta_log/00000000000000000005.json for append operations, you can see the same file under your azure container

dbutils.fs.head('dbfs:/mnt/databricksfolder/delta_new/orders_partitioned/_delta_log/00000000000000000005.json')

%sql
DESCRIBE HISTORY misgauravdb.delta_orders_partitioned
# version timestamp userId userName operation operationParameters 
# 5                                  WRITE     object mode: "Append" statsOnLoad: "false" partitionBy:\"order_status\""

# Now copy operations, place a new file in our azure container manually, then run the sql copy command
%sql
COPY INTO misgauravdb.delta_orders_partitioned
FROM 'dbfs:/mnt/databricksfolder/orders_new.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')

# col_name	data_type	comment
# order_id	string	null
# order_date	string	null
# order_customer_id	string	null
# order_status	string	null

# Partition Information		
# col_name	data_type	comment
# order_status	string	null
		
# Delta Statistics Columns		
# Column Names	"order_id, order_date, order_customer_id"	
# Column Selection Method	first-32	
		
# Detailed Table Information		
# Catalog	spark_catalog	
# Database	misgauravdb	
# Table	delta_orders_partitioned	
# Created Time	Tue Sep 02 06:56:30 UTC 2025	
# Last Access	UNKNOWN	
# Created By	Spark 3.5.2	
# Type	EXTERNAL	
# Location	dbfs:/mnt/databricksfolder/delta_new/orders_partitioned	
# Provider	delta	
# Owner	root	
# Table Properties	"[delta.enableDeletionVectors=true,delta.feature.appendOnly=supported,delta.feature.deletionVectors=supported,delta.feature.invariants=supported,delta.minReaderVersion=3,delta.minWriterVersion=7]"	


%sql
DESCRIBE FORMATTED misgauravdb.parquet_orders_partitioned

# col_name	data_type	comment
# order_id	string	null
# order_date	string	null
# order_customer_id	string	null
# order_status	string	null

# Partition Information		
# col_name	data_type	comment
# order_status	string	null
		
# Detailed Table Information		
# Catalog	spark_catalog	
# Database	misgauravdb	
# Table	parquet_orders_partitioned	
# Owner	root	
# Created Time	Tue Sep 02 06:56:42 UTC 2025	
# Last Access	UNKNOWN	
# Created By	Spark 3.5.2	
# Type	EXTERNAL	
# Provider	parquet	
# Table Properties	[databricks.hms.federation.placeholderTableLocation=dbfs:/mnt/databricksfolder/parquet_new/orders_partitioned]	
# Location	dbfs:/mnt/databricksfolder/parquet_new/orders_partitioned	
# Serde Library	org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe	
# InputFormat	org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat	
# OutputFormat	org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat	
# Partition Provider	Catalog	
