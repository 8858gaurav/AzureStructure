# 1. Read the raw data from your existing Volume
input_volume_path = "dbfs:/Volumes/misgauravcatalog/default/misgauravextvolume/input/orders_large.csv"
df = spark.read.format("csv").option("header", "true").load(input_volume_path)

# 2. Define the new destination (The New Container)
# We will create a folder inside the new container for this specific table
output_external_path = "abfss://databricks-tables@misgauravstorageaccount.dfs.core.windows.net/retail_orders/"

# 3. Write the data as Delta format to the new container
df.write.format("delta").mode("overwrite").save(output_external_path)

%sql
CREATE TABLE IF NOT EXISTS misgauravcatalog.retaildb.orders_external
USING DELTA
LOCATION "abfss://databricks-tables@misgauravstorageaccount.dfs.core.windows.net/retail_orders/";

%sql
describe detail misgauravcatalog.retaildb.orders_external;
format	delta
id	ae70ecf9-d6b8-4d59-914f-0a55209a2b7f
name	misgauravcatalog.retaildb.orders_external
description	null
location	abfss://databricks-tables@misgauravstorageaccount.dfs.core.windows.net/retail_orders
createdAt	2026-05-07T02:41:44.495Z
lastModified	2026-05-07T02:41:47.000Z
partitionColumns	[]
clusteringColumns	[]
numFiles	1
sizeInBytes	12524
properties	{"delta.enableDeletionVectors":"true"}
minReaderVersion	3
minWriterVersion	7
tableFeatures	["appendOnly","deletionVectors","invariants"]
statistics	{"numRowsDeletedByDeletionVectors":0,"numDeletionVectors":0}
clusterByAuto	FALSE

%sql
describe extended misgauravcatalog.retaildb.orders_external;
col_name,data_type,comment
order_id,string,null
order_date,string,null
customer_id,string,null
order_status,string,null
,,
# Delta Statistics Columns,,
Column Names,"order_id, order_date, customer_id, order_status",
Column Selection Method,first-32,
,,
# Detailed Table Information,,
Catalog,misgauravcatalog,
Database,retaildb,
Table,orders_external,
Created Time,Thu May 07 02:42:13 UTC 2026,
Last Access,UNKNOWN,
Created By,Spark ,
Type,EXTERNAL,
Location,abfss://databricks-tables@misgauravstorageaccount.dfs.core.windows.net/retail_orders,
Provider,delta,
Owner,gauravmishra010@gmail.com,
Table Properties,"[delta.enableDeletionVectors=true,delta.feature.appendOnly=supported,delta.feature.deletionVectors=supported,delta.feature.invariants=supported,delta.minReaderVersion=3,delta.minWriterVersion=7]",
