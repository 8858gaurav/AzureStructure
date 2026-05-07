brew update && brew install azure-cli
az login 

# create container
az storage container create --name databricks-tables-creations --account-name misgauravstorageaccount  --auth-mode login
az storage container list --account-name misgauravstorageaccount --output table

create an external location on databricks catalog UI, databricks -> catalog -> create external locations, & choose your storage credentials.

Use PySpark to read your input (misgaurav_ext_volume/rocessing-raw-data-container) and write it as a Delta folder into your new container: databricks-tables-creations

Create an external volume in unity catalog on Databricks, choose/create your external locations while creating this newly volume
gauravmishra@Gauravs-MacBook-Air ~ % az storage container create --name processing-raw-data-container --account-name misgauravstorageaccount  --auth-mode login
processing-raw-data-container & misgaurav_ext_volume are synced together

gauravmishra@Gauravs-MacBook-Air ~ % az storage container list --account-name misgauravstorageaccount --output table
Name                           Lease Status    Last Modified
-----------------------------  --------------  -------------------------
databricks-tables-creations                    2026-05-07T08:09:49+00:00
processing-raw-data-container                  2026-05-07T08:34:43+00:00

seelct your container path for your newly created Volume on databricks external volume creations: abfss://processing-raw-data-container@misgauravstorageaccount.dfs.core.windows.net/
choose catalog: misgauravcatalog, & select schema: default

here I am dumping raw file in this volume: /Volumes/misgauravcatalog/default/misgaurav_ext_volume or conatiner: processing-raw-data-container, both are synced.

gauravmishra@Gauravs-MacBook-Air ~ % databricks fs cp /Users/gauravmishra/Downloads/orders_large.csv  dbfs:/Volumes/misgauravcatalog/default/misgaurav_ext_volume/
/Users/gauravmishra/Downloads/orders_large.csv -> dbfs:/Volumes/misgauravcatalog/default/misgaurav_ext_volume/orders_large.csv  

gauravmishra@Gauravs-MacBook-Air ~ % databricks fs ls dbfs:/Volumes/misgauravcatalog/default/misgaurav_ext_volume/                                                            
orders_large.csv

gauravmishra@Gauravs-MacBook-Air ~ % az storage blob list --container-name processing-raw-data-container --account-name misgauravstorageaccount --auth-mode login --output table
Name              Blob Type    Blob Tier    Length    Content Type              Last Modified              Snapshot
----------------  -----------  -----------  --------  ------------------------  -------------------------  ----------
orders_large.csv  BlockBlob    Hot          29614     application/octet-stream  2026-05-07T08:51:51+00:00


# let's read this file from a Volume/Container -> misgaurav_ext_volume/processing-raw-data-container
code:

################### Read #######################

# 1. Read the input from your Volume
input_path = "/Volumes/misgauravcatalog/default/misgaurav_ext_volume/*.csv"
df = spark.read.format("csv").option("header", "true").load(input_path)
df.show(2)
# +--------+----------+-----------+------------+
# |order_id|order_date|customer_id|order_status|
# +--------+----------+-----------+------------+
# |       1|2020-08-27|       1551|     SHIPPED|
# |       2|2020-01-28|       1203|     SHIPPED|
# +--------+----------+-----------+------------+

# 1. Read the input from your container
input_path = "abfss://processing-raw-data-container@misgauravstorageaccount.dfs.core.windows.net/*.csv"
df = spark.read.format("csv").option("header", "true").load(input_path)
df.show(2)
# +--------+----------+-----------+------------+
# |order_id|order_date|customer_id|order_status|
# +--------+----------+-----------+------------+
# |       1|2020-08-27|       1551|     SHIPPED|
# |       2|2020-01-28|       1203|     SHIPPED|
# +--------+----------+-----------+------------+

################### Processing #######################

# write it to to your container after prrocessing: databricks-tables-creations
gauravmishra@Gauravs-MacBook-Air ~ % az storage blob list --container-name databricks-tables-creations --account-name misgauravstorageaccount --auth-mode login --output table
# blank

output_path = "abfss://databricks-tables-creations@misgauravstorageaccount.dfs.core.windows.net/ouput_delta/"

# 3. Write it out in Delta format
df.write.format("delta").mode("overwrite").save(output_path)

gauravmishra@Gauravs-MacBook-Air ~ % az storage blob list --container-name databricks-tables-creations --account-name misgauravstorageaccount --auth-mode login --output table
Name                                                                             Blob Type    Blob Tier    Length    Content Type              Last Modified              Snapshot
-------------------------------------------------------------------------------  -----------  -----------  --------  ------------------------  -------------------------  ----------
ouput_delta                                                                      BlockBlob    Hot                                              2026-05-07T09:03:12+00:00
ouput_delta/_delta_log                                                           BlockBlob    Hot                                              2026-05-07T09:03:12+00:00
ouput_delta/_delta_log/00000000000000000000.crc                                  BlockBlob    Hot          3125      application/octet-stream  2026-05-07T09:03:15+00:00
ouput_delta/_delta_log/00000000000000000000.json                                 BlockBlob    Hot          2196      application/octet-stream  2026-05-07T09:03:14+00:00
ouput_delta/_delta_log/__tmp_path_dir                                            BlockBlob    Hot                                              2026-05-07T09:03:14+00:00
ouput_delta/_delta_log/_staged_commits                                           BlockBlob    Hot                    application/octet-stream  2026-05-07T09:03:12+00:00
ouput_delta/part-00000-83fe54ca-3df7-414d-96c0-6d765238dd5d.c000.snappy.parquet  BlockBlob    Hot          12524     application/octet-stream  2026-05-07T09:03:13+00:00


# misgaurav_ext_volume is not synced with this containers: databricks-tables-creations
gauravmishra@Gauravs-MacBook-Air ~ % databricks fs ls dbfs:/Volumes/misgauravcatalog/default/misgaurav_ext_volume/                                                           
orders_large.csv


%sql
create database if not exists retaildb;

%sql
CREATE TABLE IF NOT EXISTS misgauravcatalog.retaildb.orders_processed
USING DELTA
LOCATION "abfss://databricks-tables-creations@misgauravstorageaccount.dfs.core.windows.net/ouput_delta/";


%sql
select * from misgauravcatalog.retaildb.orders_processed limit 2;

order_id,order_date,customer_id,order_status
1,2020-08-27,1551,SHIPPED
2,2020-01-28,1203,SHIPPED

%sql
describe detail misgauravcatalog.retaildb.orders_processed;
format	delta
id	fd41d6e7-4765-4b9d-8777-e75feff0a02c
name	misgauravcatalog.retaildb.orders_processed
description	null
location	abfss://databricks-tables-creations@misgauravstorageaccount.dfs.core.windows.net/ouput_delta
createdAt	2026-05-07T09:03:11.983Z
lastModified	2026-05-07T09:03:14.000Z
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
describe extended misgauravcatalog.retaildb.orders_processed;
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
Table,orders_processed,
Created Time,Thu May 07 09:12:43 UTC 2026,
Last Access,UNKNOWN,
Created By,Spark ,
Type,EXTERNAL,
Location,abfss://databricks-tables-creations@misgauravstorageaccount.dfs.core.windows.net/ouput_delta,
Provider,delta,
Owner,gauravmishra010@gmail.com,
Table Properties,"[delta.enableDeletionVectors=true,delta.feature.appendOnly=supported,delta.feature.deletionVectors=supported,delta.feature.invariants=supported,delta.minReaderVersion=3,delta.minWriterVersion=7]",


