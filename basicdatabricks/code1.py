"If I have a Volume synced to an ADLS folder, can I also register that folder as an External Table?"
"No, Unity Catalog enforces path exclusivity. A directory managed by a Volume cannot be used as the LOCATION for an External Table.

if data is present in unity catalog, then only managed table can be created only, if it's synced with cloud or not.
Volume root was set to the Container level, not a specific folder.

# we have synced this conatiner: azureextvolumedatabricks (azure portal) with unity catalog Volume: /Volumes/misgauravcatalog/default/misgauravextvolume (catalog UI),
# i.e we are not able to create an external table in databricks
# we can see all the folder(input/output folder) inside this container on my catalog UI
  
# read operations
# gauravmishra@Gauravs-MacBook-Air ~ % databricks fs ls dbfs:/Volumes/misgauravcatalog/default/misgauravextvolume/input/orders_large.csv

path =  "dbfs:/Volumes/misgauravcatalog/default/misgauravextvolume/input/*.csv"
df = spark.read.format('csv').option('header', 'true').load(path)
display(df)

order_id,order_date,customer_id,order_status
1,2020-08-27,1551,SHIPPED
2,2020-01-28,1203,SHIPPED
3,2020-11-14,1782,PENDING
4,2020-08-14,1367,COMPLETED
upto 10K records

# write operations
df.write.format('delta').mode('overwrite').save('/Volumes/misgauravcatalog/default/misgauravextvolume/output')

#gauravmishra@Gauravs-MacBook-Air ~ % databricks fs ls dbfs:/Volumes/misgauravcatalog/default/misgauravextvolume/output/
# _delta_log
# part-00000-e5b68ca3-d17a-428a-a60f-7acbe201b7f6.c000.snappy.parquet

gauravmishra@Gauravs-MacBook-Air ~ % databricks fs ls dbfs:/Volumes/misgauravcatalog/default/misgauravextvolume/output/_delta_log
# 00000000000000000000.crc
# 00000000000000000000.json
# __tmp_path_dir
# _staged_commits

gauravmishra@Gauravs-MacBook-Air ~ % databricks fs cat dbfs:/Volumes/misgauravcatalog/default/misgauravextvolume/output/_delta_log/00000000000000000000.json

{"commitInfo":{"timestamp":1778071792935,"userId":"143844140774415","userName":"gauravmishra010@gmail.com","operation":"WRITE","operationParameters":{"mode":"Overwrite","statsOnLoad":false,"partitionBy":"[]"},"notebook":{"notebookId":"966926155707402"},"queryHistoryStatementId":"adc62ef4-bbb7-4bd7-8150-ec3a9275004b","clusterId":"0506-124045-skliufgo-v2n","isolationLevel":"WriteSerializable","isBlindAppend":false,"operationMetrics":{"numFiles":"1","numRemovedFiles":"0","numRemovedBytes":"0","numDeletionVectorsRemoved":"0","numOutputRows":"1000","numOutputBytes":"12524"},"tags":{"noRowsCopied":"true","restoresDeletedRows":"false"},"engineInfo":"Databricks-Runtime/18.1.x-photon-scala2.13","txnId":"7ddf7982-690c-4179-9e19-0e2acb73a4d5"}}
{"metaData":{"id":"b50940c4-88c4-47cb-9a51-88da44f5d7ae","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"order_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"order_date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"customer_id\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"order_status\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true"},"createdTime":1778071792094}}
{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors","appendOnly","invariants"]}}
{"add":{"path":"part-00000-e5b68ca3-d17a-428a-a60f-7acbe201b7f6.c000.snappy.parquet","partitionValues":{},"size":12524,"modificationTime":1778071792000,"dataChange":true,"stats":"{\"numRecords\":1000,\"minValues\":{\"order_id\":\"1\",\"order_date\":\"2020-01-01\",\"customer_id\":\"1000\",\"order_status\":\"CANCELLED\"},\"maxValues\":{\"order_id\":\"999\",\"order_date\":\"2020-12-31\",\"customer_id\":\"2000\",\"order_status\":\"SHIPPED\"},\"nullCount\":{\"order_id\":0,\"order_date\":0,\"customer_id\":0,\"order_status\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1778071792000000","MIN_INSERTION_TIME":"1778071792000000","MAX_INSERTION_TIME":"1778071792000000","OPTIMIZE_TARGET_SIZE":"268435456"}}}

%sql
USE CATALOG misgauravcatalog;

%sql
CREATE SCHEMA IF NOT EXISTS retaildb;

%sql
DESCRIBE SCHEMA EXTENDED retaildb;

database_description_item,database_description_value
Catalog Name,misgauravcatalog
Namespace Name,retaildb
Comment,
Location,
Owner,gauravmishra010@gmail.com
Properties,
Predictive Optimization,ENABLE (inherited from METASTORE metastore_azure_centralindia)

df.write.mode("overwrite").saveAsTable("misgauravcatalog.retaildb.ordersdelta1") -- numFiles	1

%sql
DESCRIBE DETAIL misgauravcatalog.retaildb.ordersdelta1;
format	delta
id	347f7ae9-4483-49a9-8e86-5a4bcf180970
name	misgauravcatalog.retaildb.ordersdelta1
description	null
location	abfss://unity-catalog-storage@dbstorageioojaysp6swwm.dfs.core.windows.net/7405605583082922/__unitystorage/catalogs/4ea0a1b4-9c01-49d9-beb5-9a83f1396b96/tables/18a13874-211d-4be0-81d4-65f703e2a6d4
createdAt	2026-05-07T00:57:48.203Z
lastModified	2026-05-07T00:57:49.000Z
partitionColumns	[]
clusteringColumns	[]
numFiles	1
sizeInBytes	7139
properties	{"delta.parquet.compression.codec":"zstd","delta.enableDeletionVectors":"true"}
minReaderVersion	3
minWriterVersion	7
tableFeatures	["appendOnly","deletionVectors","invariants"]
statistics	{"numRowsDeletedByDeletionVectors":0,"numDeletionVectors":0}
clusterByAuto	FALSE

%sql 
insert into misgauravcatalog.retaildb.ordersdelta1 values('1001', '2023-01-01', '2222','SHIPPED') -- numFiles	2

%sql
describe detail misgauravcatalog.retaildb.ordersdelta1;
format	delta
id	347f7ae9-4483-49a9-8e86-5a4bcf180970
name	misgauravcatalog.retaildb.ordersdelta1
description	null
location	abfss://unity-catalog-storage@dbstorageioojaysp6swwm.dfs.core.windows.net/7405605583082922/__unitystorage/catalogs/4ea0a1b4-9c01-49d9-beb5-9a83f1396b96/tables/18a13874-211d-4be0-81d4-65f703e2a6d4
createdAt	2026-05-07T00:57:48.203Z
lastModified	2026-05-07T01:18:41.000Z
partitionColumns	[]
clusteringColumns	[]
numFiles	2
sizeInBytes	8485
properties	{"delta.parquet.compression.codec":"zstd","delta.enableDeletionVectors":"true"}
minReaderVersion	3
minWriterVersion	7
tableFeatures	["appendOnly","deletionVectors","invariants"]
statistics	{"numRowsDeletedByDeletionVectors":0,"numDeletionVectors":0}
clusterByAuto	FALSE

%sql 
insert into misgauravcatalog.retaildb.ordersdelta1 
values ('1002', '2023-01-01', '2223','SHIPPED'),
('1003', '2023-01-01', '2223','SHIPPED'); -- numFiles	3

%sql
describe detail misgauravcatalog.retaildb.ordersdelta1;
format	delta
id	347f7ae9-4483-49a9-8e86-5a4bcf180970
name	misgauravcatalog.retaildb.ordersdelta1
description	null
location	abfss://unity-catalog-storage@dbstorageioojaysp6swwm.dfs.core.windows.net/7405605583082922/__unitystorage/catalogs/4ea0a1b4-9c01-49d9-beb5-9a83f1396b96/tables/18a13874-211d-4be0-81d4-65f703e2a6d4
createdAt	2026-05-07T00:57:48.203Z
lastModified	2026-05-07T01:22:29.000Z
partitionColumns	[]
clusteringColumns	[]
numFiles	3
sizeInBytes	9950
properties	{"delta.parquet.compression.codec":"zstd","delta.enableDeletionVectors":"true"}
minReaderVersion	3
minWriterVersion	7
tableFeatures	["appendOnly","deletionVectors","invariants"]
statistics	{"numRowsDeletedByDeletionVectors":0,"numDeletionVectors":0}
clusterByAuto	FALSE

%sql
CREATE TABLE IF NOT EXISTS misgauravcatalog.retaildb.ordersdelta2 
USING delta 
AS SELECT * FROM delta.`/Volumes/misgauravcatalog/default/misgauravextvolume/output/`;

%sql
describe extended misgauravcatalog.retaildb.ordersdelta2;
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
Table,ordersdelta2,
Created Time,Thu May 07 00:58:09 UTC 2026,
Last Access,UNKNOWN,
Created By,Spark ,
Statistics,"7169 bytes, 1002 rows",
Type,MANAGED,
Location,abfss://unity-catalog-storage@dbstorageioojaysp6swwm.dfs.core.windows.net/7405605583082922/__unitystorage/catalogs/4ea0a1b4-9c01-49d9-beb5-9a83f1396b96/tables/e6e4b69f-f147-4fd2-963f-b168e36996a3,
Provider,delta,
Owner,gauravmishra010@gmail.com,
Is_managed_location,true,
Predictive Optimization,ENABLE (inherited from METASTORE metastore_azure_centralindia),
Table Properties,"[delta.enableDeletionVectors=true,delta.feature.appendOnly=supported,delta.feature.deletionVectors=supported,delta.feature.invariants=supported,delta.minReaderVersion=3,delta.minWriterVersion=7,delta.parquet.compression.codec=zstd]",

%sql 
insert into misgauravcatalog.retaildb.ordersdelta2 values('1001', '2023-01-01', '2222','SHIPPED')

%sql
DESCRIBE DETAIL misgauravcatalog.retaildb.ordersdelta2; -- you'll see two files here
format	delta
id	db6f91e3-dea6-4836-86b4-82bea83da0f9
name	misgauravcatalog.retaildb.ordersdelta2
description	This table contains data about customer orders, including the order identifier, date of the order, the customer who placed it, and the current status of the order. It can be used to track order histories, analyze customer purchasing patterns over time, and monitor the progression or fulfillment status of orders.
location	abfss://unity-catalog-storage@dbstorageioojaysp6swwm.dfs.core.windows.net/7405605583082922/__unitystorage/catalogs/4ea0a1b4-9c01-49d9-beb5-9a83f1396b96/tables/e6e4b69f-f147-4fd2-963f-b168e36996a3
createdAt	2026-05-07T00:58:08.066Z
lastModified	2026-05-07T01:11:10.000Z
partitionColumns	[]
clusteringColumns	[]
numFiles	2
sizeInBytes	8515
properties	{"delta.parquet.compression.codec":"zstd","delta.enableDeletionVectors":"true"}
minReaderVersion	3
minWriterVersion	7
tableFeatures	["appendOnly","deletionVectors","invariants"]
statistics	{"numRowsDeletedByDeletionVectors":0,"numDeletionVectors":0}
clusterByAuto	FALSE

# add one more file into the azure container: 
# storage_account: misgauravstorageaccount
# container_name: azureextvolumedatabricks
# inside the contaier, we have 2 folders: input, & output

gauravmishra@Gauravs-MacBook-Air ~ % databricks fs cp /Users/gauravmishra/Desktop/orders_1.csv dbfs:/Volumes/misgauravcatalog/default/misgauravextvolume/input/
/Users/gauravmishra/Desktop/orders_1.csv -> dbfs:/Volumes/misgauravcatalog/default/misgauravextvolume/input/orders_1.csv

gauravmishra@Gauravs-MacBook-Air ~ % databricks fs ls dbfs:/Volumes/misgauravcatalog/default/misgauravextvolume/input/                               
orders_1.csv
orders_large.csv

# read operations
path =  "dbfs:/Volumes/misgauravcatalog/default/misgauravextvolume/input/orders_1.csv"
df = spark.read.format('csv').option('header', 'true').load(path)
display(df)

order_id,order_date,customer_id,order_status
1001,2020-01-01,1111,PENDING
1002,2020-01-02,1112,COMPLETED


# write operations
df.write.format('delta').mode('append').save('/Volumes/misgauravcatalog/default/misgauravextvolume/output')

gauravmishra@Gauravs-MacBook-Air ~ % databricks fs ls dbfs:/Volumes/misgauravcatalog/default/misgauravextvolume/output/                         
_delta_log
part-00000-6f539bad-9e4e-471b-b037-518bd17b4da4.c000.snappy.parquet
part-00000-e5b68ca3-d17a-428a-a60f-7acbe201b7f6.c000.snappy.parquet

gauravmishra@Gauravs-MacBook-Air ~ % databricks fs ls dbfs:/Volumes/misgauravcatalog/default/misgauravextvolume/output/_delta_log/
00000000000000000000.crc
00000000000000000000.json
00000000000000000001.crc
00000000000000000001.json

gauravmishra@Gauravs-MacBook-Air ~ % databricks fs cat dbfs:/Volumes/misgauravcatalog/default/misgauravextvolume/output/_delta_log/00000000000000000001.json

{"commitInfo":{"timestamp":1778072994570,"userId":"143844140774415","userName":"gauravmishra010@gmail.com","operation":"WRITE","operationParameters":{"mode":"Append","statsOnLoad":false,"partitionBy":"[]"},"notebook":{"notebookId":"966926155707402"},"queryHistoryStatementId":"0ce9b63a-dfb6-4c3a-87a9-7d34d08c6820","clusterId":"0506-124045-skliufgo-v2n","readVersion":0,"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"1","numOutputRows":"2","numOutputBytes":"1391"},"tags":{"noRowsCopied":"true","restoresDeletedRows":"false"},"engineInfo":"Databricks-Runtime/18.1.x-photon-scala2.13","txnId":"b8495493-bcd8-41d2-a456-359df7c8003b"}}
{"add":{"path":"part-00000-6f539bad-9e4e-471b-b037-518bd17b4da4.c000.snappy.parquet","partitionValues":{},"size":1391,"modificationTime":1778072994000,"dataChange":true,"stats":"{\"numRecords\":2,\"minValues\":{\"order_id\":\"1001\",\"order_date\":\"2020-01-01\",\"customer_id\":\"1111\",\"order_status\":\"COMPLETED\"},\"maxValues\":{\"order_id\":\"1002\",\"order_date\":\"2020-01-02\",\"customer_id\":\"1112\",\"order_status\":\"PENDING\"},\"nullCount\":{\"order_id\":0,\"order_date\":0,\"customer_id\":0,\"order_status\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1778072994000000","MIN_INSERTION_TIME":"1778072994000000","MAX_INSERTION_TIME":"1778072994000000","OPTIMIZE_TARGET_SIZE":"268435456"}}}
__tmp_path_dir
_staged_commits



# we can do this only: 
gauravmishra@Gauravs-MacBook-Air ~ % databricks fs ls  dbfs:/Volumes/misgauravcatalog/default/misgauravextvolume/
input
output

# we can't do this 
gauravmishra@Gauravs-MacBook-Air ~ % databricks fs ls  dbfs:/Volumes/misgauravcatalog/retaildb/
Error: Invalid path: Path is missing a volume name

===================

error while creating the table

=====================

%sql
create table retaildb.ordersdelta using delta location "dbfs:/Volumes/misgauravcatalog/default/misgauravextvolume/output/"
error


%sql
CREATE TABLE IF NOT EXISTS misgauravcatalog.retaildb.ordersdelta 
USING delta 
LOCATION "dbfs:/Volumes/misgauravcatalog/default/misgauravextvolume/output/";
error


%sql
CREATE TABLE IF NOT EXISTS misgauravcatalog.retaildb.ordersdelta 
USING delta 
LOCATION "Volumes/misgauravcatalog/default/misgauravextvolume/output/";
error

%sql
CREATE TABLE IF NOT EXISTS misgauravcatalog.retaildb.ordersdelta 
USING delta 
LOCATION "/Volumes/misgauravcatalog/default/misgauravextvolume/output/";
error

======================

%sql
DESCRIBE VOLUME misgauravcatalog.default.misgauravextvolume;
name,catalog,database,owner,storage_location,volume_type,comment,securable_type,securable_kind
misgauravextvolume,misgauravcatalog,default,gauravmishra010@gmail.com,abfss://azureextvolumedatabricks@misgauravstorageaccount.dfs.core.windows.net/,EXTERNAL,null,VOLUME,VOLUME_EXTERNAL



