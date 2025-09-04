# vaccum comands - if we run this, then we are not able to check the version of our table which has been deleted. a table may have more than 100k version, if I want to delete all unneccessary data, which we don't need it in future. then we can't execute select commands on those version. each version has a timestamp associated with it. on the basis of this timestamps, we are deleting the data by using vaccum commands. 

dbutils.fs.mount(
    source = "wasbs://misgauravcontainer@misgauravstorageaccount1.blob.core.windows.net/",
    mount_point = "/mnt/reaildb",
    extra_configs = {
        "fs.azure.account.key.misgauravstorageaccount1.blob.core.windows.net": "YHXumJMUw0fOBAxk+Dqq7BH0/eH+ZkszUa/ybhgScBFLNuOtyGhsOdHRxxW8VMGAD0zF4tQLj80m+AStD7ly0w=="
    }
)

%sql
create database if not exists misgauravdb;

df = spark.read.csv('/mnt/reaildb/orders.csv', header=True)
df.columns
# ['order_id', 'order_date', 'order_customer_id', 'order_status']

dbutils.fs.ls('/mnt/reaildb')
# [FileInfo(path='dbfs:/mnt/reaildb/orders.csv', name='orders.csv', size=4339, modificationTime=1756970965000)]

dbutils.fs.ls('/user/hive/warehouse/misgauravdb.db')
# []

df.write.mode("overwrite").format("delta").option("path", "/mnt/reaildb/delta_tables").saveAsTable("misgauravdb.delta_orders_partitioned")

dbutils.fs.ls('/user/hive/warehouse/misgauravdb.db')
# [], cuz it's an external hive table, not internal/managed hive table. if it's managed then data will be stored under this fodler. 

dbutils.fs.ls('/mnt/reaildb/delta_tables')
# [FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/', name='_delta_log/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/part-00000-f94614b9-4733-4121-b33e-493f15bdacc9-c000.snappy.parquet', name='part-00000-f94614b9-4733-4121-b33e-493f15bdacc9-c000.snappy.parquet', size=2814, modificationTime=1756971550000)]

%sql 
describe history misgauravdb.delta_orders_partitioned

# -- version	                                    0
# -- timestamp	                                  2025-09-04T07:39:14Z
# -- userId	                                      142168046953687
# -- userName	                                    gauravmishra8858@outlook.com
# -- operation	                                  CREATE OR REPLACE TABLE AS SELECT
# -- operationParameters	                        {"partitionBy":"[]","clusterBy":"[]","description":null,"isManaged":"false","properties":"{\"delta.enableDeletionVectors\":\"true\"}","statsOnLoad":"false"}
# -- job	                                        null
# -- notebook	                                    {"notebookId":"1982829765121440"}
# -- clusterId	                                  0904-072532-nfww5cwr
# -- readVersion	                                null
# -- isolationLevel	                              WriteSerializable
# -- isBlindAppend	                              FALSE
# -- operationMetrics	                            {"numFiles":"1","numRemovedFiles":"0","numRemovedBytes":"0","numOutputRows":"104","numOutputBytes":"2814"}
# -- userMetadata	                                null
# -- engineInfo	                                  Databricks-Runtime/16.4.x-scala2.12

dbutils.fs.ls('mnt/reaildb/delta_tables/_delta_log')
# [FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000000.crc', name='00000000000000000000.crc', size=3174, modificationTime=1756971555000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000000.json', name='00000000000000000000.json', size=2263, modificationTime=1756971554000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/__tmp_path_dir/', name='__tmp_path_dir/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/_staged_commits/', name='_staged_commits/', size=0, modificationTime=0)]

%sql
describe extended misgauravdb.delta_orders_partitioned;
# -- to check datatype of any columns by using sql
# -- use extended/formatted methods

# -- col_name	data_type	comment
# -- order_id	string	null
# -- order_date	string	null
# -- order_customer_id	string	null
# -- order_status	string	null
		
# -- # Delta Statistics Columns		
# -- Column Names	"order_id, order_date, order_customer_id, order_status"	
# -- Column Selection Method	first-32	
		
# -- # Detailed Table Information		
# -- Catalog	     spark_catalog	
# -- Database	     misgauravdb	
# -- Table	       delta_orders_partitioned	
# -- Created Time	 Thu Sep 04 07:39:15 UTC 2025	
# -- Last Access	 UNKNOWN	
# -- Created By	   Spark 3.5.2	
# -- Type	         EXTERNAL	
# -- Location	     dbfs:/mnt/reaildb/delta_tables	
# -- Provider	     delta	
# -- Owner	       root	
# -- Table Properties	"[delta.enableDeletionVectors=true,delta.feature.appendOnly=supported,delta.feature.deletionVectors=supported,delta.feature.invariants=supported,delta.minReaderVersion=3,delta.minWriterVersion=7]"

%sql
INSERT INTO misgauravdb.delta_orders_partitioned (order_id, order_date, order_customer_id, order_status)
VALUES ('201', '2013-07-25 00:00:00.0', '779000', 'PENDING_PAYMENT'); -- 1st operations insert

dbutils.fs.ls('mnt/reaildb/delta_tables/_delta_log')
# [FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000000.crc', name='00000000000000000000.crc', size=3174, modificationTime=1756971555000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000000.json', name='00000000000000000000.json', size=2263, modificationTime=1756971554000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000001.crc', name='00000000000000000001.crc', size=3935, modificationTime=1756972480000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000001.json', name='00000000000000000001.json', size=1372, modificationTime=1756972480000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/__tmp_path_dir/', name='__tmp_path_dir/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/_staged_commits/', name='_staged_commits/', size=0, modificationTime=0)]


%sql
describe history misgauravdb.delta_orders_partitioned

# -- version	                1	                                                      0
# -- timestamp	              2025-09-04T07:54:40Z	                                  2025-09-04T07:39:14Z
# -- userId	                  142168046953687	                                        142168046953687
# -- userName	                gauravmishra8858@outlook.com	                          gauravmishra8858@outlook.com
# -- operation	              WRITE	                                                  CREATE OR REPLACE TABLE AS SELECT
# -- operationParameters	    {"mode":"Append","statsOnLoad":"false","partitionBy":"[]"}	{"partitionBy":"[]","clusterBy":"[]","description":null,"isManaged":"false","properties":"{\"delta.enableDeletionVectors\":\"true\"}","statsOnLoad":"false"}
# -- job	                    null	                                                   null
# -- notebook	                {"notebookId":"1982829765121440"}	                       {"notebookId":"1982829765121440"}
# -- clusterId	              0904-072532-nfww5cwr	                                   0904-072532-nfww5cwr
# -- readVersion	            0                                                        null
# -- isolationLevel	          WriteSerializable	                                       WriteSerializable
# -- isBlindAppend	          TRUE	                                                   FALSE
# -- operationMetrics	        {"numFiles":"1","numOutputRows":"1","numOutputBytes":"1637"}	{"numFiles":"1","numRemovedFiles":"0","numRemovedBytes":"0","numOutputRows":"104","numOutputBytes":"2814"}
# -- userMetadata	            null	                                                   null
# -- engineInfo	              Databricks-Runtime/16.4.x-scala2.12	                     Databricks-Runtime/16.4.x-scala2.12

%sql
UPDATE misgauravdb.delta_orders_partitioned
SET order_status = 'Value1'
WHERE order_id = '101'; -- 2nd operations update

dbutils.fs.ls('mnt/reaildb/delta_tables/_delta_log')
# [FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000000.crc', name='00000000000000000000.crc', size=3174, modificationTime=1756971555000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000000.json', name='00000000000000000000.json', size=2263, modificationTime=1756971554000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000001.crc', name='00000000000000000001.crc', size=3935, modificationTime=1756972480000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000001.json', name='00000000000000000001.json', size=1372, modificationTime=1756972480000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000002.crc', name='00000000000000000002.crc', size=4796, modificationTime=1756972947000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000002.json', name='00000000000000000002.json', size=3303, modificationTime=1756972946000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/__tmp_path_dir/', name='__tmp_path_dir/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/_staged_commits/', name='_staged_commits/', size=0, modificationTime=0)]

%sql
describe history misgauravdb.delta_orders_partitioned

# -- version	  2	                            1                             0
# -- timestamp	2025-09-04T08:02:26Z	        2025-09-04T07:54:40Z	        2025-09-04T07:39:14Z
# -- userId	    142168046953687	              142168046953687	              142168046953687
# -- userName	  gauravmishra8858@outlook.com	gauravmishra8858@outlook.com	gauravmishra8858@outlook.com
# -- operation	UPDATE	                      WRITE	                        CREATE OR REPLACE TABLE AS SELECT	
# -- operationMetrics	

# -- {"numRemovedFiles":"0","numRemovedBytes":"0","numCopiedRows":"0","numDeletionVectorsAdded":"1","numDeletionVectorsRemoved":"0","numAddedChangeFiles":"0","executionTimeMs":"6253","numDeletionVectorsUpdated":"0","scanTimeMs":"3736","numAddedFiles":"1","numUpdatedRows":"1","numAddedBytes":"1559","rewriteTimeMs":"2489"}	

# -- {"numFiles":"1","numOutputRows":"1","numOutputBytes":"1637"}	

# -- {"numFiles":"1","numRemovedFiles":"0","numRemovedBytes":"0","numOutputRows":"104","numOutputBytes":"2814"}	


%sql
DELETE FROM misgauravdb.delta_orders_partitioned
WHERE order_id = '102'; -- 3rd operation delete


dbutils.fs.ls('mnt/reaildb/delta_tables/_delta_log')
# [FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000000.crc', name='00000000000000000000.crc', size=3174, modificationTime=1756971555000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000000.json', name='00000000000000000000.json', size=2263, modificationTime=1756971554000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000001.crc', name='00000000000000000001.crc', size=3935, modificationTime=1756972480000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000001.json', name='00000000000000000001.json', size=1372, modificationTime=1756972480000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000002.crc', name='00000000000000000002.crc', size=4796, modificationTime=1756972947000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000002.json', name='00000000000000000002.json', size=3303, modificationTime=1756972946000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000003.crc', name='00000000000000000003.crc', size=4796, modificationTime=1756973314000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000003.json', name='00000000000000000003.json', size=2675, modificationTime=1756973314000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/__tmp_path_dir/', name='__tmp_path_dir/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/_staged_commits/', name='_staged_commits/', size=0, modificationTime=0)]

%sql
describe history misgauravdb.delta_orders_partitioned

# -- version	    3	                  2	                    1	                    0
# -- timestamp	 2025-09-04T08:08:34Z	2025-09-04T08:02:26Z	2025-09-04T07:54:40Z	2025-09-04T07:39:14Z
# -- operation	 DELETE	              UPDATE	              WRITE	                CREATE OR REPLACE TABLE AS SELECT
# -- operationParameters	
# -- {"predicate":"[\"(order_id#7343 = 102)\"]"}	
# -- {"predicate":"[\"(order_id#6051 = 101)\"]"}	
# -- {"mode":"Append","statsOnLoad":"false","partitionBy":"[]"}	
# -- {"partitionBy":"[]","clusterBy":"[]","description":null,"isManaged":"false","properties":"{\"delta.enableDeletionVectors\":\"true\"}","statsOnLoad":"false"}
# -- readVersion	2	1	0	null
# -- isBlindAppend	FALSE	FALSE	TRUE	FALSE
# -- operationMetrics	
# -- {"numRemovedFiles":"0","numRemovedBytes":"0","numCopiedRows":"0","numDeletionVectorsAdded":"1","numDeletionVectorsRemoved":"1","numAddedChangeFiles":"0","executionTimeMs":"2852","numDeletionVectorsUpdated":"1","numDeletedRows":"1","scanTimeMs":"1469","numAddedFiles":"0","numAddedBytes":"0","rewriteTimeMs":"1382"}	{"numRemovedFiles":"0","numRemovedBytes":"0","numCopiedRows":"0","numDeletionVectorsAdded":"1","numDeletionVectorsRemoved":"0","numAddedChangeFiles":"0","executionTimeMs":"6253","numDeletionVectorsUpdated":"0","scanTimeMs":"3736","numAddedFiles":"1","numUpdatedRows":"1","numAddedBytes":"1559","rewriteTimeMs":"2489"}	{"numFiles":"1","numOutputRows":"1","numOutputBytes":"1637"}	
# -- {"numFiles":"1","numRemovedFiles":"0","numRemovedBytes":"0","numOutputRows":"104","numOutputBytes":"2814"}

%sql
select * from misgauravdb.delta_orders_partitioned version as of 0 limit 5;

# -- order_id	order_date	order_customer_id	order_status
# -- 1	2013-07-25 00:00:00.0	11599	CLOSED
# -- 2	2013-07-25 00:00:00.0	256	PENDING_PAYMENT
# -- 3	2013-07-25 00:00:00.0	12111	COMPLETE
# -- 4	2013-07-25 00:00:00.0	8827	CLOSED
# -- 5	2013-07-25 00:00:00.0	11318	COMPLETE

%sql
select * from misgauravdb.delta_orders_partitioned version as of 1 where order_id = '201' limit 5;

# -- 1st operation insert
# -- order_id	order_date	order_customer_id	order_status
# -- 201	2013-07-25 00:00:00.0	779000	PENDING_PAYMENT

%sql
select * from misgauravdb.delta_orders_partitioned version as of 2
WHERE order_id = '101'

# -- 2nd operation update
# -- order_id	order_date	order_customer_id	order_status
# -- 101	2013-07-25 00:00:00.0	5116	Value1

%sql
select * from misgauravdb.delta_orders_partitioned version as of 3
WHERE order_id = '102'

# -- 3rd opeartion delete
# -- order_id	order_date	order_customer_id	order_status
# -- No records


############################### Vaccum Commands ##############################

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

%sql
describe history misgauravdb.delta_orders_partitioned ;

# -- version	    3	                  2	                    1	                    0
# -- timestamp	 2025-09-04T08:08:34Z	2025-09-04T08:02:26Z	2025-09-04T07:54:40Z	2025-09-04T07:39:14Z
# -- operation	 DELETE	              UPDATE	              WRITE	                CREATE OR REPLACE TABLE AS SELECT
# -- operationParameters	
# -- {"predicate":"[\"(order_id#7343 = 102)\"]"}	
# -- {"predicate":"[\"(order_id#6051 = 101)\"]"}	
# -- {"mode":"Append","statsOnLoad":"false","partitionBy":"[]"}	
# -- {"partitionBy":"[]","clusterBy":"[]","description":null,"isManaged":"false","properties":"{\"delta.enableDeletionVectors\":\"true\"}","statsOnLoad":"false"}
# -- readVersion	2	1	0	null
# -- isBlindAppend	FALSE	FALSE	TRUE	FALSE
# -- operationMetrics	
# -- {"numRemovedFiles":"0","numRemovedBytes":"0","numCopiedRows":"0","numDeletionVectorsAdded":"1","numDeletionVectorsRemoved":"1","numAddedChangeFiles":"0","executionTimeMs":"2852","numDeletionVectorsUpdated":"1","numDeletedRows":"1","scanTimeMs":"1469","numAddedFiles":"0","numAddedBytes":"0","rewriteTimeMs":"1382"}	{"numRemovedFiles":"0","numRemovedBytes":"0","numCopiedRows":"0","numDeletionVectorsAdded":"1","numDeletionVectorsRemoved":"0","numAddedChangeFiles":"0","executionTimeMs":"6253","numDeletionVectorsUpdated":"0","scanTimeMs":"3736","numAddedFiles":"1","numUpdatedRows":"1","numAddedBytes":"1559","rewriteTimeMs":"2489"}	{"numFiles":"1","numOutputRows":"1","numOutputBytes":"1637"}	
# -- {"numFiles":"1","numRemovedFiles":"0","numRemovedBytes":"0","numOutputRows":"104","numOutputBytes":"2814"}

%sql
vacuum misgauravdb.delta_orders_partitioned retain 1 hours dry run
# -- No rows returned

%sql
vacuum misgauravdb.delta_orders_partitioned retain 1 hours;
# -- path
# -- dbfs:/mnt/reaildb/delta_tables

%sql
select * from misgauravdb.delta_orders_partitioned version as of 0 limit 5;
# -- No results

%sql
describe history misgauravdb.delta_orders_partitioned

# -- version	7	6	5	4	3	2	1	0		
# -- timestamp	2025-09-04T09:17:30Z	2025-09-04T09:17:25Z	2025-09-04T09:12:20Z	2025-09-04T09:12:15Z	2025-09-04T08:08:34Z	2025-09-04T08:02:26Z	2025-09-04T07:54:40Z	2025-09-04T07:39:14Z		
# -- operation	VACUUM END	VACUUM START	VACUUM END	VACUUM START	DELETE	UPDATE	WRITE	CREATE OR REPLACE TABLE AS SELECT		
# -- operationParameters	
# -- {"status":"COMPLETED"}	
# -- {"retentionCheckEnabled":"false","defaultRetentionMillis":"604800000","specifiedRetentionMillis":"3600000"}	{"status":"COMPLETED"}	
# -- {"retentionCheckEnabled":"false","defaultRetentionMillis":"604800000","specifiedRetentionMillis":"3600000"}	
# -- {"predicate":"[\"(order_id#7343 = 102)\"]"}	
# -- {"predicate":"[\"(order_id#6051 = 101)\"]"}	
# -- {"mode":"Append","statsOnLoad":"false","partitionBy":"[]"}	
# -- {"partitionBy":"[]","clusterBy":"[]","description":null,"isManaged":"false","properties":"{\"delta.enableDeletionVectors\":\"true\"}","statsOnLoad":"false"}		

# -- isBlindAppend	TRUE	TRUE	TRUE	TRUE	FALSE	FALSE	TRUE	FALSE		
# -- operationMetrics	
# -- {"numDeletedFiles":"0","numVacuumedDirectories":"1"}	
# -- {"numFilesToDelete":"0","sizeOfDataToDelete":"0"}	
# -- {"numDeletedFiles":"1","numVacuumedDirectories":"1"}	
# -- {"numFilesToDelete":"1","sizeOfDataToDelete":"43"}

# -- {"numRemovedFiles":"0","numRemovedBytes":"0","numCopiedRows":"0","numDeletionVectorsAdded":"1","numDeletionVectorsRemoved":"1","numAddedChangeFiles":"0","executionTimeMs":"2852","numDeletionVectorsUpdated":"1","numDeletedRows":"1","scanTimeMs":"1469","numAddedFiles":"0","numAddedBytes":"0","rewriteTimeMs":"1382"}

# -- {"numRemovedFiles":"0","numRemovedBytes":"0","numCopiedRows":"0","numDeletionVectorsAdded":"1","numDeletionVectorsRemoved":"0","numAddedChangeFiles":"0","executionTimeMs":"6253","numDeletionVectorsUpdated":"0","scanTimeMs":"3736","numAddedFiles":"1","numUpdatedRows":"1","numAddedBytes":"1559","rewriteTimeMs":"2489"}

# -- {"numFiles":"1","numOutputRows":"1","numOutputBytes":"1637"}	
# -- {"numFiles":"1","numRemovedFiles":"0","numRemovedBytes":"0","numOutputRows":"104","numOutputBytes":"2814"}		


dbutils.fs.ls('mnt/reaildb/delta_tables/_delta_log')
# 10 json file will be created: 3 for operations, 1 for table creations
# 6 for vaccum ( 3 for vaccum start operations, and 3 for vaccum end operations)

# [FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000000.crc', name='00000000000000000000.crc', size=3174, modificationTime=1756971555000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000000.json', name='00000000000000000000.json', size=2263, modificationTime=1756971554000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000001.00000000000000000006.compacted.json', name='00000000000000000001.00000000000000000006.compacted.json', size=4099, modificationTime=1756977446000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000001.crc', name='00000000000000000001.crc', size=3935, modificationTime=1756972480000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000001.json', name='00000000000000000001.json', size=1372, modificationTime=1756972480000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000002.crc', name='00000000000000000002.crc', size=4796, modificationTime=1756972947000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000002.json', name='00000000000000000002.json', size=3303, modificationTime=1756972946000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000003.crc', name='00000000000000000003.crc', size=4796, modificationTime=1756973314000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000003.json', name='00000000000000000003.json', size=2675, modificationTime=1756973314000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000004.crc', name='00000000000000000004.crc', size=4796, modificationTime=1756977135000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000004.json', name='00000000000000000004.json', size=585, modificationTime=1756977135000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000005.crc', name='00000000000000000005.crc', size=4796, modificationTime=1756977141000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000005.json', name='00000000000000000005.json', size=506, modificationTime=1756977140000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000006.crc', name='00000000000000000006.crc', size=4796, modificationTime=1756977445000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000006.json', name='00000000000000000006.json', size=584, modificationTime=1756977445000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000007.crc', name='00000000000000000007.crc', size=4796, modificationTime=1756977451000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000007.json', name='00000000000000000007.json', size=506, modificationTime=1756977450000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000008.crc', name='00000000000000000008.crc', size=4796, modificationTime=1756977947000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000008.json', name='00000000000000000008.json', size=584, modificationTime=1756977947000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000009.crc', name='00000000000000000009.crc', size=4796, modificationTime=1756977952000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/00000000000000000009.json', name='00000000000000000009.json', size=506, modificationTime=1756977952000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/__tmp_path_dir/', name='__tmp_path_dir/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/_last_vacuum_info', name='_last_vacuum_info', size=50, modificationTime=1756977952000),
#  FileInfo(path='dbfs:/mnt/reaildb/delta_tables/_delta_log/_staged_commits/', name='_staged_commits/', size=0, modificationTime=0)]

