# with this, data skipping is not possible just by using optimize commands. 
# to achieve data skipping, we have to use optimize commands with Z-ordering methods. 

%sql
create database taxidb;

trip_df = spark.read.format("csv").option('header', 'true').option('inferSchema', 'true').load('dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-01.csv.gz')

# this will create a 500 files under each different vendor_name folders.
# this will store a file under hive warehouse. user/hive/warehouse/taxidb.db/trips_delta/ 3 folder (each folder will have 500 files) + _delta_logs folder
# 3 folder are vendor_name=CMT, vendor_name=DDS, vendor_name=VTS
# it's a managed hive table
trip_df.repartition(500).write.format('delta').partitionBy('vendor_name').saveAsTable('taxidb.trips_delta')

%sql
describe detail taxidb.trips_delta;

# -- format	               delta
# -- id	                   17605e41-3bb6-48b7-a9b2-a3c1ee018db1
# -- name	                 spark_catalog.taxidb.trips_delta
# -- description        	 null
# -- location	             dbfs:/user/hive/warehouse/taxidb.db/trips_delta
# -- createdAt	           2025-09-03T13:45:43.695Z
# -- lastModified	         2025-09-03T13:47:40Z
# -- partitionColumns	     ["vendor_name"]
# -- clusteringColumns	   []
# -- numFiles	             1500
# -- sizeInBytes	         713035220
# -- properties	           {"delta.enableDeletionVectors":"true"}
# -- minReaderVersion	     3
# -- minWriterVersion	     7
# -- tableFeatures	       ["appendOnly","deletionVectors","invariants"]
# -- statistics	           {"numRowsDeletedByDeletionVectors":0,"numDeletionVectors":0}
# -- clusterByAuto	       FALSE

%sql
select * from taxidb.trips_delta where total_amt = 20;
# took 42 seconds to execute it.
# if we go inside this. spark job view -> associated sql query -> explain -> you will see # of files read = 1500

%sql
optimize taxidb.trips_delta;

# -- path	    dbfs:/user/hive/warehouse/taxidb.db/trips_delta
# -- metrics	  {"numFilesAdded":5,"numFilesRemoved":1500,"filesAdded":{"min":24470016,"max":208206168,"avg":86071062.8,"totalFiles":5,"totalSize":430355314},"filesRemoved":{"min":92541,"max":703052,"avg":475356.81333333335,"totalFiles":1500,"totalSize":713035220},"partitionsOptimized":3,"zOrderStats":null,"clusteringStats":null,"numBins":0,"numBatches":1,"totalConsideredFiles":1500,"totalFilesSkipped":0,"preserveInsertionOrder":true,"numFilesSkippedToReduceWriteAmplification":0,"numBytesSkippedToReduceWriteAmplification":0,"startTimeMs":1756908678844,"endTimeMs":1756908690272,"totalClusterParallelism":4,"totalScheduledTasks":5,"autoCompactParallelismStats":null,"deletionVectorStats":{"numDeletionVectorsRemoved":0,"numDeletionVectorRowsRemoved":0},"recompressionCodec":null,"numTableColumns":18,"numTableColumnsWithStats":18,"totalTaskExecutionTimeMs":13411,"skippedArchivedFiles":0,"clusteringMetrics":null}



%sql
describe history taxidb.trips_delta;

# -- version	          1	                                                                                         0
# -- timestamp	      2025-09-03T14:11:30Z	                                                  2025-09-03T13:47:40Z
# -- userId	          142168046953687	                                                                142168046953687
# -- userName	          gauravmishra8858@outlook.com	                                          gauravmishra8858@outlook.com
# -- operation	      OPTIMIZE	                                                                CREATE TABLE AS SELECT
# -- operationParameters	{"predicate":"[]","auto":"false","clusterBy":"[]","zOrderBy":"[]","batchId":"0"}	

# -- {"partitionBy":"[\"vendor_name\"]","clusterBy":"[]","description":null,"isManaged":"true","properties":"{\"delta.enableDeletionVectors\":\"true\"}","statsOnLoad":"false"}

# -- job	             null	                                                                     null
# -- notebook	         {"notebookId":"3075960300005709"}	                                 {"notebookId":"3075960300005709"}
# -- clusterId	     0903-133645-e25kgbrv	                                                 0903-133645-e25kgbrv
# -- readVersion	     0	                                                                   null
# -- isolationLevel	 SnapshotIsolation	                                                     WriteSerializable
# -- isBlindAppend	 FALSE	                                                                 TRUE
# -- operationMetrics	{"numRemovedFiles":"1500","numRemovedBytes":"713035220","p25FileSize":"28964765","numDeletionVectorsRemoved":"0","minFileSize":"24470016","numAddedFiles":"5","maxFileSize":"208206168","p75FileSize":"127730138","p50FileSize":"40984227","numAddedBytes":"430355314"}	

# -- {"numFiles":"1500","numOutputRows":"14092413","numOutputBytes":"713035220"}

# -- userMetadata	     null	                                                                    null
# -- engineInfo	     Databricks-Runtime/16.4.x-photon-scala2.12	            Databricks-Runtime/16.4.x-photon-scala2.12


%sql
select * from taxidb.trips_delta where total_amt = 20;

# -- took 8 seconds to execute it. means opening, and closing of any files will take a time. 
# -- if we go inside this. spark job view -> associated sql query -> explain -> you will see # of files read = 5, that's the beauty of optimize commands. we are end up with opening of such a large number of files, earlier it has to opened 500 files. 


%sql 
select * from taxidb.trips_delta version as of 0 where total_amt = 20; 
# -- files read = 1500, here it is not scanning 1 files, it is scanning all 1500 files. No data skipping happens here.

select * from taxidb.trips_delta version as of 1 where total_amt = 20; 
# -- file read = 5, here it is not scanning 1 files, it is scanning all 5 files. No data skipping happens here.