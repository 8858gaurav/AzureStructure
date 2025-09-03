# run this commands for 2 times.

trip_df = spark.read.format("csv").option('header', 'true').option('inferSchema', 'true').load('dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-01.csv.gz')

trip_df.repartition(200).write.mode('append').format('delta').saveAsTable('taxidb.trips_delta_new');
# this will create a 400 files + one _delta_logs folder (inside this, we'll get a 2 transactional log files), if we run the above commands twice.

# this will store a file under hive warehouse. user/hive/warehouse/taxidb.db/trips_delta_new/ 400 files + _delta_logs folder.
# it's a managed hive table

%sql
describe detail taxidb.trips_delta_new;

# -- format	                                          delta
# -- id	                                              aa119370-981f-4de2-9f7d-3b852aafa1e6
# -- name	                                          spark_catalog.taxidb.trips_delta_new
# -- description	                                  null
# -- location	                                      dbfs:/user/hive/warehouse/taxidb.db/trips_delta_new
# -- createdAt	                                      2025-09-03T14:43:35.417Z
# -- lastModified	                                  2025-09-03T14:46:18Z
# -- partitionColumns	                              []
# -- clusteringColumns	                              []
# -- numFiles	                                      400
# -- sizeInBytes	                                  1384239450
# -- properties	                                      {"delta.enableDeletionVectors":"true"}
# -- minReaderVersion	                              3
# -- minWriterVersion	                              7
# -- tableFeatures	                                  ["appendOnly","deletionVectors","invariants"]
# -- statistics	                                      {"numRowsDeletedByDeletionVectors":0,"numDeletionVectors":0}
# -- clusterByAuto	                                  FALSE

%sql
describe history taxidb.trips_delta_new;

# -- version	             1	                                                      0
# -- timestamp	           2025-09-03T14:46:18Z	                        2025-09-03T14:44:53Z
# -- userId	               142168046953687	                              142168046953687
# -- userName	           gauravmishra8858@outlook.com	                gauravmishra8858@outlook.com
# -- operation	           WRITE	                                        CREATE TABLE AS SELECT 
# -- operationParameters.  {"mode":"Append","statsOnLoad":"false","partitionBy":"[]"}	

# -- {"partitionBy":"[]","clusterBy":"[]","description":null,"isManaged":"true","properties":"{\"delta.enableDeletionVectors\":\"true\"}","statsOnLoad":"false"}

# -- job	               null	                                         null
# -- notebook	           {"notebookId":"3075960300005709"}	             {"notebookId":"3075960300005709"}
# -- clusterId	           0903-133645-e25kgbrv	                         0903-133645-e25kgbrv
# -- readVersion	       0	                                             null
# -- isolationLevel	       WriteSerializable	                             WriteSerializable
# -- isBlindAppend	       TRUE	                                         TRUE
# -- operationMetrics	   {"numFiles":"200","numOutputRows":"14092413","numOutputBytes":"692119725"}	

# -- {"numFiles":"200","numOutputRows":"14092413","numOutputBytes":"692119725"}

# -- userMetadata	       null	                                            null
# -- engineInfo	           Databricks-Runtime/16.4.x-photon-scala2.12	      Databricks-Runtime/16.4.x-photon-scala2.12

%sql
select * from taxidb.trips_delta_new where passenger_count = 4;
# spark jobs view -> associated sql query -> explain -> # of files read = 400; # size of files read = 1320.1 MiB;

%sql
optimize taxidb.trips_delta_new zorderby (passenger_count);

%sql
select * from taxidb.trips_delta_new where passenger_count = 4;
# spark jobs view -> associated sql query -> explain -> # of files read = 1; # of files pruned = 4; # size of files read = 179.9 MiB; size of files pruned = 813.6 MiB


