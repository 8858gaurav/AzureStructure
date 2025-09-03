# how delta file works faster as compared to parquet file. 

# base layer : Data lake, Middle Layer: Transactional layer (Delta Lake (delta logs/commit logs)) to get features of Dataware house transactional log ACID properties, Top Layer: Delta Engine

# 1. delta skipping by using stats


trip_df = spark.read.format("csv").option('header', 'true').option('inferSchema', 'true').load('dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-01.csv.gz')

%sql
create database if not exists trip_db;

# for skipping please create 20 parts files

trip_df.repartition(20).write.format('delta').saveAsTable('trip_db.trips_delta')
# this will create a file under hive warehouse
# dbfs:/user/hive/warehouse/trip_db.db/trips_delta/20 files + _delta_logs folder
# its a managed hive table

trip_df.repartition(20).write.format('parquet').saveAsTable('trip_db.trips_parquet')
# this will create a file under hive warehouse
# dbfs:/user/hive/warehouse/trip_db.db/trips_parquet/20 files 
# its a managed hive table

# we can use describe detail/extended/formatted/history trip_db.trips_delta

%sql
DESCRIBE EXTENDED trip_db.trips_delta

# -- col_name	data_type	comment
# -- vendor_name	string	null
# -- Trip_Pickup_DateTime	timestamp	null
# -- Trip_Dropoff_DateTime	timestamp	null
# -- Passenger_Count	int	null
# -- Trip_Distance	double	null
# -- Start_Lon	double	null
# -- Start_Lat	double	null
# -- Rate_Code	string	null
# -- store_and_forward	int	null
# -- End_Lon	double	null
# -- End_Lat	double	null
# -- Payment_Type	string	null
# -- Fare_Amt	double	null
# -- surcharge	double	null
# -- mta_tax	string	null
# -- Tip_Amt	double	null
# -- Tolls_Amt	double	null
# -- Total_Amt	double	null
		
# -- Delta Statistics Columns		
# -- Column Names	"End_Lat, Trip_Dropoff_DateTime, Payment_Type, store_and_forward, Rate_Code, vendor_name, End_Lon, Start_Lat, Start_Lon, Fare_Amt, Tolls_Amt, Tip_Amt, Passenger_Count, Total_Amt, Trip_Distance, mta_tax, Trip_Pickup_DateTime, surcharge"	
# -- Column Selection Method	first-32	
		
# -- Detailed Table Information		
# -- Catalog	spark_catalog	
# -- Database	trip_db	
# -- Table	trips_delta	
# -- Created Time	Wed Sep 03 07:13:44 UTC 2025	
# -- Last Access	UNKNOWN	
# -- Created By	Spark 3.5.2	
# -- Type	MANAGED	
# -- Location	dbfs:/user/hive/warehouse/trip_db.db/trips_delta	
# -- Provider	delta	
# -- Owner	root	
# -- Is_managed_location	true	
# -- Table Properties	"[delta.enableDeletionVectors=true,delta.feature.appendOnly=supported,delta.feature.deletionVectors=supported,delta.feature.invariants=supported,delta.minReaderVersion=3,delta.minWriterVersion=7]"	


%sql
DESCRIBE EXTENDED trip_db.trips_parquet

# -- col_name	data_type	comment
# -- vendor_name	string	null
# -- Trip_Pickup_DateTime	timestamp	null
# -- Trip_Dropoff_DateTime	timestamp	null
# -- Passenger_Count	int	null
# -- Trip_Distance	double	null
# -- Start_Lon	double	null
# -- Start_Lat	double	null
# -- Rate_Code	string	null
# -- store_and_forward	int	null
# -- End_Lon	double	null
# -- End_Lat	double	null
# -- Payment_Type	string	null
# -- Fare_Amt	double	null
# -- surcharge	double	null
# -- mta_tax	string	null
# -- Tip_Amt	double	null
# -- Tolls_Amt	double	null
# -- Total_Amt	double	null
		
# -- Detailed Table Information		
# -- Catalog	spark_catalog	
# -- Database	trip_db	
# -- Table	trips_parquet	
# -- Owner	root	
# -- Created Time	Wed Sep 03 07:16:03 UTC 2025	
# -- Last Access	UNKNOWN	
# -- Created By	Spark 3.5.2	
# -- Type	MANAGED	
# -- Provider	parquet	
# -- Location	dbfs:/user/hive/warehouse/trip_db.db/trips_parquet	
# -- Serde Library	org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe	
# -- InputFormat	org.apache.hadoop.mapred.SequenceFileInputFormat	
# -- OutputFormat	org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat	


# Set the configuration to allow overwriting non-empty directories
spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")

# Write the DataFrame to Delta format
trip_df.repartition(20).write.format('delta').option('path', 'dbfs:/FileStore/datanew1').saveAsTable('trip_db.trips_delta_new')

# Write the DataFrame to Parquet format
trip_df.repartition(20).write.format('parquet').option('path', 'dbfs:/FileStore/datanew2').saveAsTable('trip_db.trips_parquet_new')

# this will create a file under dbfs:/FileStore/datanew1 or deltanew2
# dbfs:/FileStore/datanew/trip_db.db/trips_delta_new1/20 files + _delta_logs folder for delta table
# dbfs:/FileStore/datanew/trip_db.db/trips_parquet_new2/20 files for parquet table
# its a external hive table 

%sql
DESCRIBE EXTENDED trip_db.trips_parquet_new

# -- col_name	data_type	comment
# -- vendor_name	string	null
# -- Trip_Pickup_DateTime	timestamp	null
# -- Trip_Dropoff_DateTime	timestamp	null
# -- Passenger_Count	int	null
# -- Trip_Distance	double	null
# -- Start_Lon	double	null
# -- Start_Lat	double	null
# -- Rate_Code	string	null
# -- store_and_forward	int	null
# -- End_Lon	double	null
# -- End_Lat	double	null
# -- Payment_Type	string	null
# -- Fare_Amt	double	null
# -- surcharge	double	null
# -- mta_tax	string	null
# -- Tip_Amt	double	null
# -- Tolls_Amt	double	null
# -- Total_Amt	double	null
		
# -- Detailed Table Information		
# -- Catalog	spark_catalog	
# -- Database	trip_db	
# -- Table	trips_parquet_new	
# -- Owner	root	
# -- Created Time	Wed Sep 03 08:00:02 UTC 2025	
# -- Last Access	UNKNOWN	
# -- Created By	Spark 3.5.2	
# -- Type	EXTERNAL	
# -- Provider	parquet	
# -- Location	dbfs:/FileStore/datanew2	
# -- Serde Library	org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe	
# -- InputFormat	org.apache.hadoop.mapred.SequenceFileInputFormat	
# -- OutputFormat	org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat	


%sql
DESCRIBE EXTENDED trip_db.trips_delta_new

# -- col_name	data_type	comment
# -- vendor_name	string	null
# -- Trip_Pickup_DateTime	timestamp	null
# -- Trip_Dropoff_DateTime	timestamp	null
# -- Passenger_Count	int	null
# -- Trip_Distance	double	null
# -- Start_Lon	double	null
# -- Start_Lat	double	null
# -- Rate_Code	string	null
# -- store_and_forward	int	null
# -- End_Lon	double	null
# -- End_Lat	double	null
# -- Payment_Type	string	null
# -- Fare_Amt	double	null
# -- surcharge	double	null
# -- mta_tax	string	null
# -- Tip_Amt	double	null
# -- Tolls_Amt	double	null
# -- Total_Amt	double	null
		
# -- Delta Statistics Columns		
# -- Column Names	"End_Lat, Trip_Dropoff_DateTime, Payment_Type, store_and_forward, Rate_Code, vendor_name, End_Lon, Start_Lat, Start_Lon, Fare_Amt, Tolls_Amt, Tip_Amt, Passenger_Count, Total_Amt, Trip_Distance, mta_tax, Trip_Pickup_DateTime, surcharge"	
# -- Column Selection Method	first-32	
		
# -- Detailed Table Information		
# -- Catalog	spark_catalog	
# -- Database	trip_db	
# -- Table	trips_delta_new	
# -- Created Time	Wed Sep 03 07:58:54 UTC 2025	
# -- Last Access	UNKNOWN	
# -- Created By	Spark 3.5.2	
# -- Type	EXTERNAL	
# -- Location	dbfs:/FileStore/datanew1	
# -- Provider	delta	
# -- Owner	root	
# -- Table Properties	"[delta.enableDeletionVectors=true,delta.feature.appendOnly=supported,delta.feature.deletionVectors=supported,delta.feature.invariants=supported,delta.minReaderVersion=3,delta.minWriterVersion=7]"	

