%sql
create database retail db;

#################################################################################

# Here we are creating a delta table by using this locations: orders_bronze.delta, orders_silver.delta, & orders_gold.delta

# checking whether it's a hive managed table or hive external table.

dbutils.fs.mkdirs('/FileStore/data/')
dbutils.fs.mkdirs('/FileStore/data_test/')

#### a. with location ##################

%sql
create table retaildb.orders_testing1
(
order_id int,
order_date string,
customer_id int,
order_status string,
filename string,
createdon timestamp
)
using delta
location "dbfs:/FileStore/data_test/data_files"
partitioned by(order_status)
TBLPROPERTIES(delta.enableChangeDataFeed = true)

dbutils.fs.ls('dbfs:/FileStore/data_test')
# [FileInfo(path='dbfs:/FileStore/data_test/data_files/', name='data_files/', size=0, modificationTime=1757006479000)]

dbutils.fs.ls('dbfs:/FileStore/data_test/data_files')
# [FileInfo(path='dbfs:/FileStore/data_test/data_files/_delta_log/', name='_delta_log/', size=0, modificationTime=1757006479000)]

###### b. without locations ##############

%sql
create table retaildb.orders_testing2
(
order_id int,
order_date string,
customer_id int,
order_status string,
filename string,
createdon timestamp
)
using delta
partitioned by (order_status)
TBLPROPERTIES(delta.enableChangeDataFeed = true)

#####################################################################################

# creating bronze table.
# from which files we are getting this records, it acts like as a meta data to this table : filename, createdon 

%sql
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
TBLPROPERTIES(delta.enableChangeDataFeed = true)

# creating silver table
# At what time we created this records, or at what time we modified this records, it acts like as a meta data to this table : modifiedon, createdon 

%sql
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
TBLPROPERTIES(delta.enableChangeDataFeed = true)

# creating gold table.

%sql
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


############################ data directory ############################################################

dbutils.fs.ls('dbfs:/FileStore/data')
# [FileInfo(path='dbfs:/FileStore/data/orders_bronze.delta/', name='orders_bronze.delta/', size=0, modificationTime=1757005772000),
#  FileInfo(path='dbfs:/FileStore/data/orders_gold.delta/', name='orders_gold.delta/', size=0, modificationTime=1757005870000),
#  FileInfo(path='dbfs:/FileStore/data/orders_silver.delta/', name='orders_silver.delta/', size=0, modificationTime=1757005826000)]

dbutils.fs.ls('dbfs:/FileStore/data/orders_bronze.delta')
# [FileInfo(path='dbfs:/FileStore/data/orders_bronze.delta/_delta_log/', name='_delta_log/', size=0, modificationTime=1757005772000)]

dbutils.fs.ls('dbfs:/FileStore/data/orders_silver.delta')
# [FileInfo(path='dbfs:/FileStore/data/orders_silver.delta/_delta_log/', name='_delta_log/', size=0, modificationTime=1757005826000)]

dbutils.fs.ls('dbfs:/FileStore/data/orders_gold.delta')
# [FileInfo(path='dbfs:/FileStore/data/orders_gold.delta/_delta_log/', name='_delta_log/', size=0, modificationTime=1757005870000)]

###########################################################################################################


# all the tables are here as a external hive tables.

%sql 
describe extended retaildb.orders_bronze

# -- col_name	data_type	comment
# -- order_id	int	null
# -- order_date	string	null
# -- customer_id	int	null
# -- order_status	string	null
# -- filename	string	null
# -- createdon	timestamp	null
# -- # Partition Information		
# -- # col_name	data_type	comment
# -- order_status	string	null
		
# -- # Detailed Table Information		
# -- Catalog	spark_catalog	
# -- Database	retaildb	
# -- Table	orders_bronze	
# -- Created Time	Thu Sep 04 17:09:34 UTC 2025	
# -- Last Access	UNKNOWN	
# -- Created By	Spark 3.5.2	
# -- Type	EXTERNAL	
# -- Location	dbfs:/FileStore/data/orders_bronze.delta	
# -- Provider	delta	
# -- Owner	root	
# -- Table Properties	"[delta.enableChangeDataFeed=true,delta.enableDeletionVectors=true,delta.feature.appendOnly=supported,delta.feature.changeDataFeed=supported,delta.feature.deletionVectors=supported,delta.feature.invariants=supported,delta.minReaderVersion=3,delta.minWriterVersion=7]"	


################################################################################################################


# retaildb.orders_testing1: external hive tables

# retaildb.orders_testing2: managed hive tables

%sql
describe extended retaildb.orders_testing1

# -- col_name	data_type	comment
# -- order_id	int	null
# -- order_date	string	null
# -- customer_id	int	null
# -- order_status	string	null
# -- filename	string	null
# -- createdon	timestamp	null
# -- # Partition Information		
# -- # col_name	data_type	comment
# -- order_status	string	null
		
# -- # Detailed Table Information		
# -- Catalog	spark_catalog	
# -- Database	retaildb	
# -- Table	orders_testing1	
# -- Created Time	Thu Sep 04 17:21:19 UTC 2025	
# -- Last Access	UNKNOWN	
# -- Created By	Spark 3.5.2	
# -- Type	EXTERNAL	
# -- Location	dbfs:/FileStore/data_test/data_files	
# -- Provider	delta	
# -- Owner	root	
# -- Table Properties	"[delta.enableChangeDataFeed=true,delta.enableDeletionVectors=true,delta.feature.appendOnly=supported,delta.feature.changeDataFeed=supported,delta.feature.deletionVectors=supported,delta.feature.invariants=supported,delta.minReaderVersion=3,delta.minWriterVersion=7]"	



%sql
describe extended retaildb.orders_testing2

# -- col_name	data_type	comment
# -- order_id	int	null
# -- order_date	string	null
# -- customer_id	int	null
# -- order_status	string	null
# -- filename	string	null
# -- createdon	timestamp	null
# -- # Partition Information		
# -- # col_name	data_type	comment
# -- order_status	string	null
		
# -- # Detailed Table Information		
# -- Catalog	spark_catalog	
# -- Database	retaildb	
# -- Table	orders_testing2	
# -- Created Time	Thu Sep 04 17:21:44 UTC 2025	
# -- Last Access	UNKNOWN	
# -- Created By	Spark 3.5.2	
# -- Type	MANAGED	
# -- Location	dbfs:/user/hive/warehouse/retaildb.db/orders_testing2	
# -- Provider	delta	
# -- Owner	root	
# -- Is_managed_location	true	
# -- Table Properties	"[delta.enableChangeDataFeed=true,delta.enableDeletionVectors=true,delta.feature.appendOnly=supported,delta.feature.changeDataFeed=supported,delta.feature.deletionVectors=supported,delta.feature.invariants=supported,delta.minReaderVersion=3,delta.minWriterVersion=7]"	

 