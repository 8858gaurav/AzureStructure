# CDC/CDF will give you which records has been updated/deleted/merge/inserted. 
# CDC/CDF will help you to merge the incremental changes from bronze to silver to gold. 
# earlier this was difficult to merge the incremental changes in the case of Medallion architecture without the use of CDC/CDF

# this will create a managed hive table
%sql
create table orders(
order_id int,
order_date string,
customer_id int,
order_status string)
using delta
TBLPROPERTIES (delta.enableChangeDataFeed = true)

# for existing table with CDC/CDF disabled 
# alter table orders set TBLPROPERTIES(delta.enableChangeDataFeed = true)
# if we want to create subsequent CDC enable table
# set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true

dbutils.fs.ls('/user/hive/warehouse/orders')
# [FileInfo(path='dbfs:/user/hive/warehouse/orders/_delta_log/', name='_delta_log/', size=0, modificationTime=1756985922000)]
dbutils.fs.ls('/user/hive/warehouse/orders/_delta_log')
# [FileInfo(path='dbfs:/user/hive/warehouse/orders/_delta_log/00000000000000000000.crc', name='00000000000000000000.crc', size=2468, modificationTime=1756985923000),
#  FileInfo(path='dbfs:/user/hive/warehouse/orders/_delta_log/00000000000000000000.json', name='00000000000000000000.json', size=1445, modificationTime=1756985922000),
#  FileInfo(path='dbfs:/user/hive/warehouse/orders/_delta_log/__tmp_path_dir/', name='__tmp_path_dir/', size=0, modificationTime=1756985922000),
#  FileInfo(path='dbfs:/user/hive/warehouse/orders/_delta_log/_staged_commits/', name='_staged_commits/', size=0, modificationTime=1756985922000)]

%sql
describe history orders;

# -- version	    0
# -- timestamp	  2025-09-04T11:38:42Z
# -- userId	      142168046953687
# -- userName	    gauravmishra8858@outlook.com
# -- operation	  CREATE TABLE
# -- operationParameters	{"partitionBy":"[]","clusterBy":"[]","description":null,"isManaged":"true","properties":"{\"delta.enableChangeDataFeed\":\"true\",\"delta.enableDeletionVectors\":\"true\"}","statsOnLoad":"false"}
# -- job	        null
# -- notebook     {"notebookId":"990117552497403"}
# -- clusterId	  0904-113047-zr9suj6j
# -- readVersion	null
# -- isolationLevel	WriteSerializable
# -- isBlindAppend	TRUE
# -- operationMetrics	{}
# -- userMetadata	null
# -- engineInfo	Databricks-Runtime/16.4.x-scala2.12

%sql
insert into orders values 
(1,	'2013-07-25 00:00:00.0', 11599,	'CLOSED'),
(2,	'2013-07-25 00:00:00.0', 256,	'PENDING_PAYMENT'),
(3,	'2013-07-25 00:00:00.0', 12111,	'COMPLETE'),
(4,	'2013-07-25 00:00:00.0', 8827,	'CLOSED'),
(5,	'2013-07-25 00:00:00.0', 11318,	'COMPLETE')

# -- num_affected_rows	num_inserted_rows
# -- 5	                5

dbutils.fs.ls('/user/hive/warehouse/orders/')
# 1 file will be created in the table directory
# [FileInfo(path='dbfs:/user/hive/warehouse/orders/_delta_log/', name='_delta_log/', size=0, modificationTime=1756985922000),
#  FileInfo(path='dbfs:/user/hive/warehouse/orders/part-00000-36418ed8-b229-49c6-8633-9d21a4962339-c000.snappy.parquet', name='part-00000-36418ed8-b229-49c6-8633-9d21a4962339-c000.snappy.parquet', size=1668, modificationTime=1756986718000)]

%sql
describe history orders;

# -- version	  1	                    0
# -- timestamp	2025-09-04T11:51:59Z	2025-09-04T11:38:42Z
# -- operation	WRITE	                CREATE TABLE
# -- operationParameters	{"mode":"Append","statsOnLoad":"false","partitionBy":"[]"}	{"partitionBy":"[]","clusterBy":"[]","description":null,"isManaged":"true","properties":"{\"delta.enableChangeDataFeed\":\"true\",\"delta.enableDeletionVectors\":\"true\"}","statsOnLoad":"false"}
# -- isBlindAppend	TRUE	TRUE
# -- operationMetrics	{"numFiles":"1","numOutputRows":"5","numOutputBytes":"1668"}	{}
# -- userMetadata	null	null
# -- engineInfo	Databricks-Runtime/16.4.x-scala2.12	Databricks-Runtime/16.4.x-scala2.12

%sql
select * from table_changes('orders', 1); -- 1 means from which version we want to see the changes.

# -- order_id	order_date	customer_id	order_status	_change_type	_commit_version	_commit_timestamp
# -- 1	2013-07-25 00:00:00.0	11599	CLOSED	insert	1	2025-09-04T11:51:59Z
# -- 2	2013-07-25 00:00:00.0	256	PENDING_PAYMENT	insert	1	2025-09-04T11:51:59Z
# -- 3	2013-07-25 00:00:00.0	12111	COMPLETE	insert	1	2025-09-04T11:51:59Z
# -- 4	2013-07-25 00:00:00.0	8827	CLOSED	insert	1	2025-09-04T11:51:59Z
# -- 5	2013-07-25 00:00:00.0	11318	COMPLETE	insert	1	2025-09-04T11:51:59Z

# till now, I mean insert operation, No _change_data folder has been created. 
dbutils.fs.ls('/user/hive/warehouse/orders/')
# [FileInfo(path='dbfs:/user/hive/warehouse/orders/_delta_log/', name='_delta_log/', size=0, modificationTime=1756985922000),
#  FileInfo(path='dbfs:/user/hive/warehouse/orders/part-00000-36418ed8-b229-49c6-8633-9d21a4962339-c000.snappy.parquet', name='part-00000-36418ed8-b229-49c6-8633-9d21a4962339-c000.snappy.parquet', size=1668, modificationTime=1756986718000)]

%sql
delete from orders where order_id = 3;

# -- num_affected_rows
# -- 1

dbutils.fs.ls('/user/hive/warehouse/orders/')
# No _change_data folder will be created here for delete operations.

# 1 deletion_vector file will be created for delete operation - deletion_vector_d76a8ed3-c096-4382-b54b-735aa0944118.bin

# [FileInfo(path='dbfs:/user/hive/warehouse/orders/_delta_log/', name='_delta_log/', size=0, modificationTime=1756985922000),
#  FileInfo(path='dbfs:/user/hive/warehouse/orders/deletion_vector_d76a8ed3-c096-4382-b54b-735aa0944118.bin', name='deletion_vector_d76a8ed3-c096-4382-b54b-735aa0944118.bin', size=43, modificationTime=1756987196000),
#  FileInfo(path='dbfs:/user/hive/warehouse/orders/part-00000-36418ed8-b229-49c6-8633-9d21a4962339-c000.snappy.parquet', name='part-00000-36418ed8-b229-49c6-8633-9d21a4962339-c000.snappy.parquet', size=1668, modificationTime=1756986718000)]

%sql
select * from table_changes('orders', 1)

# -- order_id	      order_date	          customer_id	order_status	_change_type	_commit_version	_commit_timestamp
# -- 1	            2013-07-25 00:00:00.0	11599	       CLOSED	          insert	1	2025-09-04T11:51:59Z
# -- 2	            2013-07-25 00:00:00.0	256	         PENDING_PAYMENT	insert	1	2025-09-04T11:51:59Z
# -- 3	            2013-07-25 00:00:00.0	12111	       COMPLETE	        insert	1	2025-09-04T11:51:59Z
# -- 4	            2013-07-25 00:00:00.0	8827	       CLOSED	          insert	1	2025-09-04T11:51:59Z
# -- 5	            2013-07-25 00:00:00.0	11318	       COMPLETE	        insert	1	2025-09-04T11:51:59Z
# -- 3	            2013-07-25 00:00:00.0	12111	       COMPLETE	        delete	2	2025-09-04T11:59:57Z

%sql
update orders set order_status = 'value' where order_id = 4;
# -- num_affected_rows
# -- 1


dbutils.fs.ls('/user/hive/warehouse/orders/')

# 1 more deletion_vector file will be created for update operation as well - deletion_vector_965059fb-8b3b-49e2-bcb5-e7348e68f56d.bin

# [FileInfo(path='dbfs:/user/hive/warehouse/orders/_change_data/', name='_change_data/', size=0, modificationTime=1756987473000),
#  FileInfo(path='dbfs:/user/hive/warehouse/orders/_delta_log/', name='_delta_log/', size=0, modificationTime=1756985922000),
#  FileInfo(path='dbfs:/user/hive/warehouse/orders/deletion_vector_965059fb-8b3b-49e2-bcb5-e7348e68f56d.bin', name='deletion_vector_965059fb-8b3b-49e2-bcb5-e7348e68f56d.bin', size=87, modificationTime=1756987472000),
#  FileInfo(path='dbfs:/user/hive/warehouse/orders/deletion_vector_d76a8ed3-c096-4382-b54b-735aa0944118.bin', name='deletion_vector_d76a8ed3-c096-4382-b54b-735aa0944118.bin', size=43, modificationTime=1756987196000),
#  FileInfo(path='dbfs:/user/hive/warehouse/orders/part-00000-03052d1d-c261-4ae6-99af-a59d68cf5093.c000.snappy.parquet', name='part-00000-03052d1d-c261-4ae6-99af-a59d68cf5093.c000.snappy.parquet', size=1741, modificationTime=1756987473000),
#  FileInfo(path='dbfs:/user/hive/warehouse/orders/part-00000-36418ed8-b229-49c6-8633-9d21a4962339-c000.snappy.parquet', name='part-00000-36418ed8-b229-49c6-8633-9d21a4962339-c000.snappy.parquet', size=1668, modificationTime=1756986718000),
#  FileInfo(path='dbfs:/user/hive/warehouse/orders/part-00000-3ef63e48-22bc-4a4b-8551-47be1a73cd98-c000.snappy.parquet', name='part-00000-3ef63e48-22bc-4a4b-8551-47be1a73cd98-c000.snappy.parquet', size=1617, modificationTime=1756987477000)]

%sql
select * from table_changes('orders', 1)

# -- order_id	 order_date	            customer_id	order_status	_change_type	  _commit_version_commit_timestamp
# -- 4	       2013-07-25 00:00:00.0	8827	      CLOSED	    update_preimage	  3	       2025-09-04T12:04:34Z
# -- 4	       2013-07-25 00:00:00.0	8827	      value	        update_postimage  3	       2025-09-04T12:04:34Z
# -- 1	       2013-07-25 00:00:00.0	11599	      CLOSED	    insert	          1	       2025-09-04T11:51:59Z
# -- 2	       2013-07-25 00:00:00.0	256	        PENDING_PAYMENT	insert	          1	       2025-09-04T11:51:59Z
# -- 3	       2013-07-25 00:00:00.0	12111	      COMPLETE	    insert	          1	       2025-09-04T11:51:59Z
# -- 4	       2013-07-25 00:00:00.0	8827	      CLOSED	    insert	          1	       2025-09-04T11:51:59Z
# -- 5	       2013-07-25 00:00:00.0	11318	      COMPLETE	    insert	          1	       2025-09-04T11:51:59Z
# -- 3	       2013-07-25 00:00:00.0	12111	      COMPLETE	    delete	          2	       2025-09-04T11:59:57Z


