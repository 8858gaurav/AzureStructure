# dbutils.fs.ls('/')
# [FileInfo(path='dbfs:/Volume/', name='Volume/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/Volumes/', name='Volumes/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/databricks-datasets/', name='databricks-datasets/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/databricks-results/', name='databricks-results/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/volume/', name='volume/', size=0, modificationTime=0),
#  FileInfo(path='dbfs:/volumes/', name='volumes/', size=0, modificationTime=0)]

# dbutils.fs.mounts()
# Out[3]: [MountInfo(mountPoint='/databricks-datasets', source='databricks-datasets', encryptionType=''),
#  MountInfo(mountPoint='/databricks/mlflow-tracking', source='databricks/mlflow-tracking', encryptionType='sse-s3'),
#  MountInfo(mountPoint='/databricks-results', source='databricks-results', encryptionType='sse-s3'),
#  MountInfo(mountPoint='/databricks/mlflow-registry', source='databricks/mlflow-registry', encryptionType='sse-s3'),
#  MountInfo(mountPoint='/', source='DatabricksRoot', encryptionType='sse-s3')]

# IAM -> add role assignment
# give key vaults secret officer permission to yourself
# give key vault secret user permission to azuredatabricks

# gaurav [ ~ ]$ pip install --user databricks-cli
# gaurav [ ~ ]$ databricks configure --token
# how to get this -> setting -> developer -> access token
# https://adb-1413606331357845.5.azuredatabricks.net/?o=1413606331357845#secrets/createScope, we can give this name (databricks-demo-scope) here also by using this link.
# OR gaurav [ ~ ]$ databricks secrets create-scope --scope databricks-demo-scope
# gaurav [ ~ ]$ databricks secrets list-scopes

# dbutils.secrets.get("databricks-demo-scope1 ", "misgaurav-secret-scope")
   

# dbutils.fs.mount(
#     source = "wasbs://misgauravcontainer@misgauravstorageaccount1.blob.core.windows.net/",
#     mount_point = "/mnt/reaildb1",
#     extra_configs = {
#         "fs.azure.account.key.misgauravstorageaccount1.blob.core.windows.net": 
#         "Z8utZGWZeIYvwdTIo2V3hgy/Jw5Vm5BInopKRjti8MpyWdyCPWIttv9E5TXLp6dLR/NT4h+2x1GH+AStUnha1w=="
#     }
# )

# the data is in azure storage account (blob storage, but we can access the data here as well)
# df = spark.read.csv("dbfs:/mnt/retaildb1/orders.csv", header= True)
# df.show(5)
# -------+--------------------+-----------------+---------------+
# |order_id|          order_date|order_customer_id|   order_status|
# +--------+--------------------+-----------------+---------------+
# |       1|2013-07-25 00:00:...|            11599|         CLOSED|
# |       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|
# |       3|2013-07-25 00:00:...|            12111|       COMPLETE|
# |       4|2013-07-25 00:00:...|             8827|         CLOSED|
# |       5|2013-07-25 00:00:...|            11318|       COMPLETE|
# +--------+--------------------+-----------------+---------------+



# dbutils.fs.mount(
#     source = "wasbs://misgauravcontainer@misgauravstorageaccount1.blob.core.windows.net/",
#     mount_point = "/mnt/reaildb",
#     extra_configs = {
#         "fs.azure.account.key.misgauravstorageaccount1.blob.core.windows.net": dbutils.secrets.get(
#             scope = "databricks-demo-scope1",
#             key = "misgaurav-secret-scope"
#         )
#     }
# )
# the data is in azure storage account (blob storage, but we can access the data here as well)
# df = spark.read.csv("dbfs:/mnt/retaildb/orders.csv", header= True)
# df.show(5)
# -------+--------------------+-----------------+---------------+
# |order_id|          order_date|order_customer_id|   order_status|
# +--------+--------------------+-----------------+---------------+
# |       1|2013-07-25 00:00:...|            11599|         CLOSED|
# |       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|
# |       3|2013-07-25 00:00:...|            12111|       COMPLETE|
# |       4|2013-07-25 00:00:...|             8827|         CLOSED|
# |       5|2013-07-25 00:00:...|            11318|       COMPLETE|
# +--------+--------------------+-----------------+---------------+



# scope -> the name which we have given by using databricks UI (#secrets/createScope)
# key -> we created this key under Azure Key Vault, under secrets (objects), we pasted the values of key from access keys at conatiner level. 

# spark.read.csv("/mnt/reaildb1/orders.csv", header = True).head(5)

# spark.read.csv("/mnt/reaildb/orders.csv", header = True).head(5)

