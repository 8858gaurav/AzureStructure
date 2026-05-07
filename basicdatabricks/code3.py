# 1. Read the raw data from your container
# we hav not synced this conatiner databricks-tables with unity catalog Volume
# Volume in Unity catalog: Managed Volume, & External Volume, we can't create an ext table in databricks by using these two Volumes options. 
input_path = "abfss://databricks-tables@misgauravstorageaccount.dfs.core.windows.net/input_path/"
df = spark.read.format("csv").option("header", "true").load(input_path)

# 2. Define the new destination (The New Container)
# We will create a folder inside the new container for this specific table
output_external_path = "abfss://databricks-tables@misgauravstorageaccount.dfs.core.windows.net/output_path/"

# 3. Write the data as Delta format to the new container
# we hav not synced this conatiner databricks-tables with unity catalog Volume, i.e we are able to create an external table in databricks
df.write.format("delta").mode("overwrite").save(output_external_path)

%sql
CREATE TABLE IF NOT EXISTS misgauravcatalog.retaildb.orders_external_new
USING DELTA
LOCATION "abfss://databricks-tables@misgauravstorageaccount.dfs.core.windows.net/output_path/";

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
Table,orders_external_new,
Created Time,Thu May 07 02:52:42 UTC 2026,
Last Access,UNKNOWN,
Created By,Spark ,
Type,EXTERNAL,
Location,abfss://databricks-tables@misgauravstorageaccount.dfs.core.windows.net/output_path,
Provider,delta,
Owner,gauravmishra010@gmail.com,
Table Properties,"[delta.enableDeletionVectors=true,delta.feature.appendOnly=supported,delta.feature.deletionVectors=supported,delta.feature.invariants=supported,delta.minReaderVersion=3,delta.minWriterVersion=7]",
