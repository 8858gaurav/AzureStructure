# this conatiner: simple-ext-table is not sysnced with Unity Catalog Volume
# Create an external location on catalog explorer to read the input file from adls path
# Create an external location on catalog explorer to write the output file in adls path

input_path = "abfss://simple-ext-table@misgauravstorageaccount.dfs.core.windows.net/input/orders_large.csv"
df = spark.read.format("csv").option("header", "true").load(input_path)

output_external_path = "abfss://simple-ext-table@misgauravstorageaccount.dfs.core.windows.net/output_csv/"

df.write.format("csv").mode("overwrite").save(output_external_path)


gauravmishra@Gauravs-MacBook-Air ~ % az storage blob list --container-name simple-ext-table --account-name misgauravstorageaccount --auth-mode login --output table
Name                                                                                               Blob Type    Blob Tier    Length    Content Type              Last Modified              Snapshot
-------------------------------------------------------------------------------------------------  -----------  -----------  --------  ------------------------  -------------------------  ----------
input                                                                                              BlockBlob    Hot                    application/octet-stream  2026-05-07T12:33:50+00:00
input/orders_large.csv                                                                             BlockBlob    Hot          29614     text/csv                  2026-05-07T12:44:07+00:00
input/orders_small.csv                                                                             BlockBlob    Hot          102       text/csv                  2026-05-07T13:27:08+00:00
output                                                                                             BlockBlob    Hot                    application/octet-stream  2026-05-07T12:33:56+00:00
output/_delta_log                                                                                  BlockBlob    Hot                                              2026-05-07T12:44:15+00:00
output/_delta_log/00000000000000000000.crc                                                         BlockBlob    Hot          3125      application/octet-stream  2026-05-07T12:44:17+00:00
output/_delta_log/00000000000000000000.json                                                        BlockBlob    Hot          2196      application/octet-stream  2026-05-07T12:44:16+00:00
output/_delta_log/__tmp_path_dir                                                                   BlockBlob    Hot                                              2026-05-07T12:44:16+00:00
output/_delta_log/_staged_commits                                                                  BlockBlob    Hot                    application/octet-stream  2026-05-07T12:44:15+00:00
output/orders_small.csv                                                                            BlockBlob    Hot          102       text/csv                  2026-05-07T13:28:54+00:00
output/part-00000-75e7e63b-5a44-43b3-a2d6-46965f1ebaa4.c000.snappy.parquet                         BlockBlob    Hot          12524     application/octet-stream  2026-05-07T12:44:16+00:00
output_csv                                                                                         BlockBlob    Hot                    application/octet-stream  2026-05-07T13:52:17+00:00
output_csv/_SUCCESS                                                                                BlockBlob    Hot                    application/octet-stream  2026-05-07T13:56:35+00:00
output_csv/_committed_8365973282870847654                                                          BlockBlob    Hot          113       application/octet-stream  2026-05-07T13:56:35+00:00
output_csv/_started_8365973282870847654                                                            BlockBlob    Hot                    application/octet-stream  2026-05-07T13:56:35+00:00
output_csv/part-00000-tid-8365973282870847654-7c5e1321-91ba-493d-b753-78d99005228c-225-1-c000.csv  BlockBlob    Hot          28568     application/octet-stream  2026-05-07T13:56:35+00:00

%sql
CREATE TABLE IF NOT EXISTS misgauravcatalog.retaildb.orders_external_csv
USING CSV
OPTIONS (
  header "true",
  inferSchema "true"
)
LOCATION "abfss://simple-ext-table@misgauravstorageaccount.dfs.core.windows.net/output_csv/";


%sql
select * from misgauravcatalog.retaildb.orders_external_csv limit 2;
-- order_id,order_date,customer_id,order_status
-- 1,2020-08-27,1551,SHIPPED
-- 2,2020-01-28,1203,SHIPPED


############################## now place the file manually, inside the output_csv path ################################

gauravmishra@Gauravs-MacBook-Air ~ % az storage blob upload --account-name misgauravstorageaccount --container-name simple-ext-table --name "output_csv/orders_small.csv" --file "/Users/gauravmishra/Desktop/orders_small.csv" --auth-mode login
Finished[#############################################################]  100.0000%
{
  "client_request_id": "7788bc6a-4a1e-11f1-a5be-52fb618c8c4d",
  "content_md5": "LMGVcQJ+1H6BP69U+TSSAQ==",
  "date": "2026-05-07T14:10:13+00:00",
  "encryption_key_sha256": null,
  "encryption_scope": null,
  "etag": "\"0x8DEAC425BE2936C\"",
  "lastModified": "2026-05-07T14:10:14+00:00",
  "request_id": "f7dca615-901e-0024-332b-de6cdf000000",
  "request_server_encrypted": true,
  "structured_body": null,
  "version": "2026-04-06",
  "version_id": null
}


gauravmishra@Gauravs-MacBook-Air ~ % az storage blob list --container-name simple-ext-table --account-name misgauravstorageaccount --auth-mode login --output table
Name                                                                                               Blob Type    Blob Tier    Length    Content Type              Last Modified              Snapshot
-------------------------------------------------------------------------------------------------  -----------  -----------  --------  ------------------------  -------------------------  ----------
input                                                                                              BlockBlob    Hot                    application/octet-stream  2026-05-07T12:33:50+00:00
input/orders_large.csv                                                                             BlockBlob    Hot          29614     text/csv                  2026-05-07T12:44:07+00:00
input/orders_small.csv                                                                             BlockBlob    Hot          102       text/csv                  2026-05-07T13:27:08+00:00
output                                                                                             BlockBlob    Hot                    application/octet-stream  2026-05-07T12:33:56+00:00
output/_delta_log                                                                                  BlockBlob    Hot                                              2026-05-07T12:44:15+00:00
output/_delta_log/00000000000000000000.crc                                                         BlockBlob    Hot          3125      application/octet-stream  2026-05-07T12:44:17+00:00
output/_delta_log/00000000000000000000.json                                                        BlockBlob    Hot          2196      application/octet-stream  2026-05-07T12:44:16+00:00
output/_delta_log/__tmp_path_dir                                                                   BlockBlob    Hot                                              2026-05-07T12:44:16+00:00
output/_delta_log/_staged_commits                                                                  BlockBlob    Hot                    application/octet-stream  2026-05-07T12:44:15+00:00
output/orders_small.csv                                                                            BlockBlob    Hot          102       text/csv                  2026-05-07T13:28:54+00:00
output/part-00000-75e7e63b-5a44-43b3-a2d6-46965f1ebaa4.c000.snappy.parquet                         BlockBlob    Hot          12524     application/octet-stream  2026-05-07T12:44:16+00:00
output_csv                                                                                         BlockBlob    Hot                    application/octet-stream  2026-05-07T13:52:17+00:00
output_csv/_SUCCESS                                                                                BlockBlob    Hot                    application/octet-stream  2026-05-07T14:03:06+00:00
output_csv/_committed_4790749035172336934                                                          BlockBlob    Hot          113       application/octet-stream  2026-05-07T14:03:06+00:00
output_csv/_started_4790749035172336934                                                            BlockBlob    Hot                    application/octet-stream  2026-05-07T14:03:06+00:00
output_csv/orders_small.csv                                                                        BlockBlob    Hot          102       text/csv                  2026-05-07T14:10:14+00:00
output_csv/part-00000-tid-4790749035172336934-4204d842-03a4-43e3-a61d-cf3e4767bf1d-230-1-c000.csv  BlockBlob    Hot          28613     application/octet-stream  2026-05-07T14:03:06+00:00

gauravmishra@Gauravs-MacBook-Air ~ % cat /Users/gauravmishra/Desktop/orders_small.csv
order_id,order_date,customer_id,order_status
1111,2025-01-01,0000,SHIPPED
2222,2025-01-01,0000,PENDING

%sql
select * from misgauravcatalog.retaildb.orders_external_csv where order_id='1111';
# returns nothing

# refresh commands only works for personal cluster, not for serverless compute
%sql
REFRESH TABLE misgauravcatalog.retaildb.orders_external_csv;

%sql
select * from misgauravcatalog.retaildb.orders_external_csv where order_id='1111';
-- order_id,order_date,customer_id,order_status
-- 1111,2025-01-01,0,SHIPPED

gauravmishra@Gauravs-MacBook-Air ~ % az storage blob list --container-name simple-ext-table --account-name misgauravstorageaccount --auth-mode login --output table
Name                                                                                               Blob Type    Blob Tier    Length    Content Type              Last Modified              Snapshot
-------------------------------------------------------------------------------------------------  -----------  -----------  --------  ------------------------  -------------------------  ----------
input                                                                                              BlockBlob    Hot                    application/octet-stream  2026-05-07T12:33:50+00:00
input/orders_large.csv                                                                             BlockBlob    Hot          29614     text/csv                  2026-05-07T12:44:07+00:00
input/orders_small.csv                                                                             BlockBlob    Hot          102       text/csv                  2026-05-07T13:27:08+00:00
output                                                                                             BlockBlob    Hot                    application/octet-stream  2026-05-07T12:33:56+00:00
output/_delta_log                                                                                  BlockBlob    Hot                                              2026-05-07T12:44:15+00:00
output/_delta_log/00000000000000000000.crc                                                         BlockBlob    Hot          3125      application/octet-stream  2026-05-07T12:44:17+00:00
output/_delta_log/00000000000000000000.json                                                        BlockBlob    Hot          2196      application/octet-stream  2026-05-07T12:44:16+00:00
output/_delta_log/__tmp_path_dir                                                                   BlockBlob    Hot                                              2026-05-07T12:44:16+00:00
output/_delta_log/_staged_commits                                                                  BlockBlob    Hot                    application/octet-stream  2026-05-07T12:44:15+00:00
output/orders_small.csv                                                                            BlockBlob    Hot          102       text/csv                  2026-05-07T13:28:54+00:00
output/part-00000-75e7e63b-5a44-43b3-a2d6-46965f1ebaa4.c000.snappy.parquet                         BlockBlob    Hot          12524     application/octet-stream  2026-05-07T12:44:16+00:00
output_csv                                                                                         BlockBlob    Hot                    application/octet-stream  2026-05-07T13:52:17+00:00
output_csv/_SUCCESS                                                                                BlockBlob    Hot                    application/octet-stream  2026-05-07T14:03:06+00:00
output_csv/_committed_4790749035172336934                                                          BlockBlob    Hot          113       application/octet-stream  2026-05-07T14:03:06+00:00
output_csv/_started_4790749035172336934                                                            BlockBlob    Hot                    application/octet-stream  2026-05-07T14:03:06+00:00
output_csv/orders_small.csv                                                                        BlockBlob    Hot          102       text/csv                  2026-05-07T14:10:14+00:00
output_csv/part-00000-tid-4790749035172336934-4204d842-03a4-43e3-a61d-cf3e4767bf1d-230-1-c000.csv  BlockBlob    Hot          28613     application/octet-stream  2026-05-07T14:03:06+00:00


############################## now run the insert commands into external table  ################################

%sql
INSERT INTO misgauravcatalog.retaildb.orders_external_csv 
VALUES (3333, DATE '2025-01-01', 0, 'SHIPPED');

gauravmishra@Gauravs-MacBook-Air ~ % az storage blob list --container-name simple-ext-table --account-name misgauravstorageaccount --auth-mode login --output table
Name                                                                                               Blob Type    Blob Tier    Length    Content Type              Last Modified              Snapshot
-------------------------------------------------------------------------------------------------  -----------  -----------  --------  ------------------------  -------------------------  ----------
input                                                                                              BlockBlob    Hot                    application/octet-stream  2026-05-07T12:33:50+00:00
input/orders_large.csv                                                                             BlockBlob    Hot          29614     text/csv                  2026-05-07T12:44:07+00:00
input/orders_small.csv                                                                             BlockBlob    Hot          102       text/csv                  2026-05-07T13:27:08+00:00
output                                                                                             BlockBlob    Hot                    application/octet-stream  2026-05-07T12:33:56+00:00
output/_delta_log                                                                                  BlockBlob    Hot                                              2026-05-07T12:44:15+00:00
output/_delta_log/00000000000000000000.crc                                                         BlockBlob    Hot          3125      application/octet-stream  2026-05-07T12:44:17+00:00
output/_delta_log/00000000000000000000.json                                                        BlockBlob    Hot          2196      application/octet-stream  2026-05-07T12:44:16+00:00
output/_delta_log/__tmp_path_dir                                                                   BlockBlob    Hot                                              2026-05-07T12:44:16+00:00
output/_delta_log/_staged_commits                                                                  BlockBlob    Hot                    application/octet-stream  2026-05-07T12:44:15+00:00
output/orders_small.csv                                                                            BlockBlob    Hot          102       text/csv                  2026-05-07T13:28:54+00:00
output/part-00000-75e7e63b-5a44-43b3-a2d6-46965f1ebaa4.c000.snappy.parquet                         BlockBlob    Hot          12524     application/octet-stream  2026-05-07T12:44:16+00:00
output_csv                                                                                         BlockBlob    Hot                    application/octet-stream  2026-05-07T13:52:17+00:00
output_csv/_SUCCESS                                                                                BlockBlob    Hot                    application/octet-stream  2026-05-07T14:52:45+00:00
output_csv/_committed_2573423067284639534                                                          BlockBlob    Hot          111       application/octet-stream  2026-05-07T14:52:45+00:00
output_csv/_committed_4790749035172336934                                                          BlockBlob    Hot          113       application/octet-stream  2026-05-07T14:03:06+00:00
output_csv/_committed_vacuum6375415154405007968                                                    BlockBlob    Hot          96        application/octet-stream  2026-05-07T14:52:46+00:00
output_csv/_started_2573423067284639534                                                            BlockBlob    Hot                    application/octet-stream  2026-05-07T14:52:45+00:00
output_csv/orders_small.csv                                                                        BlockBlob    Hot          102       text/csv                  2026-05-07T14:10:14+00:00
output_csv/part-00000-tid-2573423067284639534-55317b3d-ccfb-4be8-8d2c-1d96d9cf100b-3-1-c000.csv    BlockBlob    Hot          71        application/octet-stream  2026-05-07T14:52:45+00:00
output_csv/part-00000-tid-4790749035172336934-4204d842-03a4-43e3-a61d-cf3e4767bf1d-230-1-c000.csv  BlockBlob    Hot          28613     application/octet-stream  2026-05-07T14:03:06+00:00

%sql
select * from misgauravcatalog.retaildb.orders_external_csv where order_id='3333';
-- order_id,order_date,customer_id,order_status
-- 3333,2025-01-01,0,SHIPPED;


%sql
INSERT INTO misgauravcatalog.retaildb.orders_external_csv 
VALUES (4444, DATE '2025-01-01', 0, 'SHIPPED'), (5555, DATE '2025-01-01', 0, 'SHIPPED'), (6666, DATE '2025-01-01', 0, 'SHIPPED');

gauravmishra@Gauravs-MacBook-Air ~ % az storage blob list --container-name simple-ext-table --account-name misgauravstorageaccount --auth-mode login --output table
Name                                                                                               Blob Type    Blob Tier    Length    Content Type              Last Modified              Snapshot
-------------------------------------------------------------------------------------------------  -----------  -----------  --------  ------------------------  -------------------------  ----------
input                                                                                              BlockBlob    Hot                    application/octet-stream  2026-05-07T12:33:50+00:00
input/orders_large.csv                                                                             BlockBlob    Hot          29614     text/csv                  2026-05-07T12:44:07+00:00
input/orders_small.csv                                                                             BlockBlob    Hot          102       text/csv                  2026-05-07T13:27:08+00:00
output                                                                                             BlockBlob    Hot                    application/octet-stream  2026-05-07T12:33:56+00:00
output/_delta_log                                                                                  BlockBlob    Hot                                              2026-05-07T12:44:15+00:00
output/_delta_log/00000000000000000000.crc                                                         BlockBlob    Hot          3125      application/octet-stream  2026-05-07T12:44:17+00:00
output/_delta_log/00000000000000000000.json                                                        BlockBlob    Hot          2196      application/octet-stream  2026-05-07T12:44:16+00:00
output/_delta_log/__tmp_path_dir                                                                   BlockBlob    Hot                                              2026-05-07T12:44:16+00:00
output/_delta_log/_staged_commits                                                                  BlockBlob    Hot                    application/octet-stream  2026-05-07T12:44:15+00:00
output/orders_small.csv                                                                            BlockBlob    Hot          102       text/csv                  2026-05-07T13:28:54+00:00
output/part-00000-75e7e63b-5a44-43b3-a2d6-46965f1ebaa4.c000.snappy.parquet                         BlockBlob    Hot          12524     application/octet-stream  2026-05-07T12:44:16+00:00
output_csv                                                                                         BlockBlob    Hot                    application/octet-stream  2026-05-07T13:52:17+00:00
output_csv/_SUCCESS                                                                                BlockBlob    Hot                    application/octet-stream  2026-05-07T14:56:44+00:00
output_csv/_committed_2451451923798307268                                                          BlockBlob    Hot          285       application/octet-stream  2026-05-07T14:56:44+00:00
output_csv/_committed_2573423067284639534                                                          BlockBlob    Hot          111       application/octet-stream  2026-05-07T14:52:45+00:00
output_csv/_committed_4790749035172336934                                                          BlockBlob    Hot          113       application/octet-stream  2026-05-07T14:03:06+00:00
output_csv/_committed_vacuum6375415154405007968                                                    BlockBlob    Hot          96        application/octet-stream  2026-05-07T14:52:46+00:00
output_csv/_started_2451451923798307268                                                            BlockBlob    Hot                    application/octet-stream  2026-05-07T14:56:44+00:00
output_csv/_started_2573423067284639534                                                            BlockBlob    Hot                    application/octet-stream  2026-05-07T14:52:45+00:00
output_csv/orders_small.csv                                                                        BlockBlob    Hot          102       text/csv                  2026-05-07T14:10:14+00:00
output_csv/part-00000-tid-2451451923798307268-85996b04-1bc8-4529-aa5d-2b329083a19b-7-1-c000.csv    BlockBlob    Hot          71        application/octet-stream  2026-05-07T14:56:44+00:00
output_csv/part-00000-tid-2573423067284639534-55317b3d-ccfb-4be8-8d2c-1d96d9cf100b-3-1-c000.csv    BlockBlob    Hot          71        application/octet-stream  2026-05-07T14:52:45+00:00
output_csv/part-00000-tid-4790749035172336934-4204d842-03a4-43e3-a61d-cf3e4767bf1d-230-1-c000.csv  BlockBlob    Hot          28613     application/octet-stream  2026-05-07T14:03:06+00:00
output_csv/part-00001-tid-2451451923798307268-85996b04-1bc8-4529-aa5d-2b329083a19b-8-1-c000.csv    BlockBlob    Hot          71        application/octet-stream  2026-05-07T14:56:44+00:00
output_csv/part-00002-tid-2451451923798307268-85996b04-1bc8-4529-aa5d-2b329083a19b-9-1-c000.csv    BlockBlob    Hot          71        application/octet-stream  2026-05-07T14:56:44+00:00

# it'll create 3 files here 
# file1: /output_csv/part-00000-tid-2451451923798307268-85996b04-1bc8-4529-aa5d-2b329083a19b-7-1-c000.csv
# file2: /output_csv/part-00001-tid-2451451923798307268-85996b04-1bc8-4529-aa5d-2b329083a19b-8-1-c000.csv
# file3: /output_csv/part-00002-tid-2451451923798307268-85996b04-1bc8-4529-aa5d-2b329083a19b-9-1-c000.csv



