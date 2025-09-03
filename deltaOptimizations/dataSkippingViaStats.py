%sql
select min(Fare_Amt), max(Fare_Amt) from trip_db.trips_delta; -- tooks .72 seconds 

# -- It has to read only metadata of files while getting this result, it doesn't need to open any files, check spark UI Job -> associated SQL Query -> expnad

select min(Fare_Amt), max(Fare_Amt) from trip_db.trips_parquet; -- tooks 2 seconds 
# -- It has to red all 20 files while getting this result, check spark UI Job -> associated SQL Query -> expnad

%sql
select COUNT(*) from trip_db.trips_parquet; -- tooks .76 seconds 
# -- It has to red all 20 files while getting this result, check spark UI Job -> associated SQL Query -> expnad

select COUNT(*) from trip_db.trips_delta; -- took .67 s
# -- It has to read only metadata of files while getting this result, it doesn't need to open any files, check spark UI Job -> associated SQL Query -> expnad

%sql
select * from trip_db.trips_delta
where Total_Amt = 234;
# -- It has to read only 1 files while getting this result, it doesn't need to open remaining 19 files, check spark UI Job -> associated SQL Query -> expnad
# -- files pruned	19
# -- files read	1

select * from trip_db.trips_parquet
where Total_Amt = 234;
# -- It has to red all 20 files while getting this result, check spark UI Job -> associated SQL Query -> expnad
# -- files read	20