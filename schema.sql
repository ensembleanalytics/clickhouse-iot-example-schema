
-- Raw sensor readings
create or replace table sensor_readings ( ts timestamp, sensorid integer, value integer ) engine = MergeTree() primary key ( ts, sensorid );

-- Latest sensor readings implemented by replacing merge tree
create or replace table sensor_readings_latest ( ts timestamp, sensorid integer, value integer ) engine = ReplacingMergeTree() primary key ( sensorid );

-- Latest sensor readings by argMax view over raw data

create or replace view sensor_readings_latest_raw as select argMax( sensorid, ts ), argMax( ts, ts ) from sensor_readings group by sensorid

-- Sensors which have breached historically
create or replace table sensor_breached ( ts timestamp, sensorid integer, value integer ) engine = MergeTree() primary key ( ts, sensorid );

-- Sensors which are currently breached
create or replace table sensor_currently_breached ( ts timestamp, cnt integer ) engine = SummingMergeTree() primary key ( ts );

-- Time series of breaches over time
create or replace table sensor_breached_by_time ( ts timestamp, cnt integer ) engine = SummingMergeTree() primary key ( ts );

-- TBC
drop view sensor_readings_latest_mv;
create materialized view sensor_readings_latest_mv
to sensor_readings_latest
as select * from sensor_readings;

-- TBC
drop view sensor_breached_mv;
create materialized view sensor_breached_mv
to sensor_breached
as select * from sensor_readings where value > 100;

-- TBC
drop view sensor_breached_by_time_mv;
create materialized view sensor_breached_by_time_mv
to sensor_breached_by_time
as select toStartOfMinute(ts) as ts, count(*) as cnt from sensor_breached group by ts;

-- Test Data
insert into sensor_readings values ( now(), 1, 97 );
insert into sensor_readings values ( now(), 1, 98 );
insert into sensor_readings values ( now(), 1, 99 );
insert into sensor_readings values ( now(), 1, 100 );
insert into sensor_readings values ( now(), 1, 101 );
insert into sensor_readings values ( now(), 1, 102 );

-- Test Data
insert into sensor_readings values ( now(), 2, 98 );
insert into sensor_readings values ( now(), 2, 99 );
insert into sensor_readings values ( now(), 2, 100 );
insert into sensor_readings values ( now(), 2, 101 );
insert into sensor_readings values ( now(), 2, 102 );
insert into sensor_readings values ( now(), 2, 103 );

-- Validation
select * from sensor_readings;
select * from sensor_readings_latest final;
select * from sensor_breached;
select * from sensor_currently_breached;
select * from sensor_breached_by_time final;
