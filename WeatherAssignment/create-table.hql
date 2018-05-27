--show all database present in hive
show databases;

--create database
create database if not exists weather;

--verify database created successfully 
show databases;

--create external weatherraw table if not exists

create  table if not exists weather.weatherraw
(
station_identifier varchar(11),
observation_date varchar(100),
observation_type varchar(4),
observation_value string,
measurement_flag varchar(1),
quality_flag varchar(1),
source_flag varchar(1),
other string
)COMMENT 'Data contains information about Weather  Observation'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/weatherdata/';

--verify table created successfully 
show tables;

--describe table 
describe weather.weatherraw;

--verify external table populated with correct datab
select * from weather.weatherraw limit 10;

--verify count of table(Total count should be 34578898)
select count(*) from weather.weatherraw;

--create weather.WeatherCurated table which will stored spark processing result

create  table if not exists weather.WeatherCurated
(
station_identifier varchar(11),
observation_date date,
Precipitation  float,
MaxTemparature  float,
MinTemparature  float,
Snowfall  float,
SnowDepth  float,
Evaporation  float,
WaterEquivalentSnowDepth  float,
WaterEquivalentSnowFall float,
Sunshine  float
)COMMENT 'Data contains weather curated result generated from spark'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/tmp/weathercurated_result/';


--verify table created successfully 
show tables;


--describe table 
describe weather.weathercurated;

--verify external table populated with correct datab
select * from weather.weathercurated limit 10;

--verify count of table
select count(*) from weather.weathercurated;




