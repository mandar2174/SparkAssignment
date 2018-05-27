
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import from_unixtime, when
from pyspark.sql.functions import udf
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.types import StringType
import sys
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType

################################################################
#Prerequisite: 
#Change the thrift server url based on hdp cluster
#How to run program:
#spark-submit --master yarn --deploy-mode client spark-job.py
##############################################################


#set hive metastore properties to access hive table
SparkContext.setSystemProperty("hive.metastore.uris", "thrift://ip-172-31-42-247.ec2.internal:9083")


#create spark session 
sparkSession = (SparkSession
                .builder
                .appName('weather-spark-processing')
                .enableHiveSupport()
                .getOrCreate())

if __name__ == '__main__':

	print "started spark job executing"
	
	#created dataframe for Precipitation:PRCP catgory
	print "Started Precipitation processing"
	prcp_result_df = sparkSession.sql("select station_identifier,observation_date,'Precipitation' as weather_category,cast((sum(observation_value)/10) as float) as calculation_result  from weather.weatherraw where observation_type='PRCP' group by station_identifier,observation_date")
	#prcp_result_df.show()
	#prcp_result_df.printSchema()
	print "End Precipitation processing"
	
	#created dataframe for MaxTemparature:TMAX  catgory
	print "Started MaxTemparature processing"
	max_temperature_df = sparkSession.sql("select station_identifier,observation_date,'MaxTemparature' as weather_category,cast((sum(observation_value)/10) as float) as calculation_result  from weather.weatherraw where observation_type='TMAX' group by station_identifier,observation_date")
	#max_temperature_df.show()
	#max_temperature_df.printSchema()
	print "End MaxTemparature processing"
	

	#created dataframe for MinTemparature:TMIN catgory
	print "Started MinTemparature processing"
	min_temperature_df = sparkSession.sql("select station_identifier,observation_date,'MinTemparature' as weather_category,cast((sum(observation_value)/10) as float) as calculation_result  from weather.weatherraw where observation_type='TMIN' group by station_identifier,observation_date")
	#max_temperature_df.show()
	#max_temperature_df.printSchema()
	print "End MinTemparature processing"

	#created dataframe for Snowfall:SNOW catgory
	print "Started Snowfall processing"
	snowfall_df = sparkSession.sql("select station_identifier,observation_date,'Snowfall' as weather_category,cast((sum(observation_value)/10) as float) as calculation_result  from weather.weatherraw where observation_type='SNOW' group by station_identifier,observation_date")
	#snowfall_df.show()
	#snowfall_df.printSchema()
	print "End Snowfall processing"

	#created dataframe for SnowDepth:SNWD catgory
	print "Started SnowDepth processing"
	snowdepth_df = sparkSession.sql("select station_identifier,observation_date,'SnowDepth' as weather_category,cast((sum(observation_value)/10) as float) as calculation_result  from weather.weatherraw where observation_type='SNWD' group by station_identifier,observation_date")
	#snowdepth_df.show()
	#snowdepth_df.printSchema()
	print "End SnowDepth processing"

	#created dataframe for Evaporation:EVAP catgory
	print "Started Evaporation processing"
	evoporation_df=sparkSession.sql("select station_identifier,observation_date,'Evaporation' as weather_category,cast((sum(observation_value)/10) as float) as calculation_result  from weather.weatherraw where observation_type='EVAP' group by station_identifier,observation_date")
	#evoporation_df.show()
	#evoporation_df.printSchema()
	print "End Evaporation processing"

	#created dataframe for WaterEquivalentSnowDepth :WESD catgory
	print "Started WaterEquivalentSnowDepth processing"
	water_equivalent_snow_depth_df = sparkSession.sql("select station_identifier,observation_date,'WaterEquivalentSnowDepth' as weather_category,cast((sum(observation_value)/10) as float) as calculation_result  from weather.weatherraw where observation_type='WESD' group by station_identifier,observation_date")
	#water_equivalent_snow_depth_df.show()
	#water_equivalent_snow_depth_df.printSchema()
	print "End WaterEquivalentSnowDepth processing"


	#created dataframe for WaterEquivalentSnowFall :WESF catgory
	print "Started WaterEquivalentSnowFall processing"
	water_equivalent_snow_fall_df = sparkSession.sql("select station_identifier,observation_date,'WaterEquivalentSnowFall' as weather_category,cast((sum(observation_value)/10) as float) as calculation_result  from weather.weatherraw where observation_type='WESF' group by station_identifier,observation_date")
	#water_equivalent_snow_fall_df.show()
	#water_equivalent_snow_fall_df.printSchema()
	print "End WaterEquivalentSnowFall processing"

	#created dataframe for sunsine :PSUN catgory
	print "Started Sunsine processing"
	sunsine_df = sparkSession.sql("select station_identifier,observation_date,'Sunshine' as weather_category,cast((sum(observation_value)/10) as float) as calculation_result  from weather.weatherraw where observation_type='PSUN' group by station_identifier,observation_date")
	sunsine_df.printSchema()
	print "End Sunsine processing"
	
	#union all dataframe so that perform piovting on entire dataset
	print "Started union processing"
	union_result_df = prcp_result_df.union(max_temperature_df)
	union_result_df = union_result_df.union(min_temperature_df)
	union_result_df = union_result_df.union(snowfall_df)
	union_result_df = union_result_df.union(snowdepth_df)
	union_result_df = union_result_df.union(evoporation_df)
	union_result_df = union_result_df.union(water_equivalent_snow_depth_df)
	union_result_df = union_result_df.union(water_equivalent_snow_fall_df)
	union_result_df = union_result_df.union(sunsine_df)
	print "End union processing"


	#union_result_df.show()
	union_result_df.printSchema()
	
	#perform pivoting based on weather category to convert summarize rows into column result
	curation_result_df = union_result_df.groupBy(union_result_df.station_identifier,union_result_df.observation_date).pivot("weather_category").agg(round(sum(union_result_df.calculation_result),2)).sort(union_result_df.observation_date)
	
	#udf function to convert observation_date in proper format
	func =  udf (lambda x: datetime.strptime(x, '%Y%m%d'), DateType())
	
	curation_result_df = curation_result_df.withColumn("observation_date_format",func(col('observation_date')))
	curation_result_df.printSchema()
	
	#write final result in hdfs location
	print 'started writing weather curated data to hdfs location'
	curation_result_df.na.fill(0.0).select("station_identifier","observation_date_format","Precipitation","MaxTemparature","Snowfall","SnowDepth","Evaporation","WaterEquivalentSnowDepth","WaterEquivalentSnowFall","Sunshine").write.format("csv").save(path="hdfs:///tmp/weathercurated_result", mode='overwrite')
	print 'End processing to writing'
	
	
