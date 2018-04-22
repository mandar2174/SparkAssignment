'''
Created on 19-Apr-2018

@author: User
'''


'''Code to perform different analysis based on different cases'''

'''How to run code using spark-submit:

spark-submit  --packages com.databricks:spark-csv_2.10:1.4.0 /home/ec2-user/SparkAssignment/com/assignment/SparkAssignment.py /home/ec2-user/SparkAssignment/resources/aadhaar_data.csv

'''

import hashlib

from pyspark.sql.context import HiveContext
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import from_unixtime, when
from pyspark.sql.functions import udf
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.types import StringType
import sys
from pyspark.sql.functions import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from  pyspark.sql.functions import abs
from subprocess import call
import math
from collections import OrderedDict

from pyspark.sql.functions import rank, col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *
import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

'''function to rename column based on input data'''
def rename_dataframe_column_name(input_dataframe,column_list):
    
    column_count = 0
    #rename default column name based on input schema
    for column in column_list:
        input_dataframe = input_dataframe.withColumnRenamed('C'+str(column_count), column)
        column_count = column_count + 1
        
                        
    return input_dataframe
                        
    
    return input_dataframe

if __name__ == '__main__':
    
    print "Total number of argument pass to program : " + str(len(sys.argv))
    
    #Read input parameter from command line
    if len(sys.argv) == 2:
        
        print "Base path " + ROOT_DIR + os.sep
        
        file_name = sys.argv[1]
        print "performing analysis on file_name " + file_name 
        
        #Read input file using spark-csv and create spark dataframe
        
        conf = SparkConf().setAppName('Spark-Assignment').setMaster('local[*]')
    
        # create spark context and sql context 
        sc = SparkContext(conf=conf)
        hive_context = HiveContext(sc)
        
        # read the input data file and create spark dataframe using com.databricks.spark.csv library
        input_dataframe = hive_context.read.format("com.databricks.spark.csv")\
        .option("header", "false") \
        .option("inferschema", "true") \
        .option("delimiter", ",") \
        .option("mode", "DROPMALFORMED") \
        .load("file://" + file_name)
    
        #dataframe schema based on data
        column_list = ["date_value","date","register","private_agency","state","district","sub_district","pincode","gender","age","aadhaar_generated","rejected","mobile_number","email_id"]      
               
        #change the dataframe column name
        data_frame_after_colun_name = rename_dataframe_column_name(input_dataframe,column_list)
                  
        #Checkpoint-1
        #View/result of the top 25 rows from each individual store (Spark DataFrame)
        window = Window.partitionBy(data_frame_after_colun_name['state']).orderBy(data_frame_after_colun_name['aadhaar_generated'].desc())
        top_25_record = data_frame_after_colun_name.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 25) 
        top_25_record.repartition(1).write.format("csv").save(path="file://" + ROOT_DIR + os.sep + "top_20_result", mode='overwrite') 
            
        #Checkpoint-2
        #Describe the schema
        data_frame_after_colun_name.printSchema()
        
        #Find the count and names of registers in the table
        
        #register dataframe as temp table for performing sql DSL Query 
        data_frame_after_colun_name.registerTempTable("temp_table")
         
        distinct_registrars_count_query = "select count(*) as register_count from (select distinct(register) from temp_table)t"
        distinct_registrars_count_df = hive_context.sql(distinct_registrars_count_query)
        distinct_registrars_count_df.show()
         
        distinct_registrars_names_query = "select  distinct(register) as register_names from temp_table"
        distinct_registrars_names_df = hive_context.sql(distinct_registrars_names_query)
        distinct_registrars_names_df.show()
        
        #Find the number of states, districts in each state and sub-districts in each district.
        state_wise_query = "select state,district,sub_district from temp_table group by state,district,sub_district"
        state_wise_query_df = hive_context.sql(state_wise_query)
       
        
        state_count_query = "select count(*) as state_total_count  from (select distinct(state) from temp_table)t"
        state_count_df = hive_context.sql(state_count_query)
        state_count_df.show()
        
        state_district_count_query = "select *  from (select state,count(district) as district_count from temp_table group by state)t"
        state_district_count_df = hive_context.sql(state_district_count_query)
        state_district_count_df.show()
        
        district_sub_district_count_query = "select *  from (select district,count(sub_district) as sub_district_count from temp_table group by district)t"
        district_sub_district_count_df = hive_context.sql(district_sub_district_count_query)
        district_sub_district_count_df.show()
        
        #Find out the names of private agencies for each state
        private_agencies_in_state_query = "select state,private_agency as private_agencies_name from temp_table group by state,private_agency"
        private_agencies_in_state_df = hive_context.sql(private_agencies_in_state_query)
        private_agencies_in_state_df.show()


        #Checkpoint-3
        # 1. Find top 3 states generating most number of Aadhaar cards?
        states_aadhaar_card_count_query =  "select state,sum(aadhaar_generated) as aadhaar_count from temp_table group by state"
        states_aadhaar_card_count_df = hive_context.sql(states_aadhaar_card_count_query)
                 
        top3_state_rank_function = Window.orderBy(col("aadhaar_count").desc())
        top_3_states_aadhaar_card_count_df = states_aadhaar_card_count_df.select('*', dense_rank().over(top3_state_rank_function).alias('rank')).filter(col('rank') <= 3)
        top_3_states_aadhaar_card_count_df.show()
        
        # 2. Find top 3 districts where enrolment numbers are maximum?
        districts_aadhaar_card_count_query = "select district,sum(aadhaar_generated) as aadhaar_count from temp_table group by district "
        districts_aadhaar_card_count_df = hive_context.sql(districts_aadhaar_card_count_query)
        top3_district_rank_function = Window.orderBy(col("aadhaar_count").desc())
          
        top_3_districts_aadhaar_card_count_df = districts_aadhaar_card_count_df.select('*', dense_rank().over(top3_district_rank_function).alias('rank')).filter(col('rank') <= 3)
        top_3_districts_aadhaar_card_count_df.show()
        
        
        # 3. Find the no. of Aadhaar cards generated in each state?
        states_aadhaar_card_count_df.show()
        
        # Checkpoint-4
        # 1. Find the number of unique pincodes in the data
        unique_pincode_query =  "select count(*) as unique_pincode from (select distinct(pincode) from temp_table)t"
        unique_pincode_query_df = hive_context.sql(unique_pincode_query)
        unique_pincode_query_df.show()
        
        # 2. Find the number of Aadhaar registrations rejected in Uttar Pradesh and Maharashtra
        rejected_Aadhaar_number_query = "select sum(rejected) as aadhaar_rejected_count from temp_table where state in ('Maharashtra', 'Uttar Pradesh')"
        rejected_Aadhaar_number_df = hive_context.sql(rejected_Aadhaar_number_query)
        rejected_Aadhaar_number_df.show()

        #Check point-5
        # 1. Find the top 3 states where the percentage of Aadhaar cards being generated for
        # males is the highest.(Completed)
        male_female_aadhaar_card_statewise_query = "select state,gender,sum(aadhaar_generated) as generated_aadhaar_card_count,sum(rejected) rejected_aadhaar_count from temp_table group by state,gender"
        male_female_aadhaar_card_statewise_df = hive_context.sql(male_female_aadhaar_card_statewise_query)

        aadhaar_function = Window.partitionBy("state").orderBy("gender")
        male_female_aadhaar_card_statewise_df = male_female_aadhaar_card_statewise_df.withColumn("total_female_aadhaar_register", lag(col("generated_aadhaar_card_count"),1,None).over(aadhaar_function))
        
         
        #register dataframe as temp table for performing sql DSL Query 
        male_female_aadhaar_card_statewise_df.registerTempTable("aadhaar_temp_table")
        state_wise_male_aadhaar_card_query = "select state,gender,max(generated_aadhaar_card_count) as aadhaar_count from aadhaar_temp_table where gender='M' and  generated_aadhaar_card_count > total_female_aadhaar_register group by state,gender"
        state_wise_male_aadhaar_card_df = hive_context.sql(state_wise_male_aadhaar_card_query)
       
        #get top 3 state where male aadhaar card % high
        top_3_state_male_card_count_df = state_wise_male_aadhaar_card_df.select('*', dense_rank().over(Window.orderBy(col("aadhaar_count").desc())).alias('rank')).filter(col('rank') <= 3)
        top_3_state_male_card_count_df.show()
        
        # 2. Find in each of these 3 states, identify the top 3 districts where the percentage of
        # Aadhaar cards being rejected for females is the highest.(Completed)
        
        female_aadhaar_card_statewise_query = "select state,district,gender,sum(aadhaar_generated) as generated_aadhaar_card_count,sum(rejected) as rejected_aadhaar_count from temp_table group by state,district,gender"
        female_aadhaar_card_statewise_df = hive_context.sql(female_aadhaar_card_statewise_query)
         
        aadhaar_function = Window.partitionBy("state","district").orderBy("gender")
        female_aadhaar_card_statewise_df = female_aadhaar_card_statewise_df.withColumn("total_female_aadhaar_rejected_register", lead(col("rejected_aadhaar_count"),1,None).over(aadhaar_function))
         
        #register dataframe as temp table for performing sql DSL Query 
        female_aadhaar_card_statewise_df.registerTempTable("aadhaar_temp_table")
        state_wise_female_aadhaar_card_query = "select state,district,gender,max(rejected_aadhaar_count) as aadhaar_rejected_count from aadhaar_temp_table where gender='F' and  rejected_aadhaar_count > total_female_aadhaar_rejected_register group by state,district,gender"
        state_wise_female_aadhaar_card_df = hive_context.sql(state_wise_female_aadhaar_card_query)
         
        top_3_state_female_rejected_card_count_df = state_wise_female_aadhaar_card_df.select('*', dense_rank().over(Window.partitionBy("state").orderBy(col("aadhaar_rejected_count").desc())).alias('rank')).filter(col('rank') <= 3)
        top_3_state_female_rejected_card_count_df.repartition(1).write.format("csv").save(path="file://" + ROOT_DIR + os.sep + "female_rejected_result", mode='overwrite')
        
        # 3. Find the summary of the acceptance percentage of all the Aadhaar cards applications
        # by bucketing the age group into 10 buckets
        #Note : currently bucketby is supported by spark in only 2.3.0 so could not able to perform bucket on age
        #data_frame_after_colun_name.write.format('csv').bucketBy(10, "age").mode("overwrite").saveAsTable("age_bucket_temp_table")
        data_frame_after_colun_name.registerTempTable("age_bucket_temp_table")
        state_wise_female_aadhaar_card_query = "select state,sum(aadhaar_generated) as aadhaar_genderated_count,sum(rejected) as aadhaar_rejected_count from age_bucket_temp_table group by state "
        state_wise_female_aadhaar_card_df = hive_context.sql(state_wise_female_aadhaar_card_query)
         
        state_wise_female_aadhaar_card_df = state_wise_female_aadhaar_card_df.withColumn("accepted_application_count",abs(state_wise_female_aadhaar_card_df["aadhaar_genderated_count"]- state_wise_female_aadhaar_card_df["aadhaar_rejected_count"]))
        state_wise_female_aadhaar_card_df = state_wise_female_aadhaar_card_df.withColumn("accepted_application_percentage",abs((state_wise_female_aadhaar_card_df["accepted_application_count"]/state_wise_female_aadhaar_card_df["aadhaar_genderated_count"]))*100)
        state_wise_female_aadhaar_card_df.show()
        
    else:
        print 'Invalid input pass to program.spark-submit SparkAssignment.py <Input.csv>'
    
    
 
