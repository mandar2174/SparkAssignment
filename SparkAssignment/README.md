
Program contains code for sample analysis on csv data

Prerequisites:
Assignment Code
Apache spark 
Sample Data file


How to run :
spark-submit  --packages com.databricks:spark-csv_2.10:1.4.0 /home/ec2-user/SparkAssignment/com/assignment/SparkAssignment.py /home/ec2-user/SparkAssignment/resources/aadhaar_data.csv



Note : 
com.databricks:spark-csv_2.10:1.4.0 : Package required to read/write csv in spark
/home/ec2-user/SparkAssignment/com/assignment/SparkAssignment.py : program path

/home/ec2-user/SparkAssignment/resources/aadhaar_data.csv : Input data path


