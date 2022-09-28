# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Databricks Demo Albertnogues.com
# MAGIC 
# MAGIC <img src="http://www.albertnogues.com/wp-content/uploads/2020/12/cropped-AlbertLogo2.png" width=150/>
# MAGIC 
# MAGIC Exploring sample NYC Taxi Data from Databricks
# MAGIC * Databricks Sample Presentation
# MAGIC * Albert Nogués 2021.

# COMMAND ----------

# MAGIC %md
# MAGIC We will load some sample data from the NYC taxi dataset available in databricks, load them and store them as table. We will use then python to do some manipulation (Extract month and year from the trip time), which will create two new additional columns to our dataframe and will check how the file is saved in the hive warehouse. We will observe we have some junk data as it created folders for months and years (partitioning), that we are not supposed to have, so we will use filter to apply some filter in python way and in sql way to filter these bad records
# MAGIC 
# MAGIC Then, we will load another month of data as a temporary view and will compare this in contrast with a delta table where we can run updates and all sort of DML.
# MAGIC 
# MAGIC As a last step, we will load some master data and will perform a join. For more on Delta Lake you can follow this tutorial --> https://delta.io/tutorials/delta-lake-workshop-primer/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/nyctaxi/tripdata/yellow

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We define the scructure of the dataframe (columns: names and types), and create a new dataframe with this schema for us to analyze through spark

# COMMAND ----------

from pyspark.sql.functions import col, lit, expr, when
from pyspark.sql.types import *
from datetime import datetime
import time

# Define schema
nyc_schema = StructType([
  StructField('Vendor', StringType(), True),
  StructField('Pickup_DateTime', TimestampType(), True),
  StructField('Dropoff_DateTime', TimestampType(), True),
  StructField('Passenger_Count', IntegerType(), True),
  StructField('Trip_Distance', DoubleType(), True),
  StructField('Pickup_Longitude', DoubleType(), True),
  StructField('Pickup_Latitude', DoubleType(), True),
  StructField('Rate_Code', StringType(), True),
  StructField('Store_And_Forward', StringType(), True),
  StructField('Dropoff_Longitude', DoubleType(), True),
  StructField('Dropoff_Latitude', DoubleType(), True),
  StructField('Payment_Type', StringType(), True),
  StructField('Fare_Amount', DoubleType(), True),
  StructField('Surcharge', DoubleType(), True),
  StructField('MTA_Tax', DoubleType(), True),
  StructField('Tip_Amount', DoubleType(), True),
  StructField('Tolls_Amount', DoubleType(), True),
  StructField('Total_Amount', DoubleType(), True)
])

rawDF = spark.read.format('csv').options(header=True).schema(nyc_schema).load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz")


# COMMAND ----------

rawDF.take(5)

# COMMAND ----------

rawDF.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS taxidata;
# MAGIC DROP TABLE IF EXISTS taxidata.taxi_2019_12;

# COMMAND ----------

# MAGIC %python
# MAGIC rawDF.write.mode("overwrite").saveAsTable("taxidata.taxi_2019_12")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended taxidata.taxi_2019_12

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/user/hive/warehouse/taxidata.db/taxi_2019_12/

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r /delta/taxi

# COMMAND ----------

processedDF = rawDF.withColumn('Year', expr('cast(year(Pickup_DateTime) as int)')).withColumn('Month', expr('cast(month(Pickup_DateTime) as int)')) 
processedDF.write.format('delta').mode('append').partitionBy('Year','Month').save("/delta/taxi")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/delta/taxi

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/delta/taxi/Year=2019/Month=12/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/delta/taxi/Year=2019/Month=11/

# COMMAND ----------

#So we found some dirty data in our dataframe! we can filter it.
processedDF.filter("year=2019").count() #the SQL way!

# COMMAND ----------

#from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, expr, when
from pyspark.sql.types import *

'''
pyspark.sql.SparkSession Main entry point for DataFrame and SQL functionality.
pyspark.sql.DataFrame A distributed collection of data grouped into named columns.
pyspark.sql.Column A column expression in a DataFrame.
pyspark.sql.Row A row of data in a DataFrame.
pyspark.sql.GroupedData Aggregation methods, returned by DataFrame.groupBy().
pyspark.sql.DataFrameNaFunctions Methods for handling missing data (null values).
pyspark.sql.DataFrameStatFunctions Methods for statistics functionality.
pyspark.sql.functions List of built-in functions available for DataFrame.
pyspark.sql.types List of data types available.
pyspark.sql.Window For working with window functions.
'''


processedDF.filter((col('Year')==2019) & (col('Month')==12)).count() #Dataframe way
#processedDF.filter($year===2019).count()
#processedDF.filter(df(year)==2019).count()

# COMMAND ----------

processedDF.filter((col('Year')!=2019) & (col('Month')!=12)).count() #Dataframe way

# COMMAND ----------

processedDF.filter("Year <> 2019 and Month <> 12").count() #The SQL Way

# COMMAND ----------

# MAGIC %sql
# MAGIC use taxidata;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r /delta/taxiclean

# COMMAND ----------

# MAGIC %python
# MAGIC #processedDF.filter("Year <> 2019 and Month <> 12").partitionBy('Year','Month').saveAsTable()
# MAGIC processedDF.filter("Year = 2019 and Month = 12").write.format('delta').mode('overwrite').partitionBy('Year','Month').save("/delta/taxiclean")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/delta/taxiclean/

# COMMAND ----------

rawDF2 = spark.read.format('csv').options(header=True).schema(nyc_schema).load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-11.csv.gz") #Lazy Execution

# COMMAND ----------

rawDF2.createOrReplaceTempView("taxi_2019_11_tmp")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %python
# MAGIC rawDF2.take(1)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists taxidata.taxi;
# MAGIC create TABLE taxidata.taxi as select * from taxidata.taxi_2019_12 limit 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) from taxidata.taxi;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxidata.taxi;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE taxidata.taxi set vendor=0 where vendor =1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taxidata.taxi;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE taxi_2019_11_tmp set vendor=0 where vendor =1; -- Not working. Only DELTA tables can be updated. Need to create derivate dataframes or other temp tables and persist them as delta

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/nyctaxi/taxizone/

# COMMAND ----------

dbutils.fs.head("dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_payment_type.csv")

# COMMAND ----------

paymentTypeDF = spark.read.format('csv').options(header=True).options(inferSchema=True).load("dbfs:/databricks-datasets/nyctaxi/taxizone/taxi_payment_type.csv") #small file, we can use inferSchema to see if Spark is capable to get the right datatypes so we avoid having to define a schema for this dataframe

# COMMAND ----------

#Now we want to join two tables. If we want to go with sql (first) we need to register the new dataframe as temporary table and perform the join. The second way, we will do the python way.
paymentTypeDF.createOrReplaceTempView("taxi_payment_types")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We perform a join
# MAGIC select t.trip_distance, t.payment_type, p.payment_desc from taxidata.taxi t, taxi_payment_types p
# MAGIC where t.Payment_type = p.payment_type

# COMMAND ----------

#The Python way. I will use alias because when joining two or more dataframes with the same column name it can get messy to refer the proper column
taxiDataDF = spark.sql("select * from taxidata.taxi")
taxiDataDF.alias("t").join(paymentTypeDF.alias("p"), on=taxiDataDF.Payment_Type == paymentTypeDF.payment_type, how="inner").select("t.Trip_Distance", "t.Payment_Type", "p.Payment_Desc").show()
