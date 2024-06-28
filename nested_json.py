# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('abc').getOrCreate()

# COMMAND ----------

df=spark.read.options(header=True,inferschema=True,delimiter=' ').csv('/FileStore/tables/sam.txt')
df.show()

# COMMAND ----------

df.withColumn('new',avg('SALARY').over(spec)).show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.agg(max('SALARY')).show()

# COMMAND ----------

spec=Window.partitionBy('STACK')

# COMMAND ----------

from pyspark.sql.window import Window


# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df=spark.read.option("multiline",True).json("/FileStore/tables/user.json")
df.show(truncate=False)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select("brewing.country").show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

df.schema.names

# COMMAND ----------

def read_nested_json(df):
    column_list=[ ]
    for column_name in df.schema.names:
        print('outside isinstance loop:'+column_name)
        if isinstance(df.schema[column_name].datatype,Arraytype):
            print("inside isinstance loop of array type"+ column_name)
            df=df.withColumn(column_name,explode(column_name).alias(column_name))
            column_list.append=(column_name)
        elif isinstance(df.schema[column_name].dataType,StructType):
            print("inside isinstanceloop"+column_name)
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + '.' +field.name).alias(column_name + "_" + field_name))
        else:
            column_list.append(column_name)
        df=df.select(column_list)
        return df
read_nested_json_flag = True

while read_nested_json_flag:
  print("Reading Nested JSON File ... ")
  df = read_nested_json(df)
  df.show(100, False)
  read_nested_json_flag = False

  for column_name in df.schema.names:
    if isinstance(df.schema[column_name].dataType, ArrayType):
      read_nested_json_flag = True
    elif isinstance(df.schema[column_name].dataType, StructType):
      read_nested_json_flag = True
df.show(100, False)
    

# COMMAND ----------

read_nested_json(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

def read_nested_json(df):
    column_list = []

    for column_name in df.schema.names:
        #print("Outside isinstance loop: " + column_name)
        # Checking column type is ArrayType
        if isinstance(df.schema[column_name].dataType, ArrayType):
            #print("Inside isinstance loop of ArrayType: " + column_name)
            df = df.withColumn(column_name, explode(column_name).alias(column_name))
            column_list.append(column_name)

        elif isinstance(df.schema[column_name].dataType, StructType):
            #print("Inside isinstance loop of StructType: " + column_name)
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)

    # Selecting columns using column_list from dataframe: df
    df = df.select(column_list)
    return df

read_nested_json_flag = True

while read_nested_json_flag:
  #print("Reading Nested JSON File ... ")
  df = read_nested_json(df)
  #df.show(100, False)
  read_nested_json_flag = False

  for column_name in df.schema.names:
    if isinstance(df.schema[column_name].dataType, ArrayType):
      read_nested_json_flag = True
    elif isinstance(df.schema[column_name].dataType, StructType):
      read_nested_json_flag = True

read_nested_json(df)

# COMMAND ----------

display(df)

# COMMAND ----------



# COMMAND ----------


