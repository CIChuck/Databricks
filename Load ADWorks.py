# Databricks notebook source
# MAGIC %md
# MAGIC General load function defs

# COMMAND ----------

#load file into dataframe

def localloadcsv(filelocation, filename, filetype = "csv", delimiter = "'.'", first_row_is_header = "true", infer_schema="true"):
    # File location and type
    target = filelocation+filename
    print("Loading " + target)

    # CSV options
    infer_schema = "true"
    first_row_is_header = "true"
    delimiter = ","

    # The applied options are for CSV files. For other file types, these will be ignored.
    df = spark.read.format(file_type) \
      .option("inferSchema", infer_schema) \
      .option("header", first_row_is_header) \
      .option("sep", delimiter) \
      .load(target)
    
    return df

# COMMAND ----------

#loads data creates temp/perm tables.

db = 'AdventureWorks'
tabName = 'Territories'
loc = '/FileStore/AW/'
filname = 'AdventureWorks_Territories.csv'

df = localloadcsv(filelocation = loc, filename= filname )
display(df)

temp_table_name = db + '_' + tabName + '_temp'
df.createOrReplaceTempView(temp_table_name)

permanent_table_name = db + '.' + tabName
df.write.format("parquet").saveAsTable(permanent_table_name)




# COMMAND ----------

from databricks import sql

import os

with sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                 http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                 access_token    = os.getenv("DATABRICKS_TOKEN")) as connection:

  with connection.cursor() as cursor:
    cursor.columns(schema_name="adventureworks", table_name="customers")
    print(cursor.fetchall())

# COMMAND ----------

#Test Query

%sql

select * from adventureworks.customers a , adventureworks.products p, adventureworks.sales2015 s 
where a.customerkey=s.customerkey 
and s.productkey= p.productkey


