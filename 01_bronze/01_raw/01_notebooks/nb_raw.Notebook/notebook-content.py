# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0e72ece3-7979-4119-a843-b64b91305444",
# META       "default_lakehouse_name": "lh_raw",
# META       "default_lakehouse_workspace_id": "d0c3a219-5d83-439c-b8b9-35f7a83de1dd",
# META       "known_lakehouses": [
# META         {
# META           "id": "0e72ece3-7979-4119-a843-b64b91305444"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Define spark session and import necessary libraries

# CELL ********************

from pyspark.sql import SparkSession
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants  
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

try:
    # Initialize SparkSession with dynamic workload optimizations
    spark = SparkSession.builder \
        .appName("demo_lh_integration") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.initialExecutors", "2") \
        .config("spark.dynamicAllocation.maxExecutors", "50") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    logger.info("SparkSession initialized successfully with optimized configurations.")
except Exception as e:
    logger.error(f"Error initializing SparkSession: {e}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Define base path's and storing the data as dataframe

# CELL ********************

input_file_path        = "abfss://ws_dev_starter_kit@onelake.dfs.fabric.microsoft.com/lh_raw.Lakehouse/Files/test_customer_data/customers-10000.csv"
destination_table_path = "abfss://ws_dev_starter_kit@onelake.dfs.fabric.microsoft.com/lh_raw.Lakehouse/Tables"  

#Read CSV file through spark_df
data_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("delimiter", ',') \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("multiLine", "true") \
    .option("mode", "PERMISSIVE") \
    .csv(input_file_path)

display(data_df)
data_df.createOrReplaceTempView('sample_cust')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM sample_cust
# MAGIC LIMIT 10
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Stroing data into lakehouse delta tables

# MARKDOWN ********************

# #### Different Modes
# 
# ###### **1. Append Mode**   : Adds new data to the existing table without overwriting or deleting existing data.
# ###### **2. Overwrite Mode**: Replaces the entire table with the new data, deleting any existing data.
# ###### **3. ErrorIfExists** : Throws an error if the table already exists, preventing accidental overwrites.

# CELL ********************

#Sanitising column names before staoring it into delta table
data_df = data_df.toDF(*[col.replace(" ", "_") for col in data_df.columns])

#storing 'data_df' as delta table into lakehouse
data_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{destination_table_path}/customer/sample_customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Session stop

# CELL ********************

mssparkutils.session.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
