# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

# python imports
import traceback
from py4j.protocol import Py4JJavaError
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# pyspark imports
from pyspark.sql import DataFrame
from pyspark.sql import functions as sf
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, FloatType, 
    TimestampType, BooleanType
)
from pyspark.sql.utils import AnalysisException

spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Helper

# CELL ********************

import traceback

from pyspark.sql import DataFrame
from pyspark.errors import AnalysisException

import sempy.fabric as fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_lakehouse_abfs_path(lakehouse: str) -> str:
    
    workspace_name = notebookutils.runtime.context.get("currentWorkspaceName")
    abfs_path = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse}.lakehouse"

    return abfs_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def load_lakehouse_table(df: DataFrame, lakehouse: str, schema: str, table: str, write_mode: str):

    """
    Writes a DataFrame to a Delta table in a Microsoft Fabric Lakehouse.

    Parameters:
    ----------
    df : DataFrame
        The DataFrame to write.
    lakehouse : str
        Name of the Lakehouse.
    schema : str
        Schema (folder) name under 'Tables'.
    table : str
        Table name.
    write_mode : str
        Write mode: ``'overwrite'``, ``'append'``, ``'error'``, or ``'ignore'``.

    Raises:
    ------
    TypeError:
        If any input is not a string.
    """
    
    if not isinstance(df, DataFrame):
        raise TypeError(f"Expected a DataFrame, but got {type(df).__name__}")

    for var_name, var_value in {"lakehouse": lakehouse, "schema": schema, "table": table, "write_mode": write_mode}.items():
        if not isinstance(var_value, str):
            raise TypeError(f"Expected '{var_name}' to be a string, but got {type(var_value).__name__}.")

    abfs_path = get_lakehouse_abfs_path(lakehouse)

    path = f"{abfs_path}/Tables/{schema}/{table}"

    try:
        df.write.format("delta").option("mergeSchema", "true").mode(write_mode).save(path)
    except Exception as e:
        error_message = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        raise RuntimeError(f"Failed to write table to {path}: {error_message}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_lakehouse_table(lakehouse: str, schema: str, table: str, columns: str="*") -> DataFrame:

    """
    Reads a Delta table from a Microsoft Fabric Lakehouse into a Spark DataFrame,
    optionally selecting specific columns.

    Parameters:
    ----------
    lakehouse : str
        Name of the Lakehouse.
    schema : str
        Schema (folder) name under 'Tables'.
    table : str
        Table name.
    columns : str, optional
        Columns to select from the table. Can be a single column name, multiple column names
        separated by commas, or "*" to select all columns. Defaults to "*".

    Returns:
    -------
    DataFrame
        A Spark DataFrame containing the selected columns from the Delta table.

    Raises:
    ------
    TypeError:
        If any of `lakehouse`, `schema`, `table`, or `columns` is not a string.
    ValueError:
        If one or more requested columns do not exist in the Delta table.
    RuntimeError:
        If reading the Delta table fails for other reasons.
    """



    for var_name, var_value in {"lakehouse": lakehouse, "schema": schema, "table": table, "columns": columns}.items():
        if not isinstance(var_value, str):
            raise TypeError(f"Expected '{var_name}' to be a string, but got {type(var_value).__name__}.")
 
    abfs_path = get_lakehouse_abfs_path(lakehouse)

    path = f"{abfs_path}/Tables/{schema}/{table}"

    try:
        df = spark.read.format("delta").load(path).select([ col.strip() for col in columns.split(',')])
        return df
    except AnalysisException as ae:
        error_message = f"{type(ae).__name__}: {str(ae)}\n{traceback.format_exc()}"
        raise ValueError(f"Column selection failed. One or more columns may not exist columns select {columns}: {error_message}")
    except Exception as e:
        error_message = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        raise RuntimeError(f"Failed to read Delta table from path '{path}': {error_message}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def is_table_exist(lakehouse: str, schema: str, table: str, folder: str="Tables") -> bool:
    try:

        for var_name, var_value in {"lakehouse": lakehouse, "schema": schema, "table": table, "folder": folder}.items():
            if not isinstance(var_value, str):
                raise TypeError(f"Expected '{var_name}' to be a string, but got {type(var_value).__name__}.")
        
        abfs_path = get_lakehouse_abfs_path(lakehouse)

        path = f"{abfs_path}/{folder}/{schema}/{table}"

        return mssparkutils.fs.exists(path)
    except Exception as e:

        try:
            path_info = path
        except NameError:
            path_info = f"{lakehouse}/{folder}/{schema}/{table}"

        error_message = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        error = f"Error checking existence of {path_info}': {error_message}"
        raise RuntimeError(error)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### SCD

# CELL ********************

log_collection = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### SCD Utility Functions
# ---
# This module contains helper functions used throughout the **Slowly Changing Dimension (SCD) Type 2 pipeline**.  
# They provide common building blocks for reading data, handling schema alignment, generating keys, 
# tracking deletes, and adding SCD metadata.
# 
# ### Functions Overview:
# - **`table_exists`** → Checks if a table exists in the sink schema.  
# - **`fetch_incremental_records`** → Gets new source records with `__data_loaded_at` greater than the last loaded timestamp.  
# - **`fetch_deleted_records`** → Retrieves records that were hard-deleted in the source after the last known deleted timestamp.  
# - **`select_columns`** → Selects only the required set of columns from a DataFrame.  
# - **`add_hash_col`** → Adds a row-level checksum (`row_check_sum`) for change detection.  
# - **`generate_primary_key`** → Handles primary key assignment:
#   - Uses existing column if single key.  
#   - Hashes multiple columns into a synthetic key if composite.  
# - **`add_scd_columns`** → Adds SCD metadata columns:
#   - `__effective_from_date`  
#   - `__effective_to_date` (default = `9999-12-01`)  
#   - `__is_active` (default = 1).  
# - **`check_same_columns`** → Verifies whether two column lists are identical (ignoring order).  


# CELL ********************

def table_exists(sink_lh, source_schema, sink_schema, table_name):
    """Test if the specified table exists in the sink schema."""
    try:
        # Try to read a single row from the table
        spark.read.table(f"{sink_lh}.{sink_schema}.{table_name}").limit(1)
        return True
    except (AnalysisException, Py4JJavaError):
        # Return False if table doesn't exist or another Spark/Java error is thrown
        return False

def fetch_incremental_records(source_schema, source_lh, sink_schema, table, max_value):
    """Fetches new records greater than the max incremental value."""
    try:
        # Construct query to get records with newer __data_loaded_at
        query = f"""
            SELECT * 
            FROM {source_lh}.{source_schema}.{table}
            WHERE __data_loaded_at > '{max_value}'
        """
        df = spark.sql(query)

        return df

    except Exception as e:
        # Log error and return empty DataFrame with correct schema
        error_message = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        return spark.createDataFrame([], schema=spark.sql(f"SELECT * FROM {source_lh}.{source_schema}.{table} LIMIT 1").schema)

def fetch_deleted_records(source_lh, source_schema, sink_schema, table, max_value, hard_delete_column):
    """Fetches soft deleted records that were deleted after the last known deleted_at value."""
    try:
        query = f"""
            SELECT * 
            FROM {source_lh}.{source_schema}.{table}
            WHERE {hard_delete_column} > '{max_value}' AND {hard_delete_column} <= current_timestamp()
        """
        return spark.sql(query)

    except Exception as e:
        # Log error and return empty DataFrame with correct schema
        error_message = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        print(f"Error fetching deleted records for staging.{table}: {error_message}")
        return spark.createDataFrame([], schema=spark.sql(f"SELECT * FROM {source_lh}.{source_schema}.{table}").schema)


def select_columns(columns, target_df):
    """Selects a specific list of columns from the target DataFrame."""
    try:
        return target_df.select(*columns)
    except Exception as e:
        # Log column selection errors
        error_message = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"    
        print(error_message)

def add_hash_col(columns, target_df):
    """Adds a row-level checksum column for change detection using selected columns."""
    return target_df.withColumn("row_check_sum", sf.md5(sf.concat_ws("|", *[sf.col(c) for c in columns])))

def generate_primary_key(df, primary_key):
    """Handles composite or single primary key generation by hashing multiple columns if needed."""
    try:  
        if "|" in primary_key:
            # Composite key case: combine multiple columns using MD5
            columns = [col_name for col_name in primary_key.split("|")]
            primary_key_in = "primary_key"
            df = df.withColumn("primary_key", sf.md5(sf.concat(*[sf.col(c) for c in columns])))
        else:
            # Single column key: use as-is
            primary_key_in = primary_key
        return primary_key_in, df

    except Exception as e:
        # Log key generation error
        error_message = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        print(f"Error generating primary key: {error_message}")
        return None, df

def add_scd_columns(df: DataFrame, incremental_column: str):
    """Adds standard SCD Type 2 tracking columns to the DataFrame."""
    scd_df = df \
                .withColumn("__effective_from_date", sf.col(incremental_column).cast(TimestampType())) \
                .withColumn("__effective_to_date", sf.lit('9999-12-01').cast(TimestampType())) \
                .withColumn("__is_active", sf.lit(1).cast(IntegerType()))
                
    return scd_df

def check_same_columns(cols1, cols2):
    # Checking both having same list of columns
    return set(cols1) == set(cols2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Track Hard Delete Function
# ---
# This helper function handles **hard delete processing** for SCD Type 2 by updating sink records 
# with deletion metadata and splitting them into "to be deleted" vs "still active" sets.
# 
# #### Core Responsibilities:
# - **Join active sink records** (`df_sink_active`) with the hard delete DataFrame (`df_hd`) on the primary key.  
# - **Rebuild records with updated metadata**:
#   - Update the hard delete column with the latest deletion timestamp.  
#   - Update `__effective_to_date` based on the delete timestamp.  
#   - Mark `__is_active = 0` for deleted records.  
#   - Preserve all other columns from the sink.  
# - **Split into two outputs**:
#   - `df_hd_1`: Records considered as deleted (`hard_delete_column <= current_timestamp()`).  
#   - `df_hd_0`: Records marked with a future delete timestamp (not yet deleted).  
# 
# ---


# CELL ********************

def track_hard_delete(df_sink_active, df_hd, hard_delete_column, primary_key):
    df_joined = df_sink_active.alias("sink") \
                        .join(
                            other = df_hd.alias("hd"), 
                            on = sf.col(f"sink.{primary_key}") == sf.col(f"hd.{primary_key}"), 
                            how = "left"
                        )

    # Rebuild the deleted dataframe with updated metadata columns
    df_hd = df_joined.select(
                        sf.when(sf.col(f"hd.{hard_delete_column}").isNotNull(), sf.col(f"hd.{hard_delete_column}")).otherwise(sf.col(f"sink.{hard_delete_column}")).alias(f"{hard_delete_column}"),
                        sf.when(sf.col(f"hd.{hard_delete_column}").isNotNull(), sf.col(f"hd.{hard_delete_column}")).otherwise(sf.col("sink.__effective_to_date")).alias("__effective_to_date"),
                        sf.when(sf.col(f"hd.{hard_delete_column}").isNotNull(), sf.lit(0)).otherwise(sf.col("sink.__is_active")).alias("__is_active"),
                        *[sf.col(f"sink.{col_name}") for col_name in df_sink_active.columns if col_name not in [hard_delete_column, "__effective_to_date", "__is_active"]]
                    )
    
    # Split the hard delete data into actual deletes and remaining records
    df_hd_1 = df_hd.filter(sf.col(f"{hard_delete_column}") <= sf.current_timestamp())
    df_hd_0 = df_hd.filter(sf.col(f"{hard_delete_column}") > sf.current_timestamp())

    return (df_hd_0, df_hd_1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### SCD Initial Full Load Function
# 
# This function implements the **initial full load logic for Slowly Changing Dimension (SCD) Type 2** in PySpark.  
# It reads source data, applies primary key handling, selects only the latest records per key using an incremental column, 
# adds SCD metadata fields, and writes the transformed data to the sink lakehouse table.
# 
# #### Key Notes for Users:  
# - **Ensure `primary_key` and `incremental_column`** are correctly set to guarantee uniqueness and ordering.  
# - **Modify `selected_columns`** to control which fields flow into the SCD table.  
# - Additional metadata or logic (e.g., new SCD attributes) should be added in helper functions like `add_scd_columns`. 
# 
# ---

# CELL ********************

def scd_initial_full_load(source_lh: str, sink_lh: str, source_table: str, source_schema: str, sink_schema: str, sink_table: str, incremental_column: str, primary_key: str, selected_columns: list[str], hard_delete_column: str):
    """
    Performs the initial full load for Slowly Changing Dimension (SCD) Type 2.
    
    Parameters:
        source_table (str): Name of the source table.
        df_source (DataFrame): source DataFrame.
        source_schema (str): source schema name.
        sink_schema (str): sink schema name.
        sink_table (str): sink table name.
        incremental_column (str): Incremental timestamp column.
        primary_key (str): Primary key column.
    """

    try:
        # Read data from source table
        df_source = spark.read.table(f"{source_lh}.{source_schema}.{source_table}")
        
        # Select only the required columns
        df_source = select_columns(selected_columns, df_source)

        # Handle primary key generation if not provided
        if primary_key is None:
            primary_key = primary_key
        else:
            primary_key, df_source = generate_primary_key(df_source, primary_key)

        # Check if source is not empty and primary key is available in columns
        if (not df_source.isEmpty()) and (primary_key in df_source.columns): 
            # Define window to order records by descending incremental_column within each primary key group
            windowSpec = Window.partitionBy(primary_key).orderBy(sf.col(incremental_column).desc())

            # Add row number and lag of __data_loaded_at
            df_source = df_source \
                            .withColumn("__row_number", sf.row_number().over(windowSpec)) \
                            .withColumn("__lag_data_loaded", sf.lag(sf.col("__data_loaded_at")).over(windowSpec)) \
                            .filter(sf.col("__row_number") == 1) \
                            .drop(*["__row_number", "primary_key", "__lag_data_loaded"])  # Drop temporary columns

        # Add SCD metadata columns like __is_active, __effective_from_date, etc.
        df_source = add_scd_columns(df_source, incremental_column)

        # Count final number of rows to be written
        row_count = df_source.count()

        # Write the transformed DataFrame to the lakehouse (bronze layer) in overwrite mode
        load_lakehouse_table(df=df_source, lakehouse=sink_lh, schema=sink_schema, table=sink_table, write_mode="overwrite")
        
        # Log success of full load
        return log_collection.append([
            "FL", source_schema, source_table, sink_schema, sink_table,
            row_count, row_count, None, None, 
            "Success", "Successfully loaded to Lakehouse", datetime.now()
        ])

    except Exception as e:
        # Handle and log any exception raised during full load
        error_message = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        return log_collection.append([
            "FL", source_schema, source_table, sink_schema, sink_table,
            None, None, None, None, 
            "Fail", error_message, datetime.now()
        ])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### SCD Type 2 Function
# ---
# This function performs **Slowly Changing Dimension (SCD) Type 2 processing** on incoming data.  
# It compares source records against the existing sink dataset, tracks changes with historical versions, 
# and maintains audit metadata for inserts, updates, deletions, and inactive records.
# 
# #### Core Responsibilities:
# - **Split sink data** into active and inactive sets.  
# - **Track hard deletes** when applicable (`hard_delete_column`).  
# - **Identify and insert new records** not previously in the sink.  
# - **Detect and version updates** by comparing row checksums and timestamps.  
# - **Expire outdated records** (`__is_active = 0`, update `__effective_to_date`).  
# - **Preserve inactive history** for audit and traceability.  
# - **Add SCD metadata fields** (`__effective_from_date`, `__effective_to_date`, `__is_active`, `__data_loaded_at`).  
# - **Write results** to the sink (Silver layer) in overwrite mode.  
# 
# ---


# CELL ********************

def scd_type_2(source_lh: str, sink_lh: str, source_table: str, df_source: DataFrame, df_hd: DataFrame, df_sink: DataFrame, primary_key: str, incremental_column: str, hard_delete_column: str, source_schema: str, sink_schema: str, sink_table: str):
    """
    Handles Slowly Changing Dimension (SCD) Type 2 processing.

    Parameters:
        source_table (str): source table name.
        df_source (DataFrame): Incoming new data.
        df_sink (DataFrame): Existing data in sink.
        primary_key (str): Primary key column.
        incremental_column (str): Incremental timestamp column.
        source_schema (str): source schema name.
        sink_schema (str): sink schema name.
        sink_table (str): sink table name
    """
    
    try:
        # Split sink data into active and inactive records
        df_sink_active = df_sink.filter(sf.col("__is_active") == 1)
        df_sink_inactive = df_sink.filter(sf.col("__is_active") == 0)

        if df_hd is not None:
            df_hd_0, df_hd_1 = track_hard_delete(df_sink_active, df_hd, hard_delete_column, primary_key)
        else:
            df_hd_0 = df_sink_active
            df_hd_1 = None 


        # Identify new records (those in source but not in sink)
        df_new = df_source.alias("source") \
                    .join(
                        other = df_sink_active.alias("sink"),
                        on = sf.col(f"source.{primary_key}") == sf.col(f"sink.{primary_key}"), 
                        how = "leftanti"
                    ).select("source.*")

        # Add SCD metadata columns to new records
        df_new = add_scd_columns(df_new, incremental_column)
        # Identify updated records based on hash comparison
        df_updated = df_source.alias("source") \
                        .join(
                            other = df_sink_active.alias("sink"), 
                            on = (sf.col(f"source.{primary_key}") == sf.col(f"sink.{primary_key}")) & (sf.col("source.row_check_sum") != sf.col("sink.row_check_sum")), 
                            how = "inner"
                        ).select("source.*") \
                        .withColumn("__effective_from_date", sf.col(f"source.{incremental_column}")) 

        # Define window spec for deduplication and historical tracking
        window_spec = Window.partitionBy(primary_key).orderBy(df_updated["__data_loaded_at"].desc())

        # Add SCD columns to track historical data in case of multiple changes
        df_with_rownum = df_updated \
                            .withColumn("lag_effective_from_date", sf.lag("__effective_from_date").over(window_spec)) \
                            .withColumn("row_number", sf.row_number().over(window_spec))

        # Set __is_active and __effective_to_date using window logic
        df_updated = df_with_rownum \
                        .withColumn(
                            "__is_active",
                            sf.when(df_with_rownum["row_number"] == 1, 1).otherwise(0).cast(IntegerType())
                        ) \
                        .withColumn(
                            "__effective_to_date",
                            sf.when(
                                df_with_rownum["row_number"] == 1, 
                                sf.lit("9999-12-01").cast(TimestampType())
                            ).otherwise(
                                sf.expr("lag_effective_from_date - INTERVAL 1 MINUTE")
                            )
                        ) \
                        .drop("row_number", "lag_effective_from_date")
        
        # Prepare for marking outdated records as inactive
        df_source_filtered = df_source.select(primary_key, incremental_column, "row_check_sum").alias("source")

        # Define update condition where checksum and timestamp differ
        update_condition = (
            (sf.col(f"source.{incremental_column}").isNotNull()) & 
            (sf.col(f"source.row_check_sum") != sf.col(f"sink.row_check_sum")) &
            (sf.col(f"source.{incremental_column}") != sf.col(f"sink.{incremental_column}"))
        )

        # Prepare list of columns excluding SCD flags
        sink_columns = [c for c in df_hd_0.columns if c not in ["__is_active", "__effective_to_date"]]

        # Mark outdated records with __is_active = 0 and update __effective_to_date
        df_hd_0 = df_hd_0.alias("sink") \
                            .join(
                                other = df_source_filtered, 
                                on = (sf.col(f"sink.{primary_key}") == sf.col(f"source.{primary_key}")) & (sf.col(f"source.row_check_sum") != sf.col(f"sink.row_check_sum")), 
                                how = "left"
                            ).select(
                                *[sf.col(f"sink.{c}") for c in sink_columns],
                                sf.when(update_condition, sf.lit(0)) \
                                        .otherwise(sf.col("sink.__is_active")).alias("__is_active"),
                                sf.when(update_condition, sf.expr(f"source.{incremental_column} - INTERVAL 1 MINUTE")) \
                                        .otherwise(sf.col("sink.__effective_to_date")).alias("__effective_to_date")
                            )

        # Combine all dataframes into final dataset

        
        df_final = df_hd_0.unionByName(df_new).unionByName(df_updated).unionByName(df_sink_inactive)

        if df_hd_1 is not None:
            df_final = df_final.unionByName(df_hd_1)

        # Reorder columns for final output
        cols_to_move = ["__data_loaded_at", "__effective_from_date", "__effective_to_date", "__is_active"]
        remaining_cols = [col for col in df_final.columns if col not in cols_to_move]
        final_column_order = remaining_cols + cols_to_move


        # Drop duplicate primary key column if present
        if "primary_key" in df_final.columns:
            df_final = df_final.select(final_column_order).drop(sf.col("primary_key"))

        # Drop hash column before write
        df_final = df_final.drop("row_check_sum")
        
        # Log statistics for monitoring
        counts = {
            "row_count": df_final.count(),
            "new": df_new.count(),
            "updated": df_updated.count(),
            "inactive": df_sink_inactive.count(),
            "deleted": df_hd_1.count()
        }

        # Write the processed data to Lakehouse (Silver layer)
        load_lakehouse_table(df=df_final, lakehouse=sink_lh, schema=sink_schema, table=sink_table, write_mode="overwrite")

        # Log success or skip based on change counts
        if counts["new"] > 0 or counts["updated"] > 0 or counts["inactive"] > 0 or count["deleted"]:
            print("Success ")
            return log_collection.append([
                "SCD", source_schema, source_table, sink_schema, sink_table,
                counts["row_count"], counts["new"], counts["updated"],
                counts["inactive"], "Success", "Successfully loaded to Lakehouse", datetime.now()
            ])
        else:
            print("Skipped - No actual data changes")
            return log_collection.append([
                "SCD", source_schema, source_table, sink_schema, sink_table,
                counts["row_count"], counts["new"], counts["updated"],
                counts["inactive"], "Skipped", "no new changes to loaded to Lakehouse", datetime.now()
            ])
    
    except Exception as e:
        # Log any exception encountered during SCD processing
        error_message = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        print(error_message)
        return log_collection.append([
            "SCD", source_schema, source_table, sink_schema, sink_table,
            None, None, None, None, 
            "Fail", error_message, datetime.now()
        ])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### SCD Orchestration Function
# ---
# This function orchestrates **Slowly Changing Dimension (SCD) Type 2 processing** by handling both the 
# **initial full load** and subsequent **incremental loads**.  
# It acts as the entry point for SCD logic, delegating to `scd_initial_full_load` when no sink table exists 
# (or schema mismatch is detected), and to `scd_type_2` for incremental change processing.
# 
# #### Core Responsibilities:
# - **Check sink table existence** → decide between full load and incremental load.  
# - **Perform initial full load** using `scd_initial_full_load` when:
#   - The sink table doesn’t exist, or  
#   - The schema has drifted (columns added/removed).  
# - **Perform incremental load** when sink already exists:
#   - Read latest timestamp from sink (`__data_loaded_at`).  
#   - Fetch new/changed records from source.  
#   - Handle **hard deletes** (if `hard_delete_column` is provided).  
#   - Add hash column for change detection.  
#   - Generate/validate primary key across datasets.  
#   - Call `scd_type_2` for change application (new, updated, deleted, inactive).  
# - **Log results** of execution: success, failure, or skipped (no changes).  


# CELL ********************

def scd(source_lh, sink_lh, source_schema, sink_schema, source_table, sink_table, primary_key, incremental_column, selected_columns, hard_delete_column):
    """
    Implements SCD Type 2 for the given table, handling both full and incremental loads.

    Parameters:
        source_schema (str): source schema name.
        sink_schema (str): sink schema name.
        source_table (str): source table name.
        sink_table (str): sink table name
        primary_key (str): Primary key column
        incremental_column (str): Incremental timestamp column
        selected_columns (list[str]): List of columns to be processed.
    """
    try:

        # If sink table does not exist or no primary key is provided, perform initial full load
        if not table_exists(sink_lh, source_schema, sink_schema, sink_table) or primary_key is None:

            try:
                # Perform initial full load for SCD
                scd_initial_full_load(
                    source_lh, sink_lh,
                    source_table, source_schema,
                    sink_schema, sink_table, incremental_column,
                    primary_key, selected_columns, hard_delete_column
                )

            except Exception as e:
                # Log failure in full load
                error_message = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
                return log_collection.append([
                    "FL", source_schema, source_table, sink_schema, sink_table,
                    None, None, None, None,
                    "Fail", error_message, datetime.now()
                ])

        else:
            try:
                # Load sink table and prepare for comparison
                df_sink = spark.sql(f"SELECT * FROM {sink_schema}.{sink_table}")
                
                # Exclude metadata columns for hashing
                if(isinstance(hard_delete_column, str)):
                    excluded_cols = ["__data_loaded_at", "__source_name", "__entity_name", hard_delete_column] 
                    # Aggregate metadata columns for deduplication
                    agg_exprs = [
                        sf.max(f"{hard_delete_column}").alias(f"{hard_delete_column}"),
                        sf.max("__data_loaded_at").alias("__data_loaded_at"),
                        sf.first("__source_name").alias("__source_name"),
                        sf.first("__entity_name").alias("__entity_name")
                    ]
                else:
                    excluded_cols = ["__data_loaded_at", "__source_name", "__entity_name"]
                    # Aggregate metadata columns for deduplication
                    agg_exprs = [
                        sf.max("__data_loaded_at").alias("__data_loaded_at"),
                        sf.first("__source_name").alias("__source_name"),
                        sf.first("__entity_name").alias("__entity_name")
                    ] 

                scd_columns = [col for col in selected_columns if col not in excluded_cols]
                filtered_sink_cols = [col for col in df_sink.columns if col not in (excluded_cols + ['__effective_from_date', '__effective_to_date', '__is_active'])]
                
                # Check if new columns added or removed
                if(not check_same_columns(filtered_sink_cols, scd_columns)):
                    scd_initial_full_load(
                        source_lh, sink_lh,
                        source_table, source_schema,
                        sink_schema, sink_table, incremental_column,
                        primary_key, selected_columns
                    )
                    return 
                
                # Get latest loaded timestamp from sink table
                df = spark.sql(f"SELECT MAX(__data_loaded_at) AS max_data_loaded_at FROM {sink_schema}.{sink_table}")
                max_incremental_value = df.collect()[0]['max_data_loaded_at']

                # Fetch incremental records from source
                df_source = fetch_incremental_records(source_schema, source_lh, sink_schema, source_table, max_incremental_value)
                df_source = select_columns(selected_columns, df_source)
                df_source = df_source.groupBy(scd_columns).agg(*agg_exprs)

                if(isinstance(hard_delete_column, str)):
                    # Get latest deleted timestamp from sink table excluding default future date
                    df = spark.sql(f"SELECT COALESCE(MAX({hard_delete_column}), '1900-01-01') AS max_deleted_at FROM {sink_lh}.{sink_schema}.{sink_table} WHERE CAST({hard_delete_column} AS DATE) != '9999-12-01'")
                    max_deleted_at_value = df.collect()[0]['max_deleted_at']
                    
                    # Fetch hard deleted records from source
                    df_hd = fetch_deleted_records(source_lh, source_schema, sink_schema, source_table, max_deleted_at_value, hard_delete_column)
                    df_hd = select_columns(selected_columns, df_hd)
                    df_hd = add_hash_col(scd_columns, df_hd)
                else:
                    df_hd = None

                # Add hash column for change tracking
                
                df_source = add_hash_col(scd_columns, df_source)

                df_sink = select_columns(
                    selected_columns + ["__effective_from_date", "__effective_to_date", "__is_active"],
                    df_sink
                )

                df_sink = add_hash_col(scd_columns, df_sink)

                # Generate or extract primary key for all dataframes
                primary_key_in, df_source = generate_primary_key(df_source, primary_key)
                primary_key_in, df_sink = generate_primary_key(df_sink, primary_key)

                # Proceed with SCD Type 2 processing only if there are changes
                if not df_source.isEmpty():
                    scd_type_2(
                        source_lh, sink_lh,
                        source_table, df_source, df_hd, df_sink,
                        primary_key_in, incremental_column, hard_delete_column,
                        source_schema, sink_schema, sink_table
                    )
                else:
                    # Log skipped processing if no changes found
                    return log_collection.append([
                        "SCD", source_schema, source_table, sink_schema, sink_table,
                        None, None, None, None, 
                        "Skipped", "No new changes detected", datetime.now()
                    ])

            except Exception as e:
                # Log failure during incremental load processing
                error_message = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
                return log_collection.append([
                    "SCD", source_schema, source_table, sink_schema, sink_table,
                    None, None, None, None, 
                    "Fail", error_message, datetime.now()
                ])

    except Exception as e:
        # Log top-level failure in SCD processing
        error_message = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        return log_collection.append([
            "SCD", source_schema, source_table, sink_schema, sink_table,
            None, None, None, None, 
            "Fail", error_message, datetime.now()
        ])

    return "Completed!"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Process Table Function
# ---
# This function orchestrates the **SCD Type 2 pipeline** for a single table based on metadata provided in a row.  
# It reads the source data, prepares the list of columns for processing, and calls the `scd` function to apply full or incremental logic.  
# It also logs outcomes for monitoring (success, skip, or fail).
# 
# #### Core Responsibilities:
# - **Extract metadata** from the input row (lakehouse names, schemas, table names, primary key, incremental column, etc.).  
# - **Read source table** into a Spark DataFrame.  
# - **Prepare selected columns**:
#   - Use all columns if none are provided.  
#   - Otherwise, parse comma-separated list and add metadata fields (`__data_loaded_at`, `__source_name`, `__entity_name`, and `hard_delete_column` if applicable).  
# - **Run SCD pipeline** by calling the `scd` function only if the source DataFrame is not empty.  
# - **Log skipped tables** when no source data is available.  
# - **Handle errors gracefully** by capturing traceback and logging the failure event.  
# 
# ---


# CELL ********************

def process_table(row):
    # Extract necessary metadata fields from the input row
    source_lh = row['source_lh']
    sink_lh = row['sink_lh']
    source_schema = row['source_schema']
    sink_schema = row['sink_schema']
    source_table = row['source_table_name']
    primary_key = row['primary_key']
    incremental_column = row['incremental_column']
    sink_table = row['sink_table_name']
    hard_delete_column = row['hard_delete_column']
   
    try:
        # Read data from the source (staging) table into a Spark DataFrame
        source_df = spark.read.table(f"{source_lh}.{source_schema}.{source_table}")
       
        # Parse and clean the selected columns from a comma-separated string
        if(row['selected_columns'] is None):
            selected_columns = source_df.columns
        else:
            selected_columns = [col.strip() for col in row['selected_columns'].split(",")]
            # Add metadata columns required for SCD processing
            if(isinstance(hard_delete_column, str)):
                selected_columns.extend(["__data_loaded_at", "__source_name", "__entity_name", hard_delete_column])
            else:
                selected_columns.extend(["__data_loaded_at", "__source_name", "__entity_name"])
        # Proceed with SCD Type 2 logic only if the source DataFrame is not empty
        if not source_df.isEmpty():
            scd(source_lh, sink_lh, source_schema, sink_schema, source_table, sink_table, primary_key, incremental_column, selected_columns,hard_delete_column)
        else:
            # Log that the table was skipped due to no data
            log_collection.append([
                "SCD", source_schema, source_table, sink_schema, sink_table,
                None, None, None, None, 
                "Skipped", "Source Table is empty", datetime.now()
            ])
 
    except Exception as e:
        # Capture the full error message and traceback for debugging
        error_message = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
       
        # Log the failure event along with error details
        log_collection.append([
            "SCD", source_schema, source_table, sink_schema, sink_table,
            None, None, None, None,
            "Fail", error_message, datetime.now()
        ])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the source configuration from a CSV file
# Note: Update the lakehouse name and metadata file path as needed
lakehouse_name = 'lh_stg'
file_path = '/metadata/metadata_staging.csv'
path = get_lakehouse_abfs_path(f'{lakehouse_name}') + '/Files' + f'{file_path}'

df_source_config = spark.read.format("csv").option("header","true").load(path)

rows = [row.asDict() for row in df_source_config.collect()]

# Set up parallel execution using ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=5) as executor:  
     executor.map(process_table, rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if there are any log entries to process
if log_collection:

    # Define schema for the log DataFrame
    log_schema = StructType([
        StructField("process_type", StringType(), True),       # Type of process (e.g., SCD, FL)
        StructField("source_schema", StringType(), True),      # Source schema name
        StructField("source_table", StringType(), True),       # Source table name
        StructField("sink_schema", StringType(), True),        # Sink schema name
        StructField("sink_table", StringType(), True),         # Sink table name
        StructField("row_count", IntegerType(), True),         # Total row count processed
        StructField("new", IntegerType(), True),               # Number of new rows
        StructField("updated", IntegerType(), True),           # Number of updated rows
        StructField("inactive", IntegerType(), True),          # Number of inactive rows
        StructField("status", StringType(), True),             # Status of the process (Success/Fail/Skipped)
        StructField("status_message", StringType(), True),     # Description of what happened
        StructField("log_date", TimestampType(), True)         # Log timestamp
    ])

    # Create a DataFrame from the log collection using the defined schema
    log_df = spark.createDataFrame(log_collection, log_schema)

    # Write the log DataFrame to the monitoring table in append mode
    load_lakehouse_table(df=log_df, lakehouse='lh_stg', schema="monitor", table="staging_log", write_mode='append')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
