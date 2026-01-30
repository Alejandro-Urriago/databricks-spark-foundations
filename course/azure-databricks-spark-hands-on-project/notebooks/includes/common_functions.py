# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
  output_df = input_df.withColumn("ingestion_date", current_timestamp())
  return output_df

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
  column_list = []
  for column_name in input_df.schema.names:
    if column_name != partition_column:
      column_list.append(column_name)
  column_list.append(partition_column)
  output_df = input_df.select(column_list)
  return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
  output_df = re_arrange_partition_column(input_df, partition_column)
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
  else:
    output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def df_column_to_list(input_df, column_name):
  df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
  
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list

# COMMAND ----------

# DBTITLE 1,Cell 5
# MAGIC %skip
# MAGIC def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
# MAGIC
# MAGIC   #spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
# MAGIC
# MAGIC   from delta.tables import DeltaTable
# MAGIC
# MAGIC   if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")): # if the table exist then do a merge ( do not overwrite partition )
# MAGIC     
# MAGIC     # Use forName() for managed Unity Catalog tables instead of forPath()
# MAGIC     deltaTable = DeltaTable.forName(spark, f"{db_name}.{table_name}")
# MAGIC
# MAGIC     # when it matches then update records
# MAGIC     # when it does not match then insert all records
# MAGIC     deltaTable.alias("tgt").merge(
# MAGIC         input_df.alias("src"),
# MAGIC         merge_condition) \
# MAGIC       .whenMatchedUpdateAll() \
# MAGIC       .whenNotMatchedInsertAll() \
# MAGIC       .execute()
# MAGIC
# MAGIC   else:
# MAGIC     # if the tables does not exist then create the table and add patition column
# MAGIC     input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

# when  Databricks Job is running on serverless compute, which doesn't support _jsparkSession (JVM access).

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):

  from delta.tables import DeltaTable
  
  # Serverless-compatible table existence check
  try:
    deltaTable = DeltaTable.forName(spark, f"{db_name}.{table_name}")
    
    # Table exists - do merge
    deltaTable.alias("tgt").merge(
        input_df.alias("src"),
        merge_condition) \
      .whenMatchedUpdateAll() \
      .whenNotMatchedInsertAll() \
      .execute()
      
  except:
    # Table doesn't exist - create it
    input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")
