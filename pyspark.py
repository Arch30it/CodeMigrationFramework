# Databricks notebook source
#  Accessing Hive Metastore tables using Spark
df_emp = spark.table("hive_metastore.hive_db.employee")
df_emp.show()

# COMMAND ----------

df_dept = spark.table("hive_metastore.hive_db.department")
df_dept.show()

# COMMAND ----------

df_proj = spark.table("hive_metastore.hive_db.project")
df_proj.display()

# COMMAND ----------

df_att = spark.table("hive_metastore.hive_db.attendance")
df_att.show()

# COMMAND ----------

# MAGIC %md
# MAGIC /Workspace/Users/archit.murgudkar@tlconsulting.com.au/MigrationDemo/CodeMigrationFramework
# MAGIC
# MAGIC /Workspace/Users/archit.murgudkar@tlconsulting.com.au/MigrationDemo/CodeMigrationFramework/sql_querying