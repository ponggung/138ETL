# Databricks notebook source
from FETetlutils import TD_CONN,TXDATE
con = TD_CONN(host="",spark=spark,dbutils=dbutils) #ifx
tx = TXDATE()

source_db_name = "MDS_MART_VIEW"
source_table_name = "ROAMING_USG_MLY"

dest_db_name = "mds_mart"
dest_table_name = "roaming_usg_mly_view"


# COMMAND ----------

print(f"{tx.LAST01TX4Y_M_B=}")

sql=f"""
SELECT *
FROM {source_db_name}.{source_table_name}
WHERE DATA_MONTH = DATE '{tx.LAST01TX4Y_M_B}'
"""
pdf = con.load(sql)


# COMMAND ----------

## DELETE INSERT

# spark.sql(f"DELETE FROM {dest_db_name}.{dest_table_name} WHERE DATA_MONTH= '{LAST01TX4Y_M_B}'")
# pdf.write.format("delta")\
# .mode("append")\
# .saveAsTable(f"{dest_db_name}.{dest_table_name}")

## Upsert
pdf.write.format("delta")\
.mode("overwrite")\
.option("replaceWhere",f"DATA_MONTH='{tx.LAST01TX4Y_M_B}'")\
.saveAsTable(f"{dest_db_name}.{dest_table_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct(DATA_MONTH)
# MAGIC FROM mds_mart.roaming_usg_mly_view
# MAGIC ORDER BY DATA_MONTH DESC

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/database/mds_mart/roaming_usg_mly_view/

# COMMAND ----------

