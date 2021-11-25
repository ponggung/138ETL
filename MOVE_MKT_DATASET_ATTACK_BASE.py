# Databricks notebook source
from FETetlutils import TD_CONN,TXDATE
con = TD_CONN(host="",spark=spark,dbutils=dbutils) #ifx
tx = TXDATE()
    
source_db_name = "MDS_MART"
source_table_name = "MKT_DATASET_ATTACK_BASE"

dest_db_name = "mds_mart"
dest_table_name = "mkt_dataset_attack_base"

# COMMAND ----------

sql = f"""
SELECT submit_month, submit_date, mining_dw_subscr_no, attack_by_cht_date_sms, attack_by_cht_date_tm, attack_by_twm_date_sms, attack_by_twm_date_tm
 FROM {source_db_name}.{source_table_name}
"""
pdf = con.load(sql)


# COMMAND ----------

pdf.write.format("delta").mode("overwrite").saveAsTable(f"{dest_db_name}.{dest_table_name}")
