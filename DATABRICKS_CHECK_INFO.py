# Databricks notebook source
# MAGIC %md
# MAGIC ## Check Conditions
# MAGIC Check COL_CNT and ROW_CNT
# MAGIC 
# MAGIC 
# MAGIC STEP 1. Calcuate at TeraData  
# MAGIC STEP 2. Calcuate at DataBricks  
# MAGIC STEP 3. Write DATABRICKS_CHECK_INFO to DataBricks  

# COMMAND ----------

# MAGIC %md
# MAGIC ## INIT

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime
import pytz
tw = pytz.timezone('Asia/Taipei')  #台灣時區
 
#ENV
host = '10.68.64.138'
user = 'B_MOBILITY'
password = "!@#$QWER"
tmode = "TERA" # TERA or ASCI

def load_data(sql):
    return spark.read.format("jdbc")\
          .option("driver", "com.teradata.jdbc.TeraDriver")\
          .option("url", f"jdbc:teradata://{host}/TMODE={tmode},TYPE=FASTEXPORT,SESSIONS=10,CHARSET=ASCII,CLIENT_CHARSET=Big5-HKSCS")\
          .option("dbtable", f"({sql}) as subq")\
          .option("user", f"{user}")\
          .option("password", f"{password}")\
          .load()
  
def create_empty(table_name):
    columns = ['DB_NAME', 'TABLE_NAME', 'TD_COL_CNT', 'TD_ROW_CNT']
    data = (np.nan for i in range(len(columns)))
    df=pd.DataFrame([dict(zip(columns, data))])
    df["DB_NAME"] = td_db_name
    df["TABLE_NAME"] = table_name
    return df
  
def cnt_td(table_name):
    query=f"""
    SELECT  '{td_db_name}'   AS DB_NAME
           ,'{table_name}' AS TABLE_NAME
           ,(
    SELECT COUNT(*)
    FROM DBC.COLUMNS
    WHERE databaseName = '{td_db_name}'
    AND TABLENAME = '{table_name}') AS TD_COL_CNT, (
    SELECT  COUNT(*)
    FROM {td_db_name}.{table_name} ) AS TD_ROW_CNT
    """

    try:
        sk1 = load_data(query)
        df = sk1.toPandas()
    except Exception as e:
        return create_empty(table_name)
    return df
  
def cnt_adb(table_name):
    df = cnt_td(table_name)
    try:
        sk2 = spark.table(f"{adb_db_name}.{table_name.lower()}")
        df["ABD_COL_CNT"] = len(sk2.columns)
        df["ABD_ROW_CNT"] = sk2.count()
      
        if (df["TD_COL_CNT"][0] == df["ABD_COL_CNT"][0]) and(df["TD_ROW_CNT"][0] == df["ABD_ROW_CNT"][0]):
            df["IS_SAME"] =True
        else:
            df["IS_SAME"] =False

    except Exception as e:
        df["IS_SAME"] =False 
        print(e)
    finally:
        df["UPDATE_DATE"] = datetime.now().date()
    return df

def check_info(table_list):
    dfs=[]
    for table in table_list:
        print(f"count {table}")
        df = cnt_adb(table)
        dfs.append(df)
    
    dfs = pd.concat(dfs,axis=0)
    dfs = dfs.reset_index(drop=True).sort_values(by="TABLE_NAME")
    return dfs
  
def write(dfs):
    dfs["DB_NAME"] = dfs["DB_NAME"].str.lower()
    dfs["TABLE_NAME"] = dfs["TABLE_NAME"].str.lower()
    spark.sql(f"DELETE FROM {adb_db_name}.DATABRICKS_CHECK_INFO WHERE UPDATE_DATE= current_date()")
    spark_df = spark.createDataFrame(dfs)
    spark_df.write.format("delta").mode("append").saveAsTable(f"{adb_db_name}.DATABRICKS_CHECK_INFO")
    print("check_info done")
    
    
def show():
    spark.sql(f"SELECT * FROM mds_mart.DATABRICKS_CHECK_INFO WHERE UPDATE_DATE= current_date").show()


# COMMAND ----------

now = datetime.now(tw).strftime("%Y-%m-%d %H:%M:%S")
print(f"start at: {now}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check

# COMMAND ----------

## main
td_db_name= "MDS_MART"
adb_db_name = "mds_mart"

# All Table
table_list = ['CHURN_DLY_VIEW', 'CRMODS_CHNL_CONTACT_CHANGE_VW', 'DM_SUBSCR_PENALTY_SMU_MLY', 'FACT_RENEW_DLY_VIEW', 'FACT_RENEW_DLY_VIEW_UAT', 'GA_DLY_UAT', 'GA_DLY_UAT_POC', 'GA_DLY_VIEW', 'GA_VINTAGE_VIEW', 'GA_VINTAGE_VIEW_201510_201812', 'GA_VINTAGE_VIEW_20210106', 'GA_VINTAGE_VIEW_BAK_202008', 'GA_VINTAGE_VIEW_FIX', 'GA_VINTAGE_VIEW_SR217877', 'GA_VINTAGE_VIEW_SR238471', 'GA_VINTAGE_VIEW_UAT', 'HANDSET_MLY', 'IDD_USG_DLY_VIEW', 'IDD_USG_DLY_VIEW_ER1', 'IDD_USG_MLY_VIEW', 'KS_MSISDN_LOAD', 'MDS_ACTIVE_MLY_ORIG', 'MDS_ACTIVE_MLY_ORIG_2016', 'MDS_ACTIVE_MLY_ORIG_2017', 'MDS_ACTIVE_MLY_ORIG_2018', 'MDS_ACTIVE_MLY_ORIG_2019', 'MDS_ACTIVE_MLY_ORIG_2020', 'MDS_ACTIVE_MLY_ORIG_202008', 'MDS_ACTIVE_MLY_UAT', 'MDS_CSM_SEG_TAG', 'MDS_DNA_MLY', 'MDS_EVENT_MLY', 'MDS_ID_BASE', 'MDS_ITT', 'MDS_PROMOTION_CATG', 'MDS_PROMOTION_VAS_2018', 'MDS_PROMOTION_VAS_CURR', 'MDS_RETAIL_STORE_CHURN_DLY', 'MDS_RETAIL_STORE_CHURN_MLY', 'MDS_SEG_IC_WALLET_IND', 'MDS_SEG_RESULT', 'MDS_TRM_CMPN_LEAD_EXEC_SMS', 'MDS_TRM_CMPN_LEAD_EXEC_STATUS', 'MDS_USAGE_AVG_MLY', 'MKT_DATASET_ATTACK_BASE', 'NTW_CDR_USG_MLY', 'RF_SEG_CD', 'ROAMING_USG_MLY_VIEW', 'SR192864_O_ABT_OUTPUT', 'SR192864_O_ABT_PAY_CHANNEL', 'SR203396_GA_BASE', 'SR207886_ESIM', 'SR229128_FINAL', 'SUSPEND_DLY_VIEW']


dfs = check_info(table_list)
write(dfs)

# COMMAND ----------

dfs= spark.sql(f"SELECT * FROM mds_mart.DATABRICKS_CHECK_INFO WHERE UPDATE_DATE= current_date").toPandas()
dfs

# COMMAND ----------

now = datetime.now(tw).strftime("%Y-%m-%d %H:%M:%S")
print(f"finish at: {now}")

# COMMAND ----------

# MAGIC %md
# MAGIC David Check

# COMMAND ----------

# DBTITLE 1,David Check
table_list = ['mds_usage_avg_mly', 'mkt_dataset_attack_base', 'ntw_cdr_usg_mly', 'redeem_event_mly', 'rf_seg_cd', 'roaming_usg_mly_view', 'sr192864_o_abt_output', 'sr192864_o_abt_pay_channel', 'sr203396_ga_base', 'sr207886_esim', 'sr229128_final', 'suspend_dly_view']
mask = dfs.TABLE_NAME.isin(table_list)
dfs[mask]

# COMMAND ----------

