# THINGS TO CHECK BEFORE RUNNING:
# 1) Check if the COMPANY_CODE is 'UNILEVER, Unilever (Non Hierarchy)' OR '1' / '20' etc. Update the queries accordingly.

import pymysql
import os.path
import csv
from datetime import datetime
import fnmatch
import shutil
import re

# STRINGS DEFINITIONS START
# DIRECTORIES - MODIFY HERE TO DEFINE INCOMING, DESTINATION DIRECTORIES

directorydest="/home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/Output/"+datetime.now().strftime("%Y%m%d")

#Create dest directory if not existing
if not os.path.exists(directorydest):
    os.makedirs(directorydest) 

outbound_filename_NUL_txn_extract= directorydest+"/IDN_TerSls_NUL_"+datetime.now().strftime("%Y%m%d%H%M%S")
outbound_filename_NUL_txn_control= "IDN_TerSls_NUL_"+datetime.now().strftime("%Y%m%d%H%M%S")

f2_ctl_rowcount=0
date_today=datetime.now().date().strftime('%d')

# DB CONNECTION DEFINITIONS 
db = pymysql.connect(host="#########", port=3306, user="dbuser", passwd="#########", db="NEW_EMPORIO_GENESIS_IDN",local_infile=True)
cursor = db.cursor()


# DEFINING QUERIES for Outbound Extract #2 - NUL Txns 
outbound_query2 = """SELECT Transactions_emporio_staging.Country_Code, Transactions_emporio_staging.OUTLET_UNIQUE_CODE, Transactions_emporio_staging.TOT_BILL_AMT, Transactions_emporio_staging.COUNTER_NUM, Transactions_emporio_staging.BILL_NO, Transactions_emporio_staging.BILL_DATE, Transactions_emporio_staging.TOTAL_LINE, Transactions_emporio_staging.CUST_NAME, Transactions_emporio_staging.CUST_PH_NUM, Transactions_emporio_staging.BILL_LEVEL_TAX_VAL, Transactions_emporio_staging.BILL_LEVEL_DISC_VAL, Transactions_emporio_staging.ONLINE_FLAG, Transactions_emporio_staging.UPDATED_TIME, Transactions_emporio_staging.CUST_TYPE, Transactions_emporio_staging.LINE_ITEM_DISC_VAL, Transactions_emporio_staging.PROM_DISC_APPL, Transactions_emporio_staging.PROM_DISC_CODE, Transactions_emporio_staging.CURR_SALES_PRICE, Transactions_emporio_staging.MRP, Transactions_emporio_staging.QTY, Transactions_emporio_staging.BARCODE, Transactions_emporio_staging.TOTAL_LINE_AMT, Transactions_emporio_staging.FREE_PROD_FLAG 
from NEW_EMPORIO_GENESIS_IDN.Transactions_emporio_staging 
LEFT JOIN NEW_EMPORIO_GENESIS_IDN.Product_master_emporio
ON NEW_EMPORIO_GENESIS_IDN.Transactions_emporio_staging.BARCODE = NEW_EMPORIO_GENESIS_IDN.Product_master_emporio.BARCODE
WHERE NEW_EMPORIO_GENESIS_IDN.Product_master_emporio.BARCODE IS NOT NULL AND (NEW_EMPORIO_GENESIS_IDN.Product_master_emporio.COMPANY_CODE NOT LIKE 'Unilever%');"""


# OPEN OUTBOUND TXN extract files 
f2 = open(outbound_filename_NUL_txn_extract+'.csv', 'w')
f2_ctl = open(outbound_filename_NUL_txn_extract+'.ctl', 'w')

# Write NUL Txn file
writer=csv.writer(f2,delimiter ="|",quoting=csv.QUOTE_NONE,quotechar='')
writer.writerow(['COUNTRY_CODE','OUTLET_UNIQUE_CODE','TOT_BILL_AMT','COUNTER_NUM','BILL_NO','BILL_DATE','TOTAL_LINE','CUST_NAME','CUST_PH_NUM','BILL_LEVEL_TAX_VAL','BILL_LEVEL_DISC_VAL','ONLINE_FLAG','UPDATED_TIME','CUST_TYPE','LINE_ITEM_DISC_VAL','PROM_DISC_APPL','PROM_DISC_CODE','CURR_SALES_PRICE','MRP','QTY','BARCODE','TOTAL_LINE_AMT','FREE_PROD_FLAG'])
cursor.execute(outbound_query2)
outbound_query2_data = cursor.fetchall()
for row in outbound_query2_data:
	f2_ctl_rowcount=f2_ctl_rowcount+1
	writer.writerow(row)

# Write NUL Txn CTL file 
writer=csv.writer(f2_ctl)
writer.writerow([outbound_filename_NUL_txn_control+'.csv'])
f2_ctl.write('Number of Rows\n')
f2_ctl.write(str(f2_ctl_rowcount))

f2.close()
f2_ctl.close()
db.commit()
cursor.close()
db.close()
