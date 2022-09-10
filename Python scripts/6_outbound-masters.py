# THINGS TO CHECK BEFORE RUNNING THIS SCRIPT:
# 1) Check if the COMPANY_CODE is 'UNILEVER' OR '1' / '20' etc. Update the queries accordingly.
# 2) Check the inbound file / outbound error file directories if they are correct
# 3) IMPORTANT: This RUNS on EMPORIO_IDN DB Master files.  For Product/Outlet/Promo masters on monthly basis, the received files would be in STAGING table.  

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

outbound_filename_product_master= directorydest+"/IDN_Prod_"+datetime.now().strftime("%Y%m%d%H%M%S")
outbound_filename_outlet_master= directorydest+"/IDN_Outlet_"+datetime.now().strftime("%Y%m%d%H%M%S")
outbound_filename_promo_master= directorydest+"/IDN_Promo_"+datetime.now().strftime("%Y%m%d%H%M%S")
outbound_filename_product_control= "IDN_Prod_"+datetime.now().strftime("%Y%m%d%H%M%S")
outbound_filename_outlet_control= "IDN_Outlet_"+datetime.now().strftime("%Y%m%d%H%M%S")
outbound_filename_promo_control= "IDN_Promo_"+datetime.now().strftime("%Y%m%d%H%M%S")

f1_ctl_rowcount=0
f2_ctl_rowcount=0
f3_ctl_rowcount=0
date_today=datetime.now().date().strftime('%d')

# DB CONNECTION DEFINITIONS 
db = pymysql.connect(host="#########", port=3306, user="dbuser", passwd="#########", db="EMPORIO_IDN",local_infile=True)
cursor = db.cursor()

# DEFINING QUERIES for Outbound Extract #1 - ULI Txns 
outbound_query1 = """SELECT BARCODE, COMPANY_CODE, PROD_CATEGORY, PROD_SUB_CATEGORY, PACK_SIZE, PACK_TYPE, PROD_BRAND, BASE_PACK_CODE, PROD_NAME, STATE_CODE, PRICE, UOM, UOM_QTY, TAX_RATE, STATUS, COUNTRY_CODE, UPDATED_TIME from NEW_EMPORIO_GENESIS_IDN.Product_staging_emporio;""" 
outbound_query2 = """SELECT OUTLET_UNIQUE_CODE, OUTLET_NAME, COUNTRY_CODE, OUTLET_TYPE, OUTLET_LEV_CODE, STATE_CODE, STREET_LAND_MARK, DISTRICT, CITY, ZIP_CODE, OUTLET_LATTITUDE, OUTLET_LONGITUDE, APP_TYPE, CURR, STATUS, UPDATED_TIME from NEW_EMPORIO_GENESIS_IDN.Outlet_staging_emporio;""" 
outbound_query3 = """SELECT PROMO_CODE,PROMO_DESC,START_DATE,END_DATE,PROMO_TYPE,PROMO_DISC_TYPE,PROMO_DISC_VALUE,APP_TYPE,COUNTRY_CODE,BARCODE,PROD_CAT_NAME,PROD_BRAND_CODE,FREE_PROD,COMBO_GRP_DISC,MIN,MAX,OUTLET_UNIQUE_CODE,OUTLET_CAP_QTY,UPDATED_TIME from NEW_EMPORIO_GENESIS_IDN.Promo_staging_emporio;""" 

# OPEN OUTBOUND TXN extract files 
f1 = open(outbound_filename_product_master+'.csv', 'w')
f2 = open(outbound_filename_outlet_master+'.csv', 'w')
f3 = open(outbound_filename_promo_master+'.csv', 'w')
f1_ctl = open(outbound_filename_product_master+'.ctl', 'w')
f2_ctl = open(outbound_filename_outlet_master+'.ctl', 'w')
f3_ctl = open(outbound_filename_promo_master+'.ctl', 'w')

# Write Product Master file
#writer=csv.writer(f1,delimiter ="|",quoting=csv.QUOTE_NONE,quotechar='',escapechar='\\')
writer=csv.writer(f1,delimiter="|",quoting=csv.QUOTE_NONE,quotechar='')
writer.writerow(['BARCODE','COMPANY_CODE','PROD_CATEGORY','PROD_SUB_CATEGORY','PACK_SIZE','PACK_TYPE','PROD_BRAND','BASE_PACK_CODE','PROD_NAME','STATE_CODE','PRICE','UOM','UOM_QTY','TAX_RATE','STATUS','COUNTRY_CODE','UPDATED_TIME'])
cursor.execute(outbound_query1)
outbound_query1_data = cursor.fetchall()
for row in outbound_query1_data:
	f1_ctl_rowcount=f1_ctl_rowcount+1
	writer.writerow(row)


# Write Outlet Master file
writer=csv.writer(f2,delimiter ="|",quoting=csv.QUOTE_NONE,quotechar='')
writer.writerow(['OUTLET_UNIQUE_CODE','OUTLET_NAME','COUNTRY_CODE','OUTLET_TYPE','OUTLET_LEV_CODE','STATE_CODE','STREET_LAND_MARK','DISTRICT','CITY','ZIP_CODE','OUTLET_LATTITUDE','OUTLET_LONGITUDE','APP_TYPE','CURR','STATUS','UPDATED_TIME'])
cursor.execute(outbound_query2)
outbound_query2_data = cursor.fetchall()
for row in outbound_query2_data:
	f2_ctl_rowcount=f2_ctl_rowcount+1
	writer.writerow(row)

# Write Promo Master file
writer=csv.writer(f3,delimiter ="|",quoting=csv.QUOTE_NONE,quotechar='')
writer.writerow(['PROMO_CODE','PROMO_DESC','START_DATE','END_DATE','PROMO_TYPE','PROMO_DISC_TYPE','PROMO_DISC_VALUE','APP_TYPE','COUNTRY_CODE','BARCODE','PROD_CAT_NAME','PROD_BRAND_CODE','FREE_PROD','COMBO_GRP_DISC','MIN','MAX','OUTLET_UNIQUE_CODE','OUTLET_CAP_QTY','UPDATED_TIME'])
cursor.execute(outbound_query3)
outbound_query3_data = cursor.fetchall()
for row in outbound_query3_data:
	f3_ctl_rowcount=f3_ctl_rowcount+1
	writer.writerow(row)
	
# Write Product Master CTL file 
writer=csv.writer(f1_ctl)
writer.writerow([outbound_filename_product_control+'.csv'])
f1_ctl.write('Number of Rows\n')
f1_ctl.write(str(f1_ctl_rowcount))

# Write Outlet master CTL file 
writer=csv.writer(f2_ctl)
writer.writerow([outbound_filename_outlet_control+'.csv'])
f2_ctl.write('Number of Rows\n')
f2_ctl.write(str(f2_ctl_rowcount))


# Write Promo master CTL file 
writer=csv.writer(f3_ctl)
writer.writerow([outbound_filename_promo_control+'.csv'])
f3_ctl.write('Number of Rows\n')
f3_ctl.write(str(f3_ctl_rowcount))

f1.close()
f2.close()
f3.close()
f1_ctl.close()
f2_ctl.close()
f3_ctl.close()
db.commit()
cursor.close()
db.close()
