# THINGS TO CHECK BEFORE RUNNING THIS SCRIPT:
# 1) Check if the COMPANY_CODE is 'UNILEVER' / 'Unilever' / '1' / '20' etc. Update the queries accordingly.
# 2) Check the inbound file / outbound error file directories if they are correct
# 3) This RUNS on EMPORIO_GENESIS_IDN DB Master files.  Do you mean to run on staging / other tables?

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

outbound_filename_areaValShare= directorydest+"/IDN_AreaValShare_"+datetime.now().strftime("%Y%m%d%H%M%S")
outbound_filename_areaValShare_control= "IDN_AreaValShare_"+datetime.now().strftime("%Y%m%d%H%M%S")

f1_ctl_rowcount=0
date_today=datetime.now().date().strftime('%d')

# DB CONNECTION DEFINITIONS 
db = pymysql.connect(host="#########", port=3306, user="dbuser", passwd="#########", db="NEW_EMPORIO_GENESIS_IDN",local_infile=True)
cursor = db.cursor()

# DEFINING QUERIES for Outbound Extract - AREA VALUE SHARE

outbound_query1 = """call test_AreaValShare;"""

# OPEN OUTBOUND Area Value Share extract files 
f1 = open(outbound_filename_areaValShare+'.csv', 'w')
f1_ctl = open(outbound_filename_areaValShare+'.ctl', 'w')

# WRITE OUTBOUND Area Value Share extract files 
writer=csv.writer(f1,delimiter ="|",quoting=csv.QUOTE_NONE,quotechar='')
writer.writerow(['COUNTRY_CODE','OUTLET_UNIQUE_CODE','COUNTER_NUM','BILL_NO','BILL_DATE','TOTAL_LINE','CUST_NAME','CUST_PH_NUM','BILL_LEVEL_TAX_VAL','BILL_LEVEL_DISC_VAL','ONLINE_FLAG','UPDATED_TIME','CUST_TYPE','LINE_ITEM_DISC_VAL','PROD_BRAND','PROD_CATEGORY','PROD_SUB_CATEGORY','QTY','TOTAL_SALES_AMT'])
cursor.execute(outbound_query1)
outbound_query1_data = cursor.fetchall()
for row in outbound_query1_data:
	f1_ctl_rowcount=f1_ctl_rowcount+1
	writer.writerow(row)

# Write Area Value Share control file 
writer=csv.writer(f1_ctl)
writer.writerow([outbound_filename_areaValShare_control+'.csv'])
f1_ctl.write('Number of Rows\n')
f1_ctl.write(str(f1_ctl_rowcount))

f1.close()
db.commit()
cursor.close()
db.close()
