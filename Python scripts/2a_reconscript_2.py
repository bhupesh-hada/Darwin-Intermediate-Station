#!/usr/bin/python

import pymysql
import csv
import os
from datetime import datetime
import fnmatch
import os.path


db = pymysql.connect(host="##########", port=3306, user="dbuser", passwd="#######", db="NEW_EMPORIO_GENESIS_IDN",local_infile=True)
cursor = db.cursor()

directory_ir="/home/FRACTAL/gokulakrishnan.s/From-Emporio/"+datetime.now().date().strftime('%Y%m%d')
directory_ir_reports="/home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/Output/"+datetime.now().date().strftime('%Y%m%d')

if not os.path.exists(directory_ir_reports):
    os.makedirs(directory_ir_reports) 

filename_log = directory_ir_reports+"/reconscript_3_IR_log_"+datetime.now().strftime("%Y-%m-%d-%H%M%S")

flog= open(filename_log+'.log', 'w')
        
query1 = """select country_code,outlet_unique_code,outlet_name, now() from NEW_EMPORIO_GENESIS_IDN.Outlet_staging_emporio 
where outlet_unique_code not in (select distinct (outlet_unique_code) from NEW_EMPORIO_GENESIS_IDN.Transactions_emporio_staging)"""

query2 = """select country_code,outlet_unique_code, now() from NEW_EMPORIO_GENESIS_IDN.Transactions_emporio_staging
where outlet_unique_code not in (select distinct (outlet_unique_code) from NEW_EMPORIO_GENESIS_IDN.Outlet_staging_emporio)"""

query3 = """select a.*, Now() As CREATED_DTM from (select  count(distinct (a.bill_no)), a.Country_Code,a.OUTLET_UNIQUE_CODE, b.outlet_name 
from NEW_EMPORIO_GENESIS_IDN.Transactions_emporio_staging a, NEW_EMPORIO_GENESIS_IDN.Outlet_staging_emporio b
where a.OUTLET_UNIQUE_CODE=b.OUTLET_UNIQUE_CODE
group by a.OUTLET_UNIQUE_CODE, a.Country_Code, b.outlet_name) a"""

query4 = """select BARCODE,outlet_unique_code,MRP, now() from NEW_EMPORIO_GENESIS_IDN.Transactions_emporio_staging
where BARCODE not in (select distinct (BARCODE) from NEW_EMPORIO_GENESIS_IDN.Product_staging_emporio where NEW_EMPORIO_GENESIS_IDN.Product_staging_emporio.COMPANY_CODE like 'Unilever%'"""

query4a= """select NEW_EMPORIO_GENESIS_IDN.Transactions_emporio_staging.BARCODE,NEW_EMPORIO_GENESIS_IDN.Transactions_emporio_staging.outlet_unique_code,NEW_EMPORIO_GENESIS_IDN.Transactions_emporio_staging.MRP, now() from NEW_EMPORIO_GENESIS_IDN.Transactions_emporio_staging
LEFT JOIN NEW_EMPORIO_GENESIS_IDN.Product_staging_emporio
ON NEW_EMPORIO_GENESIS_IDN.Transactions_emporio_staging.BARCODE = NEW_EMPORIO_GENESIS_IDN.Product_staging_emporio.BARCODE
where NEW_EMPORIO_GENESIS_IDN.Product_staging_emporio.BARCODE IS NULL;"""

#query5=""" insert into NEW_EMPORIO_GENESIS_IDN.Transactions_emporio (select * from NEW_EMPORIO_GENESIS_IDN.Transactions_emporio_staging)"""

filename1 = "MISSINGSTOREINTRANS_IDN_"+datetime.now().strftime("%Y%m%d-%H%M%S")
filename2 = "MISSINGSTOREINMASTER_IDN_"+datetime.now().strftime("%Y%m%d-%H%M%S")
filename3 = "NUMBILLSPERSTORE_IDN_"+datetime.now().strftime("%Y%m%d-%H%M%S")
filename4 = "MISSINGPRODINMASTER_IDN_"+datetime.now().strftime("%Y%m%d-%H%M%S")

print(filename1)
print(filename2)
print(filename3)
print(filename4)

####### creating the report for missing store in transactions####################

cursor.execute(query1)
data1 = cursor.fetchall()

f1 = open(directory_ir_reports+"/"+filename1+'.csv', 'w')
writer=csv.writer(f1)
writer.writerow(['COUNTRY_CODE','OUTLET_UNIQUE_CODE','OUTLET_NAME','CREATED_DTM'])
for row in data1:
    # writer.writerow([str(row)])
    writer.writerow(row)


    
f1.close()

flog.write("Output generated successfully for missing store in trans\r\n")

####### creating the report for missing store in masters####################
    
cursor.execute(query2)
data2 = cursor.fetchall()

f2 = open(directory_ir_reports+"/"+filename2+'.csv', 'w')
writer=csv.writer(f2)
writer.writerow(['COUNTRY_CODE','OUTLET_UNIQUE_CODE','CREATED_DTM'])
for row in data2:
    # writer.writerow([str(row)])
    writer.writerow(row)

    
f2.close()

flog.write("Output generated successfully for missing store in master for TEST\r\n")

####### creating the report for num of bills per store per vendor####################

cursor.execute(query3)
data3 = cursor.fetchall()


f3 = open(directory_ir_reports+"/"+filename3+'.csv', 'w')
writer=csv.writer(f3)
writer.writerow(['NUM_OF_BILLS','COUNTRY_CODE','OUTLET_UNIQUE_CODE','OUTLET_NAME','CREATED_DTM'])
for row in data3:
    # writer.writerow([str(row)])
    writer.writerow(row)

    
f3.close()

flog.write("Output generated successfully for num of bills per store for TEST\r\n")


####### creating the report for missing prod in transactions####################
cursor.execute(query4a)
data4 = cursor.fetchall()

f4 = open(directory_ir_reports+"/"+filename4+'.csv', 'w')
writer=csv.writer(f4)
writer.writerow(['BARCODE','OUTLET_CODE','PRICE/MRP','CREATED_DTM'])
for row in data4:
    # writer.writerow([str(row)])
    writer.writerow(row)

    
f4.close()

flog.write("Output generated successfully for missing prod in trans for TEST\r\n")

####### moving the data from staging trans to actual trans table####################
#cursor.execute(query5)
db.commit()

flog.write("End of Recon script 3 for TEST\r\n")

cursor.close()

flog.close()
db.close()
