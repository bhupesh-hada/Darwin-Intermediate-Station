#!/usr/bin/python

import pymysql
import csv
import os
from datetime import datetime
import fnmatch
import os.path
import time

# SOURCE, DESTINATION AND LOG FOLDERS DEFINITIONS 
directory="/home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/Output/"
directory_input="/data/From-Emporio/"
#directory_reports="/home/FRACTAL/gokulakrishnan.s/gokul_scripts/IDN_scripts/IDN-Genesis-scripts/Output/"
directory_reports="/home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/Output/"+datetime.now().date().strftime('%Y%m%d')

# CREATE DESTINATION DIRECTORIES IF NOT EXISTING 
if not os.path.exists(directory):
    os.makedirs(directory) 
	
if not os.path.exists(directory_reports):
    os.makedirs(directory_reports) 
	
#DEFINING INPUT FILE NAME WILDCARD NAMES WITH "*.CSV" 
#DEFINING OUTPUT ERROR FILE NAMES 
outlet_error_filename = "OUTLET_RECON_Report_2_IDN_"+datetime.now().strftime("%Y%m%d-%H%M%S")
product_error_filename = "PRODUCT_RECON_Report_2_IDN_"+datetime.now().strftime("%Y%m%d-%H%M%S")
promo_error_filename = "PROMO_RECON_Report_2_IDN_"+datetime.now().strftime("%Y%m%d-%H%M%S")
txn_error_filename = "TRANSACTION_RECON_Report_2_IDN_"+datetime.now().strftime("%Y%m%d-%H%M%S")
filename_log = directory_reports+"/RECON_script_2_IDN_log_"+datetime.now().strftime("%Y-%m-%d-%H%M%S")

DB_name='';
outlet_master_table='';
outlet_staging_table='';
product_master_table='';
product_staging_table='';
promo_master_table='';
promo_staging_table='';
txn_master_table='';
txn_staging_table='';
txn_master_table_with_ProdID='';
txn_staging_table_with_ProdID='';
mapping_master_table='';
mapping_staging_table='';


# START- SECTION TO READ META DATA FILE 
with open(directory+'from-IDN-metadata.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            line_count += 1
        elif line_count ==1:
            LAST_FILENAME_READ = row[0];
            line_count += 1
	else:
	    LAST_EXTRACT_DATE = row[0];

#START SECTION TO READ RUNTIME META DATA FILE FOR MAPPING
with open(directory+'runtime_metafile_mapping.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    for row in csv_reader: 	
    	filename_mapping=row[0]	



# START- SECTION TO READ DATABASE META DATA FILE FOR DB NAME AND TABLE NAMES
#writer.writerow([datetime.now().strftime("%Y%m%d - %H%M%S")+ " : Reading FROM-DATABASE META FILE."])

with open(directory+'database-metadata.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    row1=next(csv_reader)
    row2=next(csv_reader)
    row3=next(csv_reader)
    row4=next(csv_reader)
    row5=next(csv_reader)
    row6=next(csv_reader)
    row7=next(csv_reader) 
    row8=next(csv_reader)
    row9=next(csv_reader)
    row10=next(csv_reader)
    row11=next(csv_reader)
    row12=next(csv_reader)
    row13=next(csv_reader)
    row14=next(csv_reader)
    row15=next(csv_reader)
    row16=next(csv_reader)
    row17=next(csv_reader)
    row18=next(csv_reader)
    row19=next(csv_reader)
    row20=next(csv_reader)
    row21=next(csv_reader)
    row22=next(csv_reader)
    row23=next(csv_reader)
    row24=next(csv_reader)
    row25=next(csv_reader)
    row26=next(csv_reader)

    DB_name=row2[0];
    outlet_master_table=row4[0];
    outlet_staging_table=row6[0];
    product_master_table=row8[0];
    product_staging_table=row10[0];
    promo_master_table=row12[0];
    promo_staging_table=row14[0];
    txn_master_table=row16[0];
    txn_staging_table=row18[0];
    txn_master_table_with_ProdID=row20[0];
    txn_staging_table_with_ProdID=row22[0];
    mapping_master_table=row24[0];
    mapping_staging_table=row26[0];

# END - SECTION TO READ DATABASE META DATA FILE 

# DB CONNECTION DEFINITIONS 
db = pymysql.connect(host="#########", port=3306, user="dbuser", passwd="#########", db=DB_name,local_infile=True)
cursor = db.cursor()


# DEFINING QUERIES for NULL VALUE ERROR checks in PRODUCT Master TABLE
product_query1 = """select * from """+DB_name+"""."""+product_staging_table+""" where BARCODE = \"\" OR BARCODE IS NULL;""" 
product_query2 = """select * from """+DB_name+"""."""+product_staging_table+""" where PRICE = \"\" OR PRICE IS NULL;"""
product_query3 = """select * from """+DB_name+"""."""+product_staging_table+""" where PROD_CATEGORY = \"\" OR PROD_CATEGORY IS NULL;"""
product_query4 = """select * from """+DB_name+"""."""+product_staging_table+""" where PROD_NAME = \"\" OR PROD_NAME IS NULL;"""
product_query5 = """select * from """+DB_name+"""."""+product_staging_table+""" where STATUS = \"\" OR STATUS IS NULL;"""
product_query6 = """select * from """+DB_name+"""."""+product_staging_table+""" where COUNTRY_CODE = \"\" OR COUNTRY_CODE IS NULL;"""
product_query7 = """select * from """+DB_name+"""."""+product_staging_table+""" where UPDATED_TIME = \"\" OR UPDATED_TIME ='0000-00-00 00:00:00';""" 
product_query8 = """select * from """+DB_name+"""."""+product_staging_table+""" where CREATED_TIME = \"\";"""


# DEFINING QUERIES for NULL VALUE ERROR checks in OUTLET Master TABLE
outlet_query1 = """select * from """+DB_name+"""."""+outlet_staging_table+""" where OUTLET_UNIQUE_CODE= \"\" OR OUTLET_UNIQUE_CODE IS NULL;""" 
outlet_query2 = """select * from """+DB_name+"""."""+outlet_staging_table+""" where OUTLET_NAME= \"\" OR OUTLET_NAME IS NULL;"""
outlet_query3 = """select * from """+DB_name+"""."""+outlet_staging_table+""" where COUNTRY_CODE = \"\" OR COUNTRY_CODE IS NULL;"""
outlet_query4 = """select * from """+DB_name+"""."""+outlet_staging_table+""" where CITY = \"\" OR CITY IS NULL;"""
outlet_query5 = """select * from """+DB_name+"""."""+outlet_staging_table+""" where ZIP_CODE = \"\" OR ZIP_CODE IS NULL;"""
outlet_query6 = """select * from """+DB_name+"""."""+outlet_staging_table+""" where STATUS = \"\" OR STATUS IS NULL;"""
outlet_query7 = """select * from """+DB_name+"""."""+outlet_staging_table+""" where UPDATED_TIME = \"\" OR UPDATED_TIME ='0000-00-00 00:00:00';"""
outlet_query8 = """select * from """+DB_name+"""."""+outlet_staging_table+""" where CREATED_TIME = \"\" OR CREATED_TIME ='0000-00-00 00:00:00';"""

# DEFINING QUERIES for NULL VALUE ERROR checks in TXN Staging TABLE
txn_query1 = """select * from """+DB_name+"""."""+txn_staging_table+""" where COUNTRY_CODE= \"\" OR COUNTRY_CODE IS NULL;""" 
txn_query2 = """select * from """+DB_name+"""."""+txn_staging_table+""" where OUTLET_UNIQUE_CODE= \"\" OR OUTLET_UNIQUE_CODE IS NULL;"""
txn_query3 = """select * from """+DB_name+"""."""+txn_staging_table+""" where TOT_BILL_AMT IS NULL;"""
txn_query4 = """select * from """+DB_name+"""."""+txn_staging_table+""" where BILL_NO = \"\" OR BILL_NO IS NULL;"""
txn_query5 = """select * from """+DB_name+"""."""+txn_staging_table+""" where BILL_DATE = \"\" OR BILL_DATE IS NULL;"""
txn_query6 = """select * from """+DB_name+"""."""+txn_staging_table+""" where TOTAL_LINE = \"\" OR TOTAL_LINE IS NULL;"""
txn_query7 = """select * from """+DB_name+"""."""+txn_staging_table+""" where UPDATED_TIME = \"\" OR UPDATED_TIME ='0000-00-00 00:00:00';"""
txn_query8 = """select * from """+DB_name+"""."""+txn_staging_table+""" where CREATED_TIME = \"\" OR CREATED_TIME ='0000-00-00 00:00:00';"""
txn_query9 = """select * from """+DB_name+"""."""+txn_staging_table+""" where CURR_SALES_PRICE IS NULL;"""
txn_query10 = """select * from """+DB_name+"""."""+txn_staging_table+""" where MRP IS NULL;"""
txn_query11 = """select * from """+DB_name+"""."""+txn_staging_table+""" where QTY= \"\" OR QTY IS NULL;"""
txn_query12 = """select * from """+DB_name+"""."""+txn_staging_table+""" where BARCODE = '0' OR BARCODE = \"\" OR BARCODE IS NULL;"""
txn_query13 = """select * from """+DB_name+"""."""+txn_staging_table+""" where TOTAL_LINE_AMT IS NULL;"""

#DEFINING QUERIES for NULL VALUE ERROR checks in Promo Staging TABLE
promo_query1 = """select * from """+DB_name+"""."""+promo_staging_table+""" where PROMO_CODE= \"\" OR PROMO_CODE IS NULL;""" 
promo_query2 = """select * from """+DB_name+"""."""+promo_staging_table+""" where PROMO_DESC= \"\" OR PROMO_DESC IS NULL;"""
promo_query3 = """select * from """+DB_name+"""."""+promo_staging_table+""" where START_DATE IS NULL OR START_DATE ='0000-00-00 00:00:00';"""
promo_query4 = """select * from """+DB_name+"""."""+promo_staging_table+""" where END_DATE IS NULL OR END_DATE ='0000-00-00 00:00:00';"""
promo_query5 = """select * from """+DB_name+"""."""+promo_staging_table+""" where PROMO_TYPE = \"\" OR PROMO_TYPE IS NULL;"""
promo_query6 = """select * from """+DB_name+"""."""+promo_staging_table+""" where PROMO_DISC_TYPE = \"\" OR PROMO_DISC_TYPE IS NULL;"""
promo_query7 = """select * from """+DB_name+"""."""+promo_staging_table+""" where FREE_PROD = \"\" OR FREE_PROD IS NULL;"""
promo_query8 = """select * from """+DB_name+"""."""+promo_staging_table+""" where OUTLET_UNIQUE_CODE = \"\" OR OUTLET_UNIQUE_CODE IS NULL;"""
promo_query9 = """select * from """+DB_name+"""."""+promo_staging_table+""" where OUTLET_CAP_QTY  = \"\" OR OUTLET_CAP_QTY IS NULL;"""
promo_query10 = """select * from """+DB_name+"""."""+promo_staging_table+""" where UPDATED_TIME = \"\" OR UPDATED_TIME ='0000-00-00 00:00:00';"""
promo_query11 = """select * from """+DB_name+"""."""+promo_staging_table+""" where CREATED_TIME = \"\" OR CREATED_TIME ='0000-00-00 00:00:00';"""

# OPENING LOG FILE
flog= open(filename_log+'.log', 'w')

# BEGIN SECTION TO LOAD STAGING TABLES

#FOR LOOP - Description
#For each file name in the metadata file, truncate the staging table and load the correct .csv file into the staging table and generate error reports

# START- SECTION TO READ RUNTIME META DATA FILE FOR TRANSACTION, LOAD TO STAGING & GENERATE RECON REPORTS, ACTION FLAG AND UPDATE MASTERs

with open(directory+'runtime_metafile_txn.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    for row in csv_reader:
	filename_ir = row[0];
        print "Entered Transaction loop for: "+filename_ir
	query_trunc_trans="TRUNCATE table "+DB_name+"."+txn_staging_table_with_ProdID+";"
	cursor.execute(query_trunc_trans)
	query_trans = "load data local infile '"+directory_input+"/"+filename_ir+"' INTO table "+DB_name+"."+txn_staging_table_with_ProdID+" FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES SET CREATED_TIME = CURRENT_TIMESTAMP;"
	filename_IR_trans=filename_ir
	print "Truncated Transactions_emporio_staging_with_ProductID"
	cursor.execute(query_trans)
	print "Truncated Mapping_master_emporio"
	query_trunc_map="TRUNCATE table "+DB_name+"."+mapping_master_table+";"
	cursor.execute(query_trunc_map)
	print "Setting up mapping master table"
	query_mapping="load data local infile '"+directory_input+"/"+filename_mapping+"' INTO table "+DB_name+"."+mapping_master_table+" FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES SET CREATED_TIME = CURRENT_TIMESTAMP;"
	cursor.execute(query_mapping)
	print "Removing duplicates from mapping master table"
	query_remove_duplicates="""call test_remove_duplicates;"""
	cursor.execute(query_remove_duplicates)
	print "Setting up staging table to generate extracts"
	query_trunc_trans = "TRUNCATE table "+DB_name+"."+txn_staging_table+";"
	cursor.execute(query_trunc_trans)
	print "Truncated Transactions_emporio_staging"
	# DEFINING QUERIES for mapping ProductID with Barcode  
	mapping_query = """call test_mapped_file;"""
	cursor.execute(mapping_query)
#	query_trunc_trans = "TRUNCATE table "+DB_name+"."+txn_staging_table+";"
#	query_trans = "load data local infile '"+directory_input+"/"+filename_ir+"' INTO table "+DB_name+"."+txn_staging_table+" FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES SET CREATED_TIME = CURRENT_TIMESTAMP;" 
#	filename_IR_trans=filename_ir
#	cursor.execute(query_trunc_trans)
#	print "Truncated Transactions_emporio_staging"
#	cursor.execute(query_trans)
	flog.write("Loading success for Transactions_emporio_staging\r\n")
	db.commit()
	print ("Loading success for Transactions_emporio_staging")
	# Write the output from the error checks for Transaction Master into single Transaction Master Error Output file
	f5 = open(directory_reports+"/"+txn_error_filename+'.csv', 'w')
	writer=csv.writer(f5)
	f5.write("FILENAME PROCESSED: "+filename_IR_trans+"\n")
	f5.write('============================================================= \n')
	# txn ERROR FILE _ CHECK 1:  NULL VALUES FOR MANDATORY COUNTRY_CODE COLUMN
	f5.write('\nCHECK 1: NULL VALUES FOUND FOR NOT NULLABLE COUNTRY_CODE COLUMN \n')
	f5.write('============================================================= \n')
	cursor.execute(txn_query1)
	txn_query1_data = cursor.fetchall()
	text_string="'COUNTRY_CODE', 'OUTLET_UNIQUE_CODE','TOT_BILL_AMT', 'COUNTER_NUM', 'BILL_NO', 'BILL_DATE', 'TOTAL_LINE', 'CUST_NAME', 'CUST_PH_NUM', 'BILL_LEVEL_TAX_VAL', 'BILL_LEVEL_DISC_VAL', 'ONLINE_FLAG', 'UPDATED_TIME', 'CUST_TYPE', 'LINE_ITEM_DISC_VAL', 'PROM_DISC_APPL', 'PROM_DISC_CODE', 'CURR_SALES_PRICE', 'MRP', 'QTY', 'BARCODE', 'TOTAL_LINE_AMT', 'FREE_PROD_FLAG', 'CREATED_TIME'"
	f5.write(text_string+"\n")
	for row in txn_query1_data :
		# writer.writerow([str(row)])
		writer.writerow(row)
	# txn ERROR FILE _ CHECK 2:  NULL VALUES FOR MANDATORY OUTLET_UNIQUE_CODE COLUMN
	f5.write('\nCHECK 2: NULL VALUES FOUND FOR NOT NULLABLE OUTLET_UNIQUE_CODE COLUMN \n')
	f5.write('============================================================= \n')
	cursor.execute(txn_query2)
	txn_query2_data = cursor.fetchall()
	f5.write(text_string+"\n")
	for row in txn_query2_data :
		# writer.writerow([str(row)])
		writer.writerow(row)
		
	# txn ERROR FILE _ CHECK 3:  NULL VALUES FOR MANDATORY TOT_BILL_AMT COLUMN
	f5.write('\nCHECK 3: NULL VALUES FOUND FOR NOT NULLABLE TOT_BILL_AMT COLUMN \n')
	f5.write('============================================================= \n')
	cursor.execute(txn_query3)
	txn_query3_data = cursor.fetchall()
	f5.write(text_string+"\n")
	for row in txn_query3_data :
	# writer.writerow([str(row)])
		writer.writerow(row)
			
	# txn ERROR FILE _ CHECK 4:  NULL VALUES FOR MANDATORY BILL_NO COLUMN
	f5.write('\nCHECK 4: NULL VALUES FOUND FOR NOT NULLABLE BILL_NO COLUMN \n')
	f5.write('============================================================= \n')
	cursor.execute(txn_query4)
	txn_query4_data = cursor.fetchall()
	f5.write(text_string+"\n")
	for row in txn_query4_data :
		# writer.writerow([str(row)])
		writer.writerow(row)
		# txn ERROR FILE _ CHECK 5:  NULL VALUES FOR MANDATORY BILL_DATE COLUMN
	f5.write('\nCHECK 5: NULL VALUES FOUND FOR NOT NULLABLE BILL_DATE COLUMN \n')
	f5.write('============================================================= \n')
	cursor.execute(txn_query5)
	txn_query5_data = cursor.fetchall()
	f5.write(text_string+"\n")
	for row in txn_query5_data :
		# writer.writerow([str(row)])
		writer.writerow(row)
		# txn ERROR FILE _ CHECK 6:  NULL VALUES FOR MANDATORY TOTAL_LINE COLUMN
	f5.write('\nCHECK 6: NULL VALUES FOUND FOR NOT NULLABLE TOTAL_LINE COLUMN \n')
	f5.write('============================================================= \n')
	cursor.execute(txn_query6)
	txn_query6_data = cursor.fetchall()
	f5.write(text_string+"\n")
	for row in txn_query6_data :
		# writer.writerow([str(row)])
		writer.writerow(row)
	# txn ERROR FILE _ CHECK 7:  NULL VALUES FOR MANDATORY UPDATED_TIME COLUMN
	f5.write('\nCHECK 7: NULL VALUES FOUND FOR NOT NULLABLE UPDATED_TIME COLUMN \n')
	f5.write('============================================================= \n')
	cursor.execute(txn_query7)
	txn_query7_data = cursor.fetchall()
	f5.write(text_string+"\n")
	for row in txn_query7_data :
		# writer.writerow([str(row)])
		writer.writerow(row)
		# txn ERROR FILE _ CHECK 8:  NULL VALUES FOR MANDATORY CREATED_TIME COLUMN
	f5.write('\nCHECK 8: NULL VALUES FOUND FOR NOT NULLABLE CREATED_TIME COLUMN \n')
	f5.write('============================================================= \n')
	cursor.execute(txn_query8)
	txn_query8_data = cursor.fetchall()
	f5.write(text_string+"\n")
	for row in txn_query8_data :
		# writer.writerow([str(row)])
		writer.writerow(row)
	
	# txn ERROR FILE _ CHECK 9:  NULL VALUES FOR MANDATORY CURR_SALES_PRICE COLUMN
	f5.write('\nCHECK 9: NULL VALUES FOUND FOR NOT NULLABLE CURR_SALES_PRICE COLUMN \n')
	f5.write('============================================================= \n')
	cursor.execute(txn_query9)
	txn_query9_data = cursor.fetchall()
	f5.write(text_string+"\n")
	for row in txn_query9_data :
		# writer.writerow([str(row)])
		writer.writerow(row)
		# txn ERROR FILE _ CHECK 10:  NULL VALUES FOR MANDATORY MRP COLUMN
	f5.write('\nCHECK 10: NULL VALUES FOUND FOR NOT NULLABLE MRP COLUMN \n')
	f5.write('============================================================= \n')
	cursor.execute(txn_query10)
	txn_query10_data = cursor.fetchall()
	f5.write(text_string+"\n")
	for row in txn_query10_data :
		# writer.writerow([str(row)])
		writer.writerow(row)
	# txn ERROR FILE _ CHECK 11:  NULL VALUES FOR MANDATORY QTY COLUMN
	f5.write('\nCHECK 11: NULL VALUES FOUND FOR NOT NULLABLE QTY COLUMN \n')
	f5.write('============================================================= \n')
	cursor.execute(txn_query11)
	txn_query11_data = cursor.fetchall()
	f5.write(text_string+"\n")
	for row in txn_query11_data :
		# writer.writerow([str(row)])
		writer.writerow(row)
	# txn ERROR FILE _ CHECK 12:  NULL VALUES FOR MANDATORY BARCODE COLUMN
	f5.write('\nCHECK 12: NULL VALUES FOUND FOR NOT NULLABLE BARCODE COLUMN AFTER THE MAPPING HAS BEEN DONE \n')
	f5.write('============================================================= \n')
	cursor.execute(txn_query12)
	txn_query12_data = cursor.fetchall()
	f5.write(text_string+"\n")
	for row in txn_query12_data :
		# writer.writerow([str(row)])
		writer.writerow(row)
	# txn ERROR FILE _ CHECK 13:  NULL VALUES FOR MANDATORY TOTAL_LINE_AMT COLUMN
	f5.write('\nCHECK 13: NULL VALUES FOUND FOR NOT NULLABLE TOTAL_LINE_AMT COLUMN \n')
	f5.write('============================================================= \n')
	cursor.execute(txn_query13)
	txn_query13_data = cursor.fetchall()
	f5.write(text_string+"\n")
	for row in txn_query13_data :
		# writer.writerow([str(row)])
		writer.writerow(row)
	f5.close()
	flog.write("RECON report 2 generated successfully for TRANSACTION \r\n" + filename_IR_trans)
	print "RECON report 2 generated successfully for TRANSACTION : " + filename_IR_trans
	flog.write("RECON report 3 started for TRANSACTION \r\n" + filename_IR_trans)
	os.system("python /home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/2a_reconscript_2.py")
	flog.write("RECON report 3 generated successfully for TRANSACTION \r\n" + filename_IR_trans)
	flog.write("Starting Action Flag for TRANSACTION Staging\r\n")
	print "NOT Starting Action Flag for TRANSACTION Staging : " + filename_IR_trans
	#os.system("python /home/FRACTAL/gokulakrishnan.s/gokul_scripts/IDN_scripts/IDN-Genesis-scripts/3d_action-flag-txn-staging.py")
	flog.write("Completed Action Flag for TRANSACTION Staging\r\n")
	print "NOT Completed Action Flag for TRANSACTION Staging : "+ filename_IR_trans
	flog.write("Starting Update Masters for TRANSACTION \r\n")
	print "NOT Starting Update Masters for TRANSACTION" + filename_IR_trans
	#os.system("python /home/FRACTAL/gokulakrishnan.s/gokul_scripts/IDN_scripts/IDN-Genesis-scripts/4d_update-txn-master.py")
	flog.write("NOT Completed Update Masters for TRANSACTION \r\n")
	print "Completed Update Masters for TRANSACTION" + filename_IR_trans
#	INSERT DAILY TRANSACTION FILE WITH PRODUCT_ID INTO MASTER DB
	a=0
	print a
	b=1
	print b
	txn_insert_query="INSERT INTO "+DB_name+"."+txn_master_table_with_ProdID+" SELECT a.* FROM (select a.COUNTRY_CODE,a.OUTLET_UNIQUE_CODE,a.TOT_BILL_AMT,a.COUNTER_NUM,a.BILL_NO,a.BILL_DATE,a.TOTAL_LINE,a.CUST_NAME,a.CUST_PH_NUM,a.BILL_LEVEL_TAX_VAL,a.BILL_LEVEL_DISC_VAL,a.ONLINE_FLAG,a.UPDATED_TIME,a.CUST_TYPE,a.LINE_ITEM_DISC_VAL,a.PROM_DISC_APPL,a.PROM_DISC_CODE,a.CURR_SALES_PRICE,a.MRP,a.QTY,a.TOTAL_LINE_AMT,a.FREE_PROD_FLAG,a.PRODUCT_ID,(case when b.BARCODE=\"\" or b.BARCODE IS NULL then "+str(a)+" else "+str(b)+" end) as MAPPED,a.CREATED_TIME from NEW_EMPORIO_GENESIS_IDN.Transactions_emporio_staging_with_ProdID a LEFT JOIN NEW_EMPORIO_GENESIS_IDN.Mapping_master_emporio b ON a.PRODUCT_ID = b.PRODUCT_ID) a;" 
#	txn_insert_query="""load data local infile '/mnt/From-Emporio/"""+filename_IR_trans+"""' into table EMPORIO_GENESIS_IDN.Transactions_emporio FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' IGNORE 1 LINES  SET CREATED_TIME=CURRENT_TIMESTAMP;"""
	print(txn_insert_query)
	cursor.execute(txn_insert_query)
       
#	INSERT DAILY TRANSACTION FILE WITH BARCODE INTO MASTER DB
	txn_insert_query = "INSERT INTO "+DB_name+"."+txn_master_table+" SELECT COUNTRY_CODE,OUTLET_UNIQUE_CODE,TOT_BILL_AMT,COUNTER_NUM,BILL_NO,BILL_DATE,TOTAL_LINE,CUST_NAME,CUST_PH_NUM,BILL_LEVEL_TAX_VAL,BILL_LEVEL_DISC_VAL,ONLINE_FLAG,UPDATED_TIME,CUST_TYPE,LINE_ITEM_DISC_VAL,PROM_DISC_APPL,PROM_DISC_CODE,CURR_SALES_PRICE,MRP,QTY,BARCODE,TOTAL_LINE_AMT,FREE_PROD_FLAG,CREATED_TIME from "+txn_staging_table+";" 
	print(txn_insert_query)
	cursor.execute(txn_insert_query)
# WRITE LIST OF DATES IN THIS FILE TO RUNTIME_TXN_DATES_LIST file
	txn_list_dates_query = """select DISTINCT (DATE(BILL_DATE)) from """+DB_name+"""."""+txn_staging_table_with_ProdID+""";"""
	f5 = open(directory+"/"+'runtime_txn_dates_list.csv', 'a')
	writer=csv.writer(f5)
#	f5.write("FILENAME PROCESSED: "+filename_IR_trans+"\n")
	cursor.execute(txn_list_dates_query)
	txn_list_dates_data = cursor.fetchall()
	for row in txn_list_dates_data:
		writer.writerow(row)
	f5.close()
	# WRITE LIST OF AFFECTED TUPLES (BILLDATE, BILL NO, OUTLET) IN A FILE TO APPEND TO TODAY'S OUTBOUND EXTRACT
	txn_tuples_query = """select DISTINCT (DATE(BILL_DATE)), OUTLET_UNIQUE_CODE, BILL_NO from NEW_EMPORIO_GENESIS_IDN.Transactions_emporio_staging_with_ProdID where DATE(BILL_DATE)<='"""+LAST_EXTRACT_DATE+"'"""
	print "Txn tuples query:" + txn_tuples_query
	f5 = open(directory+"/"+'runtime_txn_tuples_list.csv', 'a')
	writer=csv.writer(f5)
	cursor.execute(txn_tuples_query)
	txn_tuples_data = cursor.fetchall()
	for row in txn_tuples_data:
		writer.writerow(row)
	f5.close()

# START- SECTION TO READ RUNTIME META DATA FILE FOR OUTLET, LOAD TO STAGING & GENERATE RECON REPORTS, ACTION FLAG AND UPDATE MASTERs
#with open(directory+'runtime_metafile_outlet.csv') as csv_file:
#    csv_reader = csv.reader(csv_file, delimiter=',')
#    for row in csv_reader:
#        filename_ir = row[0];
#        print "Entered Outlet loop for: "+filename_ir
#	query_trunc_outlet = "truncate table EMPORIO_GENESIS_IDN.Outlet_staging_emporio;"
#	query_outlet = "load data local infile '"+directory_input+filename_ir+"' into table EMPORIO_GENESIS_IDN.Outlet_staging_emporio FIELDS TERMINATED BY '\",\"' LINES TERMINATED BY '\n' IGNORE 1 LINES  SET CREATED_TIME=CURRENT_TIMESTAMP;"
#	filename_IR_outlet=filename_ir     
#	cursor.execute(query_trunc_outlet)
#	flog.write("Truncated Outlet_staging_emporio\r\n")
#	cursor.execute(query_outlet)
#	flog.write("Loading success for Outlet_staging_emporio\r\n")
#	db.commit()
#	print ("Loading success for Outlet_staging_emporio")   
#	# Write the output from the error checks for Outlet Master into single Outlet Master Error Output file
#	f5 = open(directory_reports+"/"+outlet_error_filename+'.csv', 'w')
#	writer=csv.writer(f5)
#	f5.write("FILENAME PROCESSED: "+filename_IR_outlet+"\n")
#	f5.write('============================================================= \n')
#
#	# outlet ERROR FILE _ CHECK 1:  NULL VALUES FOR MANDATORY OUTLET_UNIQUE_CODE COLUMN
#	f5.write('\nCHECK 1: NULL VALUES FOUND FOR NOT NULLABLE OUTLET_UNIQUE_CODE COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(outlet_query1)
#	outlet_query1_data = cursor.fetchall()
#	text_string="'OUTLET_UNIQUE_CODE', 'OUTLET_NAME', 'COUNTRY_CODE', 'OUTLET_TYPE', 'OUTLET_LEV_CODE', 'STATE_CODE', 'STREET_LAND_MARK', 'DISTRICT', 'CITY', 'ZIP_CODE', 'OUTLET_LATTITUDE', 'OUTLET_LONGITUDE', 'APP_TYPE', 'CURR', 'STATUS', 'UPDATED_TIME', 'CREATED_TIME'"
#	f5.write(text_string+"\n")
#	for row in outlet_query1_data :
#		# writer.writerow([str(row)])
#		writer.writerow(row)
#
#	# outlet ERROR FILE _ CHECK 2:  NULL VALUES FOR MANDATORY OUTLET_NAME COLUMN
#	f5.write('\nCHECK 2: NULL VALUES FOUND FOR NOT NULLABLE OUTLET_NAME COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(outlet_query2)
#	outlet_query2_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in outlet_query2_data :
#		# writer.writerow([str(row)])
#		writer.writerow(row)
#	
#	# outlet ERROR FILE _ CHECK 3:  NULL VALUES FOR MANDATORY COUNTRY_CODE COLUMN
#	f5.write('\nCHECK 3: NULL VALUES FOUND FOR NOT NULLABLE COUNTRY_CODE COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(outlet_query3)
#	outlet_query3_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in outlet_query3_data :
#		# writer.writerow([str(row)])
#		writer.writerow(row)
#		
#	# outlet ERROR FILE _ CHECK 4:  NULL VALUES FOR MANDATORY CITY COLUMN
#	f5.write('\nCHECK 4: NULL VALUES FOUND FOR NOT NULLABLE CITY COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(outlet_query4)
#	outlet_query4_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in outlet_query4_data :
#		# writer.writerow([str(row)])
#		writer.writerow(row)
#
#	# outlet ERROR FILE _ CHECK 5:  NULL VALUES FOR MANDATORY ZIP_CODE COLUMN
#	f5.write('\nCHECK 5: NULL VALUES FOUND FOR NOT NULLABLE ZIP_CODE COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(outlet_query5)
#	outlet_query5_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in outlet_query5_data :
#		# writer.writerow([str(row)])
#		writer.writerow(row)
#
#	# outlet ERROR FILE _ CHECK 6:  NULL VALUES FOR MANDATORY STATUS COLUMN
#	f5.write('\nCHECK 6: NULL VALUES FOUND FOR NOT NULLABLE STATUS COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(outlet_query6)
#	outlet_query6_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in outlet_query6_data :
#		# writer.writerow([str(row)])
#		writer.writerow(row)
#
#	# outlet ERROR FILE _ CHECK 7:  NULL VALUES FOR MANDATORY UPDATED_TIME COLUMN
#	f5.write('\nCHECK 7: NULL VALUES FOUND FOR NOT NULLABLE UPDATED_TIME COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(outlet_query7)
#	outlet_query7_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in outlet_query7_data :
#		# writer.writerow([str(row)])
#		writer.writerow(row)
#
#	# outlet ERROR FILE _ CHECK 8:  NULL VALUES FOR MANDATORY CREATED_TIME COLUMN
#	f5.write('\nCHECK 8: NULL VALUES FOUND FOR NOT NULLABLE CREATED_TIME COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(outlet_query8)
#	outlet_query8_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in outlet_query8_data :
#		# writer.writerow([str(row)])
#		writer.writerow(row)
#
#	f5.close()
#	flog.write("Output generated successfully for OUTLET for IDN \r\n")
#	flog.write("Starting Action Flag for OUTLET Staging\r\n")
#	#os.system("python /home/FRACTAL/gokulakrishnan.s/gokul_scripts/IDN_scripts/IDN-VIKRANT-EPOS-scripts/3a_action-flag-outlet-staging.py")
#	flog.write("Completed Action Flag for OUTLET Staging\r\n")
#	flog.write("Starting Update Masters for OUTLET \r\n")
#	#os.system("python /home/FRACTAL/gokulakrishnan.s/gokul_scripts/IDN_scripts/IDN-VIKRANT-EPOS-scripts/4a_update-outlet-master.py")
#	flog.write("Completed Update Masters for OUTLET  \r\n")
#
# START- SECTION TO READ RUNTIME META DATA FILE FOR PRODUCT, LOAD TO STAGING & GENERATE RECON REPORTS, ACTION FLAG AND UPDATE MASTERs
		
#with open(directory+'runtime_metafile_prod.csv') as csv_file:
#    csv_reader = csv.reader(csv_file, delimiter=',')
#    for row in csv_reader:
#       	filename_ir = row[0];
#        print "Entered Product loop for: "+filename_ir
#	query_trunc_prod = "truncate table EMPORIO_GENESIS_IDN.Product_staging_emporio;"
#	query_prod = "load data local infile '"+directory_input+filename_ir+"' into table EMPORIO_GENESIS_IDN.Product_staging_emporio FIELDS TERMINATED BY '\",\"' LINES TERMINATED BY '\n' IGNORE 1 LINES  SET CREATED_TIME=CURRENT_TIMESTAMP;"
#	filename_IR_prod=filename_ir
#	cursor.execute(query_trunc_prod)
#	flog.write("Truncated Product_staging_emporio \r\n")
#	cursor.execute(query_prod)
#	flog.write("Loading success for Product_staging_emporio \r\n")
#	db.commit()
#	print ("Loading success for Product_staging_emporio ")  
#
#	f5 = open(directory_reports+"/"+product_error_filename+'.csv', 'w')
#	writer=csv.writer(f5)
#	f5.write("FILENAME PROCESSED: "+filename_IR_prod+"\n")
#	f5.write('============================================================= \n')
 #
#	# PRODUCT ERROR FILE _ CHECK 1:  NULL VALUES FOR MANDATORY BARCODE COLUMN
#	f5.write('\nCHECK 1: NULL VALUES FOUND FOR NOT NULLABLE BARCODE COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(product_query1)
#	product_query1_data = cursor.fetchall()
#	text_string="'BARCODE', 'COMPANY_CODE', 'PROD_CATEGORY', 'PROD_SUB_CATEGORY', 'PACK_SIZE' , 'PACK_TYPE' , 'PROD_BRAND', 'BASE_PACK_CODE','PROD_NAME','STATE_CODE','PRICE','UOM','UOM_QTY','TAX_RATE','STATUS','COUNTRY_CODE','UPDATED_TIME','CREATED_TIME'"
#	f5.write(text_string+"\n")
#	for row in product_query1_data :
#	# writer.writerow([str(row)])
#		writer.writerow(row)
#
#	# PRODUCT ERROR FILE _ CHECK 2:  NULL VALUES FOR MANDATORY PRICE COLUMN
#	f5.write('\nCHECK 2: NULL VALUES FOUND FOR NOT NULLABLE PRICE COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(product_query2)
#	product_query2_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in product_query2_data :
#	# writer.writerow([str(row)])
#		writer.writerow(row)
#	
#	# PRODUCT ERROR FILE _ CHECK 3:  NULL VALUES FOR MANDATORY PROD_CATEGORY COLUMN
#	f5.write('\nCHECK 3: NULL VALUES FOUND FOR NOT NULLABLE PROD_CATEGORY COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(product_query3)
#	product_query3_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in product_query3_data :
#	# writer.writerow([str(row)])
#		writer.writerow(row)
#		
#	# PRODUCT ERROR FILE _ CHECK 4:  NULL VALUES FOR MANDATORY PROD_NAME COLUMN
#	f5.write('\nCHECK 4: NULL VALUES FOUND FOR NOT NULLABLE PROD_NAME COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(product_query4)
#	product_query4_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in product_query4_data :
#	# writer.writerow([str(row)])
#		writer.writerow(row)
##	# PRODUCT ERROR FILE _ CHECK 5:  NULL VALUES FOR MANDATORY STATUS COLUMN
#	f5.write('\nCHECK 5: NULL VALUES FOUND FOR NOT NULLABLE STATUS COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(product_query5)
#	product_query5_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in product_query5_data :
#	# writer.writerow([str(row)])
#		writer.writerow(row)
#
#	# PRODUCT ERROR FILE _ CHECK 6:  NULL VALUES FOR MANDATORY COUNTRY_CODE COLUMN
#	f5.write('\nCHECK 6: NULL VALUES FOUND FOR NOT NULLABLE COUNTRY_CODE COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(product_query6)
#	product_query6_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in product_query6_data :
#	# writer.writerow([str(row)])
#		writer.writerow(row)
#
#	# PRODUCT ERROR FILE _ CHECK 7:  NULL VALUES FOR MANDATORY UPDATED_TIME COLUMN
#	f5.write('\nCHECK 7: NULL VALUES FOUND FOR NOT NULLABLE UPDATED_TIME COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(product_query7)
#	product_query7_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in product_query7_data :
#	# writer.writerow([str(row)])
#		writer.writerow(row)
#
#	# PRODUCT ERROR FILE _ CHECK 8:  NULL VALUES FOR MANDATORY CREATED_TIME COLUMN
#	f5.write('\nCHECK 8: NULL VALUES FOUND FOR NOT NULLABLE CREATED_TIME COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(product_query8)
#	product_query8_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in product_query8_data :
#	# writer.writerow([str(row)])
#		writer.writerow(row)
#
#	f5.close()
#	flog.write("Output generated successfully for PRODUCT for IDN \r\n")
#	flog.write("Starting Action Flag for PRODUCT Staging\r\n")
#	#os.system("python /home/FRACTAL/gokulakrishnan.s/gokul_scripts/IDN_scripts/IDN-VIKRANT-EPOS-scripts/3c_action-flag-product-staging.py")
#	flog.write("Completed Action Flag for PRODUCT Staging\r\n")
#	flog.write("Starting Update Masters for PRODUCT \r\n")
#	#os.system("python /home/FRACTAL/gokulakrishnan.s/gokul_scripts/IDN_scripts/IDN-VIKRANT-EPOS-scripts/4c_update-product-master.py")
#	flog.write("Completed Update Masters for PRODUCT \r\n")
#		
## START- SECTION TO READ RUNTIME META DATA FILE FOR PROMO, LOAD TO STAGING & GENERATE RECON REPORTS, ACTION FLAG AND UPDATE MASTERs
#
#with open(directory+'runtime_metafile_promo.csv') as csv_file:
#    csv_reader = csv.reader(csv_file, delimiter=',')
#    for row in csv_reader:
#        filename_ir = row[0];
#        print "Entered Promo loop for: "+filename_ir
#	query_trunc_promo = "truncate table EMPORIO_GENESIS_IDN.Promo_staging_emporio;"
#	query_promo = "load data local infile '"+directory_input+filename_ir+"' into table EMPORIO_GENESIS_IDN.Promo_staging_emporio FIELDS TERMINATED BY '\",\"' LINES TERMINATED BY '\n' IGNORE 1 LINES  SET CREATED_TIME=CURRENT_TIMESTAMP;"
#	filename_IR_promo=filename_ir   
#	cursor.execute(query_trunc_promo)
#	flog.write("Truncated Promo_staging_emporio\r\n")
#	cursor.execute(query_promo)
#	flog.write("Loading success for Promo_staging_emporio\r\n")
#	db.commit()
#	print ("Loading success for Promo_staging_emporio")   
#	f5 = open(directory_reports+"/"+promo_error_filename+'.csv', 'w')
#	writer=csv.writer(f5)
#	f5.write("FILENAME PROCESSED: "+filename_IR_promo+"\n")
#	f5.write('============================================================= \n')
#	# promo ERROR FILE _ CHECK 1:  NULL VALUES FOR MANDATORY PROMO_CODE COLUMN
#	f5.write('\nCHECK 1: NULL VALUES FOUND FOR NOT NULLABLE PROMO_CODE COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(promo_query1)
#	promo_query1_data = cursor.fetchall()
#	text_string="'PROMO_CODE', 'PROMO_DESC', 'START_DATE', 'END_DATE', 'PROMO_TYPE', 'PROMO_DISC_TYPE', 'PROMO_DISC_VALUE', 'APP_TYPE', 'COUNTRY_CODE', 'BARCODE', 'PROD_CAT_NAME', 'PROD_BRAND_CODE', 'FREE_PROD', 'COMBO_GRP_DISC', 'MIN', 'MAX', 'OUTLET_UNIQUE_CODE', 'OUTLET_CAP_QTY', 'UPDATED_TIME', 'CREATED_TIME'"
#	f5.write(text_string+"\n")
#	for row in promo_query1_data :
#	# writer.writerow([str(row)])
#		writer.writerow(row)
#
#	# promo ERROR FILE _ CHECK 2:  NULL VALUES FOR MANDATORY PROMO_DESC COLUMN
#	f5.write('\nCHECK 2: NULL VALUES FOUND FOR NOT NULLABLE PROMO_DESC COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(promo_query2)
#	promo_query2_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in promo_query2_data :
#	# writer.writerow([str(row)])
#		writer.writerow(row)
#	
#	# promo ERROR FILE _ CHECK 3:  NULL VALUES FOR MANDATORY START_DATE COLUMN
#	f5.write('\nCHECK 3: NULL VALUES FOUND FOR NOT NULLABLE START_DATE COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(promo_query3)
#	promo_query3_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in promo_query3_data :
#	# writer.writerow([str(row)])
#		writer.writerow(row)
#		
#	# promo ERROR FILE _ CHECK 4:  NULL VALUES FOR MANDATORY END_DATE COLUMN
#	f5.write('\nCHECK 4: NULL VALUES FOUND FOR NOT NULLABLE END_DATE COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(promo_query4)
#	promo_query4_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in promo_query4_data :
#	# writer.writerow([str(row)])
#		writer.writerow(row)
#
#	# promo ERROR FILE _ CHECK 5:  NULL VALUES FOR MANDATORY PROMO_TYPE COLUMN
#	f5.write('\nCHECK 5: NULL VALUES FOUND FOR NOT NULLABLE PROMO_TYPE COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(promo_query5)
#	promo_query5_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in promo_query5_data :
#	# writer.writerow([str(row)])
#		writer.writerow(row)
#
#	# promo ERROR FILE _ CHECK 6:  NULL VALUES FOR MANDATORY PROMO_DISC_TYPE COLUMN
#	f5.write('\nCHECK 6: NULL VALUES FOUND FOR NOT NULLABLE PROMO_DISC_TYPE COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(promo_query6)
#	promo_query6_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in promo_query6_data :
#	# writer.writerow([str(row)])
#		writer.writerow(row)
#
#	# promo ERROR FILE _ CHECK 7:  NULL VALUES FOR MANDATORY FREE_PROD COLUMN
#	f5.write('\nCHECK 7: NULL VALUES FOUND FOR NOT NULLABLE FREE_PROD COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(promo_query7)
#	promo_query7_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in promo_query7_data :
#	# writer.writerow([str(row)])
#		writer.writerow(row)
#
#	# promo ERROR FILE _ CHECK 8:  NULL VALUES FOR MANDATORY OUTLET_UNIQUE_CODE COLUMN
#	f5.write('\nCHECK 8: NULL VALUES FOUND FOR NOT NULLABLE OUTLET_UNIQUE_CODE COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(promo_query8)
#	promo_query8_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in promo_query8_data :
#	# writer.writerow([str(row)])
#		writer.writerow(row)
#	
#	# promo ERROR FILE _ CHECK 9:  NULL VALUES FOR MANDATORY OUTLET_CAP_QTY COLUMN
#	f5.write('\nCHECK 9: NULL VALUES FOUND FOR NOT NULLABLE OUTLET_CAP_QTY COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(promo_query9)
#	promo_query9_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in promo_query9_data :
#	# writer.writerow([str(row)])
#		writer.writerow(row)
#
#	# promo ERROR FILE _ CHECK 10:  NULL VALUES FOR MANDATORY UPDATED_TIME COLUMN
#	f5.write('\nCHECK 10: NULL VALUES FOUND FOR NOT NULLABLE UPDATED_TIME COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(promo_query10)
#	promo_query10_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in promo_query10_data :
#	# writer.writerow([str(row)])
#		writer.writerow(row)
#
#	# promo ERROR FILE _ CHECK 11:  NULL VALUES FOR MANDATORY CREATED_TIME COLUMN
#	f5.write('\nCHECK 11: NULL VALUES FOUND FOR NOT NULLABLE CREATED_TIME COLUMN \n')
#	f5.write('============================================================= \n')
#	cursor.execute(promo_query11)
#	promo_query11_data = cursor.fetchall()
#	f5.write(text_string+"\n")
#	for row in promo_query11_data :
#	# writer.writerow([str(row)])
#		writer.writerow(row)
#
#	f5.close()
#	flog.write("Output generated successfully for Promo MASTER for IDN \r\n")
#	flog.write("Starting Action Flag for Promo Staging\r\n")
#	#os.system("python /home/FRACTAL/gokulakrishnan.s/gokul_scripts/IDN_scripts/IDN-VIKRANT-EPOS-scripts/3b_action-flag-promo-staging.py")
#	flog.write("Completed Action Flag for Promo Staging\r\n")
#	flog.write("Starting Update Masters for Promo \r\n")
#	#os.system("python /home/FRACTAL/gokulakrishnan.s/gokul_scripts/IDN_scripts/IDN-VIKRANT-EPOS-scripts/4b_update-promo-master.py")
#	flog.write("Completed Update Masters for Promo \r\n")
#
db.commit()
cursor.close()
flog.close()
db.close()
