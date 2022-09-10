import os
import csv
import pymysql
from datetime import date, datetime, timedelta
import fnmatch
import os.path

# SECTION 1: 
# STEP 1: Read files from cdv_directory that are later than the last filename read (from IDN_metadata file), generate RECON 1 report, write filenames into runtime_metafile_prod, runtime_metafile_outlet, runtime_metafile_txn and runtime_metafile_promo files 
# STEP 2: Read filenames from above runtime_files, load into staging tables for IDN and generate RECON 2 reports, RECON 3 reports MISSINGSTOREINTRANS, MISSINGSTOREINMASTER, NUMBILLSPERSTORE, MISSINGPRODINMASTER.  Also to action flag and update masters.  Then write 'runtime_txn_dates_list.csv' and 'runtime_txn_tuples_list.csv' for affected tuples (Bill date, outlet code and bill no) for past dates 
# WARNING:  If this script doesn't run successfully please make sure the from-IDN-metadata.csv file is initiated correctly before running this again.

cdv_directory='/data/From-Emporio/'
output_directory='/home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/Output/'
log_directory=output_directory+datetime.now().strftime("%Y%m%d")+'/';

#Create log directory if not existing
if not os.path.exists(log_directory):
    os.makedirs(log_directory) 

LAST_FILENAME_READ='';
LAST_EXTRACT_DATE='';

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


# Clean up  runtime_txn_dates_list file
f_clean = open(output_directory+'runtime_txn_dates_list.csv', 'w')
f_clean.close()	

# Clean up  runtime_txn_tuples_list file
f_clean = open(output_directory+'runtime_txn_tuples_list.csv', 'w')
f_clean.close()	

# Clean up  runtime_txn_dates_list file
f_clean = open(output_directory+'runtime_date_to_extract.csv', 'w')
f_clean.close()	

# Declare logfile for writing processing actions
f_log = open(log_directory+'processing_log_'+datetime.now().strftime("%Y%m%d%H%M%S")+'.csv', 'w')
writer=csv.writer(f_log)
writer.writerow([datetime.now().strftime("%Y%m%d - %H%M%S")+ " : Script started."])

# DB CONNECTION DEFINITIONS 
db = pymysql.connect(host="#########", port=3306, user="dbuser", passwd="#########", db="NEW_EMPORIO_GENESIS_IDN",local_infile=True)
cursor = db.cursor()

# START- SECTION TO READ META DATA FILE FOR LAST FILE NAME READ AND LAST EXTRACTED DATE
writer.writerow([datetime.now().strftime("%Y%m%d - %H%M%S")+ " : Reading FROM-IDN META FILE."])

with open(output_directory+'from-IDN-metadata.csv') as csv_file:
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

#Debug
writer.writerow([datetime.now().strftime("%Y%m%d - %H%M%S")+ " : Picked up LAST_FILENAME_READ : " + LAST_FILENAME_READ])
writer.writerow([datetime.now().strftime("%Y%m%d - %H%M%S")+ " : Picked up LAST_EXTRACT_DATE: " + LAST_EXTRACT_DATE])

# END - SECTION TO READ IDN META DATA FILE

# START- SECTION TO READ DATABASE META DATA FILE FOR DB NAME AND TABLE NAMES
writer.writerow([datetime.now().strftime("%Y%m%d - %H%M%S")+ " : Reading FROM-DATABASE META FILE."])

with open(output_directory+'database-metadata.csv') as csv_file:
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

# START - SECTION TO LOAD NEW FILES (AFTER LAST_FILENAME_READ) INTO STAGING AND GENERATE RECON REPORTS 

writer.writerow([datetime.now().strftime("%Y%m%d - %H%M%S")+ " : Starting 1_checkFileName_reconscript1 : "])
os.system("python /home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/scripts_from_20200921/1_checkFileName_reconscript_1.py")
writer.writerow([datetime.now().strftime("%Y%m%d - %H%M%S")+ " : Completed 1_checkFileName_reconscript1: "])

# START - SECTION TO GENERATE RECON REPORTS AND RUN ACTION_FLAG AND UPDATE MASTERS, WRITE TUPLES METAFILE

writer.writerow([datetime.now().strftime("%Y%m%d - %H%M%S")+ " : Starting 2_reconscript_2_merged : "])
os.system("python /home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/scripts_from_20200921/2_reconscript_2_merged.py")
writer.writerow([datetime.now().strftime("%Y%m%d - %H%M%S")+ " : Completed 2_reconscript_2_merged : "])


inFile = open(output_directory+'runtime_txn_dates_list.csv','r')

outFile = open(output_directory+'final_txn_dates_list.csv','w')

listLines = []

for line in inFile:

    if line in listLines:
        continue

    else:
        outFile.write(line)
        listLines.append(line)

outFile.close()

inFile.close()


print("Back in timelag-script-main after running RECON2 and writing txn-dateslist")

# SECTION 2: TO GENERATE OUTBOUND EXTRACTS:
# STEP 1: To load the day before yesterday transaction rows (day before yesterday for IDN) into staging table from master 
# STEP 2: To load the affected tuples into the staging table from master 
# STEP 3: Generate UL, AreaValShare, AreaNumShoppers and OutletSKURepo extracts on staging table
# STEP 4: To load the (date_today - 60) transaction rows into staging table from master for TerSls_NUL extract and generate it
# STEP 5: To load the (date_today - 30) transaction rows into staging table from master for AreaSKUShare extract and generate it

with open(output_directory+'runtime_txn_dates_list.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    for row in csv_reader:
	date_object = datetime.strptime(row[0], '%Y-%m-%d')

date_today = datetime.now().strftime('%Y-%m-%d')
date_today_directory = datetime.now().strftime('%Y%m%d')
print "Date today:"+ date_today

yesterday_object = date.today() - timedelta(1)
date_yesterday = yesterday_object.strftime('%Y-%m-%d')
print "Date yesterday:"+ date_yesterday

day_b_yesterday_object = date.today() - timedelta(2)
date_day_b_yesterday = day_b_yesterday_object.strftime('%Y-%m-%d')
print "Date day before yesterday:"+ date_day_b_yesterday

# STEP 1: To load the day before yesterday transaction rows (day before yesterday for IDN) into staging table from master 
print "Setting up staging table to generate extracts for date :"+date_yesterday
#query_trunc_trans = "TRUNCATE table "+DB_name+"."+txn_staging_table_with_ProdID+";"
#cursor.execute(query_trunc_trans)
#query_load_trans = "INSERT INTO "+DB_name+"."+txn_staging_table_with_ProdID+" SELECT COUNTRY_CODE,OUTLET_UNIQUE_CODE,TOT_BILL_AMT,COUNTER_NUM,BILL_NO,BILL_DATE,TOTAL_LINE,CUST_NAME,CUST_PH_NUM,BILL_LEVEL_TAX_VAL,BILL_LEVEL_DISC_VAL,ONLINE_FLAG,UPDATED_TIME,CUST_TYPE,LINE_ITEM_DISC_VAL,PROM_DISC_APPL,PROM_DISC_CODE,CURR_SALES_PRICE,MRP,QTY,TOTAL_LINE_AMT,FREE_PROD_FLAG,PRODUCT_ID,CREATED_TIME,'','"+date_today+"' from "+txn_master_table_with_ProdID+" where DATE(BILL_DATE) ='"+date_yesterday+"';"

#print query_load_trans

#cursor.execute(query_load_trans)

# STEP 2: To load the affected tuples into the staging table from master 

#with open(output_directory+'runtime_txn_tuples_list.csv') as csv_file:
#	csv_reader = csv.reader(csv_file, delimiter=',')
#	for row in csv_reader:
#		tuple_date_string = row[0]
#		tuple_outlet_code = row[1]
#		tuple_bill_no=row[2]
#		query_load_trans = "INSERT INTO "+DB_name+"."+txn_staging_table_with_ProdID+" SELECT COUNTRY_CODE,OUTLET_UNIQUE_CODE,TOT_BILL_AMT,COUNTER_NUM,BILL_NO,BILL_DATE,TOTAL_LINE,CUST_NAME,CUST_PH_NUM,BILL_LEVEL_TAX_VAL,BILL_LEVEL_DISC_VAL,ONLINE_FLAG,UPDATED_TIME,CUST_TYPE,LINE_ITEM_DISC_VAL,PROM_DISC_APPL,PROM_DISC_CODE,CURR_SALES_PRICE,MRP,QTY,TOTAL_LINE_AMT,FREE_PROD_FLAG,PRODUCT_ID,CREATED_TIME,'','"+date_today+"' from Transactions_emporio_with_ProdID where DATE(BILL_DATE) ='"+tuple_date_string+"' AND OUTLET_UNIQUE_CODE ='"+tuple_outlet_code+"' AND BILL_NO='"+tuple_bill_no+"';"
#		cursor.execute(query_load_trans)
print "Completed setup of Transactions Staging for outbound extracts"	

# STEP_2: To load the latest mapping file sent at IS end
# Load the latest mapping file in the staging as well as master mapping table before running this script
print "Latest mapping file has already been loaded in the staging and master mapping table"


# STEP 3: To load the day before yesterday transaction rows (day before yesterday for IDN) into staging table from master 
#print "Setting up staging table to generate extracts for dates from  :"+day_one_month_back
#query_trunc_trans = "TRUNCATE table "+DB_name+"."+txn_staging_table+";"
#cursor.execute(query_trunc_trans)

# DEFINING QUERIES for mapping ProdID with Barcode  
#mapping_query = """call test_mapped_file;"""

#cursor.execute(mapping_query)

	
db.commit()

# STEP 3: Generate UL, AreaValShare, AreaNumShoppers and OutletSKURepo extracts on staging table
# 		Generate all outbound extracts for current date with affected tuples + date (day before yesterday for IDN)
#
os.system("python /home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/5_outbound-UL-extract.py")
print "Completed 5_outbound-UL-extract.py"
#os.system("python /home/FRACTAL/gokulakrishnan.s/gokul_scripts/IDN_scripts/IDN-Genesis-scripts/6_outbound-masters.py")
#print "Completed 6_outbound-masters.py"
os.system("python /home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/7_outbound-AreavalShare-extract.py")
print "Completed 7_outbound-AreavalShare-extract.py"
os.system("python /home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/9_outbound-AreaNumShoppers-extract.py")
print "Completed 9_outbound-AreaNumShoppers-extract.py"
os.system("python /home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/11_outbound-OutletSKU-extract.py")
print "Completed 11_outbound-OutletSKU-extract.py"

dbase = pymysql.connect(host="#########", port=3306, user="dbuser", passwd="#########", db=DB_name,local_infile=True)
cursor1 = dbase.cursor()

# STEP 4: To load the (date_today - 61) transaction rows into staging table from master for TerSls_NUL extract and generate it

date_object_for_NUL_extract = date.today() - timedelta(61)
date_for_NUL_extract= date_object_for_NUL_extract.strftime('%Y-%m-%d')

query_trunc_trans="TRUNCATE table "+DB_name+"."+txn_staging_table_with_ProdID+";"
cursor1.execute(query_trunc_trans)

query_load_trans = "INSERT INTO "+DB_name+"."+txn_staging_table_with_ProdID+" SELECT COUNTRY_CODE,OUTLET_UNIQUE_CODE,TOT_BILL_AMT,COUNTER_NUM,BILL_NO,BILL_DATE,TOTAL_LINE,CUST_NAME,CUST_PH_NUM,BILL_LEVEL_TAX_VAL,BILL_LEVEL_DISC_VAL,ONLINE_FLAG,UPDATED_TIME,CUST_TYPE,LINE_ITEM_DISC_VAL,PROM_DISC_APPL,PROM_DISC_CODE,CURR_SALES_PRICE,MRP,QTY,TOTAL_LINE_AMT,FREE_PROD_FLAG,PRODUCT_ID,CREATED_TIME,'','"+date_today+"' from "+txn_master_table_with_ProdID+" where DATE(BILL_DATE) ='"+date_for_NUL_extract+"';"

print query_load_trans

cursor1.execute(query_load_trans)


print "Setting up staging table to generate NUL extract for date :"+ date_for_NUL_extract
query_trunc_trans = "TRUNCATE table "+DB_name+"."+txn_staging_table+";"
print query_trunc_trans 
cursor1.execute(query_trunc_trans)

#query_load_trans = "INSERT INTO "+DB_name+"."+txn_staging_table+" SELECT COUNTRY_CODE,OUTLET_UNIQUE_CODE,TOT_BILL_AMT,COUNTER_NUM,BILL_NO,BILL_DATE,TOTAL_LINE,CUST_NAME,CUST_PH_NUM,BILL_LEVEL_TAX_VAL,BILL_LEVEL_DISC_VAL,ONLINE_FLAG,UPDATED_TIME,CUST_TYPE,LINE_ITEM_DISC_VAL,PROM_DISC_APPL,PROM_DISC_CODE,CURR_SALES_PRICE,MRP,QTY,BARCODE,TOTAL_LINE_AMT,FREE_PROD_FLAG,CREATED_TIME,'','"+date_today+"' from "+DB_name+"."+txn_master_table+" where DATE(BILL_DATE) ='"+date_for_NUL_extract+"';"

#print query_load_trans

#cursor1.execute(query_load_trans)


# DEFINING QUERIES for mapping ProductID with Barcode  
mapping_query = """call test_mapped_file;"""
cursor1.execute(mapping_query)

a=0
print a
b=1
print b
txn_insert_query="INSERT INTO NEW_EMPORIO_GENESIS_IDN.Unmapped_transactions_for_NUL SELECT a.* FROM (select a.COUNTRY_CODE,a.OUTLET_UNIQUE_CODE,a.TOT_BILL_AMT,a.COUNTER_NUM,a.BILL_NO,a.BILL_DATE,a.TOTAL_LINE,a.CUST_NAME,a.CUST_PH_NUM,a.BILL_LEVEL_TAX_VAL,a.BILL_LEVEL_DISC_VAL,a.ONLINE_FLAG,a.UPDATED_TIME,a.CUST_TYPE,a.LINE_ITEM_DISC_VAL,a.PROM_DISC_APPL,a.PROM_DISC_CODE,a.CURR_SALES_PRICE,a.MRP,a.QTY,a.TOTAL_LINE_AMT,a.FREE_PROD_FLAG,a.PRODUCT_ID,(case when b.BARCODE=\"\" or b.BARCODE IS NULL then "+str(a)+" else "+str(b)+" end) as MAPPED,a.CREATED_TIME from NEW_EMPORIO_GENESIS_IDN.Transactions_emporio_staging_with_ProdID a LEFT JOIN NEW_EMPORIO_GENESIS_IDN.Mapping_master_emporio b ON a.PRODUCT_ID = b.PRODUCT_ID) a where a.MAPPED=0;"
print(txn_insert_query)
cursor1.execute(txn_insert_query)

dbase.commit()
os.system("python /home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/5b_outbound-NUL-extract.py")
print "Completed 5b_outbound-NUL-extract.py"

# STEP 5: To load the (date_today - 31) transaction rows into staging table from master for AreaSKUShare extract and generate it

date_object_for_AreaSKUShare_extract = date.today() - timedelta(31)
date_for_AreaSKUShare_extract= date_object_for_AreaSKUShare_extract.strftime('%Y-%m-%d')

query_trunc_trans="TRUNCATE table "+DB_name+"."+txn_staging_table_with_ProdID+";"
cursor1.execute(query_trunc_trans)

query_load_trans = "INSERT INTO "+DB_name+"."+txn_staging_table_with_ProdID+" SELECT COUNTRY_CODE,OUTLET_UNIQUE_CODE,TOT_BILL_AMT,COUNTER_NUM,BILL_NO,BILL_DATE,TOTAL_LINE,CUST_NAME,CUST_PH_NUM,BILL_LEVEL_TAX_VAL,BILL_LEVEL_DISC_VAL,ONLINE_FLAG,UPDATED_TIME,CUST_TYPE,LINE_ITEM_DISC_VAL,PROM_DISC_APPL,PROM_DISC_CODE,CURR_SALES_PRICE,MRP,QTY,TOTAL_LINE_AMT,FREE_PROD_FLAG,PRODUCT_ID,CREATED_TIME,'','"+date_today+"' from "+txn_master_table_with_ProdID+" where DATE(BILL_DATE) ='"+date_for_AreaSKUShare_extract+"';"

print query_load_trans

cursor1.execute(query_load_trans)

print "Setting up staging table to generate AreaSKUShare extract for date :"+ date_for_AreaSKUShare_extract
query_trunc_trans = "TRUNCATE table "+DB_name+"."+txn_staging_table+";"
print query_trunc_trans 
cursor1.execute(query_trunc_trans)

#query_load_trans = "INSERT INTO "+DB_name+"."+txn_staging_table+" SELECT COUNTRY_CODE,OUTLET_UNIQUE_CODE,TOT_BILL_AMT,COUNTER_NUM,BILL_NO,BILL_DATE,TOTAL_LINE,CUST_NAME,CUST_PH_NUM,BILL_LEVEL_TAX_VAL,BILL_LEVEL_DISC_VAL,ONLINE_FLAG,UPDATED_TIME,CUST_TYPE,LINE_ITEM_DISC_VAL,PROM_DISC_APPL,PROM_DISC_CODE,CURR_SALES_PRICE,MRP,QTY,BARCODE,TOTAL_LINE_AMT,FREE_PROD_FLAG,CREATED_TIME,'','"+date_today+"' from "+DB_name+"."+txn_master_table+" where DATE(BILL_DATE) ='"+date_for_AreaSKUShare_extract+"';"
#print query_load_trans

#cursor1.execute(query_load_trans)




# DEFINING QUERIES for mapping ProductID with Barcode  
mapping_query = """call test_mapped_file;"""
cursor1.execute(mapping_query)

print a
print b
txn_insert_query="INSERT INTO NEW_EMPORIO_GENESIS_IDN.Unmapped_transactions_for_AreaSKUShare SELECT a.* FROM (select a.COUNTRY_CODE,a.OUTLET_UNIQUE_CODE,a.TOT_BILL_AMT,a.COUNTER_NUM,a.BILL_NO,a.BILL_DATE,a.TOTAL_LINE,a.CUST_NAME,a.CUST_PH_NUM,a.BILL_LEVEL_TAX_VAL,a.BILL_LEVEL_DISC_VAL,a.ONLINE_FLAG,a.UPDATED_TIME,a.CUST_TYPE,a.LINE_ITEM_DISC_VAL,a.PROM_DISC_APPL,a.PROM_DISC_CODE,a.CURR_SALES_PRICE,a.MRP,a.QTY,a.TOTAL_LINE_AMT,a.FREE_PROD_FLAG,a.PRODUCT_ID,(case when b.BARCODE=\"\" or b.BARCODE IS NULL then 0 else 1 end) as MAPPED,a.CREATED_TIME from NEW_EMPORIO_GENESIS_IDN.Transactions_emporio_staging_with_ProdID a LEFT JOIN NEW_EMPORIO_GENESIS_IDN.Mapping_master_emporio b ON a.PRODUCT_ID = b.PRODUCT_ID) a where a.MAPPED=0;"
print(txn_insert_query)
cursor1.execute(txn_insert_query)



dbase.commit()
os.system("python /home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/10_outbound-AreaSKU-extract.py")
print "Completed 10_outbound-AreaSKU-extract.py"

cursor.close()
cursor1.close()
f_log.close()

# STEP 6: Update config file - from-IDN-metadata.csv for next day's run

with open(output_directory+'runtime_metafile_txn.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
    	LAST_FILENAME_PROCESSED = row[0];

print "Last file processed: "+LAST_FILENAME_PROCESSED

f1 = open(output_directory+'from-IDN-metadata.csv', 'w')
writer=csv.writer(f1)
f1.write("LAST_FILENAME_READ"+"\n")
f1.write(LAST_FILENAME_PROCESSED+"\n")
f1.write("LAST_EXTRACT_DATE"+"\n")
f1.write(date_yesterday)
f1.close()


# STEP 7: Copy the 12 outbound extract files to the UDL prod folder /data/To-Unilever-UDL-Prod/

print("cp /home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/Output/"+date_today_directory+"/IDN* /data/To-Unilever-UDL-Prod/")
os.system("cp /home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/Output/"+date_today_directory+"/IDN* /data/To-Unilever-UDL-Prod/")


# STEP 8: Copy the 6 RECON files to the Emporio folder /data/To-Emporio/

print("cp /home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/Output/"+date_today_directory+"/MISSING* /data/To-Emporio/")
os.system("cp /home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/Output/"+date_today_directory+"/MISSING* /data/To-Emporio/")
print("cp /home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/Output/"+date_today_directory+"/NUMBILLS* /data/To-Emporio/")
os.system("cp /home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/Output/"+date_today_directory+"/NUMBILLS* /data/To-Emporio/")
print("cp /home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/Output/"+date_today_directory+"/RECON_1* /data/To-Emporio/")
os.system("cp /home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/Output/"+date_today_directory+"/RECON_1* /data/To-Emporio/")
print("cp /home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/Output/"+date_today_directory+"/TRANSACTION* /data/To-Emporio/")
os.system("cp /home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/Output/"+date_today_directory+"/TRANSACTION* /data/To-Emporio/")

print("Running - " + str(datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
