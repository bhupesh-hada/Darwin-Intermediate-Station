 # For each file in direcotry, writes filetype to (Txn, Outlet, Product, Promo) and flag_desc

import os.path
import csv
from datetime import date, datetime, timedelta
import fnmatch
import shutil
import re

# STRINGS DEFINITIONS START
# DIRECTORIES - MODIFY HERE TO DEFINE INCOMING, DESTINATION DIRECTORIES
directory="/data/From-Emporio/"
directorydest="/home/FRACTAL/bhupesh.hada/bhupesh_scripts/IDN_scripts/IDN_Genesis_Scripts_with_ProductID_change/Output"
recon_directory=directorydest+"/"+datetime.now().strftime("%Y%m%d")

#Create recon dest directory if not existing
if not os.path.exists(recon_directory):
    os.makedirs(recon_directory) 

recon_report_filename = recon_directory+"/RECON_1_REPORT_"+datetime.now().strftime("%Y%m%d%H%M%S")
runtime_meta_txn_filename = directorydest+"/runtime_metafile_txn"
runtime_meta_outlet_filename = directorydest+"/runtime_metafile_outlet"
runtime_meta_prod_filename = directorydest+"/runtime_metafile_prod"
runtime_meta_promo_filename = directorydest+"/runtime_metafile_promo"
runtime_meta_mapping_filename = directorydest+"/runtime_metafile_mapping"


filename_outlet_scrap = "outlet"
filename_product_scrap = "product"
filename_promo_scrap = "promo"
filename_txn_scrap = "trans"

# These flags are used to describe each file in the directory in the FOR loop below to write the ERROR attributes
flag_received="N"
flag_csv="Not CSV"
flag_desc="Unknown file error."
flag_filetype="Unknown file"

# DEFINE INCOMING FILE NAME FORMATS AS WILDCARD NAMES WITH "*.CSV"
filename_IDN_trans= "IDN_EA_transaction_"+datetime.now().date().strftime('%d%b%Y')+"_*.csv"
filename_IDN_outlet= "IDN_EA_outlet_"+datetime.now().date().strftime('%d%b%Y')+"_*.csv"
filename_IDN_promo= "IDN_EA_promotion_"+datetime.now().date().strftime('%d%b%Y')+"_*.csv"
filename_IDN_prod= "IDN_EA_product_"+datetime.now().date().strftime('%d%b%Y')+"_*.csv"
filename_IDN_mapping= "IDN_EA_productidbarcode_"+datetime.now().date().strftime('%d%b%Y')+"_*.csv"

latest_filename= ""

# STRINGS DEFINITIONS END HERE

# READ THE MAPPING FILE FILENAME
with open(directorydest+'/runtime_metafile_mapping.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
	    FILENAME_READ_1 = row[0];
	    line_count += 1



# OPEN RECON_report file 
f1= open(recon_report_filename+'.csv', 'w')
f2= open(runtime_meta_txn_filename+'.csv', 'w')
f3= open(runtime_meta_outlet_filename+'.csv', 'w')
f4= open(runtime_meta_prod_filename+'.csv', 'w')
f5= open(runtime_meta_promo_filename+'.csv', 'w')
f6= open(runtime_meta_mapping_filename+'.csv', 'w')


writer=csv.writer(f1)
runtime_meta_txn_writer=csv.writer(f2)
runtime_meta_outlet_writer=csv.writer(f3)
runtime_meta_prod_writer=csv.writer(f4)
runtime_meta_promo_writer=csv.writer(f5)
runtime_meta_mapping_writer=csv.writer(f6)


writer.writerow(['FILENAME','VENDOR_NAME','COUNTRY','DATE','RECEIVED_FLAG','FILETYPE', 'CSV', 'DESC','FILESIZE'])

# START- SECTION TO READ META DATA FILE 

with open(directorydest+'/from-IDN-metadata.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            line_count += 1
        elif line_count ==1:
            FILENAME_READ = row[0];
            line_count += 1
	else:
	    LAST_EXTRACT_DATE = row[0]; 

# END - SECTION TO READ META DATA FILE 


# DECIDING THE LAST FILENAME READ	
	     
stat_transaction = os.stat(directory+'/'+FILENAME_READ)
stat_mapping = os.stat(directory+'/'+FILENAME_READ_1)

if (stat_transaction.st_mtime >= stat_mapping.st_mtime):
	LAST_FILENAME_READ = FILENAME_READ
else:
	LAST_FILENAME_READ = FILENAME_READ_1





# FOR LOOP : DESCRIPTION
# For each file in the incoming directory defined above, the following checks are done:
# (a) If extension is .csv or not - if no, flag_CSV is set to "Not CSV"
# (b) If the filename matches the naming convention or not - if YES, flag_received="Y", identify the type
#     and set flag_filetype to (Txn, Outlet, Product, Promo) and flag_desc to "No errors."
# (c) If not csv or if not with "No errors.", the file is moved to the destination directory meant for files with errors.
# (d) Before writing entry in ERROR file for this iteration (file), guesses the type of file, by looking for terms 'outlet', 'product', 'promo', 'trans' in filename
# (e) If found, sets filetype to (Txn, Outlet, Product, Promo) and flag_desc="Filename naming convention not correct."
# (f) Finally, writes the entry in the RECON_report file for this particular iteration (file)

latest_filename= LAST_FILENAME_READ

for filename_actual in os.listdir(directory):
	stat1= os.stat(directory+'/'+filename_actual)
	stat2= os.stat(directory+'/'+LAST_FILENAME_READ) 
	stat_latest= os.stat(directory+'/'+latest_filename) 

	if (stat1.st_mtime > stat_latest.st_mtime):
		latest_filename=filename_actual

	if (stat1.st_mtime > stat2.st_mtime):
		flag_received="N"
		flag_filetype="Unknown file"
		flag_desc="Unknown file error."
		flag_csv="Not CSV"
		print('File found: '+ filename_actual)			

		if fnmatch.fnmatch(filename_actual[len(filename_actual)-4:len(filename_actual)], ".csv"):
			flag_csv="CSV"

		if(re.match(filename_actual[0:24],filename_IDN_mapping[0:24],re.IGNORECASE)):
			flag_received = "Y"
			flag_filetype="Mapping"
			flag_desc="No errors."
			print('Mapping File found: '+ filename_actual)

			
		if(re.match(filename_actual[0:19],filename_IDN_trans[0:19],re.IGNORECASE)):
			flag_received = "Y"
			flag_filetype="Transaction"
			flag_desc="No errors."
			print('Transaction File found: '+ filename_actual)

		if(re.match(filename_actual[0:16], filename_IDN_promo[0:16],re.IGNORECASE)):
			flag_received = "Y"
			flag_filetype="Promotion"
			flag_desc="No errors."
		
		if(re.match(filename_actual[0:13],filename_IDN_outlet[0:13],re.IGNORECASE)):
			flag_received = "Y"
			flag_filetype="Outlet"
			flag_desc= "No errors."

		if(re.match(filename_actual[0:15], filename_IDN_prod[0:15],re.IGNORECASE)):
			flag_received = "Y"
			flag_filetype="Product"
			flag_desc= "No errors."

		#if(flag_csv != "CSV" and os.path.isfile(directory+'/'+filename_actual)):
		#shutil.move(directory+"/"+filename_actual,directorydest+"/"+filename_actual)

		#if(flag_desc != "No errors." and os.path.isfile(directory+'/'+filename_actual)):
		#shutil.move(directory+"/"+filename_actual,directorydest+"/"+filename_actual)
		#if(re.search(filename_outlet_scrap,filename_actual,re.IGNORECASE)):
		#	flag_filetype="Outlet"
		#	flag_desc="Filename naming convention not correct."
		#if(re.search(filename_product_scrap,filename_actual,re.IGNORECASE)):
		#	flag_filetype="Product"
		#	flag_desc="Filename naming convention not correct."
		#if(re.search(filename_promo_scrap,filename_actual,re.IGNORECASE)):
		#	flag_filetype="Promotion"
		#	flag_desc="Filename naming convention not correct."
		#if(re.search(filename_txn_scrap,filename_actual,re.IGNORECASE)):
		#	flag_filetype="Transaction"
		#	flag_desc="Filename naming convention not correct."
				
		if(flag_filetype == "Mapping" and flag_csv == "CSV"):
			writer.writerow([filename_actual,'IDN-EA-map','IDN',datetime.now().strftime("%Y%m%d-%H%M%S"),flag_received, flag_filetype, flag_csv, flag_desc,stat1.st_size])
			runtime_meta_mapping_writer.writerow([filename_actual])
		if(flag_filetype == "Transaction" and flag_csv == "CSV"):
			writer.writerow([filename_actual,'IDN-EA-txn','IDN',datetime.now().strftime("%Y%m%d-%H%M%S"),flag_received, flag_filetype, flag_csv, flag_desc,stat1.st_size])
			runtime_meta_txn_writer.writerow([filename_actual])
		if(flag_filetype == "Unknown file"):
			writer.writerow([filename_actual,'Unknown Source','Unknown Country',datetime.now().strftime("%Y%m%d-%H%M%S"),flag_received, flag_filetype, flag_csv, flag_desc,stat1.st_size])
		if(flag_filetype == "Outlet" and flag_csv == "CSV"):
			writer.writerow([filename_actual,'IDN-EA','IDN',datetime.now().strftime("%Y%m%d-%H%M%S"),flag_received, flag_filetype, flag_csv, flag_desc,stat1.st_size])
			runtime_meta_outlet_writer.writerow([filename_actual])
		if(flag_filetype == "Product" and flag_csv == "CSV"):
			writer.writerow([filename_actual,'IDN-EA','IDN',datetime.now().strftime("%Y%m%d-%H%M%S"),flag_received, flag_filetype, flag_csv, flag_desc,stat1.st_size])
			runtime_meta_prod_writer.writerow([filename_actual])
		if(flag_filetype == "Promotion" and flag_csv == "CSV"):
			writer.writerow([filename_actual,'IDN-EA','IDN',datetime.now().strftime("%Y%m%d-%H%M%S"),flag_received, flag_filetype, flag_csv, flag_desc,stat1.st_size])
			runtime_meta_promo_writer.writerow([filename_actual])


#STEP: To update from-IDN-metadata file with lastfilename read and last extracted date

#meta_file= open(directorydest+'/from-IDN-metadata.csv', 'w')
#meta_writer=csv.writer(meta_file)

#yesterday_object = date.today() - timedelta(1)
#date_yesterday = yesterday_object.strftime('%Y-%m-%d')

#meta_writer.writerow(['LAST_FILENAME_READ'])
#meta_writer.writerow([latest_filename])
#meta_writer.writerow(['LAST_EXTRACT_DATE'])
#meta_writer.writerow([date_yesterday])
#meta_file.close()
