
import smtplib
import os
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.utils import formatdate



file_list=os.listdir("/home/FRACTAL/gokulakrishnan.s/gokul_scripts/IDN_scripts/IDN-Genesis-scripts/Output/"+datetime.now().strftime("%Y%m%d"))

files_path ="/home/FRACTAL/gokulakrishnan.s/gokul_scripts/IDN_scripts/IDN-Genesis-scripts/Output/"+datetime.now().strftime("%Y%m%d") 

ctrl_files=[x for x in file_list if x[-3:]=='ctl']

# Function to send the email if there is no file#	   
#def send_outbound_extract_email(): 
toaddr = ['#######@fractal.ai','########@fractal.ai','##########@fractal.ai']
me = '#######@fractal.ai' 
subject = "Email Alert from Darwin IS" 
bodyText= "PFA the following 'ctrl' files generated in the outbound extract. \nThis is auto generated mail sent through system. Please respond to regular mail Ids.\nThank You"
msg = MIMEMultipart()
msg['Subject'] = subject
msg['From'] = me
msg['To'] = ",".join(toaddr)
msg['Body'] = bodyText
msg.preamble = "test " 
msg.attach(MIMEText(bodyText,'plain'))

for f in ctrl_files:  # add files to the message
	final_file_path = os.path.join(files_path, f)
	attachment = MIMEApplication(open(final_file_path, "rb").read(), _subtype="txt")
	attachment.add_header('Content-Disposition','attachment', filename=f)
	msg.attach(attachment)

#ctype = "application/octet-stream"
#maintype, subtype = ctype.split("/", 1)

#part = MIMEBase('maintype', "subtype")

print ("Entring Try Block")
try:
	s = smtplib.SMTP('smtp.outlook.com', 587)
	print("Running - " + str(datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
	
	s.ehlo()
	s.starttls()
	s.ehlo()
	s.login(user = '#######@fractal.ai', password = '#############')
 
	s.sendmail(me, toaddr, msg.as_string())
	s.quit()
except:
	#print(e)
	print ("Error: unable to send email")
	
 


