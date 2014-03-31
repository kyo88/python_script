#!/usr/bin/python


import smtplib
import subprocess


from cm_api.api_client import ApiResource
from time import gmtime, strftime
from email.mime.text import MIMEText

import sys
import time

#Function: send mail to manager
def send_alert_mail(status,role_name,host,service):
	#Mail setting param
	me = 'cdh@manager.com'
	you = 'kyo88kyo@gmail.com'
	date =  strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
	#Role in Bad status
	if (status == "BAD"):
		subject = "[URGENT]%s is in [ %s ] health"%(role_name,status)
		content = """
		Role is in [BAD] health status:
		< Info >
			HostName : %s
			RoleName : %s
			Service  : %s
			Status   : %s
			Date     : %s
		"""%(host,role_name,service,status,date)
	#Role in Concerning status
	elif (status == "CONCERNING"):
		subject = "[WARNING]%s is in [ %s ] health"%(role_name,status)
                content = """
		Role is in [CONCERNING] health status:
                < Info >
                        HostName : %s
                        RoleName : %s
                        Service  : %s
                        Status   : %s
                        Date     : %s
                """%(host,role_name,service,status,date)            
	#Connectiong error
	elif (status == "CONNECT_ERROR"):
		host_run=subprocess.Popen("hostname",stdout=subprocess.PIPE, shell=True).stdout.read().strip()
		subject = "[URGENT] Can't connect to cdh_api"
		content = """
		Script python realtime checking cluster's roles health status can't connect to  cdh_api
		< Info >
		Scipt on host: %s
		"""%host_run
	#Something strange happen
	else :
		subject = "[WARNING] Role %s is in [ %s ] health"%(role_name,status)
                content = """
                Role is in [%s] health status:
                < Info >
                        HostName : %s
                        RoleName : %s
                        Service  : %s
                        Status   : %s
                        Date     : %s
                """%(status,host,role_name,service,status,date)
	msg = MIMEText(content)

	# me == the sender's email address
	# you == the recipient's email address
	msg['Subject'] = subject
	msg['From'] = me
	msg['To'] = you

	# Send the message via our own SMTP server, but don't include the
	# envelope header.
	s = smtplib.SMTP('localhost')
	s.sendmail(me, [you], msg.as_string())
	s.quit()



#================================== CHECK CLUSTER ROLES HEALTH STATUS ==============================#

cm_host = 'cdh-client01.cdh.local'
user = 'admin'
password = '?cx.oyb043nj1'
fd = open('/tmp/logClusterRolesHealthStatusCheck.log', 'a')
#sys.stdout = fd
#sys.stderr = fd
send_alert_mail("CONCERNING","roleA","localhost","mapred")
try:
        print """\n\n\n====================================================================================
	        --- CHECK ROLES HEALTH STATUS START ---\n\n\n"""

	api = ApiResource(cm_host, 7180, user , password)
	# Get a list of all clusters
	cdh = None
	if (api == None):
		print "COnnect error"
	try:
		for c in api.get_all_clusters():
			cdh = c
		print strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))," PRESENT CLUSTER: ",cdh.name
	except:
		print strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()) )," Error get cluster"
		send_alert_mail("CONNECT_ERROR","","","");
	if cdh != None:
		for s in api.get_cluster(cdh.name).get_all_services():
			print strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))," SERVICES: ",s.name
			cluster = api.get_cluster(cdh.name);
			service_mapred=cluster.get_service(s.name)
			roles=service_mapred.get_all_roles()
			for r in service_mapred.get_all_roles():
				print strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))," Role ",r.name," is in status [ ",r.healthSummary," ]"
				if (r.healthSummary != "GOOD" and r.roleState == "STARTED"):
					send_alert_mail(r.healthSummary,r.name,r.hostRef.hostId,s.name)
	print """\n\n\n		--- CHECK ROLES HEALTH STATUS END ---
===================================================================================="""	
except:
	print "Can't not connect to CDH API"
fd.close()
