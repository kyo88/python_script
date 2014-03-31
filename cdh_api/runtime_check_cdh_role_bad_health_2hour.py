#!/usr/bin/python


import smtplib
import subprocess


from cm_api.api_client import ApiResource
from time import gmtime, strftime
from email.mime.text import MIMEText

import sys
import time

#Function: send mail to manager
def send_alert_mail(status,role_name,host,service,state):
        #Mail setting param
        me = 'cdh@manager.com'
        you = 'jisedai-alert@freeml.com'

        date =  strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        #Role in Bad status
        if (status == "BAD"):
                subject = "** PROBLEM Service Alert: DMP/Role %s is in %s health **"%(role_name,status)
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
                subject = "** PROBLEM Service Alert: DMP/Can't connect to CDH_API **"
                content = """
                Script python realtime checking cluster's roles health status can't connect to  cdh_api
                < Info >
                 Scipt on host: %s
                          time: %s
                """%(host_run,date)
	#Connectiong error

	elif (status == "RESOLVED"):
                subject = "** RECOVERY Service Alert: DMP/Role %s is in %s health **"%(role_name,state)
                content = """
                Role is in [RECOVERY] health status:
                < Info >
                        HostName : %s
                        RoleName : %s
                        Service  : %s
			Status   : %s
                        Date     : %s
                """%(host,role_name,service,state,date)
	#resolved connect to cdh_api
        elif (status == "CONNECT_RESOLVED"):
                host_run=subprocess.Popen("hostname",stdout=subprocess.PIPE, shell=True).stdout.read().strip()                
		subject = "** RECOVERY Service Alert: DMP/Can connect to CDH_API **"
                content = """
                Script python realtime checking cluster's roles health status can connect to  cdh_api
                < Info >
                Scipt on host: %s
			 time: %s
                """%(host_run,date)
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



#================================== CHECK CLUSTER ROLES BAD HEALTH STATUS ==============================#

cm_host = 'cdh-client01.cdh.local'
user = 'admin'
password = '?cx.oyb043nj1'

#fd = open('/tmp/logClusterRolesBadHealthStatusCheck.log', 'a')
#sys.stdout = fd
#sys.stderr = fd
import json
import pprint
import datetime as dt

now_dt = dt.datetime.now().replace(microsecond=0)
timeSkip = 120
connectErrorName = 'connectCDHError'
fileJson = '/data/cdh_api/errorDataJson'

with open(fileJson) as data_file:
    dataError = json.load(data_file)
data_file.close()
try:
        print """\n\n\n====================================================================================
                --- CHECK ROLES BAD HEALTH STATUS START ---\n\n\n"""

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
                print strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()) ),"Can't connect to CDH API"
		if connectErrorName in dataError:
			timestr = dataError[connectErrorName]
			start_dt = dt.datetime.strptime(timestr, '%Y-%m-%d %H:%M:%S')
			diff = (now_dt - start_dt)
			if float(diff.seconds)/60 >= timeSkip :
				send_alert_mail("CONNECT_ERROR","","","","")
				dataError[connectErrorName] = strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
		else:
			send_alert_mail("CONNECT_ERROR","","","","")
			dataError[connectErrorName] = strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))

        if cdh != None:
		if connectErrorName in dataError:
			del dataError[connectErrorName]
			send_alert_mail("CONNECT_RESOLVED","","","","")

                for s in api.get_cluster(cdh.name).get_all_services():
                        cluster = api.get_cluster(cdh.name);
                        service_mapred=cluster.get_service(s.name)
                        roles=service_mapred.get_all_roles()
                        for r in service_mapred.get_all_roles():
                                #print strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))," Role ",r.name," is in status [ ",r.healthSummary," ]"
                                #if in BAD health
                                if (r.healthSummary == "BAD" and r.roleState == "STARTED"):
#                                        send_alert_mail(r.healthSummary,r.name,r.hostRef.hostId,s.name)
#                                        print strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))," SERVICES: ",s.name
#                                        print strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))," Role ",r.name," is in status [ ",r.healthSummary," ]"
					if r.name in dataError:
						timestr = dataError[r.name]
						start_dt = dt.datetime.strptime(timestr, '%Y-%m-%d %H:%M:%S')
						diff = (now_dt - start_dt)
						if float(diff.seconds)/60 >= timeSkip :							
							send_alert_mail(r.healthSummary,r.name,r.hostRef.hostId,s.name,"")
							dataError[r.name] = strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
					else:
						send_alert_mail(r.healthSummary,r.name,r.hostRef.hostId,s.name,"")
						dataError[r.name] = strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
				elif (r.roleState == "STARTED"):
					if r.name in dataError:
						del dataError[r.name]
						send_alert_mail("RESOLVED",r.name,r.hostRef.hostId,s.name,r.healthSummary)
        print """\n\n\n         --- CHECK ROLES HEALTH BAD STATUS END ---
===================================================================================="""
except:
        print "Error by code or read file json or run cdh api"

with open(fileJson, "w") as file:
	json.dump(dataError, file)
file.close()

#fd.close()

