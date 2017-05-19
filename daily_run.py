#! /usr/bin/env python

#version 1.0 zhang, yayun, auto build elmtp

import smtplib
import email
#from email.header import Header
from email.MIMEText import MIMEText
import datetime
import os

date =  str(datetime.datetime.now())
date = date.split(' ')[0].replace('-', '')

content = 'Hi Mfg,\n' + 'I am marlin robot, you receive this mail because you are member of Mfg\n' + 'you could find the Marlin output csv result on:	' +  '\\\\suzfsprdv01\\public\\PDE\\USERS\\yyuzhang\\' + date + '\n'

from_addr = '490878838@163.com'
password = 'zyy3228184'

smtp_server = 'SMTP.163.COM'

to_addr = 'Yayun1.Zhang@amd.com, Ji.Liu@amd.com'


   

msg = MIMEText(content, 'plain', 'utf-8')
msg['From'] = 'MarlinMailRobot <490878838@163.com>'
msg['Subject'] = 'SLT Effiency Daily Tracking ' + date
msg['To'] = to_addr

if os.path.exists('/home/python/' + date):

	shellCommand = '/home/yyuzhang/marlin.py ' + '/home/python/' + date + '/' + date + '.csv' + ' > ' + '/home/python/' + date + '/' + date + '.log'
	if os.system(shellCommand) == 1:
	#if 1 == 1:
		server = smtplib.SMTP()
		server.connect(smtp_server)                         
		server.login("490878838@163.com","zhang3228184") 
		server.sendmail(from_addr, to_addr, msg.as_string())
		server.quit()
