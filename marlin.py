#! /usr/bin/env python
#version betaA covert unit level csv data to lot level data. caculate IR, LotTT, UnitTT on lot level
#version betaB fix raw data sort sequence algorithm
#version betaC enable new IndexTime, PauseTime on unit level. optimize the column location algorihm, enhance module by module compatibility.

#usage: ./marlin.py raw.csv

import csv
from itertools import islice
import re
import datetime
import time
import os
import sys
import shelve
from HTMLParser import HTMLParser
from htmlentitydefs import name2codepoint
import urllib
import socket

socket.setdefaulttimeout(100)
FirstLine = []
content = []
ocontent = []
ocontent_Lot_level_data = []
ocontent_Unit_Level_data = []
ocontent_Unit_Level_Lost_data = []
ocontent_LOT_level_data = []
temp = []
lost_lines = []

UNIT_SERIAL_NUMBER_items = []
UNIT_SERIAL_NUMBER_items_compress = []

LOT_ID_items = []

LOT_ID_items_compress = []
LOT_ID_items_compress_index = []

FACILITY_items = []
FACILITY_items_compress = []
OPERATION_items = []
OPERATION_items_compress = []
OPERATION_BLACK = []
STAGE_items = []
EQPT_ID_items = []
EQPT_ID_items_compress = []
SLT_CAT_items = []

DEVICE_items = []
PART_NAME_items = []
TEST_CODE_items = []
PRODUCT_items = []
PRODUCT_BLACK_items = []
SUB_FAMILY_NAME_items = []

VARIANT_items = []
XT_CAT_items = []

SITE_items = []
SITE_items_compress = []

FILE_FINISH_TS_items = []
UNIT_TS_items = []
UNIT_TEST_TIME_items = []
UNIT_TEST_TIME_GROSS_items = []
UNIT_INDEX_TIME_items = []
UNIT_PAUSE_TIME_items = []
UNIT_LONG_PAUSE_TIME_items = []
UNIT_NEGATIVE_TIME_items = []
UNIT_LOT2LOT_TIME_items = []
UNIT_IDLE_TIME_items = []	
UNIT_INDEX_TIME_Cat_items = []

LOT_TESTTIME_PER_EQPT_ID_items = []

RANK_ASC_items = []
RANK_DESC_items = []
DISP_FLAG_items = []
RETEST_COUNT_items = []

testProgramPath = os.path.abspath(sys.argv[1])


	
def find_product(IDV):

	print IDV
	

	url = 'http://cybvgssweb01/dts/datagrid_target/IDVQuery.php?IDV=' +  IDV + '&Query=Query'
	try:
		wp = urllib.urlopen(url)
	except socket.error:
		try:
			wp = urllib.urlopen(url)
		except socket.error:
			try:
				wp = urllib.urlopen(url)
			except socket.error:
				print ''
				return ''
			
	wb_content = wp.readlines()[-2]
	a =  re.match(r'.*\'dgImparRow\'><td>([-A-Za-z0-9_ ]*)</td><td>([-A-Za-z0-9_ ]*)</td><td>([-A-Za-z0-9_ ]*)</td><td>([A-Za-z0-9 ]*)_([A-Za-z0-9 ]*)</td>.*', wb_content)
	if a != None:
		print a.group(4)
		return 	a.group(4)
	else:
		print ''
		return ''		


	
PRODUCT_dict = {
'ICELAND': 'ICELAND',
'WEXSPR_SL1': 'ICELAND',
'MESSXT_SL1': 'ICELAND',
'MESSLE_SL1': 'ICELAND',
'MESSLE_SL2': 'ICELAND'
}

VARIANT_dict = {
'ELLESMERE': {'XAXXXX': 'XT', 'XBXXXX': 'XT', 'XCXXXX': 'PRO', 'XDXXXX': 'PRO', 'XEXXXX': 'LE', 'XFXXXX': 'LE', 'XGXXXX': 'GLXT', 'XHXXXX': 'GLXT', 'XIXXXX': 'GLPRO', 'XJXXXX': 'GLPRO', 'XKXXXX': 'XTA', 'XLXXXX': 'XTA', 'XMXXXX': 'PROA', 'XNXXXX': 'PROA', 'XOXXXX': 'LEA', 'XPXXXX': 'LEA', 'XQXXXX': 'OEM_XT', 'XRXXXX': 'OEM_XT', 'XSXXXX': 'OEM_XL', 'XTXXXX': 'OEM_XL', 'XUXXXX': 'Polaris_20_XTX_OC', 'XVXXXX': 'Polaris_20_XTX_OC', 'XWXXXX': 'Polaris_20_XTX', 'XXXXXX': 'Polaris_20_XTX', 'XYXXXX': 'Polaris_20_XL', 'XZXXXX': 'Polaris_20_XL'},
'ELLESMERE2': {'XAXXXX': 'XT', 'XBXXXX': 'XT', 'XCXXXX': 'PRO', 'XDXXXX': 'PRO', 'XEXXXX': 'LE', 'XFXXXX': 'LE', 'XGXXXX': 'GLXT', 'XHXXXX': 'GLXT', 'XIXXXX': 'GLPRO', 'XJXXXX': 'GLPRO', 'XKXXXX': 'XTA', 'XLXXXX': 'XTA', 'XMXXXX': 'PROA', 'XNXXXX': 'PROA', 'XOXXXX': 'LEA', 'XPXXXX': 'LEA', 'XQXXXX': 'OEM_XT', 'XRXXXX': 'OEM_XT', 'XSXXXX': 'OEM_XL', 'XTXXXX': 'OEM_XL', 'XUXXXX': 'Polaris_20_XTX_OC', 'XVXXXX': 'Polaris_20_XTX_OC', 'XWXXXX': 'Polaris_20_XTX', 'XXXXXX': 'Polaris_20_XTX', 'XYXXXX': 'Polaris_20_XL', 'XZXXXX': 'Polaris_20_XL'}
}

OPERATION_dict = {
'6275': 'HSLT',
'6277': 'HSLT',
'6278': 'KSLT',
'6288': 'ASLT',
'6378': 'KSLT',
'6478': 'SSLT',
'6900': 'ASLT',
'6901': 'ASLT',
'7161': 'HSLT',
'7167': 'HSLT',
'7190': 'KSLT',
'7465': 'HSLT',
'7565': 'HSLT',
'KSLT': 'KSLT',
'SL1': 'ASLT',
'SL2': 'ASLT',
'SL3': 'ASLT',
'SL4': 'ASLT',
'SL5': 'ASLT'
}

STAGE_dict = {
'6275': 'SLT1',
'6277': 'HPBI_HSLT',
'6278': 'kSLT',
'6288': 'ASLT',
'6378': 'kSLT',
'6478': 'SSLT',
'6900': 'SL1',
'6901': 'SL2',
'7161': 'HST',
'7167': 'HST',
'7190': 'KST',
'7465': 'HSLT',
'7565': 'HSLT2',
'KSLT': 'kSLT',
'SL1': 'SL1',
'SL2': 'SL2',
'SL3': 'SL3',
'SL4': 'SL4',
'SL5': 'SL5',
'7507': 'HCESLT'
}

SLT_dict = {
'ML': 'KSLT',
'ML2': 'KSLT',
'ONTARIO': 'KSLT'
}

DIE2DIE_LIMIT_dict = {
'ASLT': {'INDEX_PAUSE': 55, 'PAUSE_OTHERS': 100},
'HSLT': {'INDEX_PAUSE': 600, 'PAUSE_OTHERS': 1500},
'KSLT': {'INDEX_PAUSE': 600, 'PAUSE_OTHERS': 1500},
'SSLT': {'INDEX_PAUSE': 600, 'PAUSE_OTHERS': 1500}
}

LOT2LOT_LIMIT_dict = {
'ASLT': 3000,
'HSLT': 5000,
'KSLT': 5000,
'SSLT': 5000
}

def extract_data(ModuleName):

	
	global temp
	global ocontent
	global lost_lines
	global OPERATION_BLACK
	
	global UNIT_SERIAL_NUMBER_items    #clear temp
	global LOT_ID_items
	global FACILITY_items
	global OPERATION_items
	global EQPT_ID_items
	global SITE_items
	global FILE_FINISH_TS_items
	global UNIT_TS_items
	global UNIT_TEST_TIME_items
	global DEVICE_items
	global PART_NAME_items
	global TEST_CODE_items
	global RANK_ASC_items
	global RANK_DESC_items
	global DISP_FLAG_items
	global SUB_FAMILY_NAME_items
		
	OPERATION_BLACK = []
	
	UNIT_SERIAL_NUMBER_items = []    #clear temp
	LOT_ID_items = []
	FACILITY_items = []
	OPERATION_items = []
	EQPT_ID_items = []
	SITE_items = []
	FILE_FINISH_TS_items = []
	UNIT_TS_items = []
	UNIT_TEST_TIME_items = []
	DEVICE_items = []
	PART_NAME_items = []
	TEST_CODE_items = []
	RANK_ASC_items = []
	RANK_DESC_items = []
	DISP_FLAG_items = []
	SUB_FAMILY_NAME_items = []
	
	
	for line in content[:]:
		if line[FirstLine.index('UNIT_SERIAL_NUMBER')] != '' and line[FirstLine.index('LOT_ID')] != '' and line[FirstLine.index('FACILITY')] != '' and line[FirstLine.index('FACILITY')] != 'SGPAMD' and line[FirstLine.index('FACILITY')] != 'TEST14' and line[FirstLine.index('OPERATION')] != '' and line[FirstLine.index('SOURCE_FILE')] != '' and line[FirstLine.index('FILE_FINISH_TS')] != '' and  line[FirstLine.index('UNIT_TS')] != ''  and line[FirstLine.index('DEVICE')] != '' and line[FirstLine.index('TEST_CODE')] != '' and line[FirstLine.index('EQPT_ID')] != '' and line[FirstLine.index('SITE_ID')] != ''  and line[FirstLine.index('RANK_ASC')] != '' and line[FirstLine.index('RANK_DESC')] != '' and line[FirstLine.index('DISP_FLAG')] != '':
			UNIT_SERIAL_NUMBER_items.append(line[FirstLine.index('UNIT_SERIAL_NUMBER')])
			LOT_ID_items.append(line[FirstLine.index('LOT_ID')])
			FACILITY_items.append(line[FirstLine.index('FACILITY')])		
			OPERATION_items.append(line[FirstLine.index('OPERATION')])		
			EQPT_ID_items.append(line[FirstLine.index('EQPT_ID')])
			SITE_items.append(line[FirstLine.index('SITE_ID')])
			FILE_FINISH_TS_items.append(line[FirstLine.index('FILE_FINISH_TS')])
			UNIT_TS_items.append(line[FirstLine.index('UNIT_TS')])	
			if line[FirstLine.index('UNIT_TEST_TIME')] != '':	                      #in some csv raw data, there no unit testtime data, it's null
				UNIT_TEST_TIME_items.append(line[FirstLine.index('UNIT_TEST_TIME')])
			else:
				UNIT_TEST_TIME_items.append((time.mktime(time.strptime(line[FirstLine.index('FILE_FINISH_TS')],"%Y-%m-%d %H:%M:%S")) - time.mktime(time.strptime(line[FirstLine.index('UNIT_TS')],"%Y-%m-%d %H:%M:%S")))*1000)
			DEVICE_items.append(line[FirstLine.index('DEVICE')])
			PART_NAME_items.append(line[FirstLine.index('PART_NAME')])
			TEST_CODE_items.append(line[FirstLine.index('TEST_CODE')])
			RANK_ASC_items.append(line[FirstLine.index('RANK_ASC')])
			RANK_DESC_items.append(line[FirstLine.index('RANK_DESC')])
			DISP_FLAG_items.append(line[FirstLine.index('DISP_FLAG')])			
			SUB_FAMILY_NAME_items.append(line[FirstLine.index('SUB_FAMILY_NAME')])
			
		else:	
			lost_lines.append(line)
#print EQPT_ID_items

	for i in range(len(UNIT_SERIAL_NUMBER_items)):
		
		temp.append(UNIT_SERIAL_NUMBER_items[i])
		temp.append(RANK_ASC_items[i])
		temp.append(RANK_DESC_items[i])
		temp.append(DISP_FLAG_items[i])		
		temp.append(LOT_ID_items[i])
		temp.append(FACILITY_items[i])
		temp.append(OPERATION_items[i])
		temp.append(EQPT_ID_items[i])
		temp.append(SITE_items[i])
		temp.append(FILE_FINISH_TS_items[i])
		temp.append(UNIT_TS_items[i])
		temp.append(UNIT_TEST_TIME_items[i])
		temp.append(DEVICE_items[i])
		temp.append(PART_NAME_items[i])
		temp.append(TEST_CODE_items[i])	
		temp.append(SUB_FAMILY_NAME_items[i])	
		ocontent.append(temp)
		temp = []
		#print ocontent


	#print LOT_ID_items_compress[0]
	ofiles=open(testProgramPath.replace('.csv', '_' + ModuleName + '_intermedia.csv'),'wb')
	Writer = csv.writer(ofiles)
	Writer.writerows(ocontent)
	print '##################' + 'intermedia data save to ' + testProgramPath.replace('.csv', '_' + ModuleName + '_intermedia.csv') + '##################'
	ocontent = []
	ofiles.close()

	ofiles=open(testProgramPath.replace('.csv', '_' + ModuleName + '_lost_lines.csv'),'wb')
	Writer = csv.writer(ofiles)
	Writer.writerows(lost_lines)
	print '##################' + 'intermedia data save to ' + testProgramPath.replace('.csv', '_' + ModuleName + '_lost_lines.csv') + '##################'
	lost_lines = []
	ofiles.close()	
	
	print list(set(EQPT_ID_items))
	
def module_extract_data(ModuleName):
	global temp
	global ocontent
	global lost_lines
	
	global UNIT_SERIAL_NUMBER_items    #clear temp
	global LOT_ID_items
	global FACILITY_items
	global OPERATION_items
	global EQPT_ID_items
	global SITE_items
	global FILE_FINISH_TS_items
	global UNIT_TS_items
	global UNIT_TEST_TIME_items
	global DEVICE_items
	global PART_NAME_items
	global TEST_CODE_items	
	global PRODUCT_items
	global VARIANT_items
	global XT_CAT_items
	global UNIT_INDEX_TIME_CAT_items
	global SLT_CAT_items	
	global UNIT_INDEX_TIME_items
	global UNIT_PAUSE_TIME_items
	global UNIT_LONG_PAUSE_TIME_items
	global UNIT_NEGATIVE_TIME_items
	global UNIT_IDLE_TIME_items
	global UNIT_LOT2LOT_TIME_items
	global RETEST_COUNT_items
	global RANK_ASC_items
	global RANK_DESC_items
	global DISP_FLAG_items
	global STAGE_items
	global UNIT_TEST_TIME_GROSS_items
	
 	UNIT_SERIAL_NUMBER_items = [x[FirstLine.index('UNIT_SERIAL_NUMBER')] for x in ocontent]
	LOT_ID_items = [x[FirstLine.index('LOT_ID')] for x in ocontent]
	FACILITY_items = [x[FirstLine.index('FACILITY')] for x in ocontent]
	OPERATION_items = [x[FirstLine.index('OPERATION')] for x in ocontent]
	EQPT_ID_items = [x[FirstLine.index('EQPT_ID')] for x in ocontent]
	SITE_items = [x[FirstLine.index('SITE_ID')] for x in ocontent]
	FILE_FINISH_TS_items = [x[FirstLine.index('FILE_FINISH_TS')] for x in ocontent]	
	UNIT_TS_items = [x[FirstLine.index('UNIT_TS')] for x in ocontent]
	UNIT_TEST_TIME_items = [x[FirstLine.index('UNIT_TEST_TIME')] for x in ocontent]
	DEVICE_items = [x[FirstLine.index('DEVICE')] for x in ocontent]
	PART_NAME_items = [x[FirstLine.index('PART_NAME')] for x in ocontent]
	PRODUCT_items = [x[FirstLine.index('PRODUCT')] for x in ocontent]		
	VARIANT_items = [x[FirstLine.index('VARIANT')] for x in ocontent]
	XT_CAT_items = [x[FirstLine.index('XT_CAT')] for x in ocontent]
	UNIT_INDEX_TIME_CAT_items = [x[FirstLine.index('UNIT_INDEX_TIME_CAT')] for x in ocontent]
	SLT_CAT_items = [x[FirstLine.index('SLT_CAT')] for x in ocontent]
	UNIT_INDEX_TIME_items = [x[FirstLine.index('UNIT_INDEX_TIME')] for x in ocontent]
	UNIT_PAUSE_TIME_items = [x[FirstLine.index('UNIT_PAUSE_TIME')] for x in ocontent]
	UNIT_LONG_PAUSE_TIME_items = [x[FirstLine.index('UNIT_LONG_PAUSE_TIME')] for x in ocontent]
	UNIT_NEGATIVE_TIME_items = [x[FirstLine.index('UNIT_NEGATIVE_TIME')] for x in ocontent]
	UNIT_IDLE_TIME_items = [x[FirstLine.index('UNIT_IDLE_TIME')] for x in ocontent]
	UNIT_LOT2LOT_TIME_items = [x[FirstLine.index('UNIT_LOT2LOT_TIME')] for x in ocontent]	
	RETEST_COUNT_items = [x[FirstLine.index('RETEST_COUNT')] for x in ocontent]
	RANK_ASC_items = [x[FirstLine.index('RANK_ASC')] for x in ocontent]
	RANK_DESC_items = [x[FirstLine.index('RANK_DESC')] for x in ocontent]	
	DISP_FLAG_items = [x[FirstLine.index('DISP_FLAG')] for x in ocontent]	
	STAGE_items_items = [x[FirstLine.index('STAGE')] for x in ocontent]	
	TEST_CODE_items = [x[FirstLine.index('TEST_CODE')] for x in ocontent]	
	UNIT_TEST_TIME_GROSS_items = [x[FirstLine.index('UNIT_TEST_TIME_GROSS')] for x in ocontent]	
	
	
	#print LOT_ID_items_compress[0]
	ofiles=open(testProgramPath.replace('.csv', '_' + ModuleName + '_intermedia.csv'),'wb')
	Writer = csv.writer(ofiles)
	Writer.writerows(ocontent)
	print '##################' + 'intermedia data save to ' + testProgramPath.replace('.csv', '_' + ModuleName + '_intermedia.csv') + '##################'
	ocontent = []
	ofiles.close()



	

def Unit_Level_data():
	#print sys._getframe().f_code.co_name  #catch current func name
	global temp
	global FirstLine
	global ocontent_Unit_Level_data
	global ocontent_Unit_Level_Lost_data
	
	for h in range(len(UNIT_SERIAL_NUMBER_items)):
		if TEST_CODE_items[h] != '':
			#print re.search(r'XT', VARIANT_dict[PRODUCT_dict[line[FirstLine.index('DEVICE')]]][line[FirstLine.index('TEST_CODE')]])
			#print line
			#print DEVICE_items[h]
			if database.has_key(DEVICE_items[h]):
				PRODUCT_items.append(database[DEVICE_items[h]])
				VARIANT_items.append('eng')
				XT_CAT_items.append('eng')
				#if re.search(r'XT', VARIANT_dict[database[DEVICE_items[h]]][TEST_CODE_items[h]]) != None:
					#XT_CAT_items.append('xt')	
				#else:
					#XT_CAT_items.append('not_xt')
			elif database.has_key(PART_NAME_items[h]):
				PRODUCT_items.append(database[PART_NAME_items[h]])
				VARIANT_items.append('eng')
				XT_CAT_items.append('eng')
				#if re.search(r'XT', VARIANT_dict[database[DEVICE_items[h]]][TEST_CODE_items[h]]) != None:
					#XT_CAT_items.append('xt')	
				#else:
			elif (DEVICE_items[h] in PRODUCT_dict) and (PART_NAME_items[h] in PRODUCT_dict):
				XT_CAT_items.append('eng')
				PRODUCT_items.append(PRODUCT_dict[DEVICE_items[h]])
				VARIANT_items.append('eng')						#XT_CAT_items.append('not_xt')
										
			elif (DEVICE_items[h] in PRODUCT_BLACK_items) or (PART_NAME_items[h] in PRODUCT_BLACK_items):
				XT_CAT_items.append('eng')
				PRODUCT_items.append('eng')
				VARIANT_items.append('eng')				
						
			elif find_product(DEVICE_items[h]) != '':
				database[DEVICE_items[h]] = find_product(DEVICE_items[h])
				PRODUCT_items.append(database[DEVICE_items[h]])
				VARIANT_items.append('eng')
				XT_CAT_items.append('eng')				
				#if re.search(r'XT', VARIANT_dict[database[DEVICE_items[h]]][TEST_CODE_items[h]]) != None:
					#XT_CAT_items.append('xt')	
				#else:
					#XT_CAT_items.append('not_xt')	
					
			elif find_product(PART_NAME_items[h]) != '':
				database[PART_NAME_items[h]] = find_product(PART_NAME_items[h])
				PRODUCT_items.append(database[PART_NAME_items[h]])
				VARIANT_items.append('eng')
				XT_CAT_items.append('eng')				
				#if re.search(r'XT', VARIANT_dict[database[DEVICE_items[h]]][TEST_CODE_items[h]]) != None:
					#XT_CAT_items.append('xt')	
				#else:
					#XT_CAT_items.append('not_xt')					
			else:
				PRODUCT_BLACK_items.append(DEVICE_items[h])
				PRODUCT_BLACK_items.append(PART_NAME_items[h])
				print SUB_FAMILY_NAME_items[h]
				XT_CAT_items.append('eng')
				PRODUCT_items.append('eng')
				VARIANT_items.append('eng')
				
		if TEST_CODE_items[h] != '':
			if OPERATION_items[h] in OPERATION_dict:
				SLT_CAT_items.append(OPERATION_dict[OPERATION_items[h]])
			else:
			
				
				
				if EQPT_ID_items[h].find('hs') != -1:
					SLT_CAT_items.append('HSLT')

				elif EQPT_ID_items[h].find('ks') != -1:
					SLT_CAT_items.append('KSLT')
				
				elif EQPT_ID_items[h] in SLT_dict:
					SLT_CAT_items.append('KSLT')

					
				elif EQPT_ID_items[h].find('as') != -1:
			
					SLT_CAT_items.append('ASLT')
				
				else:
					SLT_CAT_items.append('ASLT')
				
				if OPERATION_items[h] not in OPERATION_BLACK:
					
					print EQPT_ID_items[h],OPERATION_items[h]
					OPERATION_BLACK.append(OPERATION_items[h])
		
		if TEST_CODE_items[h] != '':
			if OPERATION_items[h] in STAGE_dict:
				STAGE_items.append(STAGE_dict[OPERATION_items[h]])
			else:

				STAGE_items.append('unknown')
				
		
		
		
				
		if LOT_ID_items[h] != '':
			#UNIT_TEST_TIME_items.append(line[25])
			if h != 0 :
				if SITE_items[h] == SITE_items[h - 1]:
					#print UNIT_SERIAL_NUMBER_items[h], FILE_FINISH_TS_items[h]
					UNIT_TEST_TIME_GROSS_items.append(time.mktime(time.strptime(UNIT_TS_items[h],"%Y-%m-%d %H:%M:%S")) + float(UNIT_TEST_TIME_items[h])/1000 - float(UNIT_TEST_TIME_items[h-1])/1000 - time.mktime(time.strptime(UNIT_TS_items[h-1],"%Y-%m-%d %H:%M:%S")))
				else:
					UNIT_TEST_TIME_GROSS_items.append(float(UNIT_TEST_TIME_items[h])/1000)
					
			else:
				UNIT_TEST_TIME_GROSS_items.append(float(UNIT_TEST_TIME_items[h])/1000)
			
	
			time_gap = (UNIT_TEST_TIME_GROSS_items[h] - float(UNIT_TEST_TIME_items[h])/1000)
			
			#print UNIT_INDEX_TIME_items[h]
			if time_gap == 0:
				UNIT_INDEX_TIME_items.append(time_gap)
				UNIT_PAUSE_TIME_items.append(0)
				UNIT_LONG_PAUSE_TIME_items.append(0)
				UNIT_LOT2LOT_TIME_items.append(0)
				UNIT_IDLE_TIME_items.append(0)
				UNIT_NEGATIVE_TIME_items.append(0)
				UNIT_INDEX_TIME_Cat_items.append('null')

			if time_gap < 0:
				UNIT_INDEX_TIME_items.append(0)
				UNIT_PAUSE_TIME_items.append(0)
				UNIT_LONG_PAUSE_TIME_items.append(0)
				UNIT_LOT2LOT_TIME_items.append(0)
				UNIT_IDLE_TIME_items.append(0)
				UNIT_NEGATIVE_TIME_items.append(time_gap)				
				UNIT_INDEX_TIME_Cat_items.append('negative')
							
			elif LOT_ID_items[h] != LOT_ID_items[h - 1]:
				if time_gap < LOT2LOT_LIMIT_dict[SLT_CAT_items[h]]:
					UNIT_INDEX_TIME_items.append(0)
					UNIT_PAUSE_TIME_items.append(0)
					UNIT_LONG_PAUSE_TIME_items.append(0)
					UNIT_IDLE_TIME_items.append(0)
					UNIT_LOT2LOT_TIME_items.append(time_gap)
					UNIT_NEGATIVE_TIME_items.append(0)			
					UNIT_INDEX_TIME_Cat_items.append('lot2lot')
				else:
					UNIT_INDEX_TIME_items.append(0)
					UNIT_PAUSE_TIME_items.append(0)
					UNIT_LONG_PAUSE_TIME_items.append(0)
					UNIT_LOT2LOT_TIME_items.append(0)
					UNIT_IDLE_TIME_items.append(time_gap)
					UNIT_NEGATIVE_TIME_items.append(0)			
					UNIT_INDEX_TIME_Cat_items.append('idle')

							
			elif time_gap >0 and time_gap < DIE2DIE_LIMIT_dict[SLT_CAT_items[h]]['INDEX_PAUSE'] and LOT_ID_items[h] == LOT_ID_items[h - 1]:
				UNIT_INDEX_TIME_items.append(time_gap)
				UNIT_PAUSE_TIME_items.append(0)
				UNIT_LONG_PAUSE_TIME_items.append(0)
				UNIT_LOT2LOT_TIME_items.append(0)
				UNIT_IDLE_TIME_items.append(0)	
				UNIT_NEGATIVE_TIME_items.append(0)						
				UNIT_INDEX_TIME_Cat_items.append('index_time')
			elif (time_gap >= DIE2DIE_LIMIT_dict[SLT_CAT_items[h]]['PAUSE_OTHERS']) and LOT_ID_items[h] == LOT_ID_items[h - 1]:
				UNIT_INDEX_TIME_items.append(0)
				UNIT_PAUSE_TIME_items.append(0)
				UNIT_LONG_PAUSE_TIME_items.append(time_gap)
				UNIT_LOT2LOT_TIME_items.append(0)
				UNIT_IDLE_TIME_items.append(0)	
				UNIT_NEGATIVE_TIME_items.append(0)					
				UNIT_INDEX_TIME_Cat_items.append('others')
			else:
				UNIT_INDEX_TIME_items.append(0)
				UNIT_PAUSE_TIME_items.append(time_gap)
				UNIT_LONG_PAUSE_TIME_items.append(0)
				UNIT_LOT2LOT_TIME_items.append(0)
				UNIT_IDLE_TIME_items.append(0)
				UNIT_NEGATIVE_TIME_items.append(0)					
				UNIT_INDEX_TIME_Cat_items.append('pause_time')
			
				
		if LOT_ID_items[h] != '':

			if RANK_DESC_items[h] == '1' and  DISP_FLAG_items[h] == '0':
				RETEST_COUNT_items.append(int(RANK_ASC_items[h]))
			else:
				RETEST_COUNT_items.append(0)

	#print UNIT_TEST_TIME_GROSS_items
	for i in range(len(LOT_ID_items)):
		#print i
		temp.append(UNIT_SERIAL_NUMBER_items[i])
		temp.append(RANK_ASC_items[i])
		temp.append(RANK_DESC_items[i])
		temp.append(DISP_FLAG_items[i])
		temp.append(LOT_ID_items[i])
		temp.append(FACILITY_items[i])
		temp.append(OPERATION_items[i])
		temp.append(DEVICE_items[i])
		temp.append(PART_NAME_items[i])
		temp.append(TEST_CODE_items[i])
		temp.append(PRODUCT_items[i])
		temp.append(VARIANT_items[i])
		temp.append(XT_CAT_items[i])
		temp.append(EQPT_ID_items[i])
		temp.append(STAGE_items[i])
		temp.append(SLT_CAT_items[i])
		temp.append(SITE_items[i])
		temp.append(FILE_FINISH_TS_items[i])
		temp.append(UNIT_TS_items[i])
		temp.append(float(UNIT_TEST_TIME_items[i])/1000)
		temp.append(UNIT_TEST_TIME_GROSS_items[i])
		temp.append(UNIT_INDEX_TIME_items[i])
		temp.append(UNIT_PAUSE_TIME_items[i])	
		temp.append(UNIT_LONG_PAUSE_TIME_items[i])
		temp.append(UNIT_LOT2LOT_TIME_items[i])
		temp.append(UNIT_IDLE_TIME_items[i])
		temp.append(UNIT_NEGATIVE_TIME_items[i])
		temp.append(UNIT_INDEX_TIME_Cat_items[i])
		temp.append(RETEST_COUNT_items[i])
		#Writer.writerow(temp)
		if PRODUCT_items[i] != 'eng':
			ocontent_Unit_Level_data.append(temp)
		else:
			ocontent_Unit_Level_Lost_data.append(temp)
		temp = []
		#print ocontent
		
	ofiles=open(testProgramPath.replace('.', '_' + sys._getframe().f_code.co_name + '.'),'wb')
	
	Writer = csv.writer(ofiles)
	FirstLine = ['UNIT_SERIAL_NUMBER', 'RANK_ASC', 'RANK_DESC', 'DISP_FLAG', 'LOT_ID', 'FACILITY', 'OPERATION', 'DEVICE', 'PART_NAME', 'TEST_CODE', 'PRODUCT', 'VARIANT', 'XT_CAT', 'EQPT_ID', 'STAGE', 'SLT_CAT', 'SITE_ID', 'FILE_FINISH_TS', 'UNIT_TS', 'UNIT_TEST_TIME', 'UNIT_TEST_TIME_GROSS', 'UNIT_INDEX_TIME', 'UNIT_PAUSE_TIME', 'UNIT_LONG_PAUSE_TIME', 'UNIT_LOT2LOT_TIME', 'UNIT_IDLE_TIME', 'UNIT_NEGATIVE_TIME', 'UNIT_INDEX_TIME_CAT', 'RETEST_COUNT']
	Writer.writerow(FirstLine)
	Writer.writerows(ocontent_Unit_Level_data)
	
	print '##################' + 'final data save to ' + testProgramPath.replace('.', '_' + sys._getframe().f_code.co_name + '.') + '##################'
	
	ofiles.close()
	
	ofiles=open(testProgramPath.replace('.', '_' + sys._getframe().f_code.co_name + '_Lost' + '.'),'wb')
	
	Writer = csv.writer(ofiles)
	FirstLine = ['UNIT_SERIAL_NUMBER', 'RANK_ASC', 'RANK_DESC', 'DISP_FLAG', 'LOT_ID', 'FACILITY', 'OPERATION', 'DEVICE', 'PART_NAME', 'TEST_CODE', 'PRODUCT', 'VARIANT', 'XT_CAT', 'EQPT_ID', 'STAGE', 'SLT_CAT', 'SITE_ID', 'FILE_FINISH_TS', 'UNIT_TS', 'UNIT_TEST_TIME', 'UNIT_TEST_TIME_GROSS', 'UNIT_INDEX_TIME', 'UNIT_PAUSE_TIME', 'UNIT_LONG_PAUSE_TIME', 'UNIT_LOT2LOT_TIME', 'UNIT_IDLE_TIME', 'UNIT_NEGATIVE_TIME', 'UNIT_INDEX_TIME_CAT', 'RETEST_COUNT']
	Writer.writerow(FirstLine)
	Writer.writerows(ocontent_Unit_Level_Lost_data)
	
	print '##################' + 'final data save to ' + testProgramPath.replace('.', '_' + sys._getframe().f_code.co_name + '_Lost' + '.') + '##################'
	ofiles.close()
	#ocontent_Unit_Level_data = []

def Lot_Level_data():
	global temp
	global ocontent_LOT_level_data
	FACILITY_items_compress = list(set(FACILITY_items))
	FACILITY_items_compress.sort()
	for line in FACILITY_items_compress:
		
		j = FACILITY_items.index(line)   # first unit on this lot line num
		k = FACILITY_items.count(line)   #  unit num. of lot 
		#print line, j, k
		OPERATION_items_compress = list(set(OPERATION_items[j:j+k]))
		OPERATION_items_compress.sort
		
		for line2 in OPERATION_items_compress:
					
			l = j + OPERATION_items[j:j+k].index(line2)  #first tester on this lot line num
			m = OPERATION_items[j:j+k].count(line2)	#unit num. of this tester
			LOT_ID_items_compress = list(set(LOT_ID_items[l:l+m]))
			LOT_ID_items_compress.sort
				
			for line3 in LOT_ID_items_compress:
			
				n = l + LOT_ID_items[l:l+m].index(line3)  #first tester on this lot line num
				o = LOT_ID_items[l:l+m].count(line3)	#unit num. of this tester

				temp.append(line)
				temp.append(line2)
				temp.append(line3)
				#temp.append(PRODUCT_dict.get(DEVICE_items[n], 'eng'))
				temp.append(PRODUCT_items[n])
				temp.append(SLT_CAT_items[n])
				temp.append(VARIANT_items[n])
				temp.append(XT_CAT_items[n])

				
				temp.append(len(list(set(UNIT_SERIAL_NUMBER_items[n : n+o]))))
				temp.append(o)
				temp.append(float(o)/len(list(set(UNIT_SERIAL_NUMBER_items[n : n+o]))))  #IR
				temp.append(sum(UNIT_TEST_TIME_items[n : n+o])/o)  #average testtime
				
				
				temp.append(o - UNIT_INDEX_TIME_items[n : n+o].count(0))  #index time
				
				gross = (sum(UNIT_INDEX_TIME_items[n : n+o]) + sum(UNIT_PAUSE_TIME_items[n : n+o]) + sum(UNIT_LONG_PAUSE_TIME_items[n : n+o]) + sum(UNIT_LOT2LOT_TIME_items[n : n+o]))
				interval = (sum(UNIT_INDEX_TIME_items[n : n+o]) + sum(UNIT_PAUSE_TIME_items[n : n+o]) + sum(UNIT_LONG_PAUSE_TIME_items[n : n+o]) + sum(UNIT_LOT2LOT_TIME_items[n : n+o]) + sum(UNIT_IDLE_TIME_items[n : n+o]))
				
				if (o - UNIT_INDEX_TIME_items[n : n+o].count(0)) != 0:
				
					temp.append(sum(UNIT_INDEX_TIME_items[n : n+o])/(o - UNIT_INDEX_TIME_items[n : n+o].count(0))) 
				else:
					temp.append(0)				
				
				temp.append(o - UNIT_PAUSE_TIME_items[n : n+o].count(0))  #pause time
				
				if (o - UNIT_PAUSE_TIME_items[n : n+o].count(0)) != 0:
				
					temp.append(sum(UNIT_PAUSE_TIME_items[n : n+o])/(o - UNIT_PAUSE_TIME_items[n : n+o].count(0))) 	
				else:
					temp.append(0)
									
				temp.append(o - UNIT_LONG_PAUSE_TIME_items[n : n+o].count(0))  #long pause time
				if (o - UNIT_LONG_PAUSE_TIME_items[n : n+o].count(0)) != 0:
					temp.append(sum(UNIT_LONG_PAUSE_TIME_items[n : n+o])/(o - UNIT_LONG_PAUSE_TIME_items[n : n+o].count(0))) 
				else:
					temp.append(0)	
				temp.append(o - UNIT_LOT2LOT_TIME_items[n : n+o].count(0))  #lot2lot time
				
				if (o - UNIT_LOT2LOT_TIME_items[n : n+o].count(0)) != 0:
				
					temp.append(sum(UNIT_LOT2LOT_TIME_items[n : n+o])/(o - UNIT_LOT2LOT_TIME_items[n : n+o].count(0))) 
				
				else:
					temp.append(0)
					
				temp.append(o - UNIT_IDLE_TIME_items[n : n+o].count(0))  #idle time
					
				if (o - UNIT_IDLE_TIME_items[n : n+o].count(0)) != 0:
				
					temp.append(sum(UNIT_IDLE_TIME_items[n : n+o])/(o - UNIT_IDLE_TIME_items[n : n+o].count(0))) 
				
				else:
					temp.append(0)

				temp.append(o - UNIT_NEGATIVE_TIME_items[n : n+o].count(0))  #negative time
					
				if (o - UNIT_NEGATIVE_TIME_items[n : n+o].count(0)) != 0:
				
					temp.append(sum(UNIT_NEGATIVE_TIME_items[n : n+o])/(o - UNIT_NEGATIVE_TIME_items[n : n+o].count(0))) 
				
				else:
					temp.append(0)
					
				temp.append(o - RETEST_COUNT_items[n : n+o].count(0))  #negative time
				
				if (o - RETEST_COUNT_items[n : n+o].count(0)) != 0:        
				
					temp.append(sum(RETEST_COUNT_items[n : n+o])/float(o - RETEST_COUNT_items[n : n+o].count(0)))
				
				else:
					temp.append(0)	
				
				#usage
				if (interval) != 0:        
				
					temp.append(float(gross)/interval)
				else:
					temp.append(0)
				#uhpe
				if (gross) != 0:        

					temp.append((float(o)*3600)/gross)
				
				else:
					temp.append(0)									
				ocontent_LOT_level_data.append(temp)
				temp = []
	ofiles=open(testProgramPath.replace('.', '_' + sys._getframe().f_code.co_name + '.'),'wb')
	
	Writer = csv.writer(ofiles)
	Writer.writerow(['FACILITY', 'OPERATION', 'LOT_ID', 'PRODUCT', 'SLT_CAT', 'VARIANT','XT_CAT', 'QTY_IN', 'TOTAL_TD', 'IR', 'UNIT_TEST_TIME', 'UNIT_INDEX_COUNT', 'UNIT_INDEX_TIME', 'UNIT_PAUSE_COUNT', 'UNIT_PAUSE_TIME', 'UNIT_OTHER_COUNT', 'UNIT_OTHER_TIME', 'UNIT_LOT2LOT_COUNT', 'UNIT_LOT2LOT_TIME', 'UNIT_IDLE_COUNT', 'UNIT_IDLE_TIME', 'UNIT_NEGATIVE_COUNT', 'UNIT_NEGATIVE_TIME', 'RETEST_RATE_COUNT', 'RETEST_RATE', 'USAGE', 'UPHe'])
	Writer.writerows(ocontent_LOT_level_data)
	print '##################' + 'final data save to ' + testProgramPath.replace('.', '_' + sys._getframe().f_code.co_name + '.') + '##################'
	ofiles.close()	
	ocontent_LOT_level_data = []
			
if __name__ == "__main__":
	print DIE2DIE_LIMIT_dict['ASLT']['INDEX_PAUSE']
	database = shelve.open(sys.path[0] + '/' + 'product.db')
#   lot level data	
	#files=open(testProgramPath,'rb')
	#files.readline()

	#Reader=csv.reader(files)
	#Reader = sorted(Reader, key = lambda x: (x[6], x[17], x[15][-11:-8], x[18]))   # lot fistly, then tester, then site, then begin time for capture lot start end time
 
	#for line in Reader:
    	
   		#content.append(line)
	#files.close()
	
	#extract_data()
	#Lot_level_data()


#   times sort level data
	#files=open(testProgramPath,'rb')
	#FirstLine = files.readline().replace('"', '').replace('\n', '')
	
	#FirstLine = FirstLine.split(',')

	#Reader=csv.reader(files)
	#Reader = sorted(Reader, key = lambda x: (x[FirstLine.index('EQPT_ID')], x[FirstLine.index('UNIT_TS')], x[FirstLine.index('SITE_ID')]))   # tester firstly, unit start time, then site
 	
	#for line in Reader:
    	
   	#	content.append(line)
	#files.close()

	#extract_data('IR_LotTT_UnitTT')
	#IR_LotTT_UnitTT_data()
	#content = []
# index time and pause time

	files=open(testProgramPath,'rb')
	FirstLine = files.readline().replace('"', '').replace('\n', '')
	
	FirstLine = FirstLine.split(',')

	Reader=csv.reader(files)
	
	#Reader = sorted(Reader, key = lambda x: (x[FirstLine.index('EQPT_ID')], x[FirstLine.index('SITE_ID')], x[FirstLine.index('UNIT_TS')]))  # tester firstly, then site, then unit start time
 	
	for line in Reader:
    		if line[FirstLine.index('EQPT_ID')].find('_') != -1:
			line[FirstLine.index('EQPT_ID')] = line[FirstLine.index('EQPT_ID')].split('_')[0]
   		content.append(line)
	files.close()
	
	content = sorted(content, key = lambda x: (x[FirstLine.index('EQPT_ID')], x[FirstLine.index('SITE_ID')], x[FirstLine.index('UNIT_TS')]))  # tester firstly, then site, then unit start time

	#print content
	extract_data('')
	Unit_Level_data()
	content = []
	
	print PRODUCT_BLACK_items
# IR on Facility -> Operation -> Lot ID -> Product -> Variant -> QTY in	-> Total TD -> IR
	

	
	ocontent = sorted(ocontent_Unit_Level_data, key = lambda x: (x[FirstLine.index('FACILITY')], x[FirstLine.index('OPERATION')], x[FirstLine.index('LOT_ID')]))  # facility firstly, then operation, then lot_ID

	#print content
	module_extract_data('Lot_Level')
	Lot_Level_data()
	
	
	
	database.sync()
	database.close()