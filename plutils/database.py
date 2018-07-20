#This module contains to infrastructure for handling communication with the VERITAS database

import pymysql
import datetime

class DBConnection:
	online_db="VERITAS"
	offline_db="VOFFLINE"

	def __init__(self, *args, **kwargs):
		#Extract the database information from the configuration dictionary
		configdict=kwargs.get('configdict')
		self.host=configdict.get('GLOBALCONFIG').get('DBHOSTNAME')
		self.user=configdict.get('GLOBALCONFIG').get('DBUSERNAME')
		
		#connect to servers and create cursors
		self.connect()
	
	#Connect to the dbs and create cursors
	def connect(self):
		self.online_dbcnx = pymysql.connect(host=self.host, db=self.online_db, user=self.user, cursorclass=pymysql.cursors.DictCursor)
		self.online_cursor = self.online_dbcnx.cursor()
	
		self.offline_dbcnx = pymysql.connect(host=self.host, db=self.offline_db, user=self.user, cursorclass=pymysql.cursors.DictCursor)
		self.offline_cursor = self.offline_dbcnx.cursor()	
	
	#Close connection
	def close(self):
		self.online_dbcnx.close()
		self.offline_dbcnx.close()
	
	#return the calibration (laser/flasher) run assigned to a given data run
	def get_calib_runs(self, runnum):
		tel_calibs = [None,None,None,None]

		query="SELECT run_id FROM tblRun_Info WHERE run_id IN (SELECT run_id from tblRun_Group WHERE group_id = %s) AND run_type IN ('flasher', 'laser')"

		groups = self.get_calib_groups(runnum)
		for g in groups:
			self.online_cursor.execute(query, g)
			try:
				calib_run=str(self.online_cursor.fetchone().get('run_id'))
				inc_tels = self.get_included_tels(g)
				for i,v in enumerate(inc_tels):
					if inc_tels[i]:
						tel_calibs[i] = calib_run
			except AttributeError:
				calib_run=None
				err_str = "Could not determine the calibration run for run {0} for calibration group {1}".format(runnum,g)
				print(err_str)
			
		
		return tel_calibs
	
	#Return a list with the calibration groups for each run. Most of the time this will have only one element.
	#In the case where there are more than calibrations runs per data run, this list will have more than one element
	def get_calib_groups(self, runnum):
		query = "SELECT group_id from tblRun_GroupComment WHERE group_id IN (SELECT group_id from tblRun_Group where run_id = %s) AND group_type = 'laser'"
		self.online_cursor.execute(query, runnum)
		groups = []
		try:
			temp = self.online_cursor.fetchall()
			for v in temp:
				groups.append(str(v['group_id']))
					
		except AttributeError:
			err_str = "Could not determine the calibration group(s) for run {0}".format(runnum)
			print(err_str)
		
		return groups
			
	
	#Returns a list indicating whether a telescope is included in particular calibration group
	#This is a list of 1's and 0's, providing an inclusion boolean for each telescope in the order [T1,T2,T3,T4]
	def get_included_tels(self, group_id):
		inc_tels = [None,None,None,None]
		query = "SELECT excluded_telescopes FROM tblRun_GroupComment WHERE group_id = %s"
		self.online_cursor.execute(query, group_id)

		ex_int = self.online_cursor.fetchone().get('excluded_telescopes')
		ex_bin = bin(ex_int)[2:]
		ex_bin = '0'*(4-len(ex_bin)) + ex_bin
		
		for i in range(0,4):
			if ex_bin[3-i] == "0":
				inc_tels[i] = 1
			else:
				inc_tels[i] = 0

		return inc_tels

		

						

	#return the datetime associated with a given run
	#note that this will return an object from the standard datetime class	
	def get_datetime(self, runnum):
		query="SELECT db_start_time FROM tblRun_Info WHERE run_id = %s"
		self.online_cursor.execute(query, runnum)
		try:
			dt=self.online_cursor.fetchone().get('db_start_time')
		except AttributeError:
			dt = None
			err_str = "No datetime found for run {0}. Are you sure you have a valid run number?".format(runnum)
			print(err_str)
		return dt

	#returns the dyyyymmdd date string associated with a given run
	def get_ddate(self, runnum):
		dt=self.get_datetime(runnum)

		year=str(dt.year)
		if(dt.month < 10):
			month = '0' + str(dt.month)
		else:
			month = str(dt.month)
		if(dt.day < 10):
			day = '0' + str(dt.day)
		else:
			day = str(dt.day)
		
		ddate = 'd' + year + month + day
		return ddate

	#returns the source_id for a given run
	def get_source_id(self, runnum):
		query="SELECT source_id FROM tblRun_Info WHERE run_id = %s"
		self.online_cursor.execute(query, runnum)
		source_id=self.online_cursor.fetchone().get('source_id')

		return source_id
	
	#Get the timecut string from the offline database.
	def get_dqm_timecuts(self, runnum):
		query="SELECT time_cut_mask FROM tblRun_Analysis_Comments WHERE run_id = %s"
		self.offline_cursor.execute(query, runnum)
		timecuts=self.offline_cursor.fetchone().get('time_cut_mask')

		if timecuts == None:
			return ''
		else:
			return timecuts



