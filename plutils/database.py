#This module contains to infrastructure for handling communication with the VERITAS database

import pymysql
import datetime

class DBConnection:
	def __init__(self, *args, **kwargs):
		#Extract the database information from the configuration dictionary
		configdict=kwargs.get('configdict')
		self.host=configdict.get('GLOBALCONFIG').get('DBHOSTNAME')	
		self.db=configdict.get('GLOBALCONFIG').get('DBNAME')
		self.user=configdict.get('GLOBALCONFIG').get('DBUSERNAME')
		
		#connect to server and create cursor
		self.connect()
	
	#Connect to the db and create cursor
	def connect(self):
		self.dbcnx = pymysql.connect(host=self.host, db=self.db, user=self.user, cursorclass=pymysql.cursors.DictCursor)
		self.cursor = self.dbcnx.cursor()	
	
	#Close connection
	def close(self):
		self.dbcnx.close()
	
	#return the calibration (laser/flasher) run assigned to a given data run
	def get_calib_run(self, runnum):
		query="SELECT run_id FROM tblRun_Info WHERE run_type IN ('flasher', 'laser') AND run_id IN (SELECT run_id FROM tblRun_Group WHERE group_id = (SELECT group_id FROM tblRun_GroupComment WHERE group_id IN (SELECT group_id FROM tblRun_Group WHERE run_id = %s) AND group_type IN ('laser', 'flasher') LIMIT 1))"
		
		self.cursor.execute(query, runnum)
		try: 
			calib_run=str(self.cursor.fetchone().get('run_id'))
		except AttributeError:
			calib_run = None
			err_str = "Could not determine the calibration run  for run {0}.".format(runnum)
			print(err_str)
		
		return calib_run
	
	#return the datetime associated with a given run
	#note that this will return an object from the standard datetime class	
	def get_datetime(self, runnum):
		query="SELECT db_start_time FROM tblRun_Info WHERE run_id = %s"
		self.cursor.execute(query, runnum)
		try:
			dt=self.cursor.fetchone().get('db_start_time')
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
		self.cursor.execute(query, runnum)
		source_id=self.cursor.fetchone().get('source_id')

		return source_id


