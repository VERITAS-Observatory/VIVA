#analysis.py: Top-level analysis class for the pipeline

import sys
import subprocess
import time
import re
from . import vastage

class AnalysisCore():
	def __init__(self, **kwargs):

		self.update_T = 0.25 #Period of the update cycle during the execution loop

		self.configdict=kwargs.get('configdict')
		self.runmanager=kwargs.get('runmanager')

		self.workingdir=self.configdict.get('GLOBALCONFIG').get('WORKINGDIR')
		
		self.n_stgs = 0
		self.stg_objs = {}
		self.stg_dirs = {}
		self.stg_reqs = {}
		self.stg_status = {}

		self.make_directories()
		self.anl_reqs()

		self.configure()

		self.status = 'initialized'
		
	#Creat an object associated with each run tag
	def configure(self):
		for k in configdict.keys():
			#Separate the stage from the group string
			if k.lower().startswith('vastage'):

				stage = k.lower().partition(':')[0]
				group_tag = k.partition(':')[2]
				stg_inputdir = self.get_input_dir(k)
				stg_outputdir = self.get_output_dir(k)				
					
				#Get the rungroups to be processed through a different state
				stg_rungroups=self.runmanger.get_run_groups(group_tag)
			
				#Get an object of the correct class
				stg_obj_type = getattr(sys.modules[__name__], stage)
				stg_obj = stg_obj_type(configdict=self.configdict, rungroups=stg_rungroups, grouptag=group_tag, inputdir=stg_inputdir, outputdir=stg_outputdir)

				self.stg_objs.update({key, stg_obj})

		self.n_stgs = len(self.stg_objs.keys())
		if self.n_stgs == 0:
			err_str = 'No configuration options found for any known stages of VEGAS.\n'
			err_str = err_str + 'Check the instructions file to ensure that VASTAGE configurations are defined correctly'
			raise NoStgConfigsError(err_str)	

	def get_input_dir(self, stgkey):
		
		stg_config = self.configdict.get(stgkey)
		inputdir = None		
		
		if 'INPUTDIR' in stg_config.keys():
			#user override of the inputdirectory
			inputdir=stg_config.get('INPUTDIR')
		elif stage_num == '1':
			inputdir = self.configdict.get('GLOBALCONFIG').get('RAWDATADIR')
		else:
			req_stg = self.stg_reqs.get(stgkey)
			inputdir = self.stg_dirs.get(req_stg)[1]
		
		return inputdir

	def get_output_dir(self, stgkey):
		stg_config = self.configdict.get(stgkey)
		outputdir = None	

		if 'OUTPUTDIR' in stg_config.keys():
			#user override of the output directory
			outputdir = stg_config.get('OUTPUTDIR')
		else:
			outputdir = self.stg_dirs.get(stgkey)[1]
		
		return outputdir
			 
		
	#Create the directory structure needed for this analysis
	def make_directories(self):
		
		for k in configdict.keys():
			top_dir = None
			sub_dir = None
			if 'OUTPUTDIR' in configdict.get(k):
				#user override of the ouput directory
				top_dir = configdict.get(k).get('OUTPUTDIR')
				subprocess.run(['mkdir','-p', top_dir)
				sub_dir = top_dir
			else:
				if k.lower().startswith('vastage'):
					stg_num = stgnum_from_key(k)
			
					top_dir = self.workingdir + '/' + 'vastage' + stg_num 
					subprocess.run(['mkdir', '-p', top_dir])

					if k.partition(':')[2] != '':
						sub_dir = top_dir + '/' + k.partition(':')[2].replace(':','-').lower()
						subprocess.run(['mkdir', '-p', sub_dir)
					else:
						sub_dir = top_dir
			self.stg_dirs.update({k : [top_dir, sub_dir]})  
				
	
	#Get the vegas stage number from a configuration key			
	def stgnum_from_key(self, stgkey)

		pat = re.compile('vastage([0-9])')
                m = pat.match(stgkey.lower())
                stg_num = m.group(1)
	
		return stg_num				

	#Analyze the requirments for each stage
	#For now, indictating an input directory is understood as meaning the data from the previous stage already exist in that directory. 
	def anl_reqs(self):
		for k1 in configdict.keys():
			if k1.lower().startswith('vastage'):
				stg_num = stgnum_from_key(k1)
				if 'INPUTDIR' in stg_config.keys(k1):
					self.stg_reqs.update({k1 : ''})
				elif stg_num == 1:
					#The VAStage1 object checks for the existance of the raw data, and there are no requriments based on previous stages.
					self.stg_req.update({k1 : ''})
				else:
					#Otherwise, each stage depends on the status of the previous stage.
					prev_stg = str( int(stg_num) - 1)
					group_str = k1.partition(':')[2]
					for k2 in configdict.keys():
						if k2.lower().startswith('vastage'+prev_stg):
							if k2.partition(':')[2].find(group_str) != -1:
								self.stg_reqs.update({k1 : k2})
	
	#For a given stage key, return the requirement for that stage and the status of that requirment
	def check_reqs(self, stgkey):
		req_stg = self.stg_reqs(stgkey)
		status = None
		if req_stg == ''
			status = 'succeeded'
		else:
			status = self.stg_objs(req_stg).get_status()
		
		return [req_stg, status]

	def update_status(self):

		n_submitted = 0
		n_executing = 0
		n_failed = 0
		n_succeeded = 0

		n_stgs = len(self.stg_objs.keys())

		for key, stg in self.stg_objs.items():
			stg_status = stg.get_status()
			if stg_status == 'submitted':
				n_submitted = n_submitted + 1
			elif stg_status == 'executing':
				n_executing = n_executing + 1
			elif stg_status == 'failed':
				n_failed = n_failed + 1
			elif stg_status == 'succeeded':
				n_succeeded = n_succeeded + 1
			
			self.stg_status.update({key, stg_status})			

		if n_executing > 0 or n_submitted > 0:
			self.status = 'executing'
		elif n_succeeded == self.n_stgs:
			self.status = 'done_good'
		elif n_failed + n_succeeded == self.n_stgs:
			self.status = 'done_bad'
			 
	def get_status(self):
		self.update_status()
		return self.status

	def is_working(self)
		self.update_status()
		if self.status in ['initialized', 'executing']:
			return True 
		else:
			return False
	
	#Primary execution loop
	def execute(self):
		while self.is_working():

			for key, stg in self.stg_objs.items():
				if self.check_reqs(key)[1] == 'succeeded' and stg.status == 'initialized':
					stg.execute()
				elif self.check_reqs(key)[1] == 'failed'
					stg.status = 'failed'

			time.sleep(self.update_T)
		
class NoStgConfigsError(Exception):
	pass		
			
				
					
				
	

