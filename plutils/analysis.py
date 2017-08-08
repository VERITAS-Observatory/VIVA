#analysis.py: Top-level analysis class for the pipeline

import sys
import os
import subprocess
import time
import re
from . import vastage
from . import runmanager

class AnalysisCore():
	def __init__(self, **kwargs):

		self.update_T = 0.25 #Period of the update cycle during the execution loop

		self.configdict=kwargs.get('configdict')
		self.runmanager=kwargs.get('runmanager')

		self.workingdir=None 		
		try: 
			self.workingdir = self.configdict['GLOBALCONFIG'].get('WORKINGDIR')
		except KeyError:
			print('The working directory was not specified using WORKINGDIR=')
			raise
		else:
			print('Top-level (working) directory: {0}'.format(self.workingdir))

		self.n_stgs = 0
		self.stg_objs = {}
		self.stg_dirs = {}
		self.stg_reqs = {}
		self.stg_status = {}

		self.make_directories()
		self.anl_reqs()

		self.configure()

		self.status = 'initialized'

	def __str__(self):
		return 'Analysis Core'
		
	#Creat an object associated with each run tag
	def configure(self):
		for k in self.configdict.keys():
			#Separate the stage from the group string
			if k.lower().startswith('vastage'):

				group_tag = k.partition(':')[2]
				stg_inputdirs = self.get_input_dirs(k)
				stg_outputdir = self.get_output_dir(k)				
					
				#Get the rungroups to be processed through a different state
				stg_rungroups=self.runmanager.get_run_groups(group_tag)
			
				#Get an object of the correct class
				stg_obj_type = self.get_vastage_obj_type(k)
				stg_obj = stg_obj_type(configdict=self.configdict, rungroups=stg_rungroups, grouptag=group_tag, inputdirs=stg_inputdirs, outputdir=stg_outputdir)

				self.stg_objs.update({k : stg_obj})

		self.n_stgs = len(self.stg_objs.keys())
		if self.n_stgs == 0:
			err_str = 'No configuration options found for any known stages of VEGAS.\n'
			err_str = err_str + 'Check the instructions file to ensure that the VASTAGE configurations are defined correctly'
			raise NoStgConfigsError(err_str)
	
	#Get a vastage object of the correct using stage key 
	def get_vastage_obj_type(self, stgkey):
		stg_type = stgkey.partition(':')[0].lower()
		vastgs = dir(sys.modules['plutils.vastage'])
		for stg in vastgs:
			if stg_type == stg.lower():
				return getattr(sys.modules['plutils.vastage'], stg)
		return None #No such object found
	

	def get_input_dirs(self, stgkey):
		print('{0} : Analyzing input directories...'.format(self))
		stg_config = self.configdict.get(stgkey)
		inputdirs = []
		stg_num = self.stgnum_from_key(stgkey)		
		if 'INPUTDIR' in stg_config.keys():
			#user override of the input directory
			idir = stg_config['INPUTDIR']
			print('    {0} : User specified input directory = {1}'.format(stgkey,idir))
			inputdirs.append(idir)
		elif stg_num == '1':
			idir = self.configdict['GLOBALCONFIG'].get('RAWDATADIR')
			print('    {0} : Raw data directory = {1}'.format(stgkey, idir))
			inputdirs.append(idir)
		else:
			req_stgs = self.stg_reqs.get(stgkey)
			for req in req_stgs:
				idir = self.stg_dirs.get(req)[1]
				print('    {0} : Input directory = {1}'.format(stgkey,idir))
				inputdirs.append(idir)
		
		return inputdirs

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
		print('{0} : Building directory structure...'.format(self))
		for k in self.configdict.keys():
			top_dir = None
			sub_dir = None
			if 'OUTPUTDIR' in self.configdict.get(k):
				#user override of the ouput directory
				top_dir = self.configdict.get(k).get('OUTPUTDIR')
				print('    {0} : User specified output directory = {1}'.format(k,top_dir))
				subprocess.run(['mkdir','-p', top_dir])
				sub_dir = top_dir
			else:
				if k.lower().startswith('vastage'):
					stg_num = self.stgnum_from_key(k)
			
					top_dir = self.workingdir + '/' + 'vastage' + stg_num 
					subprocess.run(['mkdir', '-p', top_dir])

					if k.partition(':')[2] != '':
						sub_dir = top_dir + '/' + k.partition(':')[2].replace(':','-').lower()
						subprocess.run(['mkdir', '-p', sub_dir])
					else:
						sub_dir = top_dir
				print('    {0} : Creating output directory = {1}'.format(k,sub_dir))
			self.stg_dirs.update({k : [top_dir, sub_dir]})  
				
	
	#Get the vegas stage number from a configuration key			
	def stgnum_from_key(self, stgkey):

		pat = re.compile('vastage([0-9])')
		m = pat.match(stgkey.lower())
		stg_num = m.group(1)
	
		return stg_num				

	#Analyze the requirments for each stage
	#For now, indictating an input directory is understood as meaning the data from the previous stage already exist in that directory. 
	def anl_reqs(self):
		print('Analyzing stage requirements...')
		for k1 in self.configdict.keys():
			reqs = []
			if k1.lower().startswith('vastage'):
				stg_num = self.stgnum_from_key(k1)
				if 'INPUTDIR' in self.configdict[k1].keys():
					reqs.append('')
				elif stg_num == 1:
					#The VAStage1 object checks for the existance of the raw data, and there are no requriments based on previous stages.
					reqs.append('')
				else:
					#Otherwise, each stage depends on the status of the previous stage.
					prev_stg = self.get_prev_stgnum(stg_num)
					group_strs = k1.partition(':')[2].split(':')
					for k2 in self.configdict.keys():
						if k2.lower().startswith('vastage'+prev_stg):
							for gs in group_strs:
								if gs in k2.partition(':')[2].split(':') and k2 not in reqs:
									reqs.append(k2)
				print('    {0} : Required stages = {1}'.format(k1,reqs))
				self.stg_reqs.update({k1 : reqs})
	
	def get_prev_stgnum(self,stg_num):
		if str(stg_num) in ['4','4.2']:
			return '2'
		else:
			return str(int(stg_num)-1)
		

	#For a given stage key, return the requirement for that stage and the status of that requirment
	def check_reqs(self, stgkey):
		req_stgs = self.stg_reqs[stgkey]
		status = None
		if len(req_stgs) == 0: #No reqs case 1 (this case should not currently occur)
			status = 'succeeded'
		elif len(req_stgs) == 1 and req_stgs[0] == '': #No reqs case 2
			status = 'succeeded'
		else:
			n_initialized = 0
			n_submitted = 0
			n_executing = 0
			n_succeeded = 0
			n_failed = 0

			for stg in req_stgs:
				req_status = self.stg_objs.get(stg).get_status()
				if req_status == 'initialized':
					n_initialized = n_initialized + 1
				elif req_status == 'submitted':
					n_submitted = n_submitted + 1
				elif req_status == 'executing':
					n_executing = n_executing + 1
				elif req_status == 'succeeded':
					n_succeeded = n_succeeded + 1
				elif req_status == 'failed':
					n_failed = n_failed + 1
			
			#print('reqs for ', stgkey, ' : ' , [n_initialized, n_submitted,n_executing,n_succeeded,n_failed])
			
			if n_initialized == len(req_stgs):
				status='initialized'
			elif n_submitted > 0 and n_executing==0:
				status = 'submitted'
			elif n_executing > 0 and n_failed == 0:
				status = 'executing'
			elif n_failed > 0:
				status = 'failed'
			elif n_succeeded == len(req_stgs):
				status = 'succeeded'
		
		if status == None:
			err_str = '{0}: Could not resolve status of requirements.'.format(stgkey)
			raise Exception(err_str)

		return [req_stgs, status]

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
			
			self.stg_status.update({key : stg_status})			

		if n_executing > 0 or n_submitted > 0:
			self.status = 'executing'
		elif n_succeeded == self.n_stgs:
			self.status = 'done_good'
		elif n_failed + n_succeeded == self.n_stgs:
			self.status = 'done_bad'
			 
	def get_status(self):
		self.update_status()
		return self.status

	def is_working(self):
		self.update_status()
		if self.status in ['initialized', 'executing']:
			return True 
		else:
			return False
	
	#Primary execution loop
	def execute(self):
		print('Starting main analysis loop!')
		while self.is_working():
				
			for key, stg in self.stg_objs.items():
				#print('{0} : Status = {1}; Req check = {2}'.format(key,stg.status,self.check_reqs(key)[1]))
				if self.check_reqs(key)[1] == 'succeeded' and stg.status == 'initialized':
					print('Starting analysis for stage {0}.'.format(key))
					stg.execute()
				elif self.check_reqs(key)[1] == 'failed' and stg.status != 'failed':
					print('Requirements for stage {0} failed. Cannot proceed with this analysis chain.'.format(key))
					stg.status = 'failed'

			time.sleep(self.update_T)
	
	#Clean up as specified in the instructions file. Local clean up options override global clean up options.
	def clean_up(self):
		print('{0} : Cleaning up...'.format(self))
		for stg in self.stg_objs.keys():
			if "CLEANUP" in self.configdict.get(stg).keys():
				clean_opts = self.configdict.get(stg).get('CLEANUP').split(':')
				self.stg_objs[stg].clean_up(clean_opts)
			elif "CLEANUP" in self.configdict.get('GLOBALCONFIG').keys():
				clean_opts = self.configdict.get('GLOBALCONFIG').get("CLEANUP").split(':')
				self.stg_objs[stg].clean_up(clean_opts)

	#Print the status of the overall analysis and the individual stages
	def print_status(self):
		print('+++ Analysis status = {0} +++'.format(self.status))
		for k,s in self.stg_objs.items():
			s.print_status()
	
		
class NoStgConfigsError(Exception):
	pass		
			
				
					
				
	

