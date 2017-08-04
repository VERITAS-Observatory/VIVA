#This module contains the classes for managing the processing of a runlist through a stage of vegas

import subprocess
import os
import re
import time
from . import configwriter
from . import runmanager
from . import condor

#Base class consisting of the shared methods of the VEGAS analysis stages.
class VAStage:
	jobs = {}
			
	def is_condor_enabled(self):
		
		if 'USECONDOR' in self.configdict.get(self.stgconfigkey).keys():
			if self.configdict.get(self.stgconfigkey).get('USECONDOR') in ['1','True', 'true']:
				self.usecondor = True
		elif 'USECONDOR' in self.configdict.get('GLOBALCONFIG').keys():
			if self.configdict.get('GLOBALCONFIG').get('USECONDOR') in ['1', 'True', 'true']:
				self.usecondor = True
		else:
			self.usecondor = False
		return self.usecondor

	#Loop over the rungroup and write the condor submission files
	def write_condor_files(self):
		print('{0} : Writing condor files in {1}.'.format(self.stgconfigkey,self.outputdir))
		vegas_path = self.get_vegas_path()	
		juniverse='vanilla'
		jexecutable = vegas_path + '/bin/vaStage' + self.stage
		jenvironment = "\"VEGAS={0}\"".format(vegas_path)
		jrequirements=''
				
		self.jobs = {}  
		for run in self.runlist.keys():
			jsubid = run
			jarguments = self.get_arguments_str(run)
			jlog = 'condor_' + run + '.log'	
			jout = 'condor_' + run + '.out'
			jerror = 'condor_' + run + '.error'
			cj = condor.CondorJob(executable=jexecutable, universe=juniverse, requirements=jrequirements, arguments=jarguments, log=jlog, error=jerror, output=jout, subid=jsubid, workingdir=self.outputdir, image_size='', environment=jenvironment)
			self.jobs.update({run : cj})		
	
	def execute(self):

		if self.copies_input:
			self.copy_input_to_output()
		
		#Check that the input files exist
		if self.needs_cvbf:
			self.check_for_input('cvbf', [self.configdict.get('GLOBALCONFIG').get('RAWDATADIR')], True)
		if self.needs_root:		
			self.check_for_input('root', self.inputdirs)
		if self.stage == '6':
			self.write_stg6_runlist()
                
		#write condor files (if condor enables)
		if self.is_condor_enabled():
			self.write_condor_files()
		else:
			raise Exception("Cannot yet process jobs locally, add USECONDOR=True to the global configuration.")

		if self.is_condor_enabled():
			for run, job in self.jobs.items():
				if self.use_existing == True:
					if self.existing_output.get(run) == True:
						#A file for this run already exist in the output directory
						print('{0}: Found existing file for run {1}'.format(self.stgconfigkey,run))
						job.status = 'terminated'
						job.existstatus = '0'
					else:
						print('{0}: Submitting job for run {1} to condor.'.format(self.stgconfigkey,run))
						job.execute()
				else:
					print('{0}: Submitting job for run {1} to condor.'.format(self.stgconfigkey,run))
					job.execute()
		else:
			raise JobExecError('I don\'t know how to exectute properly without using condor yet...')	
		self.status = 'started'
	
	#Defined for each stage individually
	def get_arguments_str(self,run):
		return ''
	
	def update_status(self):
		n_submitted=0
		n_executing=0
		n_terminated=0
		n_failed=0
		if self.status == 'initialized':
			return  #stage not yet executed
		else:
			for run in self.jobs.keys():
				jstatus=self.jobs[run].get_status()
				if jstatus == 'submitted':
					n_submitted = n_submitted + 1
				elif jstatus == 'executing':
					n_executing = n_executing + 1
				elif jstatus in ['terminated', 'aborted']:
					n_terminated = n_terminated + 1
					jexitstatus = self.jobs[run].exitstatus
					if jexitstatus == None:
						#Give the logfile a chance to update
						time.sleep(0.5)
						jstatus=self.jobs[run].get_status()
						jexitstatus = self.jobs[run].exitstatus
					if jexitstatus != '0':
						n_failed = n_failed + 1
		#print([self.stgconfigkey,n_submitted, n_executing,n_terminated,n_failed])
		#Abort processing this run group if one of the runs fails	
		if n_failed > 0:
			print('Error occured while running analysis for ', self.stgconfigkey, '. This analysis has been aborted.')
			self.kill()
			self.status = 'failed'
		elif n_terminated == len(self.jobs.keys()):
			self.status = 'succeeded'
		elif n_executing > 0:
			self.status = 'executing'
		elif n_submitted > 0:
			self.status = 'submitted'
		

	def get_status(self):
		self.update_status()
		return self.status

	def is_working(self):
		if self.status in ['initialized', 'executing', 'submitted']:
			return True
		else:
			return False

	def kill(self):
		if self.usecondor:
			for run in self.jobs.keys():
				self.jobs[run].kill()
		else:	
			raise JobExecError('I don\'t know how to exectute properly without using condor yet...')
	
	#copies root files from the input directory to the outputdirectory. This is needed as some stages modify an existing file rather than creating a new one
	def copy_input_to_output(self):
		print('{0} : Copying output root file from previous stage...\n Directories: {1}'.format(self.stgconfigkey,self.inputdirs))
		copyprocs = []
		file_pat = re.compile('([0-9]+)([.]*.*[.])root')
		for dir in self.inputdirs:
			for file in os.listdir(dir):
				m = file_pat.match(file)
				if m != None :
					#Only the copy the files if they appear in the runlist for this stage
					if m.group(1) in self.runlist.keys():
						#Only copy if we intend to overwrite the existing output for this run
						if self.use_existing and self.existing_output[m.group(1)]:
							pass
						else:
							oldfile = dir + '/' + m.group()
							newfile = self.outputdir + '/' + m.group(1) + '.stg' + self.stage + '.root'
							proc = subprocess.Popen(['cp', oldfile, newfile])
							copyprocs.append(proc)
		#Ensure we wait for all the copying jobs to finish
		is_copying=True
		while(is_copying):
			poll_results = []
			for p in copyprocs:
				poll_results.append(p.poll())
			if not None in poll_results:
				is_copying = False
			else:
				time.sleep(0.25)

	#returns the path to the file for a specific run. Searchs directories in the list dirs.
	#Setting the ddate_dir argument to True will a append a prefix directory of the form dyyyymmdd to the search path 
	def get_file(self, run, filetype, dirs, ddate_dir=False):
		
		filename=None
		file_pat = re.compile(run + '([.]*.*[.])' + filetype)
		
		for fd in dirs:
			#append the ddata directory if necessary
			if ddate_dir == True:
				fd = fd + '/' + self.runlist[run].ddate

			files = os.listdir(fd)
			for file in files:
				m = file_pat.match(file)
				if m != None:
					filename = fd + '/' + file
		return filename
				
		
		

	#Check the input directorys for a file associated with a run
	def check_for_input(self, filetype, inputdirs, ddate_dir=False):
										
		for run in self.runlist.keys():
			file = self.get_file(run,filetype,inputdirs,ddate_dir)
			if file == None:
				err_str = self.stgconfigkey + ' : input ' + filetype + ' file could not be found for run ' + run
				raise InputFileError(err_str)

		return True #No errors raised

	#check whether to us existing output root files. Overwrite old files by default.
	def use_existing_output(self):

		use_existing = False
		
		if 'USEEXISTINGOUTPUT' in self.configdict.get(self.stgconfigkey).keys():
			if self.configdict.get(self.stgconfigkey).get('USEEXISTINGOUTPUT').lower() in ['true', '1', 'totally']:
				use_existing = True
		elif 'USEEXISTINGOUTPUT' in self.configdict.get('GLOBALCONFIG').keys():
			if self.configdict.get('GLOBALCONFIG').get('USEEXISTINGOUTPUT').lower() in ['true', '1', 'totally']:
                                use_existing = True
		
		return use_existing
	
	#Check the output directory to determine what runs have an existing output root file.
	def anl_existing_output(self):
		has_existing = {}
		for run in self.runlist.keys():
			file = self.get_file(run,'root',[self.outputdir])
			if file != None:
				has_existing.update({run : True})
			else:
				has_existing.update({run : False})
		return has_existing

	
	#get the path the vegas directory (the parent directory of bin, common, resultsExtractor, etc.)
	def get_vegas_path(self):
		print('{0} : Looking for the appropriate version of VEGAS to use...'.format(self.stgconfigkey))
		vp = None
		if 'VEGASPATH' in self.configdict[self.stgconfigkey].keys():
			vp = self.configdict[self.stgconfigkey].get('VEGASPATH')
		elif 'VEGASPATH' in self.configdict['GLOBALCONFIG'].keys():
			vp = self.configdict['GLOBALCONFIG'].get('VEGASPATH')
		else:
			try: 
				vp = os.environ['VEGAS']
			except KeyError:
				try:
					vp = os.environ['VERITASBASE'] + '/vegas'
				except KeyError:
					vp = None
		
		if vp == None or not os.path.isdir(vp): 
			err_str = '{0}: could not resolve vegas path'.format(self.stgconfigkey)
			raise Exception(err_str)
		else:
			if vp.endswith('/'):
				vp = vp[:-1]
			print('{0} : OK, I\'ll use the version of VEGAS found in {1}.'.format(self.stgconfigkey,vp))
			return vp
			
			
		
	#Cleans up files produced by this stage based on the contents of the clean_opts list.						
	def clean_up(self, clean_opts):
		for co in clean_opts:
			if co.lower() == 'all':
				subprocess.run(['rm','-r', self.outputdir])
			else:
				outfile_pat = re.compile('[0-9]+[.]*.*[.]root')
				logfile_pat = re.compile('.*[.](log|err|out|sub)')
				if co.lower() == 'output':
					for file in os.listdir(self.outputdir):
						m = outfile_pat.match(file)
						if m != None:
							path_to_file = self.outputdir + '/' + file
							subprocess.run(['rm', path_to_file])
						
				elif co.lower() == 'output_bad' and self.status != 'initialized':
					for run in self.runlist.keys():
						if self.jobs[run].exitstatus != '0':
							file = self.get_file(run,'root',[self.outputdir])
							subprocess.run(['rm', file])
                                                                
				if co.lower() == 'logs':
					for file in os.listdir(self.outputdir):
						m = logfile_pat.match(file)
						if m != None:
							path_to_file = self.outputdir + '/' + self.outputdir
							subprocess.run(['rm', path_to_file])
					
				elif co.lower() == 'logs_bad' and self.status != 'initialized':
					for run in self.runlist.key():
						if self.jobs[run].exitstatus != '0':
							run_logfile_pat = re.compile('.*' + run + '[.](log|err|out|sub)')
							for file in os.listdir(self.outputdir):
								m = run_logfile_pat.match(file)
								if m != None:
									path_to_file = self.outputdir + '/' + file
									subprocess.run(['rm', path_to_file])
						
	
#Standard stage 1 analysis 
class VAStage1(VAStage):

	stage = '1'
	copies_input=False
	needs_cvbf=True
	needs_root=False

	def __init__(self, **kwargs):
		self.rungroups=kwargs.get('rungroups')
		self.grouptag=kwargs.get('grouptag')
		self.configdict=kwargs.get('configdict')
		self.inputdirs=kwargs.get('inputdirs')
		self.outputdir=kwargs.get('outputdir')

		if self.grouptag != '':
                	self.stgconfigkey='VASTAGE' + str(self.stage) + ':' + self.grouptag
		else:
			self.stgconfigkey='VASTAGE' + str(self.stage)

		#for stage 1, we need to create a combined run list with both calibration and data runs
		
		self.runlist = {}
		for k, rg in self.rungroups.items():
			self.runlist.update(rg.datarundict)
			self.runlist.update(rg.calibrundict)
		
		self.existing_output = self.anl_existing_output()
		self.use_existing = self.use_existing_output()
		
                #write config file
		cw = configwriter.ConfigWriter(self.configdict, self.stgconfigkey, self.stage, self.outputdir)
		self.config = cw.write('config')

		self.status='initialized'

	def get_arguments_str(self,run):
		arg_str = ''
    			
		arg_str = arg_str + "-Stage1_DBHost=" + self.configdict.get('GLOBALCONFIG').get('DBHOSTNAME')
		#arg_str = arg_str + r' -Stage1_DBPort=\"\"'
		if self.runlist.get(run).runtype == 'calib':
			arg_str = arg_str + " -Stage1_RunMode=" + "flasher"
		else:
			arg_str = arg_str + " -Stage1_RunMode=" + "data"
		
		arg_str = arg_str + " -config=" + self.config

		rawdir = self.configdict.get('GLOBALCONFIG').get('RAWDATADIR') + '/' + self.runlist[run].ddate
		#Check for the file in the raw data directory
		pattern=re.compile(run + '.*[.]cvbf')
		file_exist=False
		for file in os.listdir(rawdir):
			m = pattern.match(file)
			if(m != None):
				inputfile = m.group()
				file_exist = True
				break
		if not file_exist:
			err_str = self.stgconfigkey + ' : Could not find a raw data file for run ' + run + ' in input directory ' + rawdir
			raise InputFileError(err_str)
			
		arg_str = arg_str + " " + rawdir + "/" + inputfile
		arg_str = arg_str + " " + self.outputdir + "/" + run + '.stg1.root'
			
		return arg_str

class VAStage2(VAStage):

	stage='2'
	copies_input=True
	needs_cvbf=True
	needs_root=True

	def __init__(self,*args,**kwargs):
               	
		self.rungroups=kwargs.get('rungroups')
		self.grouptag=kwargs.get('grouptag')
		self.configdict=kwargs.get('configdict')
		self.inputdirs=kwargs.get('inputdirs')
		self.outputdir=kwargs.get('outputdir')

		if self.grouptag != '':
			self.stgconfigkey='VASTAGE' + str(self.stage) + ':' + self.grouptag
		else:
			self.stgconfigkey='VASTAGE' + str(self.stage)
 
		self.runlist = {}
		for k, rg in self.rungroups.items():
			self.runlist.update(rg.datarundict)
		
		self.existing_output = self.anl_existing_output()
		self.use_existing = self.use_existing_output()
		
                #write config file
		cw = configwriter.ConfigWriter(self.configdict, self.stgconfigkey, self.stage, self.outputdir)
		self.config = cw.write('config')

		self.status='initialized'
	
	def get_arguments_str(self, run):
		arg_str = ''
		arg_str = arg_str + '-config='+self.config
		
		rawdatafile = self.get_file(run, 'cvbf', [self.configdict.get('GLOBALCONFIG').get('RAWDATADIR')], True)
		calibfile = self.get_file(self.runlist.get(run).calib, 'root', self.inputdirs)
		#Recall that stage 1 file has been copied into the data directory
		datafile = self.get_file(run, 'root', [self.outputdir])

		arg_str = arg_str + ' ' + rawdatafile + ' ' + datafile + ' ' + calibfile
		
		return arg_str

class VAStage4(VAStage):

	stage='4.2'
	copies_input=True
	needs_cvbf=False
	needs_root=True

	def __init__(self, **kwargs):
		self.rungroups=kwargs.get('rungroups')
		self.grouptag=kwargs.get('grouptag')
		self.configdict=kwargs.get('configdict')
		self.inputdirs=kwargs.get('inputdirs')
		self.outputdir=kwargs.get('outputdir')

		if self.grouptag != '':
			self.stgconfigkey='VASTAGE' + str(self.stage[0]) + ':' + self.grouptag
		else:
			self.stgconfigkey='VASTAGE' + str(self.stage[0])

		self.runlist = {}
		for k, rg in self.rungroups.items():
			self.runlist.update(rg.datarundict)

		self.existing_output = self.anl_existing_output()
		self.use_existing = self.use_existing_output()

                #write config and cut files
		cw = configwriter.ConfigWriter(self.configdict, self.stgconfigkey, self.stage, self.outputdir)
		self.config = cw.write('config')
		self.cuts = cw.write('cuts')
		
		if 'LTM_LookupTableFile' not in self.configdict[self.stgconfigkey].keys():
			print('{0} : No lookup table specified. If this is a standard analysis, your jobs will likely fail.'.format(self.stgconfigkey)) 
			print('Consider adding LTM_LookupTableFile=\'filepath_to_lt\' to your instructions file definition for this stage.')
	
		self.status='initialized'
	
	def get_arguments_str(self, run):
		arg_str = ''
		arg_str = arg_str + '-config=' + self.config
		arg_str = arg_str + ' -cuts=' + self.cuts
		
		datafile = self.get_file(run, 'root', [self.outputdir])
		arg_str = arg_str + ' ' + datafile
		
		return arg_str

class VAStage5(VAStage):
	
	stage='5'
	copies_input=False
	needs_cvbf=False
	needs_root=True

	def __init__(self, **kwargs):

		self.rungroups=kwargs.get('rungroups')
		self.grouptag=kwargs.get('grouptag')
		self.configdict=kwargs.get('configdict')
		self.inputdirs=kwargs.get('inputdirs')
		self.outputdir=kwargs.get('outputdir')

		if self.grouptag != '':
			self.stgconfigkey='VASTAGE' + str(self.stage) + ':' + self.grouptag
		else:
			self.stgconfigkey='VASTAGE' + str(self.stage)

		self.runlist = {}
		for k, rg in self.rungroups.items():
			self.runlist.update(rg.datarundict)
			
		self.existing_output = self.anl_existing_output()
		self.use_existing = self.use_existing_output()
		
		cw = configwriter.ConfigWriter(self.configdict, self.stgconfigkey, self.stage, self.outputdir)
		self.config = cw.write('config')
		self.cuts = cw.write('cuts')
        	
		if not 'Method' in self.configdict[self.stgconfigkey].keys():
			info_str = '{0} : Warning, no event selection method was specified with Method=<method_type>.\n'.format(self.stgconfigkey)
			info_str = info_str + 'Note: vaStage5 will default to VANullEventSelection, which is unlikely to be desireable.\n'
			info_str = info_str + 'Consider adding Method=\"VAStereoEventSelection\" or Method=\"VACombinedEventSelection\" to the instructions file.'
			print(info_str)
        
		self.status='initialized'


	def get_arguments_str(self, run):
		arg_str = ' '
		arg_str = arg_str + '-config=' + self.config
		arg_str = arg_str + ' -cuts=' + self.cuts
		
		#time cuts
		timecut_str = self.runlist.get(run).timecuts
		if not timecut_str == '':
			arg_str = arg_str + ' -ES_CutTimes=' + timecut_str		

		stg4file = self.get_file(run, 'root', self.inputdirs)
		stg5file = self.outputdir + '/' + run + '.stg5.root'
		
		arg_str = arg_str + ' -inputFile ' + stg4file
		arg_str = arg_str + ' -outputFile ' + stg5file

		return arg_str

class VAStage6(VAStage):
	
	stage='6'
	copies_input=False
	needs_cvbf=False
	needs_root=True

	def __init__(self, **kwargs):

		self.rungroups=kwargs.get('rungroups')
		self.grouptag=kwargs.get('grouptag')
		self.configdict=kwargs.get('configdict')
		self.inputdirs=kwargs.get('inputdirs')
		self.outputdir=kwargs.get('outputdir')

		if self.grouptag != '':
			self.stgconfigkey='VASTAGE' + str(self.stage) + ':' + self.grouptag
		else:
			self.stgconfigkey='VASTAGE' + str(self.stage)
		
		#For stage 6, this is only used internally, as separate runlist file is written.
		self.runlist = {}
		for k, rg in self.rungroups.items():
			self.runlist.update(rg.datarundict)

		self.existing_output = self.anl_existing_output()
		self.use_existing = self.use_existing_output()

		self.stg6_group_config()

                #write config and cut files
		cw = configwriter.ConfigWriter(self.configdict, self.stgconfigkey, self.stage, self.outputdir)
		self.config = cw.write('config')
		self.cuts = cw.write('cuts')

		self.status='initialized'

	def get_arguments_str(self):

		arg_str = ''
		arg_str = arg_str + '-S6A_Batch=1'
		arg_str = arg_str + ' -S6A_OutputFileName=' + self.stgconfigkey.lower().replace(':','-') + '_'
		arg_str = arg_str + ' -S6A_ConfigDir=' + self.outputdir
		arg_str = arg_str + ' -S6A_Batch=1'
		arg_str = arg_str + ' -cuts=' + self.cuts 
		arg_str = arg_str + ' -config=' + self.config
		arg_str = arg_str + ' ' + self.stg6_rlfilename

		return arg_str
	
	#Writes the runlist for stage6
	#The file format follows the specifications found at https://veritas.sao.arizona.edu/OAWGwiki/index.php/Vegas_v2_5_newRunStyle	
	def write_stg6_runlist(self):

		self.stg6_rlfilename = self.outputdir + '/' + 'runlist_stg6.txt'
		
		with open(self.stg6_rlfilename, 'w') as rl:
			for gidx, group in enumerate(self.rungroups.keys()):

				if gidx != 0:
					rl.write('[RUNLIST ID: ' + str(gidx) + ']\n')

				for ridx, run in enumerate(self.rungroups.get(group).datarundict):
					stg5file = self.get_file(run, 'root', self.inputdirs)
					rl.write(stg5file + '\n')

				if gidx !=0:
					rl.write('[/RUNLIST ID: ' + str(gidx) + ']\n')
				
				rl.write('[EA ID: ' + str(gidx) + ']\n')
				try:
					rl.write(self.ea_dict[group] + '\n')
				except KeyError:
					pass
				rl.write('[/EA ID: ' + str(gidx) + ']\n')
				
				rl.write('[CONFIG ID: ' + str(gidx) + ']\n')
				try:
					for opt in self.group_config[group]:
						rl.write(opt + '\n')
				except KeyError:
					pass
				rl.write('[/CONFIG ID: ' + str(gidx) + ']\n')

	#determine the the configuration and effective area to be used for each rungroup in stage 6
	def stg6_group_config(self):
		self.group_config = {}
		self.ea_dict = {}		
		for group in self.rungroups.keys():
			config_list = []
			for k, v in self.configdict.get(self.stgconfigkey).items():
				if group == k.partition(':')[2]:
					opt = k.partition(':')[0]
					if opt == 'EA':
						self.ea_dict.update({group : v})
					else:
						config_list.append(opt + ' ' + v)
			self.group_config.update({group : config_list})
	
	#Need only one submit file needed for stage6
	def write_condor_files(self):

		vegas_path = self.get_vegas_path()	
		juniverse='vanilla'
		jexecutable = vegas_path + '/bin/vaStage' + self.stage
		jenvironment = "\"VEGAS={0}\"".format(vegas_path)
		jrequirements=''

		self.jobs = {}

		jsubid = 'stg6'
		jarguments = self.get_arguments_str()
		jlog = 'condor_' + jsubid + '.log'
		jout = 'condor_' + jsubid + '.out'
		jerror = 'condor_' + jsubid + '.error'

		cj = condor.CondorJob(executable=jexecutable, universe=juniverse, requirements=jrequirements, arguments=jarguments, log=jlog, error=jerror, output=jout, subid=jsubid, workingdir=self.outputdir, image_size='', environment=jenvironment)
		self.jobs.update({'stg6' : cj})
						
	def anl_existing_output(self):

		has_existing = {}
		file = self.get_file('','root',[self.outputdir])
		if file != None:
			has_existing.update({'stg6' : True})
		else:
			has_existing.update({'stg6' : False})

		return has_existing
		
class InputFileError(Exception):
	pass
				
class JobExecError(Exception):
	pass	
