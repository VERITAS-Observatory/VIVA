#This module contains the classes for managing the processing of a runlist through a stage of vegas

import subprocess
import os
import re
import time
from . import configwriter
from . import runmanager
from . import condor
from . import fontstyle

#Base class consisting of the shared methods of the VEGAS analysis stages.
class VAStage:
	
	stage = '0'
	vegas_path = ''
	vegas_exec = ''
	
	jobs = {}
	valid_opts = ['USECONDOR', 'USEEXISTINGOUTPUT', 'KILLONFAILURE', 'VEGASPATH', 'CLEANUP', 'INPUTDIR', 'OUTPUTDIR', 'USEDBTIMECUTS', 'EA']
	
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
		juniverse='vanilla'
		jexecutable = self.vegas_exec
		jenvironment = "\"VEGAS={0}\"".format(self.vegas_path)
		jrequirements=''
				
		self.jobs = {} 
		for run in self.runlist.keys():
			jsubid = run
			jarguments = self.get_arguments_str(run)
			jlog = 'condor_' + run + '.log'	
			jout = 'condor_' + run + '.out'
			jerror = 'condor_' + run + '.error'
			if self.needs_cvbf:
				cvbf_file = self.get_file(run, 'cvbf', [self.configdict.get('GLOBALCONFIG').get('RAWDATADIR')], True)
				machine = self.get_file_host(cvbf_file)
				jrequirements = "(machine == \"" + machine + "\")"
				
			cj = condor.CondorJob(executable=jexecutable, universe=juniverse, requirements=jrequirements, arguments=jarguments, log=jlog, error=jerror, output=jout, subid=jsubid, workingdir=self.outputdir, image_size=str(1600*1024), environment=jenvironment)
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

		if n_failed > 0 and n_executing > 0:
			if self.status != 'failed' and 'KILLONFAILURE' in self.configdict[self.stgconfigkey].keys():
				if self.configdict[self.stgconfigkey]['KILLONFAILURE'].lower() in ['true', '1']:
					f_str = '{0} : Failed job detect! Killing this analysis stage...'.format(self.stgconfigkey)
					f_str = self.bad_fmt(f_str)
					print(f_str)
					self.status = 'failed'
					self.kill()
			elif self.status != 'executing':
				self.status = 'executing'
		elif n_failed > 0 and n_terminated == len(self.jobs.keys()):
			if self.status != 'failed':
				f_str = '{0} : Failed runs detected!'.format(self.stgconfigkey)
				f_str = self.bad_fmt(f_str)
				print(f_str) 
				self.status = 'failed'
		elif n_terminated == len(self.jobs.keys()) and self.status != 'failed':
			if self.status != 'succeeded':
				s_str = '{0} : Succeeded!'.format(self.stgconfigkey)
				s_str = self.good_fmt(s_str)
				print(s_str)
				self.status = 'succeeded'
		elif n_executing > 0:
			if self.status != 'executing':
				print('{0} : Executing!'.format(self.stgconfigkey))
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
		print('    {0} : Copying required output root file(s) from previous stage...\n\tDirectories: {1}'.format(self.stgconfigkey,self.inputdirs))
		print('\tThis may take a while. Please be patient...')
		#copyprocs = []
		n_files = len(self.runlist.keys())
		file_cntr = 0
		file_pat = re.compile('([0-9]+)([.]*.*[.])root')
		for dir in self.inputdirs:
			for file in os.listdir(dir):
				m = file_pat.match(file)
				if m != None :
					#Only the copy the files if they appear in the runlist for this stage
					if m.group(1) in self.runlist.keys():
						#Only copy if we intend to overwrite the existing output for this run
						if self.use_existing and self.existing_output[m.group(1)]:
							file_cntr = file_cntr + 1
							info_str = '\t-Skipping file {0} of {1}'
							print(info_str.format(file_cntr, n_files))					
						else:
							file_cntr = file_cntr + 1
							oldfile = dir + '/' + m.group()
							newfile = self.outputdir + '/' + m.group(1) + '.stg' + self.stage + '.root'
							info_str = '\t-Copying file {0} of {1}'
							info_str = info_str.format(file_cntr, n_files)
							print(info_str)

							#New copy behavoir: Copy only one file at times using rsync 
							proc = subprocess.Popen(['rsync', '-t', oldfile, newfile])
							proc.wait()
							
							#copyprocs.append(proc)
		#Old copy behvoir:
		#Ensure we wait for all the copying jobs to finish
		#is_copying=True
		#while(is_copying):
			#poll_results = []
			#for p in copyprocs:
				#poll_results.append(p.poll())
			#if not None in poll_results:
				#is_copying = False
			#else:
				#time.sleep(0.25)

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
				err_str = self.bad_fmt(err_str)
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
		print('    {0} : Looking for the appropriate version of VEGAS to use...'.format(self.stgconfigkey))
		vp = None
		if 'VEGASPATH' in self.configdict[self.stgconfigkey].keys():
			#vp = self.configdict[self.stgconfigkey].get('VEGASPATH')
			wrn_str = "    {0} : VEGASPATH option no longer supported. The VEGAS version will be determined from your environment variables."
			wrn_str = wrn_str.format(self.stgconfigkey)
			wrn_str = self.wrn_fmt(wrn_str)
			print(wrn_str)
		elif 'VEGASPATH' in self.configdict['GLOBALCONFIG'].keys():
			#vp = self.configdict['GLOBALCONFIG'].get('VEGASPATH')
			wrn_str = "    {0} : VEGASPATH option no longer supported. The VEGAS version will be determined from your environment variables."
			wrn_str = wrn_str.format(self.stgconfigkey)
			wrn_str = self.wrn_fmt(wrn_str)
			print(wrn_str)
		
		try: 
			vp = os.environ['VEGAS']
		except KeyError:
			try:
				vp = os.environ['VERITASBASE'] + '/vegas'
			except KeyError:
				vp = None
		
		if vp == None or not os.path.isdir(vp): 
			err_str = '    {0}: could not resolve vegas path'.format(self.stgconfigkey)
			err_str = self.bad_fmt(err_str)
			raise Exception(err_str)
		else:
			if vp.endswith('/'):
				vp = vp[:-1]
			print('    {0} : OK, I\'ll use the version of VEGAS found in {1}.'.format(self.stgconfigkey,vp))
			return vp
			
	#Print the current status of the stage and it's constituent jobs
	def print_status(self):
		print('{0} : Stage status = {1}'.format(self.stgconfigkey, self.status))
		if len(self.jobs.keys()) > 0:
			print('    run : status')
			for r,j in self.jobs.items():
				tmp_str = '    ' + r + ' : '
				if j.exitstatus == '0':
					tmp_str = tmp_str + self.good_fmt(j.exitstatus)
				elif j.exitstatus == '1':
					tmp_str = tmp_str + self.bad_fmt(j.exitstatus)
				else:
					tmp_str = tmp_str + self.bad_fmt('?')
				print(tmp_str)
	
	#Return the overlap among the list of unknown options
	def unk_opts_check(self, *args):
		unk_opts = []
		for i in range(len(args)):
			for opt_tmp in args[i]:
				opt = opt_tmp.split(':')[0] #Should handle the special case of OPT:GRP
				opt_known = False
				if opt in self.valid_opts:
					opt_known = True
				for j in [v for v in range(len(args)) if v != i]:
					if not opt in args[j]:
						opt_known = True
				if not opt_known and opt not in unk_opts:
					unk_opts.append(opt)
				
		return unk_opts		
	
	#Common text formattings
	def bad_fmt(self, err_str):
		return fontstyle.set_style(err_str, txt_clr = 'white', bg_clr='red', format='bold')

	def wrn_fmt(self, wrn_str):
		return fontstyle.set_style(wrn_str, txt_clr = 'black', bg_clr='yellow', format='bold')

	def good_fmt(self, gd_str):
		return fontstyle.set_style(gd_str, txt_clr='white', bg_clr='green', format='bold')	
	
	#Return the hostname of the machine on which the file specified by path is located.
	def get_file_host(self, path):
		#print(path)
		sp = subprocess.Popen(['df', path], stdout=subprocess.PIPE)
		device = sp.communicate()[0].decode('utf-8').split('\n')[1].split()[0]
		sr = device.partition(':')
		hostname = ''
		if sr[1] == '':
			hostname = os.getenv('HOSTNAME')
		else:
			domain = os.getenv('HOSTNAME').split('.',1)[1]
			hostname = sr[0] + '.' + domain
		return hostname
	
	#Return the VEGAS executable for the specified stage.
	def get_executable(self, vegas_path):
		bin_pat = re.compile('vaStage' + self.stage + '.*')
		bins = os.listdir(os.path.join(vegas_path,'bin'))
		executable = ''
		for b in bins:
			m = bin_pat.match(b)
			if m != None:
				executable = os.path.join(vegas_path, 'bin', b)
				break
		if executable == '':
			err_str = '{0}: Executable file for this stage could no be found!'.format(self.stgconfigkey)
			err_str = self.bad_fmt(err_str)
			raise Exception(err_str)
		else:
			return executable			 
	
	#Cleans up files produced by this stage based on the contents of the clean_opts list.						
	def clean_up(self, clean_opts):
		for co in clean_opts:
			if co.lower() == 'all':
				print('{0} : Removing everything from {1}'.format(self.stgconfigkey,self.outputdir))
				subprocess.run(['rm','-r', self.outputdir])
			else:
				outfile_pat = re.compile('[0-9]+[.]*.*[.]root')
				logfile_pat = re.compile('.*[.](log|err|out|sub)')
				if co.lower() == 'output':
					print('{0} : Removing output root files from {1}'.format(self.stgconfigkey,self.outputdir))
					for file in os.listdir(self.outputdir):
						m = outfile_pat.match(file)
						if m != None:
							path_to_file = self.outputdir + '/' + file
							print('    Deleting {0}'.format(path_to_file))
							subprocess.run(['rm', path_to_file])
						
				elif co.lower() == 'output_bad' and self.status != 'initialized':
					print('{0} : Removing output root files of failed jobs from {1}'.format(self.stgconfigkey, self.outputdir))
					for r,j in self.jobs.items():
						if j.exitstatus != '0':
							file = self.get_file(r,'root',[self.outputdir])
							if file != None:
								print('    Deleting {0}'.format(file))
								subprocess.run(['rm', file])
							else:
								print('    No root file found for failed run {0}'.format(r))
                                                                
				if co.lower() == 'logs':
					print('{0} : Removing all logs from {1}'.format(self.stgconfigkey, self.outputdir))
					for file in os.listdir(self.outputdir):
						m = logfile_pat.match(file)
						if m != None:
							path_to_file = self.outputdir + '/' + self.outputdir
							print('    Deleting {0}'.format(path_to_file))
							subprocess.run(['rm', path_to_file])
					
				elif co.lower() == 'logs_bad' and self.status != 'initialized':
					print('{0} : Removing logs for failed jobs from {1}'.format(self.stgconfigkey, self.outputdir))
					for r,j in self.jobs.items():
						if j.exitstatus != '0':
							run_logfile_pat = re.compile('.*' + r + '[.](log|err|out|sub)')
							for file in os.listdir(self.outputdir):
								m = run_logfile_pat.match(file)
								if m != None:
									path_to_file = self.outputdir + '/' + file
									print('    Deleting {0}'.format(path_to_file))
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

		#Get the executable for this stage
		self.vegas_path = self.get_vegas_path()
		self.vegas_exec = self.get_executable(self.vegas_path)
		
                #write config file
		cw = configwriter.ConfigWriter(self.configdict, self.stgconfigkey, self.stage, self.vegas_exec, self.outputdir)
		conf_t = cw.write('config')
		self.config=conf_t[0]
		
		self.unk_opts = self.unk_opts_check(conf_t[1])
		for opt in self.unk_opts:
			wrn_str = '{0} : Unknown options/configuration/cut : {1}'.format(self.stgconfigkey, opt)
			wrn_str = self.wrn_fmt(wrn_str)	
			print(wrn_str)	

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
			err_str = self.bad_fmt(err_str)
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

		self.write_calib_log()
		
		self.existing_output = self.anl_existing_output()
		self.use_existing = self.use_existing_output()

		#Get the executable for this stage
		self.vegas_path = self.get_vegas_path()
		self.vegas_exec = self.get_executable(self.vegas_path)
		
                #write config file
		cw = configwriter.ConfigWriter(self.configdict, self.stgconfigkey, self.stage, self.vegas_exec, self.outputdir)
		conf_t = cw.write('config')
		self.config=conf_t[0]
		
		self.unk_opts = self.unk_opts_check(conf_t[1])
		for opt in self.unk_opts:
			wrn_str = '{0} : Unknown options/configuration/cut : {1}'.format(self.stgconfigkey, opt)
			wrn_str = self.wrn_fmt(wrn_str)	
			print(wrn_str)	

		self.status='initialized'
	
	def get_arguments_str(self, run):
		arg_str = ''
		arg_str = arg_str + '-config='+self.config
		
		rawdatafile = self.get_file(run, 'cvbf', [self.configdict.get('GLOBALCONFIG').get('RAWDATADIR')], True)
		if self.runlist.get(run).multi_calibs:
			calibfile = self.combine_calibs(run)
			info_str = '{0} : Using the following combined calibration file for run {1}\n\t{2}'
			info_str = info_str.format(self.stgconfigkey, run, calibfile)
			print(info_str)
			
		else:
			calibfile = self.get_file(self.runlist.get(run).calibs[0], 'root', self.inputdirs)
		#Recall that stage 1 file has been copied into the data directory
		datafile = self.get_file(run, 'root', [self.outputdir])

		arg_str = arg_str + ' ' + rawdatafile + ' ' + datafile + ' ' + calibfile
		
		return arg_str
	
	#Create a composite calibration run and return its file name
	def combine_calibs(self, runnum):

		tel_calib_files = [None,None,None,None]
		calibs = self.runlist.get(runnum).calibs
		combine_id = ''

		for i,cr in enumerate(calibs):
			tel_calib_files[i] = self.get_file(cr, 'root', self.inputdirs)
			combine_id = combine_id + cr
			if i != len(calibs)-1:
				combine_id = combine_id + '_'

		outfilepath = os.path.join(self.outputdir, combine_id + '.root')

		
		#Only create the combined calibration run if does not already exist
		if self.get_file(combine_id, 'root', [self.outputdir]) == None:
			
			info_str = "{0} : Creating combined calibration run {1}.root in output directory."
			info_str = info_str.format(self.stgconfigkey, combine_id)
			print(info_str)

			macros_dir = self.get_macros_dir()
			
			info_str = '{0} : Macros Directory: {1}'
			print(info_str.format(self.stgconfigkey, macros_dir))		
			#logon_macro = 'rootlogon.C' #os.path.join(macros_dir,'rootlogon.C')
			#combine_macro = 'combineLaser.C' #os.path.join(macros_dir,'combineLaser.C')

			#copy base file to stg2 outputdir
			sp = subprocess.Popen(['cp', tel_calib_files[0], outfilepath], stdout=subprocess.PIPE)
			sp.wait()

			combine_cmd =  'combineLaser(\"{0}\",\"{1}\",\"{2}\",\"{3}\",\"{4}\")'
			combine_cmd = combine_cmd.format(outfilepath, tel_calib_files[0], tel_calib_files[1], tel_calib_files[2], tel_calib_files[3])

			combine_macro = self.write_combine_macro(combine_id, combine_cmd)

			sp = subprocess.Popen(['root', '-b', '-q', macros_dir, 'rootlogon.C', combine_macro],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
			sp.wait()
			
			root_out = sp.communicate()[0].decode('utf-8').split('\n')
			root_out_log = os.path.join(self.outputdir, 'combine_' + combine_id + '.out')
			
			with open(root_out_log, 'w') as f:
				for line in root_out:
					f.write(line + '\n')
		
		return outfilepath
	
	#return the path to the vegas macros directory
	def get_macros_dir(self):
		vb = os.getenv('VEGAS')
		sp = subprocess.Popen(['find',vb,'-type','f','-name','combineLaser.C'], stdout=subprocess.PIPE)
		sp.wait()
		results1 = sp.communicate()[0].decode('utf-8').split('\n')
		macros_dir = ''
		#In newer builds of VEGAS, there are mutiple copies of the macro, so must find one in a directory with a logon script
		for r in results1:
			dir = r.rpartition('/')[0]
			sp = subprocess.Popen(['find', dir,'-type','f','-name','rootlogon.C'], stdout=subprocess.PIPE)
			sp.wait()
			results2 = sp.communicate()[0].decode('utf-8').split('\n')
			if results2[0] != '':
				macros_dir = results2[0].rpartition('/')[0]
				break				
			
		if macros_dir == '':
			err_str = "{0}: Could not find VEGAS macros directory while trying to combine calibration runs!".format(self.stgconfigkey)
			err_str = self.bad_fmt(err_str)
			raise Exception(err_str)
		
		return macros_dir

	#Write a temporary macro for the combination process (needed to work around ROOT weirdness)
	#Need to be executed from the $VEGAS/macros directory
	def write_combine_macro(self, combine_id, combine_cmd):
		macro_file = os.path.join(self.outputdir, 'combine_' + combine_id + '.C')
		with open(macro_file,'w') as mf:
			mf.write('void combine_' + combine_id + '(){\n')
			#if os.getenv("CXX") != '':
				#mf.write('gSystem->Setenv(\"PATH\",\"' + os.getenv("CXX").rpartition('/')[0] +'\");\n')
			mf.write(combine_cmd + ';\n')
			mf.write('}')
		return macro_file
	
	def write_calib_log(self):
		logfile = os.path.join(self.outputdir, "calib_log.txt")
		info_str = '{0}: Writing calibration log file to {1}'
		info_str = info_str.format(self.stgconfigkey, logfile)
		with open(logfile,'w') as lf:
			for k,rg in self.rungroups.items():
				for r,robj in rg.datarundict.items():
					calib_str = "{0}    {1}    {2}    {3}    {4}\n"
					calib_str = calib_str.format(r,robj.calibs[0],robj.calibs[1],robj.calibs[2],robj.calibs[3])
					lf.write(calib_str)
		
		

class VAStage4(VAStage):

	stage='4'
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

		#Get the executable for this stage
		self.vegas_path = self.get_vegas_path()
		self.vegas_exec = self.get_executable(self.vegas_path)

                #write config and cut files
		cw = configwriter.ConfigWriter(self.configdict, self.stgconfigkey, self.stage, self.vegas_exec, self.outputdir)
		conf_t = cw.write('config')
		cuts_t = cw.write('cuts')
		self.config = conf_t[0]
		self.cuts = cuts_t[0]
		
		self.unk_opts = self.unk_opts_check(conf_t[1], cuts_t[1])
		for opt in self.unk_opts:
			wrn_str = '{0} : Unknown options/configuration/cut : {1}'.format(self.stgconfigkey, opt)
			wrn_str = self.wrn_fmt(wrn_str)	
			print(wrn_str)	
		
		if 'LTM_LookupTableFile' not in self.configdict[self.stgconfigkey].keys():
			wrn_str = '{0} : No lookup table specified. If this is a standard analysis, your jobs will likely fail.\n'.format(self.stgconfigkey) 
			wrn_str = wrn_str + 'Consider adding LTM_LookupTableFile=\'filepath_to_lt\' to your instructions file definition for this stage.'
			wrn_str = self.wrn_fmt(wrn_str)
			print(wrn_str)
	
		self.status='initialized'
	
	def get_arguments_str(self, run):
		arg_str = ''
		arg_str = arg_str + '-config=' + self.config
		arg_str = arg_str + ' -cuts=' + self.cuts
		if 'OverrideLTCheck' in self.configdict[self.stgconfigkey].keys():
			if self.configdict[self.stgconfigkey]['OverrideLTCheck'].lower() in ['true', '1', 'roger']:
				wrn_str = '    {0} : Overriding the check on the values used in the creation of the LookUp Table.\n'
				wrn_str = wrn_str + '    If this was not a deliberate selection, set OverrideLTCheck=0.'
				wrn_str = wrn_str.format(self.stgconfigkey)
				wrn_str = self.wrn_fmt(wrn_str)
				print(wrn_str)
				arg_str = arg_str + ' -OverrideLTCheck=1'
			else:
				arg_str = arg_str + ' -OverrideEACheck=0'		
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

		#Get the executable for this stage
		self.vegas_path = self.get_vegas_path()
		self.vegas_exec = self.get_executable(self.vegas_path)
		
		cw = configwriter.ConfigWriter(self.configdict, self.stgconfigkey, self.stage, self.vegas_exec, self.outputdir)
		conf_t = cw.write('config')
		cuts_t = cw.write('cuts')
		self.config = conf_t[0]
		self.cuts = cuts_t[0]
		
		self.unk_opts = self.unk_opts_check(conf_t[1], cuts_t[1])
		for opt in self.unk_opts:
			wrn_str = '{0} : Unknown options/configuration/cut : {1}'.format(self.stgconfigkey, opt)
			wrn_str = self.wrn_fmt(wrn_str)	
			print(wrn_str)	
        	
		if 'USEDBTIMECUTS' in self.configdict[self.stgconfigkey].keys():
			if self.configdict[self.stgconfigkey]['USEDBTIMECUTS'].lower() in ['true', '1', 'totally']:
				info_str = '\t{0} : Database timecuts will be used in addition to any user specifed timecuts.'
				info_str = info_str.format(self.stgconfigkey)
				print(info_str)

				self.use_db_timecuts = True
			else:
				info_str = '\t{0} : Using only user specifed timecuts. Set USEDBTIMECUTS=True to include DB timecuts'
				info_str = info_str.format(self.stgconfigkey)
				info_str = self.wrn_fmt(info_str)
				print(info_str)

				self.use_db_timecuts = False
		else:
			info_str = '\t{0} : Using only user specifed timecuts. Add option USEDBTIMECUTS=True to include DB timecuts.'
			info_str = info_str.format(self.stgconfigkey)
			info_str = self.wrn_fmt(info_str)
			print(info_str)

			self.use_db_timecuts = False

		if not 'Method' in self.configdict[self.stgconfigkey].keys():
			info_str = '{0} : Warning, no event selection method was specified with Method=<method_type>.\n'.format(self.stgconfigkey)
			info_str = info_str + 'Note: vaStage5 will default to VANullEventSelection, which is unlikely to be desireable.\n'
			info_str = info_str + 'Consider adding Method=\"VAStereoEventSelection\" or Method=\"VACombinedEventSelection\" to the instructions file.'
			info_str = self.wrn_fmt(info_str)
			print(info_str)
		
		timecut_log_name = self.stgconfigkey + '_timecuts_log.txt'
		timecut_log_name = timecut_log_name.lower().replace(':','-')
		self.timecutlog = os.path.join(self.outputdir, timecut_log_name)
		tcl = open(self.timecutlog, 'w+')
		tcl.close()
		
        
		self.status='initialized'


	def get_arguments_str(self, run):
		arg_str = ' '
		arg_str = arg_str + '-config=' + self.config
		arg_str = arg_str + ' -cuts=' + self.cuts
		
		#time cuts
		timecut_str = self.build_timecut_str(run)
		self.log_timecut(run, timecut_str)
		if not timecut_str == '':
			arg_str = arg_str + ' -ES_CutTimes=' + timecut_str		

		stg4file = self.get_file(run, 'root', self.inputdirs)
		stg5file = self.outputdir + '/' + run + '.stg5.root'
		
		arg_str = arg_str + ' -inputFile ' + stg4file
		arg_str = arg_str + ' -outputFile ' + stg5file

		return arg_str

	def build_timecut_str(self, run):
		user_timecuts = self.runlist.get(run).user_timecuts
		db_timecuts = self.runlist.get(run).db_timecuts
		
		#tmp_str = '{0} : usr={1}, db={2}'
		#tmp_str = tmp_str.format(run,user_timecuts,db_timecuts)
		#print(tmp_str)
		
		timecuts = user_timecuts
		if self.use_db_timecuts and db_timecuts != '':
			if timecuts != '':
				timecuts = timecuts + ',' + db_timecuts
			else:
				timecuts = db_timecuts

		return timecuts

	def log_timecut(self, runnum, timecuts):
		with open(self.timecutlog, 'a') as tcl:
			line = runnum + ' ' + timecuts + '\n'
			tcl.write(line)
		 

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

		#Get the executable for this stage
		self.vegas_path = self.get_vegas_path()
		self.vegas_exec = self.get_executable(self.vegas_path)

                #write config and cut files
		cw = configwriter.ConfigWriter(self.configdict, self.stgconfigkey, self.stage, self.vegas_exec, self.outputdir)
		conf_t = cw.write('config')
		cuts_t = cw.write('cuts')
		self.config = conf_t[0]
		self.cuts = cuts_t[0]
		
		self.unk_opts = self.unk_opts_check(conf_t[1], cuts_t[1])
		for opt in self.unk_opts:
			wrn_str = '{0} : Unknown options/configuration/cut : {1}'.format(self.stgconfigkey, opt)
			wrn_str = self.wrn_fmt(wrn_str)	
			print(wrn_str)	

		self.status='initialized'

	def get_arguments_str(self):

		arg_str = ''
		arg_str = arg_str + '-S6A_Batch=1'
		arg_str = arg_str + ' -S6A_OutputFileName=' + self.stgconfigkey.lower().replace(':','-')
		arg_str = arg_str + ' -S6A_ConfigDir=' + self.outputdir
		arg_str = arg_str + ' -S6A_Batch=1'
		arg_str = arg_str + ' -cuts=' + self.cuts 
		arg_str = arg_str + ' -config=' + self.config
		if 'OverrideEACheck' in self.configdict[self.stgconfigkey].keys():
			if self.configdict[self.stgconfigkey]['OverrideEACheck'].lower() in ['true', '1', 'roger']:
				wrn_str = '    {0} : Overriding the check on the cut values used in the creation of the effective area.\n'
				wrn_str = wrn_str + '    If this was not a deliberate selection, set OverrideEACheck=0.'
				wrn_str = wrn_str.format(self.stgconfigkey)
				wrn_str = self.wrn_fmt(wrn_str)
				print(wrn_str)
				arg_str = arg_str + ' -OverrideEACheck=1'
			else:
				arg_str = arg_str + ' -OverrideEACheck=0'
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
						v_tmp = v.strip('"') #Strips "'s if provided in EA path -- more user friendly
						self.ea_dict.update({group : v_tmp})
					#Process the bdt cuts file for each group 
					elif opt == 'BDTCUTSFILE':
						v_temp = v.strip('"')
						with open(v_temp, 'r') as fbdt:
							lines = fbdt.readlines()
							for line in lines:
								if line != '':
									config_list.append(line)		
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

		cj = condor.CondorJob(executable=jexecutable, universe=juniverse, requirements=jrequirements, arguments=jarguments, log=jlog, error=jerror, output=jout, subid=jsubid, workingdir=self.outputdir, image_size=str(1600*1024), environment=jenvironment)
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
