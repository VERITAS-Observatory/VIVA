#Module for handling the configuration and submission of jobs via condor

import subprocess
import os
import time

class CondorJob:
	
	def __init__(self, **kwargs):
		self.workingdir=kwargs.get('workingdir')
		self.universe=kwargs.get('universe')
		self.executable=kwargs.get('executable')
		self.arguments=kwargs.get('arguments')
		self.requirements=kwargs.get('requirements')
		self.log=kwargs.get('log')
		self.output=kwargs.get('output')
		self.error=kwargs.get('error')
		if 'subid' in kwargs.keys():
			self.subid=kwargs.get('subid')
		else:
			self.subid = ''
		
		if not self.workingdir.endswith('/'):
			self.workingdir=self.workingdir+'/'

		self.exitstatus=None
		self.write_submit_file()
		self.status='configured'
	
	#Method for writing the condor submit file
	def write_submit_file(self):
		self.submit_filename = self.workingdir + 'condor_' + self.subid + '.sub'
		with open(self.submit_filename,'w') as sf:
			sf.write("Universe     = " + self.universe + '\n')
			sf.write("Executable   = " + self.executable + '\n')
			sf.write("Arguments    = " + self.arguments + '\n')
			sf.write("Requirements = " + self.requirements + '\n')
			sf.write("GetEnv       = True" + '\n')
			sf.write('\n')
			sf.write("Output = " + self.workingdir + self.output + '\n')
			sf.write("Error  = " + self.workingdir + self.error + '\n')
			sf.write("Log    = " + self.workingdir + self.log + '\n')
			sf.write('\n')
			sf.write("Queue")		
	
	#Method for getting information on the job from the first two lines of the condor log file
	def get_job_id(self):
		log_filename= self.workingdir + self.log

		loginfo_lines=[]
		try:
			lf = open(log_filename, 'r')
		except OSError:
			raise CondorLogError('Condor log file cannot be opended for reading. I need a log file to function!')
		else:
			loginfo_lines=lf.readlines()
			lf.close()	
		
		jobid_tmp = loginfo_lines[0].split('(')[1].split(')')[0]
		jobid_num = jobid_tmp.split('.')[0]
		jobid_subidx = jobid_tmp.split('.')[2]
		jobid_subidx = jobid_subidx[:2].lstrip('0') + jobid_subidx[2]
		
		self.jobid = jobid_num + '.' + jobid_subidx

	def submit(self):

		#check for old logfile and remove if found.
		log_filename = self.workingdir + self.log
		if os.path.isfile(log_filename):
			os.remove(log_filename)		

		sp=subprocess.Popen(['condor_submit', self.submit_filename], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
		sub_out=sp.stdout.read().decode('utf-8')
		
		
		#Make sure the job actually submitted...
		while(sp.poll() == None):
			time.sleep(0.05)
		if sp.poll() != 0:
			err_str = 'Condor submission failed:\n {0}'.format(sub_out)
			raise CondorSubmissionError(err_str) 
				
		#errors = []
		#for line in sub_out:
		#	err_idx = line.lower().find('error')
		#	if err_idx != -1:
		#		errors.append(line)
		#if len(errors) != 0:
		#	err_str=""
		#	for e in errors:
		#		err_str = err_str + e + '\n'
		#	raise CondorSubmissionError(err_str)
 		
		self.get_job_id()
		self.status='submitted'

	def execute(self):
		self.submit()
	
	#checks the status of the job using the logfile
	def update_status(self):
		log_filename = self.workingdir + self.log
		with open(log_filename) as lf:
			for line in lf.readlines():
				line = line.lower()
				if line.find('job terminated') != -1 and self.status in ['submitted', 'executing']:
					self.status='terminated'
				elif line.find('aborted') != -1 and self.status in ['submitted', 'executing']:
					self.status='aborted'
					self.exitstatus = 1
				elif line.find('job executing') != -1 and self.status == 'submitted':
					self.status='executing'
				
				#Check the exit status of the job if it has terminated
				if line.find('return value ') != -1 and self.status == 'terminated':
					self.exitstatus=line.split('return value ')[1][0]
	
	def get_status(self):
		self.update_status()
		return self.status

	def kill(self):
		subprocess.run(['condor_rm', self.jobid])
		self.update_status()
		
		
class CondorSubmissionError(Exception):
	pass		
		
class CondorLogError(Exception):
	pass
		
				
	
