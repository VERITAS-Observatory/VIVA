#Module for handling the configuration and submission of jobs via condor

import subprocess
import os
import time

#class CondorHandler:
	
#	self.use_condor_key="USECONDOR"

#	def __init__(self, *args, **kwargs):
#		self.config_dict=kwargs.get('configdict')

	#Checks whether condor has been enabled for each key in the configuration dictionary
	#Creates a dictionary in the form {CONFIGKEY : True/False}
	#The local configuration had precedence over the global configuration
#	def build_config_dict(self):
#		self.condor_config_dict = {}
#		for k in self.config_dict.keys()
#			if self.use_condor_key in self.config_dict.get(k).keys()
#				if self.config_dict.get(k).get(self.use_condor_key) in ['1', 'true', 'True', 'TRUE']:
#					self.condor_config_dict.update({k : True})
#				else:
#					self.condor_config_dict.update({k : False})
					
	

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
		else
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
			sf.write("Universe    = " + self.universe + '\n')
			sf.write("Executable  = " + self.executable + '\n')
			sf.write("Arguments  = " + self.arguments + '\n')
			sf.write("Requirements = " + self.requirements + '\n')
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
		#sp=subprocess.run(['condor_userlog', log_filename], stdout=subprocess.PIPE)
		#loginfo=sp.stdout.decode('utf-8')
		#Parse the information returned by condor_userlog to get the job ID
		#The following was written for the output from condor_userlog using Condor 8.2.2
		#loginfo_lines=loginfo.splitlines()
		#for line in loginfo_lines:
		#	print(line)
		#	if line == '':
		#		loginfo_lines.remove(lines)

		#self.jobid=loginfo_lines[1].split()[0]
		
		
	def submit(self):

		#check for old logfile and remove if found.
		log_filename = self.workingdir + self.log
		if os.path.isfile(log_filename):
			os.remove(log_filename)		

		sp=subprocess.run(['condor_submit', self.submit_filename], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
		sub_out=sp.stdout.decode('utf-8').splitlines()
		
		#Make sure the job actually submitted...
		errors = []
		for line in sub_out:
			err_idx = line.lower().find('error')
			if err_idx != -1:
				errors.append(line)
		if len(errors) != 0:
			err_str=""
			for e in errors:
				err_str = err_str + e + '\n'
			raise CondorSubmissionError(err_str)
 		
		#time.sleep(1.0) #Condor needs some time to write the log file
		self.get_job_id()
		self.status='submitted'
	
	#checks the status of the job using the logfile
	def update_status(self):
		log_filename = self.workingdir + self.log
		with open(log_filename) as lf:
			for line in lf.readlines():
				line = line.lower()
				if line.find('job terminated') != -1 and (self.status == 'submitted' or self.status == 'executing'):
					self.status='terminated'
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
		
				
	
