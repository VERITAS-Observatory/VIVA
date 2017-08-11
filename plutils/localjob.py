#Module for handling the processing of job local to the machine

import subprocess
import os
from datetime import datetime
import time
import re

class ShellJobQueue():
	
	def __init__(self, **kwargs):

		self.update_T = 0.1

		self.joblist=kwargs.get('joblist')
		self.proc_limit=int(kwargs.get('proc_limit'))

		self.jobdict = dict(enumerate(self.joblist))
		self.job_status = {}
		self.total_procs = len(self.joblist)
		self.active_procs = 0
		self.exitstatus = None		

		self.status='initialized'
		
	def execute(self):
		print('Executing local job queue...')
		print('Active process limit = ', self.proc_limit)
		while(self.get_status() != 'terminated'):
			for k,j in self.jobdict.items():
				if j.get_status() == 'initialized' and self.active_procs < self.proc_limit:
					print(j)
					j.execute()
					print(j.pid+'p')
					self.active_procs = self.active_procs + 1

			time.sleep(self.update_T)
				
		
	def update_status(self):

		n_executing = 0
		n_failed = 0
		n_succeeded = 0

		for k,j in self.jobdict.items():

			jstatus = j.get_status()
			print(k, ' : status = ', jstatus)	
			if jstatus == 'executing':
				n_executing = n_executing + 1
			elif jstatus == 'terminated':
				if j.exitstatus == '0':
					n_succeeded = n_succeeded + 1
				else:
					n_failed = n_failed + 1
			
			 
			self.job_status.update({k : jstatus})
		
		self.active_procs = n_executing		

		if n_executing > 0:
			self.status = 'executing'
		elif n_succeeded == self.total_procs:
			self.status = 'terminated'
			self.exitstatus = '0'
		elif n_succeeded + n_failed == self.total_procs:
			self.status = 'terminated'
			self.exitstatus = '1'
	
	def get_status(self):
		self.update_status()
		return self.status
		

class ShellJob():

	def __init__(self, **kwargs):

		self.executable=kwargs.get('executable')
		self.args = kwargs.get('args')	
		self.workingdir=kwargs.get('workingdir')
		
		if not self.workingdir.endswith('/'):
			self.workingdir = self.workingdir + '/'

		self.remotehost=None
		if 'remotehost' in kwargs.keys():
			self.remotehost = kwargs.get('remotehost')

		self.out = False
		if 'out' in kwargs.keys():
			self.out = kwargs.get('out')

		self.err = False
		if 'err' in kwargs.keys():
			self.err = kwargs.get('err')

		self.log = True #Popen.poll() doesn't seem to play well with shell scripts...

		self.subid = ''
		if 'subid' in kwargs.keys():
			self.subid  = kwargs.get('subid')
		
		self.id_str = str('job_' + self.subid).rstrip('_')
		self.logf = self.workingdir + self.id_str  + '.log'
		
		#Remove old log file
		subprocess.Popen(['rm','-f', self.logf])		
	
		self.pid = None

		self.exec_file = None
		self.write_exec_file()
		self.logw('Job script: {0}\n'.format(self.exec_file))

		self.proc = None
		self.exitstatus = None
			
		self.status = 'initialized'
	
	def __repr__(self):
		return self.executable + ' ' + str(self.args)

	def __str__(self):
		return self.id_str

	def execute(self):
		if self.remotehost != None:
			rem_cmd = 'bash -s < ' + self.exec_file 
			self.proc = subprocess.Popen(['ssh', self.remotehost, rem_cmd], shell=True)
		else:
			self.proc = subprocess.Popen([self.exec_file],shell=True)
		self.logw('({0}) job started\n'.format(datetime.now()))
		#When a machine is busy, it can a bit of time for a job to start...
		while self.get_pid() == None:
			time.sleep(0.25)
		self.pid = self.get_pid()
		self.status = 'submitted'

	def update_status(self):

		if self.status == 'initialized':
			pass
		elif self.poll() == None:
			self.status = 'executing'
		elif self.poll() == 0 and self.status != 'terminated':
			self.status = 'terminated'
			self.exitstatus = '0'
			self.logw('({0}) job ended\n'.format(datetime.now()))
		elif self.poll() != 0 and self.status != 'terminated':
			self.status = 'terminated'
			self.exitstatus = '1'
			self.logw('({0}) job end\n'.format(datetime.now()))
	
	def get_status(self):
		self.update_status()
		return self.status

	#Writes an executable shell script
	def write_exec_file(self):
		self.exec_file = self.workingdir + self.id_str + '.sh'
		print('Writing executable file: {0}'.format(self.exec_file))
		with open(self.exec_file,'w') as ef:
			ef.write('#!/bin/bash\n')
			ef.write('\n')
			ef.write('echo "PID: $BASHPID" >>{0}\n'.format(self.logf))
			ef.write('\n')
			exec_cmd = '{0} {1}'.format(self.executable, self.args)
			if self.out:
				outf = self.workingdir + self.id_str + '.out'
				exec_cmd = exec_cmd + ' 1>{0}'.format(outf)
			if self.err:
				errf = self.workingdir + self.id_str + '.err'
				exec_cmd = exec_cmd + ' 2>{0}'.format(errf)
			ef.write(exec_cmd + '\n')
			ef.write('\n')
			ef.write('wait')
			ef.write('\n')
			ef.write('echo "return value: $?" >>{0}'.format(self.logf))
		os.chmod(self.exec_file, 0o777)
			

	def kill(self):
		subprocess.Popen(['kill','-9', self.pid])
		self.update_status()

	def logw(self, content):
		with open(self.logf, 'a') as lf:
			lf.write(content)

	def logr(self, content):
		rlines = []
		with open(self.logf, 'r') as lf:
			lines = lf.readlines()
			for line in lines:
				if line.lower().find(content.lower()) != -1:
					rlines.append(line.rstrip(' \n'))
		return rlines
				
	def get_pid(self):
		pid = None
		num_pat = re.compile('[^0-9]*([0-9]+)')
		lc = self.logr('PID')
		if len(lc) > 0:
			m = num_pat.match(lc[0])
			if m != None:
				pid = m.group(1)
		return pid

	def get_pid_list(self):
		if self.remotehost != None:
			sp = subprocess.Popen(['ssh', self.remotehost, 'ps', 'aux'], stdout=subprocess.PIPE)
		else:
			sp = subprocess.Popen(['ps','aux'], stdout=subprocess.PIPE)
		sp.wait()
		psl = sp.communicate()[0].decode('utf-8').split('\n')[1:]
		pids = [line.split()[1] for line in psl if len(line.split()) > 1]
		return pids

	def is_defunct(self):
		is_defunct = False
		if self.remotehost != None:
			sp = subprocess.Popen(['ssh', self.remotehost, 'ps', 'aux'], stdout=subprocess.PIPE)
		else:
			sp = subprocess.Popen(['ps','aux'], stdout=subprocess.PIPE)
		sp.wait()
		psl = sp.communicate()[0].decode('utf-8').split('\n')[1:]
		for line in psl: 
			if len(line.split()) > 1:
				sl = line.split()
				if self.pid == sl[1] and line.find('defunct') != -1:
					is_defunct = True
		return is_defunct
				

	def is_executing(self):
		if self.pid == None:
			return False
		elif self.pid in self.get_pid_list():
			return True
		else:
			return False

	def poll(self):
		if self.is_executing() and not self.is_defunct():
			print(self.pid, ' executing')
			return None
		else:
			lc = self.logr('return')
			if len(lc) > 0:
				rv = lc[0][-1]
				print(self.pid, ' rv = ', rv)
				return rv
			else:
				raise IOError("{0} : Return value expected in log file, but not found.".format(self))
					

	#def get_stdout(self):
	#	return self.proc.stdout.peek().decode('utf-8')
	
	#Read from the buffer for stdout. Note that this operation clears the buffer.
	#def read_stdout(self):
	#	return self.proc.stdout.read().decode('utf-8')
	
	#def get_stderr(self):
	#	return self.proc.stderr.peek().decode('utf-8')

	#Read from the buffer for stdout. Note that this operation clears the buffer.
	#def read_stderr(self):
	#	return self.proc.stderr.read().decode('utf-8')		
