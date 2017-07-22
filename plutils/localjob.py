#Module for handling the processing of job local to the machine

import subprocess
import time

class LocalJobQueue():
	
	def __init__(self, **kwargs):

		self.update_T = 0.1

		self.joblist=kwargs.get('joblist')
		self.proc_limit=kwargs.get('proc_limit')

		self.jobdict = enumerate(self.joblist)
		self.job_status = {}
		self.total_procs = len(joblist)
		self.active_procs = 0
		self.exitstatus = None		

		self.status='initialized'
		
	def execute(self):
		while(self.get_status() != 'terminated'):
			for k,j in self.jobdict.items():
				if j.get_status() == 'initialized' and self.active_procs < self.proc_limit:
					j.execute()
					self.active_procs = self.active_procs + 1

			time.sleep(self.update_T)
				
		
	def update_status(self):

		n_executing = 0
		n_failed = 0
		n_succeeded = 0

		for k,j in self.jobdict.items():

		
			if j.get_status == 'executing':
				n_executing = n_executing + 1
			elif j.get_status == 'terminated':
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
		

class LocalJob():

	def __init__(self, **kwargs):

		self.executable=kwargs.get('executable')
		self.args = kwargs.get('args')
		
		self.out = None
		if 'out' in kwargs.keys():
			self.out = kwargs.get('out')

		self.error = None
		if 'error' in kwargs.keys()
			self.error = kwargs.get('error')
	
		self.proc = None
		self.exitstatus = None
			
		self.status = 'initialized'

	def execute(self):
		self.proc = subprocess.Popen([self.executable, self.args], stdout=subprocess.PIPE, stderr = subprocess.PIPE)
		self.status = 'submitted'

	def update_status(self):
		if self.proc.poll() == None and self.status == 'submitted':
			self.status = 'executing'
		elif self.proc.poll() == 0:
			self.status = 'terminated'
			self.exitstatus = '0'
		elif self.proc.poll() == 1:
			self.status = 'terminated'
			self.exitstatus = '1'

	def get_status(self):
		self.update_status()
		return self.status

	def kill(self):
		self.proc.kill()
		self.update_status()

	def get_stdout(self):
		return self.proc.stdout.peek().decode('utf-8')
	
	#Read from the buffer for stdout. Note that this operation clears the buffer.
	def read_stdout(self):
		return self.proc.stdout.read().decode('utf-8')
	
	def get_stderr(self)
		return self.proc.stderr.peek().decode('utf-8')

	#Read from the buffer for stdout. Note that this operation clears the buffer.
        def read_stderr(self):
                return self.proc.stderr.read().decode('utf-8')		
