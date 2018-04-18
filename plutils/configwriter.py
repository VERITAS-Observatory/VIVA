#Class for managing the writing of the cuts and config files for each analysis stage

import os
import sys
import subprocess

class ConfigWriter:
	
	def __init__(self, configdict, stagekey, stagenum, executable, outputdir):
		self.configdict = configdict
		self.stagekey = stagekey
		self.stagenum = stagenum
		self.executable = executable
		self.stagename = stagekey.lower().replace(':','-')
		self.configfilename = self.stagename + '_config.txt'
		self.cutsfilename = self.stagename + '_cuts.txt'
		self.outputdir = outputdir
		if not self.outputdir.endswith('/'):
			self.outputdir = self.outputdir + '/'
	
		self.configfilepath = None
		self.cutsfilepath = None
			
	#test existance and write original if not 
	def write_template(self,template_type):
		template_file = self.outputdir + template_type + '.tmp'

		#Remove existing templates, just in case vegas version has changed.
		subprocess.run(['rm','-f',template_file])

		subprocess.run([self.executable, '-save_' + template_type + '_and_exit=' + template_file], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
		if os.path.isfile(template_file):
			return template_file
		else:
			err_str = 'Failed to write template {0} file for VASTAGE{1}:{2} in {3}.'.format(template_type, self.stagenum,self.stagekey,self.outputdir)
		raise Exception(err_str)
	
	#write config
	def write(self, conf_type):
		template_lines = []
		template_file = self.write_template(conf_type)
		with open(template_file, 'r') as f:
			template_lines = f.readlines() 
		
		if conf_type == 'config':
			filepath = self.outputdir + self.configfilename
		elif conf_type == 'cuts': 
			filepath = self.outputdir + self.cutsfilename

		unk_opts = list(self.configdict[self.stagekey].keys())

		with open(filepath, 'w') as conf_file:
			for line in template_lines:
				if not line.startswith('#') and not line.isspace():
					opt = line.split()[0]
					if opt in self.configdict[self.stagekey].keys():
						val = self.configdict[self.stagekey][opt]
						rep = opt + ' ' + val
						line = line.replace(line, rep)
						unk_opts.remove(opt)
						conf_file.write(line)
					else:
						conf_file.write(line)
				else:
					conf_file.write(line)
		
		if conf_type == 'config':
			self.configfilepath = filepath
		elif conf_type == 'cuts':		
			self.cutsfilepath = filepath
		return (filepath, unk_opts)	 

