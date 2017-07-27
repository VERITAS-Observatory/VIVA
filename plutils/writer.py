#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 17 18:57:34 2017

@author: yuqing
"""

import os
#import fileinput
import sys
import subprocess


class ConfigWriter:
	
	def __init__(self, configdict, stagekey, stagenum, outputdir):
		self.configdict = configdict
		self.stagekey = stagekey
		self.stagenum = stagenum
		self.stagename = stagekey.lower().replace(':','-')
		self.configfilename = self.stagename + '_config.txt'
		self.cutsfilename = self.stagename + '_cuts.txt'
		self.vastage = 'vaStage' + str(stagenum)
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

		subprocess.run([self.vastage,'-save_' + template_type + '_and_exit=' + template_file], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
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

		with open(filepath, 'w') as conf_file:
			for line in template_lines:
				if not line.startswith('#') and not line.isspace():
					opt = line.split()[0]
					if opt in self.configdict[self.stagekey].keys():
						val = self.configdict[self.stagekey][opt]
						rep = opt + ' ' + val
						line = line.replace(line, rep)
						conf_file.write(line)
					else:
						conf_file.write(line)
				else:
					conf_file.write(line)
		
		if conf_type == 'config':
			self.configfilepath = filepath
		elif conf_type == 'cuts':		
			self.cutsfilepath = filepath
		return filepath	 

''' run example
stagedict = {1:'Stage1', 2:'Stage2', 4.2:'Stage4', 5:'Stage5', 6:'Stage6'}
configdict = {'GlobalConfig': {'Database': [''],
  'RawDataDir': [''],
  'Runlist1': ['runlist1.txt'],
  'Runlist2': [''],
  'Runlist3': [''],
  'VEGASDir': [''],
  'WorkingDir': ['']},
 'Stage1': {},
 'Stage2': {},
 'Stage4': {'DistanceUpper': [''], 'NTubesMin': [''], 'SizeLower': ['']},
 'Stage5': {'MaxHeightLower': ['7'],
  'MeanScaledLengthLower': ['0.05'],
  'MeanScaledLengthUpper': ['1.3'],
  'MeanScaledWidthLower': ['0.05'],
  'MeanScaledWidthUpper': ['1.1']},
 'Stage6': {'RBM_SearchWindowSqCut': ['0.03'], 'S6A_RingSize': ['0.17']}}
 
stg5 = writer(configdict, stagedict, 5, '/home/vhep/yuqing/dev_yuqing/vegas-analysis-pipeline/plutils/ConfigDir/')
stg5.run()
'''


