#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 10:43:48 2017

@author: yuqing
"""

from . import database

class RunGroupManager:
	def __init__(self, configdict, DBconnection):
		self.configdict = configdict
		self.gconfig = self.configdict.get('GLOBALCONFIG')
		self.rldict = self.make_rl_dict()
		self.groupdict = self.make_group_dict(DBconnection)
		
	#Produce a dictionary mapping a group name to its respective runlist
	def make_rl_dict(self):
		rldict = {}
		for k,v in self.gconfig.items():
			if k.startswith('RUNLIST'):
				group = k.partition(':')[2]
				rldict.update({group:v}) 
		return rldict

		
	def make_group_dict(self, dbconnection):
		grp_dict = {}
		for grp,rl in self.rldict.items():
		
			grp_obj = RunGroup(grp,rl,dbconnection)
	
		#In the case of concatenated groups (i.e. GRP1:GRP2:GRP3:...),
		#copy the object and change the group id for each group.
			gs = grp.split(':')	
			if len(gs) > 1:
				for rg in gs:
					tmp_grp_obj = grp_obj
					tmp_grp_obj.groupid=rg
					grp_dict.update({rg : tmp_grp_obj})
			else:
				grp_dict.update({grp : grp_obj})

		return grp_dict
	
	def get_group_dict(self):
		return self.groupdict	
	
	def get_run_groups(self, sub_grp_str):
		sub_grp_dict = {}
		for grp in sub_grp_str.split(':'):
			if grp in self.groupdict.keys(): 
				sub_grp_dict[grp] = self.groupdict[grp]
			else:
				print ('No group name ', grp, ' found in group dictionary.')
		return sub_grp_dict

class RunGroup:
	def __init__(self, groupid, rlfile, DBconnection):
		self.rlfile = rlfile 
		self.groupid=groupid
		rundicts = self.make_run_dicts(DBconnection)
		self.datarundict = rundicts[0]
		self.calibrundict = rundicts[1]

	#Make the dictionaries mapping the run numbers to the Run class objects for those runs		
	def make_run_dicts(self, dbconnection):
		datarundict = {}
		calibrundict = {}
		with open(self.rlfile) as rl:
			content = rl.readlines()
			for s in content:
				s = s.strip('\n')
				sl = s.split(maxsplit=1)
				len_sl = len(sl) 
				if len_sl >= 2:
					runnum = sl[0]
					timecuts = sl[1]
				elif len_sl == 1:
					runnum = sl[0]
					timecuts = ""
				elif len_sl == 0:
					continue
				#calib and date
				calibrunnum = dbconnection.get_calib_run(runnum)
				ddate = dbconnection.get_ddate(runnum)
				run_obj = Run(runnum, 'data', calibrunnum, ddate, timecuts)
				datarundict.update({runnum:run_obj})  
				if not calibrunnum in calibrundict.keys():
					calibrun_obj = Run(calibrunnum, 'calib', None, ddate, None)
					calibrundict.update({calibrunnum:calibrun_obj})

		return [datarundict, calibrundict] 
 
	
class Run:
	def __init__(self, runnum, runtype, calib, ddate, timecuts):
		self.runnum = runnum
		self.runtype = runtype
		self.calib = calib
		self.ddate = ddate
		self.timecuts = timecuts
