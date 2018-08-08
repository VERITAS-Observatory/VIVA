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
					user_timecuts = sl[1]
				elif len_sl == 1:
					runnum = sl[0]
					user_timecuts = ""
				elif len_sl == 0:
					continue
				#calib and date
				calibs = dbconnection.get_calib_runs(runnum)
				ddate = dbconnection.get_ddate(runnum)
				src_id = dbconnection.get_source_id(runnum)
				db_timecuts = dbconnection.get_dqm_timecuts(runnum)
				run_obj = Run(runnum, 'data', calibs, ddate, user_timecuts, db_timecuts, src_id)
				datarundict.update({runnum:run_obj})
				for calibrunnum in calibs:  
					if not calibrunnum in calibrundict.keys():
						ddate_calib = dbconnection.get_ddate(calibrunnum)
						calibrun_obj = Run(calibrunnum, 'calib', None, ddate_calib, None, None, 'laser')
						calibrundict.update({calibrunnum:calibrun_obj})

		return [datarundict, calibrundict] 
 
	
class Run:
	def __init__(self, runnum, runtype, calibs, ddate, user_timecuts, db_timecuts, src_id):
		self.runnum = runnum
		self.runtype = runtype
		self.calibs = calibs
		self.ddate = ddate
		self.user_timecuts = user_timecuts
		self.db_timecuts = db_timecuts
		self.source_id = src_id
		
		self.multi_calibs = False
		if self.runtype == 'data':
			self.multi_calibs = self.has_multi_calibs()

	def has_multi_calibs(self):
		multi_calibs = False

		for i in range(0, len(self.calibs)):
			for j in range(i+1,len(self.calibs)):
				if self.calibs[i] == None:
					wrn_str = "{0}: No calibration run found for T{1}. Make certain this telescope was actually missing from the array.\n"
					wrn_str = wrn_str + "This run will be assigned the same calibration run as used for T{2}"
					wrn_str = wrn_str.format(self.runnum, i+1,j+1)
					print(wrn_str)
					if self.calibs[j] != None:
						self.calibs[i] = self.calibs[j]
				elif self.calibs[j] == None:
					wrn_str = "{0}: No calibration run found for T{1}. Make certain this telescope was actually missing from the array.\n"
					wrn_str = wrn_str + "This run will be assigned the same calibration run as used for T{2}"
					wrn_str = wrn_str.format(self.runnum, j+1, i+1)
					print(wrn_str)
					if self.calibs[i] != None:
						self.calibs[j] = self.calibs[i]
				elif self.calibs[i] != self.calibs[j]:
					multi_calibs = True

		return multi_calibs
				
				
				
				
		
			
			
				
			
