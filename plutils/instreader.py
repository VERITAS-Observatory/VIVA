"""

instfilereader.py: Defines class that handles the reading in of the instructions file and creation of the configuration dictionary

"""

class InstFileReader:
	
	def __init__(self,inst_filename):
		self.start_sep = '{'
		self.end_sep = '}'
		self.instlines = []
		with open(inst_filename, 'r') as infile:
			self.instlines = infile.readlines()
		plines = self.prep_lines(self.instlines, self.start_sep,self.end_sep)
		self.configdict = self.build_configdict(plines,self.start_sep,self.end_sep)		
	
	def get_config_dict(self):
		if len(self.configdict.keys()) == 0:
			print("Warning: Configuration dictionary is empty!")
		return self.configdict
	
	#Remove empty lines, leading white space, comments, and move curly brackets			 
	def prep_lines(self,lines, start_sep, end_sep):
		new_lines = []
		for line in lines:
			newline = line.partition('#')[0]
			newline = newline.lstrip().rstrip('\n')
			parts = newline.partition(start_sep)
			more_parts = []
			for p in parts:
				more_parts = more_parts + list(p.partition(end_sep))
			if more_parts.count('') > 0:
				for n in range(more_parts.count('')):
					more_parts.remove('')
			new_lines = new_lines + more_parts
		return new_lines

	 #Build the configuration dictionary. Not that the list lines needs to be in format of the list returned by prep_lines   
	def build_configdict(self,lines, start_sep, end_sep):
		confdict = {}
		n_lines = len(lines)
		for i,line in enumerate(lines):
			if i+1 < n_lines:
				if lines[i+1] == start_sep:
					configkey = line
					opt_dict = {}
					end_idx = None
					for j,sub_line in enumerate(lines[i+1:]):
						if sub_line == end_sep:
							end_idx = j
							break
						elif sub_line != start_sep:
							parts = sub_line.partition('=') #Assumed format OPT=VAL
							if parts[1] == '': #Maybe the user used the OPT VAL format instead
								parts = sub_line.partition(' ')
							opt = parts[0].strip()
							val = parts[2].strip()
							opt_dict.update({opt:val})
					
					if end_idx == None:
						err_str='Could not find closing \'{0}\' for configuration key {1}.'.format(end_sep, configkey)
						raise Exception(err_str)
					else:
						confdict.update({configkey:opt_dict})
		return confdict		
						
						
						 

		





