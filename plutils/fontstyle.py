#Simple module for setting the color/style of a string by inserting the appropriate ANSI escape sequence

#Text color enumerations
tc_dict = {}
tc_dict.update({'black' : '30', 'red' : '31', 'green' : '32'})
tc_dict.update({'yellow' : '33', 'blue' : '34', 'magenta' : '35', 'cyan' :'36'})
tc_dict.update({'white' : '37'})

#Background color enumerations
bg_dict = {}
bg_dict.update({'black' : '40', 'red' : '41', 'green' : '42'})
bg_dict.update({'yellow' : '43', 'blue' : '44', 'magenta' : '45', 'cyan' : '46'})
bg_dict.update({'white' : '47'})

#Widely support formatting options

fm_dict = {}
fm_dict.update({'normal' : '0', 'bold' : '1', 'underline' : '4'})
fm_dict.update({'negative' : '7', 'reversed' : '26'})

opt_dict = {}
opt_dict.update(tc_dict)
opt_dict.update(bg_dict)
opt_dict.update(fm_dict)

#Set the style of string.
def set_style(str, **kwargs):
	opt_str = ""
	for opt,val in kwargs.items():
		if opt.lower() in ['txt_clr', 'text_color', 'color', 'text']:
			opt_str = opt_str + tc_dict[val] + ';'
		elif opt.lower() in ['bg_clr', 'background_color', 'background']:
			opt_str = opt_str + bg_dict[val] + ';'
		elif opt.lower() in ['txt_fmt', 'text_formatting', 'format']:
			fmt_vals = val.split(':')
			for fv in fmt_vals:
				opt_str = opt_str + fm_dict[fv] + ';'
		else:
			print('set_style : unreconized arguement: {0}'.format(opt))
	
	return '\033[' + opt_str.rstrip(';') + 'm' + str + '\033[0m'
			
