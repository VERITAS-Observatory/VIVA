#Main python script for the VEGAS analysis pipeline

import sys
from plutils import *
from plutils import reader

inst_filename = sys.argv[1]

try:
	f = open(inst_filename)
except OSError:
	print("Instructions file ", inst_filename, " could not be opened.")
	raise
else:
	f.close()

print(inst_filename)

testmod.testmod()

#Test the database functionality
print('-'*25)
print('Testing DB functionality')
tmpCD={'GLOBALCONFIG' : {'DBHOSTNAME' : 'romulus.ucsc.edu', 'DBNAME' : 'VERITAS', 'DBUSERNAME' : 'readonly'}}
dbcnx=database.DBConnection(configdict=tmpCD)

host=dbcnx.host
db=dbcnx.db
user=dbcnx.user

print('host = ', host)
print('db = ', db)
print('user = ', user)

tmp_runnum='79227'
print('Info for run ', tmp_runnum, ':')

flasher=dbcnx.get_calib_run(tmp_runnum)
ddate=dbcnx.get_ddate(tmp_runnum)
src_id=dbcnx.get_source_id(tmp_runnum)

print('    flasher run = ', flasher)
print('    ddate = ', ddate)
print('    src_id = ', src_id)

read_inst = reader.reader(inst_filename)
configdict = read_inst.dict
print ('configdict: ', configdict)

#RunGroupmanager
readrl = RunGroupManager(**configdict)
rldict = readrl.rldict
