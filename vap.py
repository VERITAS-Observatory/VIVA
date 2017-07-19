#Main python script for the VEGAS analysis pipeline

import sys
import time
from plutils import *

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

#Test condor functionality
print('-'*25)
print('Testing condor functionality')

print('Attempting initalization/configuration...')
cs=condor.CondorJob(executable='sleep_test.sh', arguments='20', universe='vanilla', workingdir='/home/vhep/ssscott/tmp', log='condor_test.log', output='condor_test.out', error='condor_test.error', requirements='')

print('    status = ', cs.status)
print('Attempting submission...')

cs.submit()

print('    status = ', cs.status)
print('    job id = ', cs.jobid)

print('Waiting for test job to terminate...')
while(cs.get_status() != 'terminated'):
	print('    status = ', cs.get_status())
	time.sleep(1)
print('Job should have terminated...')
print('    status = ', cs.status)
print('    exit status = ', cs.exitstatus)
	

