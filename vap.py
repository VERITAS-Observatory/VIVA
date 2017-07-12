#Main python script for the VEGAS analysis pipeline

import sys
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
