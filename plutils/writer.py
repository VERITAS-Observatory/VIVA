#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 17 18:57:34 2017

@author: yuqing
"""

import os
import fileinput
import sys

class writer:
    
    def __init__(self, configdict, stagedict, stagenum):
        self.configdict = configdict
        self.stagedict = stagedict
        self.stagenum = stagenum
        self.stagename = self.stagedict[stagenum]
        self.configfile = str(self.stagename) + 'config.txt'
        self.cutsfile = str(self.stagename) + 'cuts.txt'
        self.vastage = 'vaStage' + str(stagenum)
            
    #test existance and write original if not 
    def check(self):
        if os.path.isfile(self.configfile) == False:     
            subprocess.call([self.vastage,'-save_config_and_exit','=', self.configfile])
        if os.path.isfile(self.cutsfile) == False:   
            subprocess.call([self.vastage,'-save_cuts_and_exit','=', self.cutsfile])
    
    #write config
    def write(self):
        for line in fileinput.input(self.cutsfile, inplace=1):
                for key in self.configdict[self.stagename]:
                    if key in line:
                        strconfig = str(self.configdict[self.stagename][key])
                        strconfig = strconfig.replace('[','')
                        strconfig = strconfig.replace(']','')
                        rep = str(key) + '=' + strconfig
                        line = line.replace(line, rep)
                    sys.stdout.write(line)
                       
    def run(self):
        writer.check()
        writer.run()  
