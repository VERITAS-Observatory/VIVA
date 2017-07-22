#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 17 18:57:34 2017

@author: yuqing
"""

import os
import fileinput
import sys
import subprocess


class writer:
    
    def __init__(self, configdict, stagedict, stagenum, outputdir):
        self.configdict = configdict
        self.stagedict = stagedict
        self.stagenum = stagenum
        self.stagename = self.stagedict[stagenum]
        self.configfile = str(self.stagename) + 'config.txt'
        self.cutsfile = str(self.stagename) + 'cuts.txt'
        self.vastage = 'vaStage' + str(stagenum)
        self.outputdir = outputdir
        
            
    #test existance and write original if not 
    def writeori(self):   
        subprocess.call([self.vastage,'-save_config_and_exit=' + self.outputdir + self.configfile])
        subprocess.call([self.vastage,'-save_cuts_and_exit=' + self.outputdir + self.cutsfile])
    
    #write config
    def write(self):
        for line in fileinput.input(self.outputdir + self.cutsfile):
            for key in self.configdict[self.stagename]:
                    if key in line:
                        strconfig = str(self.configdict[self.stagename][key])
                        strconfig = strconfig.replace('[','')
                        strconfig = strconfig.replace(']','')
                        rep = str(key) + '=' + strconfig
                        line = line.replace(line, rep)
                        sys.stdout.write(line)
                    
        for line in fileinput.input(self.outputdir + self.configfile):
            for key in self.configdict[self.stagename]:
                    if key in line:
                        strconfig = str(self.configdict[self.stagename][key])
                        strconfig = strconfig.replace('[','')
                        strconfig = strconfig.replace(']','')
                        rep = str(key) + '=' + strconfig
                        line = line.replace(line, rep)
                        sys.stdout.write(line)         
        
    def run(self):
        writer.writeori(self)
        writer.write(self)
         
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


