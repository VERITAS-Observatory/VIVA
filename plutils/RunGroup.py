#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 11:19:43 2017

@author: yuqing
"""
#from plutils import database(???)
       
        
class RunGroup:
    def __init__(self, groupdict):
        self.groupdict = groupdict
        self.rundict = createrundict(self)
        self.rundict = getcalibrun(self)
        self.rundict = getddate(self) 
        
    #createrundict and timecuts         
    def createrundict(self):
        rundict = {}
        for key,value in self.groupdict.items():
            rundict[key] = {}
            for runlistfile in value:
                with open(runlistfile) as rl:
                    content = rl.readlines()
                    for s in content:
                        runnum = s.split(' ',1)[0]
                        timecut = s.split(' ',1)[1].strip('\n')
                        rundict[key][runnum] = {}
                        rundict[key][runnum]['timecut'] = timecut
            del rundict[key]['runnum']
        return rundict
    
    def getcalibrun(self):
        for groupname in self.rundict:
            for runnum in self.rundict[groupname]:
                self.rundict[groupname][runnum] = {}
                self.rundict[groupname][runnum]['calibrun'] = dbcnx.get_calib_run(runnum)
        return self.rundict
        
        
        
        
    
    def getddate(self):
        for groupname in self.rundict:
            for runnum in self.rundict[groupname]:
                self.rundict[groupname][runnum] = {}
                self.rundict[groupname][runnum]['calibrun'] = dbcnx.get_ddate(runnum)
        return self.rundict
        
