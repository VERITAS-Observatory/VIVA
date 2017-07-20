#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 10:43:48 2017

@author: yuqing
"""

class RunGroupManager:
    def __init__(self, configdict, groupname):
        self.configdict = configdict
        self.gconfig = self.configdict.get('GlobalConfig')
        self.groupdict = RunGroupManager.getdict(self)
        
    
    def getdict(self):
        groupdict = {}
        i = 0
        for k,v in self.gconfig.items():
            if k.startswith('Runlist'):
                groupdict['Group'+str(i)] = v
                i += 1
        return groupdict
    
    def getpart(self):
        partdict = {}
        for groupname in self.groupname.split(':'):
            if groupname in self.groupdict.keys(): 
                partdict[groupname] = self.groupdict[groupname]
            else:
                print ('no groupname ' + groupname)
        return partdict
                
dict = {}
group = RunGroup(
