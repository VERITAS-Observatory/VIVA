#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 10:43:48 2017

@author: yuqing
"""

class RunGroupManager:
    def __init__(self, **kwargs):
        self.gconfig = kwargs.get('GlobalConfig')
        self.rldict = RunGroupManager.getitem(self)
        
    
    def getitem(self):
        rldict = {}
        for k,v in self.gconfig.items():
            if k.startswith('Runlist'):
                rldict[k] = v
        return rldict
                

