#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 13 11:28:47 2017
@author: yuqing
"""

class reader:
    
    def __init__(self,input_file):
        with open(input_file, 'r') as i:
            self.lines = str(i.read())
        self.stage_keys = reader.read(self.lines,'\n','{')
        self.stage_values = reader.read(self.lines,'{','}')
        self.dict = reader.write_dict(self.stage_keys,self.stage_values)
    
    def read(resource, start_sep, end_sep):
        results = []
        resource = str(resource)
        tmp = resource.split(start_sep)
        for par in tmp:
            if end_sep in par:
                results.append(par.split(end_sep)[0])
        return results  
   
    def write_dict(keys,values):
        dictionary = {}
        for i in range(len(keys)):
            config_keys = reader.read(values[i],'\n','=' ) 
            config_values = reader.read(values[i],'=','\n')
            dictionary[keys[i]] = {}
            for j in range(len(config_keys)):
                dictionary[keys[i]][config_keys[j]] = [config_values[j]]
        return dictionary
                


     





