#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 13 11:28:47 2017

@author: yuqing
"""

#import 
import re

def read_file(self):
    with open(self, 'r') as i:
        lines = str(i.read())
    return lines


class reader(str):
    
    def clean(file,string):
        file = file.replace(string,'')
        return file
    
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
            config_keys = reader.read(values[i],'\\n','=' ) 
            config_values = reader.read(values[i],'=','\\n')
            dictionary[keys[i]] = {}
            for j in range(len(config_keys)):
                config_keys[j].strip('\\')                       
                config_values[j].strip('\\') 
                
                dictionary[keys[i]][config_keys[j]] = [config_values[j]]
        return dictionary
                
    def run(self):
        stage_keys = reader.read(lines,'\\n','{')
        stage_values = reader.read(lines,'{','}')
        instruciton_dict = reader.write_dict(stage_keys, stage_values)
        return instruciton_dict
        
     





