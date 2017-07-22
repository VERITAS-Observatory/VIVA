#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 11:19:43 2017

@author: yuqing
"""
#from plutils import database(???)
       
        
class RunGroup:
    def __init__(self, rlfile, DBconnection):
        self.rlfile = rlfile 
        self.dbconnection = DBconnection
       #change to update instead of reassignment
        self.rundict = createrundict(self)
        self.rundict = getcalibrun(self)
        self.rundict = getddate(self) 
        rundicts = createrundicts()
        self.datarundict = rundicts[0]
        self.calibrundict = rundicts[1]
    #createrundict and timecuts         
    def createrundicts(self):
        datarundict = {}
        calibrundict = {}
                with open(self.rlfile) as rl:
                    content = rl.readlines()
                    for s in content:
                        sl = s.split(maxsplit=1)
                        len_sl = len(sl) 
                        if len_sl >= 2:
                            runnum = sl[0]
                            timecuts = sl[1]
                        elif len_sl == 1:
                            runnum = sl[0]
                            timecuts = ""
                        elif len_sl == 0:
                            continue
                        #calib and date
                        calibrunnum = self.dbconnection.get_calib_run(runnum)
                        ddate = self.dbconnection.get_ddate(runnum)
                        run = Run(runnum, calibrunnum, ddate, timecuts)
                        datarundict.update({runnum:run})  
                        if not calibrunnum in calibrundict.keys():
                                   calibrun = Run(calibrunnum, None, ddate, None)
                                   calibrundict.update({calibrunnum:calibrun})
        return [datarundict, calibrundict] 
 
    
