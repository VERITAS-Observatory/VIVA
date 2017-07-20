class Run:
    def __init__(self, runnum):
        self.runnum = runnum
        self.timecuts = tcdict[self.runnum]
        self.ddate = DBConnection.get_ddate[self.runnum]
        self.calib_run = DBConnection.get_calib_run[self.runnum]
