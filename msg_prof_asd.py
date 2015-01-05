import os
from pyodec.core import MessageDecoder, VariableList, FixedVariableList
import numpy as np
import re
import time
import datetime as dt
import calendar
"""
Read a .ASD data file from a profiler, allowing for different height settings (returned as a unique identifier... hopefully)

This was written to comform to the SDGE profiler files
"""

class AsdMsg(MessageDecoder):
    
    def decode(self, message):
        
        l = message.strip().split("\n")
        # time is line [3]
        otime = calendar.timegm(dt.datetime.strptime(l[3][:22],"%Y-%m-%d    %H:%M:%S").timetuple())
        obset = [otime]
        # get self-defined station name
        r1=l[0].split()
        obset.append(r1[0])
        # get the mode (THIS IS THE UNIQUE IDENTIFIER USED IN THE FILE DECODER)
        obset.append(l[4].split()[0])
        # get the size of the profile
        LEN = int(l[6].split()[0])
        # now read the data lines, 4:
        cols = len(l[8].split())
        # note, these are backwards!
        obs = np.zeros((LEN,cols))
        k=0
        for i in np.arange(LEN) + 9:
            dline=l[i].strip().split()
            obs[k]=dline[:] # don't ignore the heights!
            k+=1
        # and make them column wise, and add that to the obset
        obs[obs == 999.9] = np.nan
        obs=  obs.T.tolist()
        hts = obs[0]
        obset += obs[1:9]
        # then merge the beam-wise columns
        obset += [obs[10:15]]
        obset += [obs[15:20]]
        obset += [obs[20:25]]
        obset += [obs[25:30]]
        obset += [obs[30:35]]
        # set the variables
        
        self.vars = VariableList()
        self.vars.addvar("DATTIM",'',int,1,'secs since 1970010100000')
        self.vars.addvar('STNAME','',str,20,'')
        self.vars.addvar('MODE','',str,20,'')
        self.vars.addvar('WSPD','','float32',(LEN,),'m/s')
        self.vars.addvar('WDIR','','float32',(LEN,),'deg')
        self.vars.addvar('IQC','','float32',(LEN,),'') # internal QC, asn already, unwisely, stole "QC"
        self.vars.addvar('U','','float32',(LEN,),'m/s')
        self.vars.addvar('V','','float32',(LEN,),'m/s')
        self.vars.addvar('W','','float32',(LEN,),'m/s')
        self.vars.addvar('SDH','Horizontal Standard deviation','float32',(LEN,),'m/s')
        self.vars.addvar('SDW','Vertical Standard deviation','float32',(LEN,),'m/s')
        self.vars.addvar('VEL','','float32',(5,LEN),'mv')
        self.vars.addvar('NUM','','float32',(5,LEN),'mv')
        self.vars.addvar('POW','','float32',(5,LEN),'mv')
        self.vars.addvar('SNR','','float32',(5,LEN),'mv')
        self.vars.addvar('WDTH','','float32',(5,LEN),'mv')
        
        # yes, there is a variable fixed dimension.... get over it.
        self.fixed_vars = FixedVariableList()
        self.fixed_vars.addvar('HEIGHT','m AGL',int,hts)
        

        
        return obset
        
decoder = AsdMsg() # rename to 'decoder'