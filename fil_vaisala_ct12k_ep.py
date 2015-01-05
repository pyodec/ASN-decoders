from pyodec.core import FileDecoder, VariableList
from msg_vaisalact12k import decoder as msgdecode, fvars
import numpy as np
import os
import time
import gzip

"""
U of Utah CT-12k: epoch times stored *before* the message
"""

class Epct12kD(FileDecoder):
    init_vars = VariableList()
    init_vars.addvar('DATTIM','Observation time',int,1,'Seconds since 1970-01-01 00:00:00 UTC')
    init_vars += msgdecode.vars
    init_fixed_vars = msgdecode.fixed_vars
    def on_chunk(self, message):
        # this is an end-spliced message, so we will get the timestamp
        ob = message.split(unichr(002))
        try:
            tmstr = ob[0].split()[-1]
            # don't add extra newlines after the time string if you want to be happy
            otime = float(tmstr)
        except:
            return False
        data = msgdecode.decode(ob[1])
        if data is False:
            return None
        output = (otime,data[0],data[1])
        return output
    
    def decode_proc(self, filepath, yieldcount=1000):
        # open the file
        if not os.path.exists(filepath):
            print "NO SUCH FILE"
            return 
        gzfh = self.open_ascii(filepath)
        for d in self.read_chunks(yieldcount, gzfh, end=unichr(003)):
            yield d
        gzfh.close()

decoder = Epct12kD()
