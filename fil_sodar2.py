"""
STI sodars, which are currently all in SOCAL, and operate on local time as best
I can tell..
"""
from pyodec.core import FileDecoder, VariableList
import msg_sodarmsg1 as sd
import time
import os
import datetime as dt

#os.environ['TZ']='America/Los_Angeles'
#time.tzset()

class SodarGrid1(FileDecoder):
    #code
    vars = VariableList()
    vars.addvar('DATTIM','Observation time','int',1,'Seconds since 1970 1 1 00:00:00 UTC')
    vars += sd.decoder.vars # and add the vars from the message decoder
    vars.addvar('OBSTART','Begging of Ob time bin','int',1,'Seconds since 1970 1 1 00:00:00 UTC')
    vars.addvar('OBEND','End of ob time bin','int',1,'Seconds since 1970 1 1 00:00:00 UTC')
    init_fixed_vars = sd.decoder.fixed_vars
    
    def on_chunk(self, message):
        # grab the time from this message
        l=message.split('\n')
        if len(l)<21: return False
        start = time.mktime(dt.datetime.strptime(l[0][5:24],'%m/%d/%Y %H:%M:%S').timetuple())
        end = time.mktime(dt.datetime.strptime(l[0][28:47],'%m/%d/%Y %H:%M:%S').timetuple())
        obtime = (start+end)/2
        decode = sd.decoder.decode(message)
        dat = [obtime]+decode+[start,end]
        return dat
    
    def decode_proc(self, filepath, yieldcount=1000):
        os.environ['TZ']='America/Los_Angeles'
        time.tzset()
        fil = FileDecoder.open_ascii(filepath)
        if not fil:
            print "NO SUCH FILE"
            return
        # delimeter is based on the date string, which is in CA time harumph
        h = fil.readline().split(":")

        try:
            # ID is the first element of the header, the common style
            float(h[2])
            ftime = dt.datetime.fromtimestamp(int(h[4]))
        except:
            # then this is an old style header
            ftime = dt.datetime.fromtimestamp(int(h[3]))
        for d in self.read_chunks(yieldcount,fil, begin=' {:%m/%d/%Y} '.format(ftime)):
            yield d
            
        fil.close()

decoder = SodarGrid1()
    
if __name__ == "__main__":
   import sys
   print decoder.decode(sys.argv[1])
