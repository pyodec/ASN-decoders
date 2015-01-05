"""
Read single-ob axys lidar wind profiles
"""
import numpy
from pyodec.core import FileDecoder, VariableList, FixedVariableList
import traceback
import time
import datetime
import pytz
import os

class AxysSingleDecoder(FileDecoder):
    init_vars = VariableList()
    init_vars.addvar('DATTIM','Observation time',int,1,'seconds since 1970-01-01 00:00 UTC')
    init_vars.addvar('HEADER','NMEA Header',"str",12,'')
    init_vars.addvar('SERIAL','System Serial Number',"str",30,"")
    init_vars.addvar('MSGID','Message ID',"str",3,"")
    init_vars.addvar('LATITUDE','Instrument Latitude',"float32",1,"degrees*100")
    init_vars.addvar('LONGITUDE','Instrument Longitude',"float32",1,"degrees*100") 
    init_vars.addvar('PRES','Surface Pressure',"float32",1,"hPa")
    init_vars.addvar('ATEMP','Surface Air Temperature',"float32",1,"C")
    init_vars.addvar('ARELH','Surface Relative Humidity',"float32",1,"%")
    init_vars.addvar('ADEWP','Surface Dew Point',"float32",1,"C")
    init_vars.addvar('AWSPD','Average wind speed',"float32",1,"m/s")
    init_vars.addvar('MXWSPD','Maxiumum wind speed',"float32",1,"m/s")
    init_vars.addvar('uSPD','Horizontal wind speed',"float32",3,"m/s")
    init_vars.addvar('uDIR','Horizontal wind direction',"float32",3,"degrees")
    init_vars.addvar('uMAX','Maximum horizontal wind',"float32",3,"m/s")
    init_vars.addvar('wSPD','Vertical wind speed',"float32",3,"m/s")
    init_vars.addvar('uDEV','Horizontal wind speed standard deviation',"float32",3,"m/s")
    init_fixed_vars = FixedVariableList()
    init_fixed_vars.addvar("HEIGHT",'','float32',[29,49,60])
    
    def on_line(self, message):
        """
        Read a chunk of data from the file, the chunk being spliced from read_chunks.
        """
        os.environ['TZ'] = 'UTC' # grumble grumble grumble
        time.tzset()
        # this is an end-spliced message, so we will get the timestamp
        ob = map(lambda x: "NaN" if x=="" else x,message.split(","))
        try:
            tmstr = ob[1]+ob[2]
            # don't add extra newlines after the time string if you want to be happy
            otime = int(time.mktime(datetime.datetime.strptime(tmstr,"%y%m%d%H%M%S").timetuple()))
        except:
            return False
        try:
            lat = float(ob[5][:-1])
            if "S" in ob[5]: lat *= -1
            lon = float(ob[6][:-1])
            if "W" in ob[6]: lon *= -1
            data = [ob[0]] + ob[3:5] + [lat, lon] +map(float,ob[7:13]) +\
                [map(float, ob[13:16])] +\
                [map(float,ob[19:22])] +\
                [map(float,ob[25:28])] +\
                [map(float,ob[31:34])] +\
                [map(float,ob[37:40])] 
        except:
            print "decode failure, handled, skipping this MESSAGE"
            traceback.print_exc()
            # there was something ugly in this data... serial hiccups.
            data=False
        if data is False:
            return None
        output = [otime]+data
        return tuple(output)
    
    def decode_proc(self, filepath, yieldcount=1000, **kwargs):
        # open the file
        #return self.read_chunks(yieldcount, self.open_ascii(filepath), end=unichr(004))
        # problem with above: who closes the file handle??
        with self.open_ascii(filepath) as filehandle:
            for d in self.read_lines(yieldcount, filehandle):
                yield d
        

decoder = AxysSingleDecoder()

if __name__ == '__main__':
   import sys
   fil = sys.argv[1]
   dat = decoder.decode(fil)
   print dat
   print len(dat[0]), len(decoder.get_dtype()), numpy.array(dat,dtype=decoder.get_dtype())

"""
example list of observations
$W5M5A,
141125,
183000,
720f0037403d9b18,
1,
3702.7842N,
7603.7487W,
1015.08,
11.8,
85.4,
9.4,
3.3,
4.7,
8.0, # prof 1
8.4,
8.2,
,
,
,
26.6,
28.2,
29.4,
,
,
,
9.8,
10.4,
10.2,
,
,
,
0.2,
0.2,
0.3,
,
,
,
0.8,
0.9,
0.9,
,
,
*37
"""