"""
Read the wind profiles sent by the WF sodar
"""
import numpy as np
from pyodec.core import FileDecoder, VariableList, FixedVariableList
import traceback
import time
import datetime


class AxysSingleDecoder(FileDecoder):
    init_vars = VariableList()
    init_vars.addvar('DATTIM','Observation time',int,1,'seconds since 1970-01-01 00:00 UTC')
    init_vars.addvar('TIMESTAMP','String date stamp',"str",20,'')
    init_vars.addvar('WDIR','Horizontal wind direction',"float32",11,"m/s")
    init_vars.addvar('WSPD','Horizontal wind speed',"float32",11,"degrees")
    init_vars.addvar('WVERT','Vertical Wind',"float32",11,"m/s")
    init_vars.addvar('QUAL','Data Quality Parameter',"float32",11,"m/s")
    init_vars.addvar('TURB','Wind Turbulence',"float32",11,"m/s")
    init_fixed_vars = FixedVariableList()
    init_fixed_vars.addvar("HEIGHT",'','float32',[30,40,50,60,80,100,120,140,160,180,200])
    
    def decode_proc(self, filepath, yieldcount=1000, **kwargs):
        # open the file
        #return self.read_chunks(yieldcount, self.open_ascii(filepath), end=unichr(004))
        # problem with above: who closes the file handle??
        with self.open_ascii(filepath) as filehandle:
            # we are just going to use the friggin timestamp assigned at ingest... best we can do, let's hope they stay with 1 ob per file
            lines = filehandle.readlines()
            data =  [ int(lines[-3].split(":")[4]) ]
            obline = lines[-1].split(",")
            datline = np.array(map(float,obline[1:]))
            # append the "original date"
            data += [obline[0]] 
            # append directions
            data += [ obline[1:44][::4] ] + [ obline[2:44][::4] ] + [ obline[3:44][::4] ] + [ obline[4:45][::4] ] + [obline[45:]]
            yield [tuple(data)]
        

decoder = AxysSingleDecoder()

if __name__ == '__main__':
   import sys
   fil = sys.argv[1]
   dat = decoder.decode(fil)
   print dat
   print len(dat[0]), len(decoder.get_dtype()), np.array(dat,dtype=decoder.get_dtype())

"""
example list of observations
Date and Time,
30m Wind Direction,30m Wind Speed,30m Wind Vert,30m Quality,
40m Wind Direction,40m Wind Speed,40m Wind Vert,40m Quality,
50m Wind Direction,50m Wind Speed,50m Wind Vert,50m Quality,
60m Wind Direction,60m Wind Speed,60m Wind Vert,60m Quality,
80m Wind Direction,80m Wind Speed,80m Wind Vert,80m Quality,
100m Wind Direction,100m Wind Speed,100m Wind Vert,100m Quality,
120m Wind Direction,120m Wind Speed,120m Wind Vert,120m Quality,
140m Wind Direction,140m Wind Speed,140m Wind Vert,140m Quality,
160m Wind Direction,160m Wind Speed,160m Wind Vert,160m Quality,
180m Wind Direction,180m Wind Speed,180m Wind Vert,180m Quality,
200m Wind Direction,200m Wind Speed,200m Wind Vert,200m Quality,
30m Wind Turbulence,40m Wind Turbulence,50m Wind Turbulence,60m Wind Turbulence,80m Wind Turbulence,100m Wind Turbulence,120m Wind Turbulence,140m Wind Turbulence,160m Wind Turbulence,180m Wind Turbulence,200m Wind Turbulence
11/5/2014 12:20 AM,
195.00,7.41,-0.01,99,197.30,8.06,0.01,99,198.00,8.90,0.03,98,198.10,9.46,-0.02,99,201.00,10.66,-0.04,99,198.30,11.80,0.00,99,202.30,12.60,-0.14,99,202.20,12.95,-0.04,98,205.60,13.34,-0.09,97,212.20,13.10,-0.13,95,212.40,14.19,-0.18,91,0.07,0.05,0.05,0.05,0.04,0.04,0.04,0.03,0.03,0.06,0.10
"""