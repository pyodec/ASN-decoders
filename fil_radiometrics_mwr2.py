"""
    Read a second format of a radiometrics radiometer
"""
from pyodec.core import FileDecoder, VariableList, FixedVariableList
import numpy as np
from scipy.interpolate import interp1d as interp
import os
import time
import calendar
import os

# its just easier to specify the heights manually
heights = [ 0.00, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00, 1.25, 1.50, 1.75, 2.00, 2.25, 2.50, 2.75, 3.00, 3.25, 3.50, 3.75, 4.00, 4.25, 4.50, 4.75, 5.00, 5.25, 5.50, 5.75, 6.00, 6.25, 6.50, 6.75, 7.00, 7.25, 7.50, 7.75, 8.00, 8.25, 8.50, 8.75, 9.00, 9.25, 9.50, 9.75,10.00 ]


class RadMWR2D(FileDecoder):
    init_vars = VariableList()
    init_vars.addvar('DATTIM','seconds since 1970-01-01 00:00 UTC',int,1,'S') 
    init_vars.addvar('TEMP','Temperature profile', 'float32', (47,), 'K')
    init_vars.addvar('RH','Relative humidity profile', 'float32', (47,), '%')
    init_vars.addvar('VAPDEN','liquid vapor density profile', 'float32', (47,), 'g/m^3')
    init_vars.addvar('LIQDEN','Liquid water density profile', 'float32', (47,), 'g/m^3')
    
    init_vars.addvar('TAIR','Surface air temperature', 'float32', 1, 'K')
    init_vars.addvar('RELH','Surface relative humidity profile', 'float32', 1, '%')
    init_vars.addvar('PRES','', 'float32', 1, 'hPa')
    init_vars.addvar('TIR','', 'float32', 1, 'K')
    init_vars.addvar('RAIN','', 'str', 1, 'bin')
    init_vars.addvar('INTVAP','', 'float32', 1, 'cm')
    init_vars.addvar('INTLIQ','', 'float32', 1, 'mm')


    init_fixed_vars = FixedVariableList()
    init_fixed_vars.addvar('HEIGHT', 'm AGL', int, np.array(heights) * 1000)  # outheight*1000)
    ob_persist = [False] * 12

    def on_line(self, dat):
        l = dat.split(',')
        if l[2] == "10": return
        # check
        if  self.ob_persist[5] is False:
            # this is the last element of the ob!!!
            self.ob_persist[5] = float(l[3])  # tair (k)
            self.ob_persist[6] = float(l[4])  # RH
            self.ob_persist[7] = float(l[5])  # pres hPa
            self.ob_persist[8] = float(l[6])  # tIR (k)
            self.ob_persist[9] = l[7]  # rain
            self.ob_persist[10] = float(l[8])  # VInt
            self.ob_persist[11] = float(l[9])  # LQint
        # grab the time
        if self.ob_persist[0] is False:
            ep = calendar.timegm(time.strptime(l[1], '%m/%d/%y %H:%M:%S'))
            self.ob_persist[0] = ep

        # a full ob has a 14
        if l[2] == '14':
            # append the last row
            self.ob_persist[4] = map(float,l[10:])
            prep = [r for r in self.ob_persist]
            self.ob_persist = [False] * 12
            return prep
        elif l[2] == "11": k=1
        elif l[2] == "12": k=2
        elif l[2] == '13': k=3
        self.ob_persist[k] = map(float, l[10:])

        

    def decode_proc(self, filepath, yieldcount=1000, location=False):
        # open the file
        self.ob_persist = [False]*12
        self.meta = False
        if location:
            # they want to be updated if the location changes
            self._throw_updates = True

            self.meta = location
        fh = self.open_ascii(filepath)
        if not fh:
            print "NO SUCH FILE", filepath
            return
        for d in self.read_lines(yieldcount, fh):
            # every 1000 obs, this should return somethin
            yield d

        fh.close()




decoder = RadMWR2D()

if __name__ == '__main__':
    import sys
    # run this on a test file
    for ob in decoder.decode(sys.argv[1] ,generator=True):
        times = np.min(np.array(zip(*ob)[0]))
        print times
    # for ob in decoder.decode('/data/ASN/RAW/LAXR_201402/a61bf695e0b6d4b92e9a78e0fe54f7f7.dat.gz'):
    #print np.min(np.array(map(tuple,obs),dtype=decoder.get_dtype())['DATTIM'])
        # print zip(*ob)[15]
    #print decoder.vars

