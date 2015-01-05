from pyodec.core import FileDecoder, VariableList
import msg_prof_asd as sd
import time
import os
import datetime as dt

class AsdFilDec(FileDecoder):
    #code
    init_vars = sd.decoder.vars # and add the vars from the message decoder
    init_fixed_vars = sd.decoder.fixed_vars
    
    def on_chunk(self, message):
        decode = sd.decoder.decode(message)
        dat = decode
        return dat
    
    def decode_proc(self, filepath, yieldcount=1000):
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
        for d in self.read_chunks(1, fil, end='$'):
            if d:
                self.vars = sd.decoder.vars
                self.fixed_vars = sd.decoder.fixed_vars
                self.state['identifier']= d[0][2]
                yield d
            
        fil.close()

decoder = AsdFilDec()

if __name__ == "__main__":
    import sys
    for d in decoder.decode(sys.argv[1]):
        print d