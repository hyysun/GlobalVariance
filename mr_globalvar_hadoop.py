"""
MapReduce job to compute global variance of a series of simulation runs for all node space.

Example:

python mr_globalvar_hadoop.py \
hdfs://icme-hadoop1.localdomain/user/yangyang/simform/data3/thermal_maze00*/thermal_*.seq \
-r hadoop --no-output -o var_temp1 --variable TEMP 

"""

__author__ = 'Yangyang Hou <hyy.sun@gmail.com>'

import sys
import os

from mrjob.job import MRJob
from numpy import *

class MRGlobalVar(MRJob):

    STREAMING_INTERFACE = MRJob.STREAMING_INTERFACE_TYPED_BYTES
    
    def configure_options(self):
        """Add command-line options specific to this script."""
        super(MRGlobalVar, self).configure_options()
        
        self.add_passthrough_option(
            '--variable', dest='variable',
            help='--variable VAR, the variable need to compute global variance'       
        )
       
    def load_options(self, args):
        super(MRGlobalVar, self).load_options(args)
            
        if self.options.variable is None:
            self.option_parser.error('You must specify the --variable VAR')
        else:
            self.variable = self.options.variable
    
    def mapper(self, key, value):
        # ignore coordinate (x,y,z) data
        if (key != -1) and (key != -2) and (key != -3) : 
            for i, var in enumerate(value):
                name = var[0]
                if name == self.variable:
                    data = var[1]
                    data = array(data)
                    len = data.size
                    i = 40
                    for j in range(0,i):
                        if j != i-1:
                            tmpdata = data[len/i*j:len/i*(j+1)]
                        else:
                            tmpdata = data[len/i*j:]
                        yield (j, tmpdata)
           

    def reducer(self, key, values): 
        mean = 0
        mean2 = 0 
        for i, value in enumerate(values):
            mean2 = (i*mean2+value*value)/(i+1)
            mean = (i*mean+value)/(i+1)
        variance = mean2 - mean*mean
        
        # To avoid PICKLE type in typedbytes files
        variance2 = []
        for i, ele in enumerate(variance):
            variance2.append(float(ele))
            
        yield (key, variance2)    
        
    def steps(self):
        return [self.mr(self.mapper, self.reducer),]

if __name__ == '__main__':
    MRGlobalVar.run()

