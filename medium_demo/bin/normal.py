# coding=utf-8
from __future__ import absolute_import, division, unicode_literals
from splunklib.searchcommands import dispatch, ReportingCommand, Configuration, Option, validators
import exec_anaconda
exec_anaconda.exec_anaconda()
import numpy as np
import sys

@Configuration(requires_preop=False)
class Normal(ReportingCommand):   
    loc = Option(require=True)
    scale = Option(require=True)

    @Configuration()
    def map(self, records):
        for record in records:
            record['gen_normal'] = np.random.normal(loc=float(record[self.loc]), scale=float(record[self.scale]))
            yield record
            
    @Configuration(requires_preop=False)
    def reduce(self, records):  
        for record in records:          
            yield record    
            
dispatch(Normal, sys.argv, sys.stdin, sys.stdout, __name__)
