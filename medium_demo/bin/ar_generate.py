# coding=utf-8
from __future__ import absolute_import, division, unicode_literals
from splunklib.searchcommands import dispatch, ReportingCommand, Configuration, Option, validators
import exec_anaconda
exec_anaconda.exec_anaconda()
import numpy as np
import sys

@Configuration(requires_preop=False)
class ArGenerate(ReportingCommand):   
    time_field = Option(require=True)
    order = Option(require=True)
    k = Option(require=False)

    @Configuration()
    def map(self, records):
        for record in records:
            yield {'time': record[self.time_field]}
            
    @Configuration(requires_preop=False)
    def reduce(self, records):
        order = int(self.order)
        y = list(np.zeros(order))
        coefs = [float(k.strip()) for k in str(self.k).split(',')] if self.k else [0.1]*order
        for record in records:
            val = np.dot(y[-order:], coefs[::-1]) + np.random.normal(loc=0.0, scale=1.0)
            y.append(val)
            yield {'_time': record['time'], 'gen_ar': val}
            
dispatch(ArGenerate, sys.argv, sys.stdin, sys.stdout, __name__)
