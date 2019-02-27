''' Example for using SparkSpl class for translating SPL command to pySpark functions '''

import sys
from sparkspl import SparkSpl

PATH = sys.argv[1]
SPL = sys.argv[2]
OUT_PATH = sys.argv[3]

sparkspl = SparkSpl(path=PATH)
sparkspl.spl(SPL)
sparkspl.to_csv(OUT_PATH)
