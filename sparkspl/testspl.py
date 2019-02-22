from sparkspl import SparkSpl
import sys

path = sys.argv[1]
spl = sys.argv[2]
out_path = sys.argv[3]

sparkspl = SparkSpl(path)
sparkspl.spl(spl)
sparkspl.to_csv(out_path)
