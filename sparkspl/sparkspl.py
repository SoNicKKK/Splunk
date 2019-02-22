import sys
import re
import pyspark as spark
from pyspark.sql import functions as F

sc = spark.SQLContext(spark.SparkContext())

class SparkSpl():
    def __return_func(self, command, params):
        func = self.__switcher.get(command)
        return func(params)

    def __splWhere(self, condition):
        condition = condition.lower()
        for col in self.df.columns:
            condition = condition.replace(col, 'self.df.' + col)
        condition = condition.replace(' and ', ' & ').replace(' or ', ' | ')    
        return self.df.filter(eval(condition))

    def __splEval(self, statement):
        statement = statement.lower()
        modCol = statement.split('=')[0].strip()
        statement = ''.join(statement.split('=')[1:]).strip()
        for col in self.df.columns:
            statement = statement.replace(col, 'self.df.' + col)        
        return self.df.withColumn(modCol, eval(statement))

    def __splTimechart(self, statement):
        statement = statement.lower()
        pos, ret = self.__parsePositionalAndAs(statement)        
        span = self.__getSpanInSeconds(pos['span'])

        agg_string = []
        for key in ret:
            newname, func = key, ret[key]
            r = re.search(r'(?P<func>\w+)\((?P<oldname>\w+)\)', func)
            func, oldname = r.group('func'), r.group('oldname')
            agg_string.append(eval('F.%s(\'%s\').alias(\'%s\')' % (func, oldname, newname)))
        
        return self.df.withColumn('newtime', \
            (self.df._time.cast('bigint') / span).cast('bigint') * span) \
                .groupby('newtime') \
                .agg(*agg_string) \
                .withColumnRenamed('newtime', '_time') \
                .orderBy('_time')
    
    def __parsePositionalAndAs(self, statement):
        pos, ret = {}, {}
        sp = statement.split(' ')
        for i, s in enumerate(sp):
            if '=' in s:
                pos[s.split('=')[0]] = s.split('=')[1]
            elif (sp[i] == 'as') | (sp[i-1] == 'as'):
                continue
            elif i == len(sp) - 1:
                ret[sp[i]] = sp[i]
            elif sp[i+1] == 'as':        
                ret[sp[i+2]] = sp[i]
            else:
                ret[sp[i]] = sp[i]
        return pos, ret

    def __getSpanInSeconds(self, statement):
        r = re.search(r'(?P<num>\d+)(?P<dim>[a-z]+)', statement)
        num = int(r.group('num'))
        if r.group('dim') == 's':
            span = num
        elif r.group('dim') == 'min':
            span = num * 60
        elif r.group('dim') == 'h':
            span = num * 60 * 60
        elif r.group('dim') == 'd':
            span = num * 60 * 60 * 24
        elif r.group('dim') == 'w':
            span = num * 60 * 60 * 24 * 7
        else:
            span = num
        return span

    def __create_spark_dataframe(self):
        self.df = sc.read.option('header', True).csv(self.input_path)

    def __init__(self, path):
        self.input_path = path
        self.__switcher = {
            'eval': self.__splEval,
            'where': self.__splWhere,
            'timechart': self.__splTimechart
        }
        self.__create_spark_dataframe()    

    def spl(self, commandtext):
        command = commandtext.split(' ')[0]
        params = ' '.join(commandtext.split(' ')[1:])
        self.df = self.__return_func(command, params)

    def to_csv(self, out_path):
        self.df.toPandas().to_csv(out_path, index=False)

    def write(self, out_path, mode='overwrite'):
        self.df.write.save(out_path, header='true', format='csv', mode=mode)