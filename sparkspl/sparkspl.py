''' Module for translating SPL queries to pySpark functions '''

import re
import pyspark as spark
from pyspark.sql import functions as F

class SparkSpl():
    ''' Class for translating SPL queries to pySpark functions '''
    def __return_func(self, command, params):
        func = self.__switcher.get(command)
        return func(params)

    def spl_where(self, statement):
        '''External where function'''
        self.df = self.__spl_where(statement)
        return self.df

    def __spl_where(self, condition):
        condition = condition.lower()
        for col in self.df.columns:
            condition = condition.replace(col, 'self.df.' + col)
        condition = condition.replace(' and ', ' & ').replace(' or ', ' | ')
        return self.df.filter(eval(condition))

    def spl_eval(self, statement):
        '''External eval function'''
        self.df = self.__spl_eval(statement)
        return self.df

    def __spl_eval(self, statement):
        statement = statement.lower()
        new_field = statement.split('=')[0].strip()
        statement = ''.join(statement.split('=')[1:]).strip()
        for col in self.df.columns:
            statement = statement.replace(col, 'self.df.' + col)
        return self.df.withColumn(new_field, eval(statement))

    def spl_timechart(self, statement):
        '''External timechart function'''
        self.df = self.__spl_timechart(statement)
        return self.df

    def __spl_timechart(self, statement):
        statement = statement.lower()
        keywords, positionals, _, _ = self.__parse_stats_statement(statement)
        span = self.__get_span_in_seconds(keywords['span'])

        # TODO: Percentiles in timechart!!

        agg_string = []
        for key in positionals:
            newname, (name, func) = key, positionals[key]
            agg_string.append(eval('F.%s(\'%s\').alias(\'%s\')' % (func, name, newname)))

        return self.df.withColumn('newtime', \
                    (F.col('_time').cast('bigint') / span).cast('bigint') * span) \
                .groupby('newtime') \
                .agg(*agg_string) \
                .withColumnRenamed('newtime', '_time') \
                .orderBy('_time')

    def spl_stats(self, statement):
        '''Stats function'''
        self.df = self.__spl_stats(statement)
        return self.df

    def __spl_stats(self, statement):
        _, positionals, _, categories = self.__parse_stats_statement(statement)

        # Using `by _time` with `earliest` or `latest` is forbidden
        if ('_time' in categories) & \
            (('earliest' in set([f for _, (_, f) in positionals.items()])) | \
                ('latest' in set([f for _, (_, f) in positionals.items()]))):
            raise AttributeError

        # Add fake column for 'count' function and empty grouping
        self.df = self.df.withColumn('fake', F.lit(1))
        df_results = []

        # Add groupby if needed
        categories = categories if categories else ['fake']
        df_grouped = self.df.groupby(categories)

        earliest_latest = {newfield: (field, func) for newfield, (field, func) \
            in positionals.items() if func in ['earliest', 'latest']}

        # Handling of stats functions except `earliest` and `latest`
        # statslist, earliest_latest = [], {}
        # for newfield, (field, func) in positionals.items():
        #     if func in ['earliest', 'latest']:
        #         earliest_latest[newfield] = (field, func)
        #     elif field is not None:
        #         pval = int(func[1:]) / 100 if func.startswith('p') else \
        #             (0.5 if func == 'median' else None)
        #         func = 'perc' if func.startswith('p') | (func == 'median') else func
        #         statslist.append(self.__get_stats_function(func, field, newfield, pval))
        statslist = self.__get_stats_eval(positionals)
        if statslist:
            df_results.append(df_grouped.agg(*statslist))

        # Handling `earliest` and `latest`
        for newfield, (field, func) in earliest_latest.items():
            func_df = F.min('_time') if func == 'earliest' else F.max('_time')
            func_df = df_grouped.agg(func_df.alias('_time'))
            df_join = self.df.withColumnRenamed(field, newfield) \
                .join(func_df, ['_time'] + categories).select([newfield] + categories)
            df_results.append(df_join)

        # Join all results together
        if df_results:
            df_out = df_results[0]
            for df_r in df_results[1:]:
                df_out = df_out.join(df_r, categories)
        else:
            df_out = self.df

        df_out = df_out.drop('fake') if 'fake' in df_out.columns else df_out
        return df_out

    @staticmethod
    def __parse_stats_statement(statement):
        keywords, positionals, percentiles = {}, {}, {}
        split_statement = statement.replace(',', '').split('by')
        statement_params = split_statement[0].strip().split(' ')
        categories = [s.strip() for s in split_statement[1].strip().split(' ')] \
            if len(split_statement) > 1 else []
        for i, split_item in enumerate(statement_params):
            if '=' in split_item:
                keywords[split_item.split('=')[0]] = split_item.split('=')[1]
            elif (statement_params[i] == 'as') | (statement_params[i-1] == 'as'):
                continue
            elif i == len(statement_params) - 1:
                key, val = statement_params[i], statement_params[i]
            elif statement_params[i+1] == 'as':
                key, val = statement_params[i+2], statement_params[i]
            else:
                key, val = statement_params[i], statement_params[i]

            if '(' in val:
                func, pvar = val.split('(')
                positionals[key] = (pvar[:-1], func)
            else:
                positionals[key] = 'fake', val

        return keywords, positionals, percentiles, categories

    def __get_stats_eval(self, positionals):
        # Handling of stats functions except `earliest` and `latest`
        statslist = []
        for newfield, (field, func) in positionals.items():
            if (func not in ['earliest', 'latest']) & (field is not None):
                pval = int(func[1:]) / 100 if func.startswith('p') else \
                    (0.5 if func == 'median' else None)
                func = 'perc' if func.startswith('p') | (func == 'median') else func
                statslist.append(self.__get_stats_function(func, field, newfield, pval))
        return statslist

    @staticmethod
    def __get_stats_function(func, var, newfield, perc=None):
        stats_switcher = {
            'avg': F.mean,
            'max': F.max,
            'count': F.count,
            'stdev': F.stddev,
            'mean': F.mean,
            'sum': F.sum,
            'var': F.variance,
            'min': F.min,
            'first': F.first,
            'last': F.last,
            'dc': F.countDistinct,
            'approxdc': F.approx_count_distinct,
            'list': F.collect_list,
            'values': F.collect_set,
            'perc': F.expr
        }
        stats_func = stats_switcher[func]
        var = 'percentile_approx(%s, %f)' % (var, perc) if perc is not None else var
        return stats_func(var).alias(newfield)

    @staticmethod
    def __get_span_in_seconds(statement):
        rstatement = re.search(r'(?P<num>\d+)(?P<dim>[a-z]+)', statement)
        num = int(rstatement.group('num'))
        if rstatement.group('dim') == 's':
            mult = 1
        elif rstatement.group('dim') == 'min':
            mult = 60
        elif rstatement.group('dim') == 'h':
            mult = 60 * 60
        elif rstatement.group('dim') == 'd':
            mult = 60 * 60 * 24
        elif rstatement.group('dim') == 'w':
            mult = 60 * 60 * 24 * 7
        else:
            mult = 1
        return num * mult

    def __init__(self, **kwargs):
        self.input_path = kwargs.get('path')
        self.df = kwargs.get('df', spark.SQLContext(spark.SparkContext()) \
            .read \
            .option('header', True) \
            .csv(self.input_path, inferSchema=True))
        self.__switcher = {
            'eval': self.__spl_eval,
            'where': self.__spl_where,
            'timechart': self.__spl_timechart,
            'stats': self.__spl_stats
        }

    def spl(self, commandtext):
        '''
        Pass SPL command to SPL-Spark translator
        Params:
        - commandtext - full SPL command (example: timechart span=1d max(val) as maxval
        Returns nothing. Updates self.df of SparkSPL object
        '''
        command = commandtext.split(' ')[0]
        params = ' '.join(commandtext.split(' ')[1:])
        self.df = self.__return_func(command, params)

    def to_csv(self, out_path):
        '''
        Write self.df dataframe as csv.
        Params:
        - out_path - path for output csv.
        '''
        self.df.toPandas().to_csv(out_path, index=False)

    def write(self, out_path, mode='overwrite'):
        '''
        Write self.df dataframe as output hadoop object.
        Params:
        - out_path - path for output;
        - mode - overwrite | append.
        '''
        self.df.write.save(out_path, header='true', format='csv', mode=mode)
