"""
Database Handler Base on cx_Oracle

@author: xiang
"""

import numpy as np
import pandas as pd
import datetime
import tempfile
import re
import cx_Oracle

class db_handler_base:
    """
    Oracle Python Handler based on cx_Oracle

    Args:
        connection: schema
        uid: user id
        pwd: password
        verbose: if True, output SQL and result stats to console and write log to tmpdir
        tmpdir: directory to write log file
        reconnection_interval: automatically reconnect if exceed time interval
    function:
        reconnect: reconnect if exceed time interval when execute
        check_connection: check whether exceed time interval
        db_execute: write log file, output to console, execute SQL query, and return result.
    Return:
        result: if select return raw result
    """
    def __init__(self, connection, uid, pwd, verbose = False, tmpdir = None, reconnection_interval = None):
        try:
            self.connectionConfig = str(uid) + '/' + str(pwd) + '@' + str(connection)
            self.conn = cx_Oracle.connect( self.connectionConfig)
            self.cursor = self.conn.cursor()
            print('Connect to {} now'.format(connection))
        except Exception as e:
            print("Fail to build connection with {}".format(connection))
            print('Failing Error {}'.format(e))
        self.connection = connection
        self.verbose = verbose
        self.tmpdir = tmpdir
        self.connection_time = datetime.datetime.now()
        self.reconnection_interval = reconnection_interval #Interval in minutes

    def reconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception as e:
            print('Disconnection Failed:{}',format(e))
        self.conn = cx_Oracle.connect( self.connectionConfig)
        self.cursor = self.conn.cursor()
        self.connection_time = datetime.datetime.now()

    def check_connection(self):
        if (self.reconnection_interval is not None):
            diff = self.connection_time - datetime.datetime.now()
            if( diff.seconds/60 > self.reconnection_interval):
                print('Reconnecting {} minutes reconnect interval'.format(self.reconnection_interval))
                self.reconnect()

    def db_execute(self, query, bind_value):
        if (self.verbose):
            ## Creating temporary file for SQL Log: Query + Bind_variable + Run Time + Records
            with tempfile.NamedTemporaryFile(delete = False, dir = self.tmpdir, mode = 'wt',
                                             prefix= 'SQLLog', suffix = '.txt' ) as logger:
                logger.writelines("=" * 10 + "SQL" + "=" * 10 + "\n")
                logger.writelines(query)
                logger.writelines("\n" + "=" * 10 + "DATA" + "=" * 10 + "\n")
                logger.writelines("\n".join([str(date) for date in bind_value]))
                print("Log query in", logger.name)

        ## replace ? with bind variable
        if (len(bind_value) >0) :
            ## replace ? with :1, :2 bind variable
            for bind_id in range(len(bind_value)):
                query = re.compile(r'\?').sub(":"+ str(bind_id), query, count = 1) # replace ? one at a time

        ## Truncate SQL and output to console. Make sure table name and join method got printed
        if (self.verbose):
            queryMsg = re.compile(r'[\r\n]').sub(' ', query) ## remove line splitter
            queryMsg = re.compile(r'\s{2,}').sub(' ', queryMsg) ## remove more than 1 white space
            source_all = [matchobj.start() for matchobj in re.compile(r'\b(from|join)[^\w]', re.I ).finditer(queryMsg)]##find from or join
            source_subselect = [matchobj.start() for matchobj in re.compile(r'\b(from|join)\s*\(', re.I).finditer(queryMsg)]
            source_remain = list(set(source_all) - set(source_subselect))

            first_pos = source_all[0] if (len(source_remain)==0) else source_remain[0]

            if ( (first_pos is None) | first_pos <3):
                # 'from' < 35. Only truncate the too long string
                if(len(queryMsg) >86):
                    queryMsg = queryMsg[:83] + '...'
            else:
                # from >35. paste first 30 with 50 after 'from'
                trim_end = len(queryMsg) > first_pos + 55
                if trim_end:
                    queryMsg = queryMsg[:30] + '...' + queryMsg[(first_pos):(first_pos+50)] + '...'
                else:
                    queryMsg = queryMsg[:30] + '...' + queryMsg[(first_pos):]

            print('[{:%Y-%m-%d %H:%M:%S}] Query [{}]'.format(datetime.datetime.now(), queryMsg))

            ## fetching data
            result = None
            start_time = datetime.datetime.now()
            self.cursor.execute(query, bind_value)
            colname = [col[0].lower() for col in self.cursor.description] #change colname to lower
            result = self.cursor.fetchall()
            result = pd.DataFrame(result, columns = colname)

        ## output RunTime and # of records to console
        if (self.verbose):
            timeElapsed = np.round( (datetime.datetime.now() - start_time).seconds)
            SelectFlag = re.compile(r'^\s*(/\\*.*?\\*/\s*)?select', re.I).search(query)
            records = '{} records'.format(result.shape[0]) if (result is not None) & (SelectFlag is not None) else ""
            print('Took {}s - {}'.format(timeElapsed, records))

        if (self.verbose):
            with open(logger.name, 'a') as logger:
                ## output RunTime and # of records to log file
                logger.writelines("\n\n\n" + "=" * 10 + "INFO" + "=" * 10 + "\n")
                logger.writelines("{} seconds \n{}".format(timeElapsed, records))
                print("Log result in", logger.name)

        return result


class db_handler:
    """
    Add all kinds of SQL statement, including select, execute[delete/update/truncate]
    Add create table/drop table function
    Add SQL_Loader

    Args:
        connection: schema
        uid: user id
        pwd: password
        verbose: if True, output SQL and result stats to console and write log to tmpdir
        tmpdir: directory to write log file
        reconnection_interval: automatically reconnect if exceed time interval
        bind_value: pass in value for bind variable
        nahandle: dictionary {column: nafill} how to fill in nan for column specified
        cast: dictionary{column:dtype} how to coerce datatype for column specified

    Returns:
        result, after cleaning na column and transfrom datatype.
        cursor, Oracle API

    """
    def __init__(self, connection, uid, pwd, verbose = False, tmpdir = None, reconnection_interval = None):
        super(db_handler,self).__init__(connection, uid, pwd, verbose, tmpdir, reconnection_interval)

    def execute(self, query, bind_value):
        self.check_connection()
        self.db_execute(query, bind_value)

    def select(self, query, bind_value, nahandle = None, cast = None):
        self.check_connection()

        # Fetch data
        result = self.db_execute(query, bind_value)
        if (result is None) or (result.shape[0]==0) or not cast:
            return result

        ## NA handling before cast data type
        nacount = result.isnull().sum()
        for naPattern, fillvalue in nahandle.items():
            match = [ colname for colname in result.columns
                     if  re.compile(r'{}'.format(naPattern), re.I).match(colname) is not None ]
            if len(match)==0:
                continue
            for column in match:
                if nacount[column] == 0:
                    continue # no NAN
                else:
                    print('FillNA {} [{}] with [{}]'.format(column, nacount[column], fillvalue))
                    result.loc[:,column] = result.loc[:,column].fillna(fillvalue)

        # coerce column type, according to cast
        # Panda column type  int64/float64/bool/datetime64/object
        datatype = result.dtypes
        for castPattern, rule in cast.items():
            match = [ colname for colname in result.columns
                     if  re.compile(r'{}'.format(castPattern), re.I).match(colname) is not None ]
            if len(match)==0:
                continue
            for column in match:
                if datatype[column] == rule:
                    continue # same data type
                else:
                    print('Convert {} [{}] to [{}]'.format(column,datatype[column],rule))
                    if ( re.compile(r'^int').match(rule) ):
                        result.loc[:,column] = pd.to_numeric(result.loc[:, column],
                                                              downcast = 'integer',
                                                              errors = 'coerce')
                    elif( re.compile(r'^float').match(rule) ):
                        result.loc[:,column] = pd.to_numeric(result.loc[:, column],
                                      downcast = 'float',
                                      errors = 'coerce')
                    elif ( re.compile(r'^(date|time)').match(rule) ):
                        result.loc[:,column] = pd.to_datetime(result.loc[:, column],
                                                              errors = 'coerce')
                    else:
                        print('Currently only int/float/datetime are supported')
        return result

    def gethandler(self):
        return self.cursor

    def bs_sql_load(self, query, table_name):
        raise NotImplementedError
