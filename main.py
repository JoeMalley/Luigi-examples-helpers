from datetime import datetime
from email.policy import default
from fileinput import filename
from sqlite3 import Timestamp
import luigi
import pandas as pd
#import ApiRequests
import psycopg2
import luigi.contrib.postgres
from contextlib import contextmanager
import time

#TODO examples that
#connect to api
#use postgres
#save file to input file

class GlobalParams(luigi.Config):
    url = luigi.Parameter(default="localhost:8080")
    user = luigi.Parameter(default="admin")
    password = luigi.Parameter(default="admin")
    host = luigi.Parameter(default="localhost")
    database = luigi.Parameter(default="db_b4t")
    user = luigi.Parameter(default="b4tuser")
    password = luigi.Parameter(default="b4tuser1")

class save_DF_example(luigi.Task):
    def output(self):
        return luigi.LocalTarget('report_data.csv')
    
    def run(self):
        #apiServer = ApiRequests()
        #token = apiServer.connect(GlobalParams.url, GlobalParams.user, GlobalParams.password)

        #Get the data above from api or something
        dict ={
            "col1": ["1-1","1-2","1-3"],
            "col2": ["2-1","2-2","2-3"]
        }
        df_out = pd.DataFrame(data=dict)
        df_out.to_csv(self.output().path,index=True,index_label="index")

class edit_df_example(luigi.Task):

    def requires(self):
        return save_DF_example()
    def output(self):
        return luigi.LocalTarget('filtered_data.csv')
    
    def run(self):
    
        df_in = pd.read_csv(self.input().path,index_col="index")

        df_out = df_in
        #Edit a collum
        df_out['col2'] = ['Edited','Data','Test']
        
        #Adding a new row
        new_row = pd.Series(data= {
            "col1": "1-4",
            "col2": "2-4"
        },name=len(df_out))
        df_out = df_out.append(new_row,ignore_index=False)

        df_out.to_csv(self.output().path)


class edit_df_resave_example(luigi.Task):

    def requires(self): # A task that saves to the same target as input
        return edit_df_example()
    def output(self):
        return luigi.LocalTarget(self.input().path)
    
    def run(self):
        df_in = pd.read_csv(self.input().path,index_col="index")

        df_out = df_in
        
        #Adding a new row
        new_row = pd.Series(data= {
            "col1": "1-5",
            "col2": "2-5"
        },name=len(df_out))
        df_out = df_out.append(new_row,ignore_index=False)

        df_out.to_csv(self.output().path)      








# --- Postgres Queries ---

# A b4t query class using the postgres module, that modifies the behaviour of the PostgresQuery class to be slightly more useful
class b4t_postgres_query(luigi.contrib.postgres.PostgresQuery):
    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()
    query = luigi.Parameter()
    rows = []
    code = luigi.IntParameter()

    def run(self):
        connection = self.output().connect()
        cursor = connection.cursor()
        sql = self.query
        cursor.execute(sql)

        #Extra behaviour added in this subclass
        for row in cursor.fetchall():
            print("APPENDING A ROW")
            self.rows.append(row)
        #

        self.output().touch(connection)
        connection.commit()
        connection.close()

    def output(self):
        return b4t_postgres_target(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.update_id,
            rows=self.rows
        )

#A subclass of the generic query that uses global params instead
# Many uses for such thing, such as a query class with hardcoded values, or one that asks for proper auth e.t.c
#Or could make all parameters command line passed instead for a fully customisable boilerplate class
class b4t_postgres_queryQuick(b4t_postgres_query):
    host = GlobalParams().host
    database = GlobalParams().database
    user = GlobalParams().user
    password = GlobalParams().password
    table = 'na'
    query = luigi.Parameter()
    rows = []
    code = luigi.IntParameter()

#Custom luigi target class that contains the data that was retrieved in the query
class b4t_postgres_target(luigi.contrib.postgres.PostgresTarget):
    rows = []
    def __init__(self, host, database, user, password, table, update_id, port=None, rows=[]):
        super().__init__(host, database, user, password, table, update_id, port)
        self.rows = rows
    
    @contextmanager
    def open(self):
        yield self.rows
    
#An example of using the B4TQuery Subclasses to query the db
class do_query_example(luigi.Task):
    def requires(self):
        return b4t_postgres_query(host="localhost",database="db_b4t",user="b4tuser",password="b4tuser1",table="device",query="SELECT devicename, databytes, datatime, devicetype_id FROM device",code=44)
    
    def run(self):
        with self.input().open() as rows:
            print(rows)
            #do whatever we want with the data

#An example of doing the quick version of the query
class do_query_example_quick(luigi.Task):
    def requires(self):
        return b4t_postgres_queryQuick(query="SELECT devicename, databytes, datatime, devicetype_id FROM device",code=9)
    
    def run(self):
        with self.input().open() as rows:
            print(rows)
            #do whatever we want with the data







# --- Customisable generic query function - does not depend on any parent classes other than luigi baseTask ---

# This task querys a postgres database and then saves the files as a dataframe
#It does not use specific luigi postgres modules instead implements the behaviour manually
#This can be a fallback for very quick and dirty queries of a bespoke nature that need repeating: 
#For example this one queries some specific stuff then saves it as a dataframe.
#Would be best used if we have very complex queries, or even multiple queries required in the same task
class query_postgres_example(luigi.Task):
    def output(self):
        # the output will be a .csv file
        return luigi.LocalTarget("postgres_example.csv")

    def run(self):
        #Use global config params in actual code
        host = "localhost"
        database = "db_b4t"
        user = "b4tuser"
        password = "b4tuser1"

        conn = psycopg2.connect(
            dbname=database,
            user=user,
            host=host,
            password=password)
        cur = conn.cursor()
        cur.execute("""SELECT
          devicename,
          databytes,
          datatime,
          devicetype_id
          FROM device
        """)
        rows = cur.fetchall()
        df = pd.DataFrame(data=rows,columns=['devicename','databytes','datatime','devicetype_id'])
        print(df)
        df.to_csv(self.output().path,index_label='index')

# --- Specific Data saving in postgres ---

#An example of a task that saves the file that is required for the specific save task
class save_DF_example_test(luigi.Task):
    def output(self):
        return luigi.LocalTarget()
    
    def run(self):
        dict ={
            "id" : [3],
            "devicename": ["8888"],
            "databytes": [12],
            "datatime" : [datetime.now()],
            "devicetype_id" : [1],
            "codec_id":[15],
            "location_id" : [1]
        }
        df_out = pd.DataFrame(data=dict)
        df_out.to_csv(self.output().path,index=False)

#A Task that takes in a CSV file as input and uses it to save in a specific table, modifications can be made to how the input is processed
#However it will probably be best used for specific save functions that depend on other specific tasks
class b4t_copy_to_db_specific(luigi.contrib.postgres.CopyToTable):
    host = GlobalParams().host
    database = GlobalParams().database
    user = GlobalParams().user
    password = GlobalParams().password
    table = 'device'
    def requires(self):
        return save_DF_example_test()
    
    #Override this function to change input format - this one takes in CSVs that specifiy a db id
    #TODO make it accept none values
    def rows(self):
        """
        Return/yield tuples or lists corresponding to each row to be inserted.
        """
        with self.input().open('r') as fobj:
            self.columns = fobj[0].strip('\n').split(",")
            for line in fobj:
                if(line.strip('\n').split(",") != self.columns):
                    yield line.strip('\n').split(',')


# --- Generic saving data in postgres ---


#A more generic save function that is completely configured via params, 
#this task can be depended on to ensure data is in the DB before being run for example
class b4t_copy_to_db_generic(luigi.contrib.postgres.CopyToTable):
    host = GlobalParams().host
    database = GlobalParams().database
    user = GlobalParams().user
    password = GlobalParams().password
    table = 'device'
    rowArray = luigi.ListParameter()
    columns = luigi.ListParameter()
    def rows(self):
        for i in self.rowArray:
            yield i

#A simple task that calls the generic save taks
class test_caller(luigi.Task):
    def requires(self):
        time1 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return b4t_copy_to_db_generic(columns = ['id','devicename','databytes','datatime','devicetype_id','codec_id','location_id'],
        rowArray=
        [
            [5,"9982",12,time1,1,15,1],
            [6,"1232",14,time1,1,15,2]
        ]
        )