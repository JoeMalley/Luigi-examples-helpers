from datetime import datetime
import os
import luigi
import pandas as pd
#import ApiRequests
import psycopg2
import luigi.contrib.postgres
from contextlib import contextmanager
import polars as pl
from helpers.luigi_postgres import *

#--- dataframe examples ----

#A simple task that creates a new data frame and saves it to a file
class save_DF_example(luigi.Task):
    def output(self):
        return luigi.LocalTarget('report_data.csv')
    
    def run(self):
        dict ={
            "col1": ["1-1","1-2","1-3"],
            "col2": ["2-1","2-2","2-3"]
        }
        df_out = pd.DataFrame(data=dict)
        df_out.to_csv(self.output().path,index=True,index_label="index")

#A task that requires the previous one
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


#---- Postgres example - using helpers ----#

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

#A simple task that deletes a value
class delete_db_entry_example(luigi.Task):
    def requires(self):
        return b4t_postgres_queryQuick(query="DELETE FROM device WHERE id=6",code=9)


#A simple update task that updates a value in the db
class update_db_entry_example(luigi.Task):
    def requires(self):
        return b4t_postgres_queryQuick(query="UPDATE device SET devicename='edited' WHERE id=5",code=5)


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

#An example of a task that saves the file that is required for the specific copy task
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
#Will be useful in cases where we definetly only want to run the SQL once
class b4t_copy_to_db_specific(luigi.contrib.postgres.CopyToTable):
    host = GlobalParams().host
    database = GlobalParams().database
    user = GlobalParams().user
    password = GlobalParams().password
    table = 'device'
    def requires(self):
        return save_DF_example_test()
    
    #Override this function to change input format - this one takes in CSVs that specifiy a db id
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

#A simple task that calls the generic save task
#Easier to use than a generic query since we can define the data as lists
class test_caller(luigi.Task):
    def requires(self):
        time1 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return b4t_copy_to_db_generic(columns = ['id','devicename','databytes','datatime','devicetype_id','codec_id','location_id'],
        rowArray=
        [
            [5,"9982",12,time1,1,15,1],
            [6,"1232",14,time1,1,15,2]
        ],
        table = 'device'
        )