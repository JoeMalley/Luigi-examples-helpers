from datetime import datetime
import os
import luigi
import pandas as pd
#import ApiRequests
import psycopg2
import luigi.contrib.postgres
from contextlib import contextmanager
import polars as pl

class GlobalParams(luigi.Config):
    url = luigi.Parameter(default="localhost:8080")
    user = luigi.Parameter(default="admin")
    password = luigi.Parameter(default="admin")
    host = luigi.Parameter(default="localhost")
    database = luigi.Parameter(default="db_b4t")
    user = luigi.Parameter(default="b4tuser")
    password = luigi.Parameter(default="b4tuser1")


#Custom luigi target class that contains the data that was retrieved in the query
class b4t_postgres_target(luigi.contrib.postgres.PostgresTarget):
    rows = []
    def __init__(self, host, database, user, password, table, update_id, port=None, rows=[]):
        super().__init__(host, database, user, password, table, update_id, port)
        self.rows = rows
    
    @contextmanager
    def open(self):
        yield self.rows

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
        try:
            for row in cursor.fetchall():
                self.rows.append(row)
        except psycopg2.ProgrammingError:
            print("No row data")

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



#A more generic save function that is completely configured via params, 
#this task can be depended on to ensure data is in the DB before being run for example
#It also does not depend on any other tasks
class b4t_copy_to_db_generic(luigi.contrib.postgres.CopyToTable):
    host = GlobalParams().host
    database = GlobalParams().database
    user = GlobalParams().user
    password = GlobalParams().password
    table = luigi.Parameter()
    rowArray = luigi.ListParameter()
    columns = luigi.ListParameter()
    def rows(self):
        for i in self.rowArray:
            yield i