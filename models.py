from mysql.connector import connect
import mysql.connector
import pandas as pd
import re
import sys


class Database:
    def __init__(self, appname, port):
        self.connection = connect(
            user=appname,
            password=f'5425{appname}',
            database=appname,
            host='127.0.0.1',
            port=port
        )
        self.appname = appname
        self.cursor = self.connection.cursor()

    def get_count(self, tablename, where=''):
        query = (f'SELECT COUNT(*) FROM {tablename} {where};')
        self.cursor.execute(query)
        data = self.cursor.fetchone()[0]

        return data

    def get_table(self, tablename, columns=[]):
        if not columns:
            columns = self.get_column_names(tablename)

        quoted_columns = [f'`{column}`' for column in columns]
        column_string = ','.join(quoted_columns)
        query = (f'SELECT {column_string} FROM {tablename};')
        self.cursor.execute(query)
        data = [row for row in self.cursor]

        return pd.DataFrame(data, columns=columns)

    def get_join_table(self, tablename, join, columns=[]):
        #if not columns:
        #    columns = self.get_column_names(tablename)
        
        # remove the table associations
        new_columns = [re.sub(r't1.Id|t3.Id|t4.Id|t2.Id|t1.|t2.|t3.|t4.| AS ', '', col) for col in columns]
        column_string = ','.join(columns)
        query = (f'SELECT {column_string} FROM {tablename} {join};')
        self.cursor.execute(query)
        data = [row for row in self.cursor]

        return pd.DataFrame(data, columns=new_columns)

    def get_column_names(self, tablename):
        query = (f"""
            SELECT `COLUMN_NAME`
            FROM `INFORMATION_SCHEMA`.`COLUMNS`
            WHERE `TABLE_SCHEMA`='{self.appname}'
            AND `TABLE_NAME`='{tablename}';
        """)
        self.cursor.execute(query)
        return [row[0] for row in self.cursor]

    def close(self):
        self.cursor.close()
        self.connection.close()