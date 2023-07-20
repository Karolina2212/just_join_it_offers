import psycopg2
from psycopg2 import sql

class DBConnector:

    def __init__(self, host, dbname, user):
        self.host = host
        self.dbname = dbname
        self.user = user

        conn_string = "host={0} user={1} dbname={2}".format(host, user, dbname)
        self.conn = psycopg2.connect(conn_string)
        self.cursor = self.conn.cursor()

    def select_one(self, tab_name, dict={}):
        # loop creating part of the select query that will take columns on which where clause is used
        keys = []
        for index in range(len(dict)):
            keys.append("{{{}}} = %s".format(str(index)))
        columns = " AND ".join(keys)

        # sql_select query should work for both cases: when where clause is used or not
        sql_select = "SELECT * FROM {tab_name}" + (" WHERE " if len(columns) > 0 else "") + columns

        # sql_query - takes sql_select query (with or without 'where' clause) + columns used in where clause:
        # *map(lambda col_name: sql.Identifier(col_name), list(dict.keys()) -> for each column name from dict: 'sql.identifier' is implied and it is added to the end of sql query (as "*" has been used before 'map').
        sql_query = sql.SQL(sql_select).format(*map(lambda col_name: sql.Identifier(col_name), list(dict.keys())),
                                               tab_name=sql.Identifier(tab_name))

        self.cursor.execute(sql_query, list(dict.values()))
        existing_row = self.cursor.fetchone()

        return existing_row

    def insert_one(self, tab_name, dict):
        sql_insert = sql.SQL("INSERT INTO {tab_name} ({col_name}) VALUES ({values})" + 'RETURNING id;').format(
            col_name=sql.SQL(', ').join(map(lambda col_name: sql.Identifier(col_name), list(dict.keys()))),
            tab_name=sql.Identifier(tab_name),
            values=sql.SQL(', ').join(map(lambda value: sql.Placeholder(value), list(dict.keys()))))

        try:
            self.cursor.execute(sql_insert, dict)
            internal_id = self.cursor.fetchone()[0]
            self.conn.commit()
            return internal_id

        except psycopg2.errors.UniqueViolation:
            self.conn.rollback()

    def close_cursor(self):

        self.cursor.close()
        self.conn.close()

