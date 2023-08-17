import psycopg2
from lib.settings import Settings
from psycopg2 import sql

class DBConnector:

    def __init__(self):
        settings = Settings()
        conn_string = "host={0} user={1} dbname={2}".format(settings.host, settings.user, settings.dbname)
        self.conn = psycopg2.connect(conn_string)
        self.cursor = self.conn.cursor()

    def select_one(self, tab_name, dict={}):
        self.__select(tab_name, dict)
        existing_row = self.cursor.fetchone()
        return existing_row

    def select_all(self, tab_name, dict={}):
        self.__select(tab_name, dict)
        existing_rows = self.cursor.fetchall()
        return existing_rows

    def insert_one(self, tab_name, dict):

        try:
            sql_insert = sql.SQL("INSERT INTO {tab_name} ({col_name}) VALUES ({values})" + 'RETURNING id;').format(
            col_name=sql.SQL(', ').join(map(lambda col_name: sql.Identifier(col_name), list(dict.keys()))),
            tab_name=sql.Identifier(tab_name),
            values=sql.SQL(', ').join(map(lambda value: sql.Placeholder(value), list(dict.keys()))))

            self.cursor.execute(sql_insert, dict)
            internal_id = self.cursor.fetchone()[0]
            self.conn.commit()
            return internal_id

        except psycopg2.errors.UniqueViolation:
            self.conn.rollback()
            return None

    def truncate_table(self, tab_name):
        sql_truncate = sql.SQL("TRUNCATE TABLE {tab_name}").format(tab_name=sql.SQL(', ').join(map(lambda tab_name: sql.Identifier(tab_name), tab_name)))

        self.cursor.execute(sql_truncate)
        self.conn.commit()


    def update_row(self, tab_name, dict_condition, dict_updated):

        up_col = []
        for index in range(len(dict_updated)):
            up_col.append("{{{}}} = %s".format(str(index)))

        columns_updated = ", ".join(up_col)

        con_col = []
        for index in range(len(dict_condition)):
            con_col.append("{{{}}} = %s".format(str(index)))

        columns_condition = " AND ".join(con_col)

        sql_set = sql.SQL("UPDATE {tab_name} SET "+ columns_updated).format(*map(lambda col_name: sql.Identifier(col_name), list(dict_updated.keys())),tab_name=sql.Identifier(tab_name))
        sql_condition = sql.SQL(" WHERE " + columns_condition).format(*map(lambda col_name: sql.Identifier(col_name), list(dict_condition.keys())))

        sql_query = sql_set + sql_condition

        values = list(dict_updated.values())+list(dict_condition.values())
        self.cursor.execute(sql_query, values)
        self.conn.commit()

    def close_cursor(self):
        self.cursor.close()
        self.conn.close()

    def __select(self,tab_name, dict):
        keys = []
        for index in range(len(dict)):
            keys.append("{{{}}} = %s".format(str(index)))
        columns = " AND ".join(keys)

        sql_select = "SELECT * FROM {tab_name}" + (" WHERE " if len(columns) > 0 else "") + columns

        sql_query = sql.SQL(sql_select).format(*map(lambda col_name: sql.Identifier(col_name), list(dict.keys())),
                                               tab_name=sql.Identifier(tab_name))
        self.cursor.execute(sql_query, list(dict.values()))
