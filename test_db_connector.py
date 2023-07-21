import unittest
from db_connector import DBConnector
import psycopg2
from psycopg2 import sql
from datetime import date

class TestDBConnector(unittest.TestCase):

    def setUp(self):
        self.db_connector = DBConnector('127.0.0.1','just_join_it_offers_test','lisek')

        conn_string = "host={0} user={1} dbname={2}".format('127.0.0.1','lisek', 'just_join_it_offers_test')
        self.conn = psycopg2.connect(conn_string)
        self.cursor = self.conn.cursor()

    def tearDown(self):
        self.cursor.close()
        self.conn.close()

    def test_db_insert_one(self):

        self.cursor.execute('truncate table offers_empl_type, offers_info ,offers_locations, offers_per_location_id , offers_skills, skills_per_offer ')
        self.conn.commit()

        self.db_connector.insert_one('offers_info',{'jjit_id': 'test_1', 'title': 'test_title', 'company_name': 'test_company', 'marker_icon': 'test_marker', 'workplace_type': 'test_type', 'experience_level': 'test_exp_lvl' , 'import_date': date.today(),'country_code': 'test_cc'})

        sql_insert_check = sql.SQL("SELECT jjit_id, title, company_name, marker_icon, workplace_type, experience_level, import_date, country_code FROM {table}").format(table=sql.Identifier('offers_info'))
        self.cursor.execute(sql_insert_check)
        data_check = self.cursor.fetchall()
        data_check.sort()

        check_list = [('test_1','test_title','test_company','test_marker','test_type','test_exp_lvl', date.today(), 'test_cc')]
        self.assertEqual(data_check, check_list)

    def test_db_select_one(self):

        data_check = self.db_connector.select_one('offers_info',{'jjit_id': 'test_1', 'title': 'test_title', 'company_name': 'test_company', 'marker_icon': 'test_marker', 'workplace_type': 'test_type', 'experience_level': 'test_exp_lvl' , 'country_code': 'test_cc'})

        check_list = ('test_1', 'test_title', 'test_company', 'test_marker', 'test_type', 'test_exp_lvl', date.today(),'test_cc')
        self.assertEqual(data_check[1:], check_list)

    def test_truncate_tab(self):

        self.db_connector.truncate_table(['offers_empl_type', 'offers_info','offers_locations', 'offers_per_location_id' , 'offers_skills', 'skills_per_offer'])
        sql_truncate_check = sql.SQL( "SELECT * FROM {table}").format(table=sql.Identifier('offers_info'))
        self.cursor.execute(sql_truncate_check)
        data_check = self.cursor.fetchall()

        check = []
        self.assertEqual(data_check, check)

if __name__ == '__main__':
    unittest.main()