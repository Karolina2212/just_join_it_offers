import unittest
import os
from lib.db_connector import DBConnector
import psycopg2
from psycopg2 import sql
from datetime import date

class TestDBConnector(unittest.TestCase):

    def setUp(self):
        self.db_connector = DBConnector()
        conn_string = "host={0} user={1} dbname={2}".format(os.getenv("HOST"),os.getenv("USER"),os.getenv("DB_NAME"))
        self.conn = psycopg2.connect(conn_string)
        self.cursor = self.conn.cursor()

        self.cursor.execute('TRUNCATE TABLE offers_empl_type, offers_info ,offers_locations, offers_per_location_id , offers_skills, skills_per_offer ')

        sql_insert_test = 'INSERT INTO offers_info( jjit_id, title, company_name, marker_icon, workplace_type, experience_level, import_date, country_code) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
        self.test1_values = ('test_1','test_title1','test_company1','test_marker1','test_type1','test_exp_lvl1', date.today(), 'test_cc1')
        self.test2_values = ('test_2','test_title2','test_company2','test_marker2','test_type2','test_exp_lvl2', date.today(), 'test_cc2')
        self.cursor.execute(sql_insert_test,self.test1_values)
        self.cursor.execute(sql_insert_test,self.test2_values)

        self.conn.commit()

    def tearDown(self):
        self.cursor.close()
        self.conn.close()
        self.db_connector.close_cursor()

    def test_db_insert_one(self):

        internal_id = self.db_connector.insert_one('offers_info',{
            'jjit_id': 'test',
            'title': 'test_title',
            'company_name': 'test_company',
            'marker_icon': 'test_marker',
            'workplace_type': 'test_type',
            'experience_level': 'test_exp_lvl' ,
            'import_date': date.today(),
            'country_code': 'test_cc'})

        sql_insert_check = sql.SQL("SELECT jjit_id, title, company_name, marker_icon, workplace_type, experience_level, import_date, country_code FROM {table} WHERE id = %s").format(table=sql.Identifier('offers_info'))
        self.cursor.execute(sql_insert_check,[internal_id,])
        data_check = self.cursor.fetchone()

        check_list = ('test','test_title','test_company','test_marker','test_type','test_exp_lvl', date.today(), 'test_cc')
        self.assertEqual(data_check, check_list)

    def test_db_select_one(self):

        data_check = self.db_connector.select_one('offers_info',{
            'jjit_id': 'test_1',
            'title': 'test_title1',
            'company_name': 'test_company1',
            'marker_icon': 'test_marker1',
            'workplace_type': 'test_type1',
            'experience_level': 'test_exp_lvl1',
            'country_code': 'test_cc1'})

        self.assertEqual(data_check[1:], self.test1_values)

    def test_db_select_all(self):

        data_check = self.db_connector.select_all('offers_info')

        self.assertEqual(data_check[0][1:],self.test1_values)
        self.assertEqual(data_check[1][1:],self.test2_values)
        self.assertEqual(len(data_check),2)


    def test_db_update_row(self):

        self.db_connector.update_row('offers_info',{'jjit_id':'test_1'},{'title':'updated_title','company_name':'updated_company_name'})

        sql_update_check = "SELECT jjit_id, title, company_name FROM offers_info WHERE jjit_id = %s"
        self.cursor.execute(sql_update_check, ['test_1',])
        data_check = self.cursor.fetchone()

        check_list = ('test_1','updated_title','updated_company_name')
        self.assertEqual(data_check, check_list)

    def test_truncate_tab(self):

        self.db_connector.truncate_table(['offers_empl_type', 'offers_info','offers_locations', 'offers_per_location_id' , 'offers_skills', 'skills_per_offer'])
        sql_truncate_check = sql.SQL( "SELECT * FROM {table}").format(table=sql.Identifier('offers_info'))
        self.cursor.execute(sql_truncate_check)
        self.conn.commit()
        data_check = self.cursor.fetchall()
        check = []
        self.assertEqual(data_check, check)

if __name__ == '__main__':
    unittest.main()