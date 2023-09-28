import unittest
from unittest.mock import patch
from lib.sync_offers import SyncOffers
from lib.settings import Settings
import psycopg2
from psycopg2 import sql
import json

class TestSyncOffers(unittest.TestCase):



    def setUp(self):
        self.sync_offers = SyncOffers()
        settings = Settings()
        conn_string = "host={0} user={1} dbname={2}".format(settings.host, settings.user, settings.dbname)
        self.conn = psycopg2.connect(conn_string)
        self.cursor = self.conn.cursor()

        self.cursor.execute('truncate table offers_empl_type, offers_info ,offers_locations, offers_per_location_id , offers_skills, skills_per_offer ')
        self.conn.commit()


    def tearDown(self):
        self.cursor.close()
        self.conn.close()

    def test_offers_info(self):
        with patch('lib.sync_offers.requests.get') as mocked_get:
            mocked_get.return_value.status_code = 200
            test_file = open('./tests/jjit_page1_test.json')
            test_file2 = open('./tests/jjit_page2_test.json')
            mocked_get.side_effect = [
                unittest.mock.Mock(**{'json.return_value': json.load(test_file), 'status_code': 200}),
                unittest.mock.Mock(**{'json.return_value': json.load(test_file2), 'status_code': 200})]
            test_file.close()
            test_file2.close()

            self.sync_offers.call()

            sql_select_offers = sql.SQL("SELECT jjit_id, title, company_name, marker_icon, workplace_type, experience_level FROM {table}").format(table=sql.Identifier('offers_info'))
            self.cursor.execute(sql_select_offers)
            data_check = self.cursor.fetchall()
            data_check.sort()

            #check of total num of records
            self.assertEqual(len(data_check),3)

            # check of data accuracy in records
            check_list = [('business-reporting-advisory-group-javascript-developer-wroclaw','JavaScript Developer','BR-AG P.S.A','javascript', 'remote', 'mid'),
                          ('netguru-senior-mendix-low-code-developer-warszawa', 'Senior Mendix Low-code Developer', 'Netguru', 'other', 'partly_remote', 'senior'),
                          ('roxart-agency-dowozacy-taski-prestashop-developer-rzeszow', 'Mid/Sr Prestashop Developer', 'ROXART Agency', 'php', 'remote', 'senior')]
            check_list.sort()
            self.assertEqual(data_check,check_list)

    def test_offers_empl_type(self):

        with patch('lib.sync_offers.requests.get') as mocked_get:
            mocked_get.return_value.status_code = 200
            test_file = open('./tests/jjit_page1_test.json')
            test_file2 = open('./tests/jjit_page2_test.json')
            mocked_get.side_effect = [
                unittest.mock.Mock(**{'json.return_value': json.load(test_file), 'status_code': 200}),
                unittest.mock.Mock(**{'json.return_value': json.load(test_file2), 'status_code': 200})]
            test_file.close()
            test_file2.close()

            self.sync_offers.call()

            sql_select = sql.SQL("SELECT empl_type,salary_from,salary_to,currency FROM {table}").format(table=sql.Identifier('offers_empl_type'))
            self.cursor.execute(sql_select)
            data_check = self.cursor.fetchall()
            data_check.sort()

            #check of total num of records
            self.assertEqual(len(data_check),5)

            #check of employment type list completness
            check_list = [('permanent', 12000.00, 16000.00, 'pln'), ('b2b', 15000.00, 20000.00, 'pln'), ('b2b', 5700.00, 8060.00, 'eur'), ('b2b', 0.0, 0.0, ''), ('permanent', 0.00, 0.00, '')]
            check_list.sort()
            self.assertEqual(data_check,check_list)

            # check of data accuracy for join offers with empl type (example check for 1 offer)
            sql_select = sql.SQL( "select oet.empl_type , oet.salary_from, oet.salary_to, oet.currency, oi.jjit_id, oi.title, oi.company_name, oi.marker_icon, oi.workplace_type, oi.experience_level from offers_info oi join offers_empl_type oet on oi.id = oet.offer_id  WHERE {pkey} = 'business-reporting-advisory-group-javascript-developer-wroclaw'").format(pkey=sql.Identifier('jjit_id'))
            self.cursor.execute(sql_select)
            data_check_select = self.cursor.fetchall()

            self.assertEqual(data_check_select[0][4:], ('business-reporting-advisory-group-javascript-developer-wroclaw', 'JavaScript Developer', 'BR-AG P.S.A', 'javascript', 'remote', 'mid'))

            data_check=list(map(lambda empl_type: empl_type[:4],data_check_select))
            data_check.sort()

            self.assertEqual(data_check,[('b2b', 15000.00, 20000.00, 'pln'), ('permanent', 12000.00, 16000.00, 'pln')])

    def test_offers_skills(self):

        with patch('lib.sync_offers.requests.get') as mocked_get:
            mocked_get.return_value.status_code = 200
            test_file = open('./tests/jjit_page1_test.json')
            test_file2 = open('./tests/jjit_page2_test.json')
            mocked_get.side_effect = [
                unittest.mock.Mock(**{'json.return_value': json.load(test_file), 'status_code': 200}),
                unittest.mock.Mock(**{'json.return_value': json.load(test_file2), 'status_code': 200})]
            test_file.close()
            test_file2.close()

            self.sync_offers.call()

            sql_select = sql.SQL("SELECT skill_name FROM {table}").format(table=sql.Identifier('offers_skills'))
            self.cursor.execute(sql_select)
            data_check_select = self.cursor.fetchall()
            data_check = list(map(lambda skill: skill[0],data_check_select))
            data_check.sort()

            #check of total num of records
            self.assertEqual(len(data_check),15)

            #check of skills list completness
            check_list = ['Cypress', 'Node.Js', 'Agile', 'English', 'Angular', 'Javascript', 'Css3', 'Html', 'Java', 'Mendix', 'Css', 'Mysql','Php', 'Prestashop', 'Wordpress']
            check_list.sort()
            self.assertEqual(data_check, check_list)

            #check of data accuracy for join offers with skills (example check for 1 offer)
            sql_select = sql.SQL("select os.skill_name , oi.jjit_id, oi.title, oi.company_name, oi.marker_icon, oi.workplace_type, oi.experience_level from offers_info oi join skills_per_offer spo on oi.id = spo.offer_id join offers_skills os on spo.skill_id = os.id WHERE {pkey} = 'netguru-senior-mendix-low-code-developer-warszawa'").format(pkey=sql.Identifier('jjit_id'))
            self.cursor.execute(sql_select)
            data_check = self.cursor.fetchall()

            self.assertEqual(data_check[0][1:],('netguru-senior-mendix-low-code-developer-warszawa', 'Senior Mendix Low-code Developer', 'Netguru', 'other', 'partly_remote', 'senior'))
            skill_check = list(map(lambda record: record[0],data_check))
            skill_check.sort()

            self.assertEqual(skill_check,['Css','Java', 'Mendix'])

    def test_offers_locations(self):

        with patch('lib.sync_offers.requests.get') as mocked_get:
            test_file = open('./tests/jjit_page1_test.json')
            test_file2 = open('./tests/jjit_page2_test.json')
            mocked_get.side_effect = [unittest.mock.Mock(**{'json.return_value': json.load(test_file), 'status_code': 200}),
                                      unittest.mock.Mock(**{'json.return_value': json.load(test_file2), 'status_code': 200})]
            test_file.close()
            test_file2.close()

            self.sync_offers.call()

            sql_select = sql.SQL("SELECT city FROM {table}").format(table=sql.Identifier('offers_locations'))
            self.cursor.execute(sql_select)
            data_check_select = self.cursor.fetchall()
            data_check = list(map(lambda city: city[0], data_check_select))
            data_check.sort()

            #check of total num of records
            self.assertEqual(len(data_check),19)

            # check of cities name list completness
            check_list = ['Białystok', 'Bydgoszcz', 'Gdańsk', 'Katowice', 'Kielce', 'Kraków', 'Lublin', 'Olsztyn', 'Opole', 'Poznań', 'Rzeszów', 'Szczecin', 'Warszawa', 'Wrocław', 'Zielona Góra', 'Łódź','Toruń','Bielsko-Biała','Częstochowa']
            check_list.sort()
            self.assertEqual(data_check,check_list)

            #check of data accuracy for join offers with cities (example check for 1 offer)

            sql_select = sql.SQL("select ol.city, oi.jjit_id, oi.title, oi.company_name, oi.marker_icon, oi.workplace_type, oi.experience_level from offers_info oi join offers_per_location_id opli on oi.id = opli.offer_id join offers_locations ol on opli.location_id = ol.id WHERE {pkey} = 'roxart-agency-dowozacy-taski-prestashop-developer-rzeszow'").format(pkey=sql.Identifier('jjit_id'))
            self.cursor.execute(sql_select)
            data_check = self.cursor.fetchall()

            self.assertEqual(data_check[0][1:],('roxart-agency-dowozacy-taski-prestashop-developer-rzeszow', 'Mid/Sr Prestashop Developer', 'ROXART Agency', 'php','remote', 'senior'))

            city_check = list(map(lambda record: record[0],data_check))
            city_check.sort()

            self.assertEqual(city_check,['Bydgoszcz', 'Gdańsk', 'Katowice', 'Kraków', 'Lublin', 'Rzeszów', 'Szczecin', 'Warszawa', 'Wrocław', 'Łódź'])

if __name__ == '__main__':
    unittest.main()