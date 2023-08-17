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
            test_file = open('./tests/jjit_json_test.json')
            mocked_get.return_value.json = unittest.mock.Mock(return_value=json.load(test_file))
            test_file.close()

            self.sync_offers.call()

            sql_select_offers = sql.SQL("SELECT jjit_id, title, company_name, marker_icon, workplace_type, experience_level, country_code FROM {table}").format(table=sql.Identifier('offers_info'))
            self.cursor.execute(sql_select_offers)
            data_check = self.cursor.fetchall()
            data_check.sort()

            #check of total num of records
            self.assertEqual(len(data_check),3)

            # check of data accuracy in records
            check_list = [('gowork-pl-php-developer-ce53115e-66a0-4940-bb1d-8d3fa1be2be4', 'PHP Developer', 'GoWork.pl', 'php', 'remote', 'mid', 'PL'), ('itcard-s-a-it-operations-specialist-warszawa', 'IT Operations Specialist', 'ITCARD S.A.', 'admin', 'partly_remote', 'mid', 'PL'), ('nordhealth-senior-frontend-developer-helsinki', 'Senior Frontend Developer', 'Nordhealth', 'php', 'remote', 'senior', 'FI')]
            self.assertEqual(data_check,check_list)

    def test_offers_empl_type(self):

        with patch('lib.sync_offers.requests.get') as mocked_get:
            mocked_get.return_value.status_code = 200
            test_file = open('./tests/jjit_json_test.json')
            mocked_get.return_value.json = unittest.mock.Mock(return_value=json.load(test_file))
            test_file.close()

            self.sync_offers.call()

            sql_select = sql.SQL("SELECT empl_type,salary_from,salary_to,currency FROM {table}").format(table=sql.Identifier('offers_empl_type'))
            self.cursor.execute(sql_select)
            data_check = self.cursor.fetchall()
            data_check.sort()

            #check of total num of records
            self.assertEqual(len(data_check),5)

            #check of employment type list completness
            check_list = [('b2b', 0.0, 0.0, ''), ('b2b', 3500.00, 5500.00, 'eur'), ('b2b', 12000.00, 18000.00, 'pln'), ('permanent', 0.00, 0.00, ''), ('permanent', 3500.00, 5500.00, 'eur')]
            self.assertEqual(data_check,check_list)

            # check of data accuracy for join offers with empl type (example check for 1 offer)
            sql_select = sql.SQL( "select oet.empl_type , oet.salary_from, oet.salary_to, oet.currency, oi.jjit_id, oi.title, oi.company_name, oi.marker_icon, oi.workplace_type, oi.experience_level, oi.country_code from offers_info oi join offers_empl_type oet on oi.id = oet.offer_id  WHERE {pkey} = 'nordhealth-senior-frontend-developer-helsinki'").format(pkey=sql.Identifier('jjit_id'))
            self.cursor.execute(sql_select)
            data_check_select = self.cursor.fetchall()


            self.assertEqual(data_check_select[0][4:], ('nordhealth-senior-frontend-developer-helsinki', 'Senior Frontend Developer', 'Nordhealth', 'php','remote', 'senior', 'FI'))

            data_check=list(map(lambda empl_type: empl_type[:4],data_check_select))
            data_check.sort()

            self.assertEqual(data_check,[('b2b', 3500.00, 5500.00, 'eur'), ('permanent', 3500.00, 5500.00, 'eur')])

    def test_offers_skills(self):

        with patch('lib.sync_offers.requests.get') as mocked_get:
            mocked_get.return_value.status_code = 200
            test_file = open('./tests/jjit_json_test.json')
            mocked_get.return_value.json = unittest.mock.Mock(return_value=json.load(test_file))
            test_file.close()

            self.sync_offers.call()

            sql_select = sql.SQL("SELECT skill_name FROM {table}").format(table=sql.Identifier('offers_skills'))
            self.cursor.execute(sql_select)
            data_check_select = self.cursor.fetchall()
            data_check = list(map(lambda skill: skill[0],data_check_select))
            data_check.sort()

            #check of total num of records
            self.assertEqual(len(data_check),9)

            #check of skills list completness
            self.assertEqual(data_check,['Mssql', 'Nuxt', 'Oop', 'Oracle', 'Php 7.X', 'Phpunit', 'Sql', 'Typescript', 'Vue'])

            #check of data accuracy for join offers with skills (example check for 1 offer)
            sql_select = sql.SQL("select os.skill_name , oi.jjit_id, oi.title, oi.company_name, oi.marker_icon, oi.workplace_type, oi.experience_level, oi.country_code  from offers_info oi join skills_per_offer spo on oi.id = spo.offer_id join offers_skills os on spo.skill_id = os.id WHERE {pkey} = 'gowork-pl-php-developer-ce53115e-66a0-4940-bb1d-8d3fa1be2be4'").format(pkey=sql.Identifier('jjit_id'))
            self.cursor.execute(sql_select)
            data_check = self.cursor.fetchall()

            self.assertEqual(data_check[0][1:],('gowork-pl-php-developer-ce53115e-66a0-4940-bb1d-8d3fa1be2be4', 'PHP Developer', 'GoWork.pl', 'php','remote', 'mid', 'PL'))
            skill_check = list(map(lambda record: record[0],data_check))
            skill_check.sort()

            self.assertEqual(skill_check,['Oop', 'Php 7.X', 'Phpunit'])

    def test_offers_locations(self):

        with patch('lib.sync_offers.requests.get') as mocked_get:
            mocked_get.return_value.status_code = 200
            test_file = open('./tests/jjit_json_test.json')
            mocked_get.return_value.json = unittest.mock.Mock(return_value=json.load(test_file))
            test_file.close()

            self.sync_offers.call()

            sql_select = sql.SQL("SELECT city FROM {table}").format(table=sql.Identifier('offers_locations'))
            self.cursor.execute(sql_select)
            data_check_select = self.cursor.fetchall()
            data_check = list(map(lambda city: city[0], data_check_select))
            data_check.sort()

            #check of total num of records
            self.assertEqual(len(data_check),23)

            # check of cities name list completness
            check_list = ['Berlin', 'Białystok', 'Budapeszt', 'Bydgoszcz', 'Gdańsk', 'Gdynia', 'Gorzów Wielkopolski', 'Helsinki', 'Katowice', 'Kielce', 'Kraków', 'Lublin', 'Madryt', 'Olsztyn', 'Opole', 'Poznań', 'Praga', 'Rzeszów', 'Szczecin', 'Warszawa', 'Wrocław', 'Zielona Góra', 'Łódź']
            self.assertEqual(data_check,check_list)

            #check of data accuracy for join offers with cities (example check for 1 offer)

            sql_select = sql.SQL("select ol.city, oi.jjit_id, oi.title, oi.company_name, oi.marker_icon, oi.workplace_type, oi.experience_level, oi.country_code  from offers_info oi join offers_per_location_id opli on oi.id = opli.offer_id join offers_locations ol on opli.location_id = ol.id WHERE {pkey} = 'gowork-pl-php-developer-ce53115e-66a0-4940-bb1d-8d3fa1be2be4'").format(pkey=sql.Identifier('jjit_id'))
            self.cursor.execute(sql_select)
            data_check = self.cursor.fetchall()

            self.assertEqual(data_check[0][1:],('gowork-pl-php-developer-ce53115e-66a0-4940-bb1d-8d3fa1be2be4', 'PHP Developer', 'GoWork.pl', 'php','remote', 'mid', 'PL'))

            city_check = list(map(lambda record: record[0],data_check))
            city_check.sort()

            self.assertEqual(city_check,['Białystok', 'Bydgoszcz', 'Gdańsk', 'Gdynia', 'Gorzów Wielkopolski', 'Katowice', 'Kielce', 'Kraków', 'Lublin', 'Olsztyn', 'Opole', 'Poznań', 'Rzeszów', 'Szczecin', 'Warszawa', 'Wrocław', 'Zielona Góra', 'Łódź'])

if __name__ == '__main__':
    unittest.main()