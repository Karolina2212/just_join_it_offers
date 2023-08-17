import unittest
from unittest.mock import patch
from lib.sync_add_country import AddCountry
from lib.settings import Settings
import psycopg2
import json

class TestSyncAddCountry(unittest.TestCase):

    def setUp(self):
        self.add_country = AddCountry()
        settings = Settings()
        conn_string = "host={0} user={1} dbname={2}".format(settings.host, settings.user, settings.dbname)
        self.conn = psycopg2.connect(conn_string)
        self.cursor = self.conn.cursor()

        self.cursor.execute('TRUNCATE TABLE offers_empl_type, offers_info ,offers_locations, offers_per_location_id , offers_skills, skills_per_offer ')

        sql_insert_test = 'INSERT INTO offers_locations(city) VALUES (%s)'
        self.test1_values = ('Warszawa',)
        self.test2_values = ('Belgrad',)
        self.test3_values = ('Kyiv',)
        self.cursor.execute(sql_insert_test, self.test1_values)
        self.cursor.execute(sql_insert_test, self.test2_values)
        self.cursor.execute(sql_insert_test, self.test3_values)

        self.conn.commit()

    def tearDown(self):
        self.cursor.close()
        self.conn.close()

    def test_country_added(self):

        with patch('lib.sync_add_country.openai.ChatCompletion.create') as mocked_create:
            test_file = open('./tests/open_ai_response_test.json')
            mocked_create.return_value = json.load(test_file)
            test_file.close()

        self.add_country.find_country_for_city()

        sql_select = "SELECT city, country FROM offers_locations"
        self.cursor.execute(sql_select)
        data_check = self.cursor.fetchall()

        check_list = [('Warszawa','Poland'),('Belgrad','Serbia'),('Kyiv','Ukraine')]

        self.assertEqual(data_check, check_list)

if __name__ == '__main__':
    unittest.main()

