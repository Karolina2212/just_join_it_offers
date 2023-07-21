import unittest
from unittest.mock import patch
from dotenv import load_dotenv

load_dotenv(".env")
load_dotenv(".env.test", override=True)

from sync_exchange_rates import SyncExchangeRates
import json
import psycopg2
from psycopg2 import sql
from decimal import Decimal


class TestSyncExchangeRates(unittest.TestCase):

    def setUp(self):
        self.sync_exchange_rates = SyncExchangeRates()
        conn_string = "host={0} user={1} dbname={2}".format(self.sync_exchange_rates.host, self.sync_exchange_rates.user, self.sync_exchange_rates.dbname)
        self.conn = psycopg2.connect(conn_string)
        self.cursor = self.conn.cursor()

    def tearDown(self):
        self.cursor.close()
        self.conn.close()

    def test_exchange_rates(self):

        with patch('sync_exchange_rates.requests.get') as mocked_get:
            mocked_get.return_value.status_code = 200
            test_file = open('exch_rates_test.json')
            mocked_get.return_value.json = unittest.mock.Mock(return_value=json.load(test_file))
            test_file.close()

            self.sync_exchange_rates.call()

            sql_exchange_rates = sql.SQL("SELECT currency_descr, currency_code, rate FROM {table}").format(table=sql.Identifier('exchange_rates'))
            self.cursor.execute(sql_exchange_rates)
            data_check = self.cursor.fetchall()
            data_check.sort()

            # check of total num of records
            self.assertEqual(len(data_check), 33)

            # check of data accuracy in records
            check_list = [('SDR (MFW)', 'xdr', Decimal('5.3554')), ('bat (Tajlandia)', 'thb', Decimal('0.1167')), ('dolar Hongkongu', 'hkd', Decimal('0.5083')), ('dolar amerykański', 'usd', Decimal('3.9710')), ('dolar australijski', 'aud', Decimal('2.7128')), ('dolar kanadyjski', 'cad', Decimal('3.0260')), ('dolar nowozelandzki', 'nzd', Decimal('2.4959')), ('dolar singapurski', 'sgd', Decimal('3.0014')), ('euro', 'eur', Decimal('4.4511')), ('forint (Węgry)', 'huf', Decimal('0.0117')), ('frank szwajcarski', 'chf', Decimal('4.6276')), ('funt szterling', 'gbp', Decimal('5.1321')), ('hrywna (Ukraina)', 'uah', Decimal('0.1081')), ('jen (Japonia)', 'jpy', Decimal('0.0285')), ('korona czeska', 'czk', Decimal('0.1860')), ('korona duńska', 'dkk', Decimal('0.5974')), ('korona islandzka', 'isk', Decimal('0.0304')), ('korona norweska', 'nok', Decimal('0.3985')), ('korona szwedzka', 'sek', Decimal('0.3876')), ('lej rumuński', 'ron', Decimal('0.9008')), ('lew (Bułgaria)', 'bgn', Decimal('2.2758')), ('lira turecka', 'try', Decimal('0.1475')), ('nowy izraelski szekel', 'ils', Decimal('1.1078')), ('peso chilijskie', 'clp', Decimal('0.0049')), ('peso filipińskie', 'php', Decimal('0.0728')), ('peso meksykańskie', 'mxn', Decimal('0.2373')), ('rand (Republika Południowej Afryki)', 'zar', Decimal('0.2234')), ('real (Brazylia)', 'brl', Decimal('0.8288')), ('ringgit (Malezja)', 'myr', Decimal('0.8727')), ('rupia indonezyjska', 'idr', Decimal('0.0003')), ('rupia indyjska', 'inr', Decimal('0.0484')), ('won południowokoreański', 'krw', Decimal('0.0031')), ('yuan renminbi (Chiny)', 'cny', Decimal('0.5529'))]

            self.assertEqual(data_check, check_list)

if __name__ == '__main__':
    unittest.main()