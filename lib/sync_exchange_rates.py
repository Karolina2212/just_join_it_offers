import requests
from datetime import date
from lib.db_connector import DBConnector


class SyncExchangeRates:

    def __init__(self):

        self.exchange_rates = DBConnector()
        self.exchange_rates.truncate_table(['exchange_rates'])

    def call(self):

        rates_data = self.__fetch_nbp_rates()
        self.__insert_exchange_rates(rates_data)

        self.exchange_rates.close_cursor()

    def __fetch_nbp_rates(self):

        api_request = requests.get('http://api.nbp.pl/api/exchangerates/tables/a/?format=json')

        if api_request.status_code != 200:
            raise Exception(f"Request failed with status: {api_request.status_code}")

        return api_request.json()[0]['rates']

    def __insert_exchange_rates(self, rates_data):

        for elem in rates_data:
            curr_code = str(elem['code']).lower()
            self.exchange_rates.insert_one('exchange_rates', {
                                            'currency_descr': elem['currency'],
                                            'currency_code': curr_code,
                                            'rate': elem['mid'],
                                            'import_date': date.today()
                                            })
