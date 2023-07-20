import requests
from datetime import date
from db_connector import DBConnector


class SyncExchangeRates:

    def call(self):
        api_request = requests.get('http://api.nbp.pl/api/exchangerates/tables/a/?format=json')

        if api_request.status_code != 200:
            raise Exception(f"Request failed with status: {api_request.status_code}")

        rates_data = api_request.json()[0]['rates']

        exchange_rates = DBConnector("127.0.0.1","just_join_it_offers","lisek")
        exchange_rates.truncate_table('exchange_rates')

        for elem in rates_data:

            curr_code = str(elem['code']).lower()
            exchange_rates.insert_one('exchange_rates', {'currency_descr': elem['currency'],'currency_code': curr_code,'rate': elem['mid'],'import_date': date.today()})

        exchange_rates.close_cursor()