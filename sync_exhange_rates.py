import requests
import psycopg2

api_request = requests.get('http://api.nbp.pl/api/exchangerates/tables/a/?format=json')

if api_request.status_code != 200:
    raise Exception(f"Request failed with status: {api_request.status_code}")

rates_data = api_request.json()[0]['rates']

host = "127.0.0.1"
dbname = "just_join_it_offers"
user = "lisek"

conn_string = "host={0} user={1} dbname={2}".format(host, user, dbname)
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

cursor.execute('truncate table exchange_rates')
conn.commit()

for elem in rates_data:
    SQL_insert = "INSERT INTO exchange_rates(currency_descr,currency_code,rate,import_date) VALUES (%(cur_desc)s, %(code)s, %(rate)s, CURRENT_DATE);"
    curr_code = str(elem['code']).lower()
    currency_data = {'cur_desc':elem['currency'], 'code': curr_code, 'rate':elem['mid']}
    cursor.execute(SQL_insert, currency_data)
    conn.commit()

cursor.close()
conn.close()