import openai
import json
from lib.db_connector import DBConnector
from lib.settings import Settings

class AddCountry:

    def find_country_for_city(self):

        database_city_info = DBConnector()
        settings = Settings()
        openai.api_key = settings.openai_api_key

        city_list = []
        for city_row in database_city_info.select_all('offers_locations',{'country':'-'}):
            city_name = city_row[1]
            city_list.append(city_name)

        model_engine = 'gpt-3.5-turbo-16k'
        message = "User provided a list of cities, assign country to each city. Return result in json format {city:country}"
        city_list_string = ", ".join(map(lambda city: f"'{city}'", city_list))

        completion = openai.ChatCompletion.create(model=model_engine, messages=[{'role': 'system', 'content': message},
                                                                                {'role': 'user', 'content': city_list_string}])

        json_returned = completion['choices'][0]['message']['content']

        city_country_dict = json.loads(json_returned)

        print(city_country_dict)

        for city,country in city_country_dict.items():
            city_str = str(city)
            country_str = str(country)
            database_city_info.update_row('offers_locations', {'city':city_str}, {'country':country_str})

        database_city_info.close_cursor()
