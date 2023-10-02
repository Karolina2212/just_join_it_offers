import openai
import json
from lib.db_connector import DBConnector
from lib.settings import Settings


class AddCountry:

    def __init__(self):

        self.database_city_info = DBConnector()
        self.settings = Settings()

    def call(self):

        city_list = self.__get_cities_list()
        city_country_dict = self.__fetch_openai_return(city_list)

        self.__load_database_with_countries(city_country_dict)

        self.database_city_info.close_cursor()

    def __get_cities_list(self):

        city_list = []
        for city_row in self.database_city_info.select_all('offers_locations', {'country': '-'}):
            city_name = city_row[1]
            city_list.append(city_name)
        return city_list

    def __fetch_openai_return(self, city_list):

        openai.api_key = self.settings.openai_api_key
        model_engine = 'gpt-3.5-turbo-16k'
        message = "User provided a list of cities, assign country to each city. Return result in json format {city:country}"
        city_list_string = ", ".join(map(lambda city: f"'{city}'", city_list))

        completion = openai.ChatCompletion.create(model=model_engine,
                                                  messages=[{'role': 'system', 'content': message},
                                                            {'role': 'user', 'content': city_list_string}]
                                                  )

        json_returned = completion['choices'][0]['message']['content']

        return json.loads(json_returned)

    def __load_database_with_countries(self, city_country_dict):

        for city, country in city_country_dict.items():
            city_str = str(city)
            country_str = str(country)
            self.database_city_info.update_row('offers_locations', {'city': city_str}, {'country': country_str})
