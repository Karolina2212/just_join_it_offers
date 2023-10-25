import requests
import json
from datetime import date
from lib.db_connector import DBConnector


class SyncOffers():
    def __init__(self):
        self.db_connector = DBConnector()

        category_file = open('./lib/jjit_marker_icons.json')
        self.marker_icons_data = json.load(category_file)
        category_file.close()

    def call(self):
        data = self.__fetch_justjoin_data()

        for offer in data:

            category_id = self.__get_category_label(self.marker_icons_data, offer['categoryId'])

            internal_order_id = self.__insert_offers(offer, category_id)
            if not internal_order_id:
                continue

            self.__insert_location(offer, internal_order_id)
            self.__insert_skills(offer, internal_order_id)
            self.__insert_employment_info(offer, internal_order_id)

        self.db_connector.close_cursor()

    def __fetch_api_data(self, page_num):

        url = 'https://api.justjoin.it/v2/user-panel/offers?&page=' + str(page_num) + '&sortBy=published&orderBy=DESC&perPage=100&salaryCurrencies=PLN'
        headers = {'Version': '2'}
        api_request = requests.get(url, headers=headers)

        if api_request.status_code != 200:
            raise Exception(f"Request failed with status: {api_request.status_code}")

        return api_request.json()

    def __fetch_justjoin_data(self):

        page_num = 1
        data = []

        while page_num:
            api_page_data = self.__fetch_api_data(page_num)
            page_num = api_page_data['meta']['nextPage']
            print("Page loaded" + str(page_num))
            for offer_on_page in api_page_data['data']:
                data.append(offer_on_page)
            page_num = api_page_data['meta']['nextPage']

        return data

    def __get_category_label(self, marker_icons_data, category_id):

        for category in marker_icons_data['categories']:
            if category['value'] == category_id:
                return category['slug']

    def __insert_offers(self, offer, category_id):
        internal_order_id = self.db_connector.insert_one('offers_info',
                                                             {'jjit_id': offer['slug'],
                                                              'title': offer['title'],
                                                              'company_name': offer['companyName'],
                                                              'marker_icon': category_id,
                                                              'workplace_type': offer['workplaceType'],
                                                              'experience_level': offer['experienceLevel'],
                                                              'import_date': date.today()
                                                              })

        return internal_order_id

    def __insert_location(self, offer, internal_order_id):
        for city_name in offer['multilocation']:

            existing_city = self.db_connector.select_one('offers_locations', {'city': city_name['city']})

            if not existing_city:
                existing_city_id = self.db_connector.insert_one('offers_locations', {'city': city_name['city']})
            else:
                existing_city_id = existing_city[0]

            self.db_connector.insert_one('offers_per_location_id', {
                                            'offer_id': internal_order_id,
                                            'location_id': existing_city_id
                                            })

    def __insert_skills(self, offer, internal_order_id):

        for skill in offer['requiredSkills']:
            skill_name = skill.title()
            existing_skill = self.db_connector.select_one('offers_skills', {'skill_name': skill_name})

            if not existing_skill:
                existing_skill_id = self.db_connector.insert_one('offers_skills', {'skill_name': skill_name})
            else:
                existing_skill_id = existing_skill[0]

            self.db_connector.insert_one('skills_per_offer', {
                                            'offer_id': internal_order_id,
                                            'skill_id': existing_skill_id
                                            })

    def __insert_employment_info(self, offer, internal_order_id):
        for empl_info in offer['employmentTypes']:
            if empl_info['from']:
                self.db_connector.insert_one('offers_empl_type', {
                                                'offer_id': internal_order_id,
                                                'empl_type': empl_info['type'],
                                                'salary_from': empl_info['from'],
                                                'salary_to': empl_info['to'],
                                                'currency': empl_info['currency']
                                                })
            else:
                self.db_connector.insert_one('offers_empl_type', {
                                                'offer_id': internal_order_id,
                                                'empl_type': empl_info['type'],
                                                'salary_from': 0,
                                                'salary_to': 0,
                                                'currency': ""
                                                })
