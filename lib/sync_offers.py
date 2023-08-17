import requests
from datetime import date
from lib.db_connector import DBConnector


class SyncOffers():
    def __init__(self):
        self.db_connector = DBConnector()

    def call(self):
        data = self.__fetch_justjoin_data()

        duplicates_removed = list(filter(lambda offer: offer['display_offer'], data))

        for offer in duplicates_removed:

            internal_order_id = self.__insert_offers(offer)
            if not internal_order_id:
                continue

            self.__insert_location(offer, internal_order_id)
            self.__insert_skills(offer, internal_order_id)
            self.__insert_employment_info(offer, internal_order_id)

        self.db_connector.close_cursor()

    def __fetch_justjoin_data(self):
        api_request = requests.get('https://justjoin.it/api/offers')

        if api_request.status_code != 200:
            raise Exception(f"Request failed with status: {api_request.status_code}")

        return api_request.json()

    def __insert_offers(self, offer):
        internal_order_id = self.db_connector.insert_one('offers_info',
                                                         {'jjit_id': offer['id'], 'title': offer['title'],
                                                          'company_name': offer['company_name'],
                                                          'marker_icon': offer['marker_icon'],
                                                          'workplace_type': offer['workplace_type'],
                                                          'experience_level': offer['experience_level'],
                                                          'import_date': date.today(),
                                                          'country_code': offer['country_code']
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
        for skill in offer['skills']:
            skill_name = skill['name'].title()
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
        for empl_info in offer['employment_types']:
            if empl_info['salary']:
                self.db_connector.insert_one('offers_empl_type',{
                    'offer_id': internal_order_id,
                    'empl_type': empl_info['type'],
                    'salary_from': empl_info['salary']['from'],
                    'salary_to': empl_info['salary']['to'],
                    'currency': empl_info['salary']['currency']
                })
            else:
                self.db_connector.insert_one('offers_empl_type',{
                    'offer_id': internal_order_id,
                    'empl_type': empl_info['type'],
                    'salary_from': 0,
                    'salary_to': 0,
                    'currency': ""
                })
