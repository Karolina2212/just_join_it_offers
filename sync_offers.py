import requests
from datetime import date
from db_connector import DBConnector

class SyncOffers():
    host = "127.0.0.1"
    dbname = "just_join_it_offers"
    user = "lisek"

    def call(self):

        api_request = requests.get('https://justjoin.it/api/offers')

        if api_request.status_code != 200:
            raise Exception(f"Request failed with status: {api_request.status_code}")

        data = api_request.json()

        # some offers are duplicated for each city it applies to (main offer: display_offer == True):
        duplicates_removed = list(filter(lambda offer: offer['display_offer'], data))

        '''
        How filter function works:
        
        def filter(func, dataset):
            new_array = []
            for elem in dataset:
                if func(elem):
                    new_array.append(elem)
            return new_array
        '''

        offers_sync = DBConnector(self.host, self.dbname, self.user)

        # MAIN tab
        for offer in duplicates_removed:

            internal_order_id = offers_sync.insert_one('offers_info', {'jjit_id': offer['id'], 'title': offer['title'], 'company_name': offer['company_name'],
                          'marker_icon': offer['marker_icon'], 'workplace_type': offer['workplace_type'],
                          'experience_level': offer['experience_level'], 'import_date': date.today(), 'country_code': offer['country_code']})

            #in case offer has already been imported to the database, skip to the next offer
            if not internal_order_id:
                continue

            # LOCATION tab

            for city_name in offer['multilocation']:

                existing_city = offers_sync.select_one('offers_locations',{'city':city_name['city']})

                if not existing_city:
                    existing_city_id = offers_sync.insert_one('offers_locations',{'city':city_name['city']})
                else:
                    existing_city_id = existing_city[0]

                # table joining offer_id with location_id (joining offers_locations with offers_info tab on internal ids)

                offers_sync.insert_one('offers_per_location_id',{'offer_id':internal_order_id,'location_id':existing_city_id})


            # SKILLS tab

            for skill in offer['skills']:

                existing_skill = offers_sync.select_one('offers_skills',{'skill_name':skill['name']})

                if not existing_skill:

                    existing_skill_id = offers_sync.insert_one('offers_skills',{'skill_name':skill['name']})

                else:
                    existing_skill_id = existing_skill[0]

                # table joining offer_id with skill_id

                offers_sync.insert_one('skills_per_offer',{'offer_id':internal_order_id, 'skill_id':existing_skill_id})

            # EMPLOYMENT INFO tab

            for empl_info in offer['employment_types']:

                if empl_info['salary']:

                    offers_sync.insert_one('offers_empl_type', {'offer_id': internal_order_id, 'empl_type': empl_info['type'], 'salary_from': empl_info['salary']['from'], 'salary_to': empl_info['salary']['to'], 'currency': empl_info['salary']['currency']})

                else:
                    offers_sync.insert_one('offers_empl_type',{'offer_id': internal_order_id, 'empl_type': empl_info['type'],'salary_from': 0, 'salary_to': 0, 'currency': ""})

        offers_sync.close_cursor()

