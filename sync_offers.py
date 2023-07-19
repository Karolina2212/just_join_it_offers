import requests
import psycopg2


class SyncOffers:

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

        conn_string = "host={0} user={1} dbname={2}".format(self.host, self.user, self.dbname)
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        # MAIN tab
        for offer in duplicates_removed:

            SQL_insert = "INSERT INTO offers_info(jjit_id,title,company_name,marker_icon,workplace_type,experience_level,import_date, country_code) VALUES (%(jjit_id)s, %(title)s, %(company_name)s, %(marker_icon)s, %(workplace_type)s, %(experience_level)s, CURRENT_DATE, %(cc)s) RETURNING id;"
            offer_data = {'jjit_id': offer['id'], 'title': offer['title'], 'company_name': offer['company_name'],
                          'marker_icon': offer['marker_icon'], 'workplace_type': offer['workplace_type'],
                          'experience_level': offer['experience_level'], 'cc': offer['country_code']}
            try:
                cursor.execute(SQL_insert, offer_data)
                internal_order_id = cursor.fetchone()[0]
                conn.commit()
            except psycopg2.errors.UniqueViolation:
                conn.rollback()
                continue

            # LOCATION tab

            for city_name in offer['multilocation']:
                sql_city_id = "SELECT id FROM offers_locations where city = %s;"
                cursor.execute(sql_city_id, (city_name['city'],))
                existing_city = cursor.fetchone()

                if not existing_city:
                    SQL_insert_location = "INSERT INTO offers_locations(city) VALUES (%(city)s) RETURNING ID;"
                    location_data = {'city': city_name['city']}
                    cursor.execute(SQL_insert_location, location_data)
                    existing_city_id = cursor.fetchone()[0]
                    conn.commit()
                else:
                    existing_city_id = existing_city[0]

                # table joining offer_id with location_id (joining offers_locations with offers_info tab on internal ids)
                try:
                    sql_insert_loc_id = "INSERT INTO offers_per_location_id(offer_id,location_id) VALUES (%(offer_id)s, %(loc_id)s);"
                    offer_location_data = {'offer_id': internal_order_id, 'loc_id': existing_city_id}
                    cursor.execute(sql_insert_loc_id, offer_location_data)
                    conn.commit()
                except psycopg2.errors.UniqueViolation:
                    conn.rollback()

            # SKILLS tab

            for skill in offer['skills']:
                sql_skill_id = "SELECT id FROM offers_skills where skill_name = %s;"
                cursor.execute(sql_skill_id, (skill['name'],))
                existing_skill = cursor.fetchone()

                if not existing_skill:
                    SQL_insert_skill = "INSERT INTO offers_skills(skill_name) VALUES (%(name)s) RETURNING ID;"
                    skills_data = {'name': skill['name']}
                    cursor.execute(SQL_insert_skill, skills_data)
                    existing_skill_id = cursor.fetchone()[0]
                    conn.commit()
                else:
                    existing_skill_id = existing_skill[0]

                # table joining offer_id with skill_id
                try:
                    sql_insert_skill_id = "INSERT INTO skills_per_offer(offer_id,skill_id) VALUES (%(offer_id)s, %(skill_id)s);"
                    offer_skill_data = {'offer_id': internal_order_id, 'skill_id': existing_skill_id}
                    cursor.execute(sql_insert_skill_id, offer_skill_data)
                    conn.commit()
                except psycopg2.errors.UniqueViolation:
                    conn.rollback()

            # EMPLOYMENT INFO tab

            for empl_info in offer['employment_types']:

                SQL_empl_info_insert = "INSERT INTO offers_empl_type(offer_id,empl_type,salary_from,salary_to,currency) VALUES (%(offer_id)s, %(type)s, %(sal_fr)s, %(sal_to)s, %(curr)s);"
                if empl_info['salary'] == None:
                    salary_data = {'offer_id': internal_order_id, 'type': empl_info['type'], 'sal_fr': 0, 'sal_to': 0,
                                   'curr': ""}
                else:
                    salary_data = {'offer_id': internal_order_id, 'type': empl_info['type'],
                                   'sal_fr': empl_info['salary']['from'], 'sal_to': empl_info['salary']['to'],
                                   'curr': empl_info['salary']['currency']}

                try:
                    cursor.execute(SQL_empl_info_insert, salary_data)
                    conn.commit()
                except psycopg2.errors.UniqueViolation:
                    conn.rollback()

        cursor.close()
        conn.close()
