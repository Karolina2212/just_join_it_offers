CREATE TABLE exchange_rates (
	id serial4 NOT NULL,
	currency_descr text NULL,
	currency_code text NULL,
	rate numeric(8, 4) NULL,
	import_date date NULL,
	CONSTRAINT exchange_rates_pkey PRIMARY KEY (id)
);


CREATE TABLE offers_info (
	id serial4 NOT NULL,
	jjit_id text NULL,
	title text NULL,
	company_name text NULL,
	marker_icon text NULL,
	workplace_type text NULL,
	experience_level text NULL,
	import_date date NULL,
	country_code text NULL,
	CONSTRAINT duplicate_not_allowed UNIQUE (jjit_id, import_date),
	CONSTRAINT offers_info_pkey PRIMARY KEY (id)
);


CREATE TABLE offers_locations (
	id serial4 NOT NULL,
	city text NULL,
	CONSTRAINT locations_no_duplicate UNIQUE (city),
	CONSTRAINT offers_locations_pkey PRIMARY KEY (id)
);


CREATE TABLE offers_skills (
	id serial4 NOT NULL,
	skill_name text NULL,
	CONSTRAINT offers_skills_pkey PRIMARY KEY (id),
	CONSTRAINT offers_skills_un UNIQUE (skill_name)
);


CREATE TABLE offers_empl_type (
	id serial4 NOT NULL,
	offer_id int4 NULL,
	empl_type text NULL,
	salary_from numeric(8, 2) NULL,
	salary_to numeric(8, 2) NULL,
	currency text NULL,
	CONSTRAINT offers_empl_type_pkey PRIMARY KEY (id),
	CONSTRAINT offers_empl_type_un UNIQUE (offer_id, empl_type, salary_from, salary_to, currency),
	CONSTRAINT offers_empl_type_fk FOREIGN KEY (offer_id) REFERENCES offers_info(id)
);


CREATE TABLE offers_per_location_id (
	id serial4 NOT NULL,
	offer_id int4 NULL,
	location_id int4 NULL,
	CONSTRAINT offers_per_location_id_no_duplicates UNIQUE (offer_id, location_id),
	CONSTRAINT offers_per_location_id_pkey PRIMARY KEY (id),
	CONSTRAINT offers_per_location_id_fk FOREIGN KEY (location_id) REFERENCES offers_locations(id),
	CONSTRAINT offers_per_location_id_offer FOREIGN KEY (offer_id) REFERENCES offers_info(id)
);


CREATE TABLE skills_per_offer (
	id serial4 NOT NULL,
	offer_id int4 NULL,
	skill_id int4 NULL,
	CONSTRAINT skills_per_offer_pkey PRIMARY KEY (id),
	CONSTRAINT skills_per_offer_un UNIQUE (offer_id, skill_id),
	CONSTRAINT skills_per_offer_fk FOREIGN KEY (offer_id) REFERENCES offers_info(id),
	CONSTRAINT skills_per_offer_fk_1 FOREIGN KEY (skill_id) REFERENCES offers_skills(id)
);