This project use PostgreSQL databases and `psycopg2` adapter

# Preparing databases
1. Create two databases named:
   * just_join_it_offers
   * just_join_it_offers_test
2. Run sql script saved in the `database_structure.sql` file on both created databases.
3. Open the `.env.example` file and enter your local `HOST`,`USER` and `DB_NAME` (if different than 'just_join_it_offers') 
4. Remove `.example` from the file name
5. For the testing purposes, create `.env.test` file and paste the line below:
```
DB_NAME = "just_join_it_offers_test"
```
6. To run project use `run_all.py` file.



# Running app with command line

### Running sync files from lib in the terminal:
python run_all.py

### Running all test:
python -m unittest discover ./tests   

### Running one single test (example):
python -m unittest ./tests/test_db_connector.py


