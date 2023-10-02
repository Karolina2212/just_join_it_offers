# About Project

The challenge focused on analyzing a polish labour market for the IT segment based on the job offers published on Just Join IT website.
Data presented on visualizations help user to understand the labour demand and requirements in offers published over weeks, as well as in offers that were active in the latest week.

This project is mainly directed at those who would like to enter the IT labour market, as it helps to observe which specializations are the most popular, how many job offers were targeted at juniors, what skills are must have and what are median salary ranges for a specific specialization.

IT job market is analyzed in terms of:
* Median salary ranges (estimated based on offers with salary disclosed)
* Offers distribution between seniority levels
* Demand for employees among specializations
* Preferred employment types
* Top in-demand skills
* Job localization (also in consideration of work type preferences)

** For details on visualization metodology and definitions - please refer to the info page in the Power Bi report (link below)**

# Tools used
* Database: PostgreSQL
* Visualization tool: Power BI
* AI tool: OpenAI (ChatGPT)

# Preparing databases
1. Create two PostgreSQL databases named:
   * just_join_it_offers
   * just_join_it_offers_test
2. Run SQL script saved in the `database_structure.sql` file on both created databases.
3. Open the `.env.example` file and enter your local `HOST`,`USER` and `DB_NAME`
4. Remove `".example"` from the file name
5. For the testing purposes, create another `.env.test` file and paste the line below:
```
DB_NAME = "just_join_it_offers_test"
```
6. To run project use `run_all.py`.


## Additional requirements:

Individual OpenAI api key is required to use `sync_add_country` module that assign country name to cities described in offers. 
Country assignement is used on visualizations but it is not required to use this module to run the project. 
In order to skip country assignement please remove `sync_add_country` import from the `run_all.py` file. 

## Data refresh info
Data for active offers published on the Just Join IT website and currently valid exchange rates are imported to the database using API.
In order to prevent import of duplicates to the database - constraints have been imposed on tables (please refer to the `database_structure` file). In case the app is run multiple times per day - each offer published on the Just Join IT website with its own uniqe ID, is imported only once (per day). 
Data stored in the `exchange_rates` table is overwritten each time the app is run.

# Running app with a command line

### Run import of API data to the database:
```
python run_all.py
```

### Run all tests:
```
python -m unittest discover ./tests   
```

### Run single test (example):
```
python -m unittest tests.test_db_connector
```

# Links

* [Power BI report]()

### Project Data Sources:
* [Just join it](https://justjoin.it)
* [NBP exchange rates tab](https://nbp.pl/statystyka-i-sprawozdawczosc/kursy/tabela-a/)
