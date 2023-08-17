from lib.sync_offers import SyncOffers
from lib.sync_exchange_rates import SyncExchangeRates
from lib.sync_add_country import AddCountry

SyncOffers().call()
SyncExchangeRates().call()

# OPENAI API KEY required
#AddCountry().find_country_for_city()

print("Done")