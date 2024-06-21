# NYC Open Data API
AIR_QUALITY_API_ENDPOINT = "https://data.cityofnewyork.us/resource/c3uy-2p5r.json"
API_ROW_LIMIT = 1000

# NYC Geographies
UHF_42_SHP_URL = "https://raw.githubusercontent.com/nychealth/EHDP-data/production/geography/UHF%2042/UHF_42_DOHMH.shp"
UHF_34_SHP_URL = "https://raw.githubusercontent.com/nychealth/EHDP-data/production/geography/UHF%2034/UHF_34_DOHMH.shp"
CD_SHP_URL = (
    "https://github.com/nychealth/EHDP-data/raw/production/geography/nycd_22c/nycd.shp"
)

# ACS query
# geo
BASE_ZCTA_API_URL = "https://api.census.gov/data/{}/acs/acs5?get=NAME,{}&for=zip%20code%20tabulation%20area:{}"
ZCTA_TO_UHF_URL = (
    "https://github.com/nychealth/EHDP-data/raw/production/geography/zcta_to_uhf.csv"
)

# variable
# add to this dictionary with any variables
# that we might want to pull from the ACS API
ACS_VARS = {"B19013_001E": "median_household_income", "B19083_001E": "gini_index"}
TOT_POPULATION_VAR = "B01003_001E"
