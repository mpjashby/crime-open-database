# Data from most cities are already geocoded. Co-ordinates for the remaining
# cities are added here, usually based on data from the US Census Bureau
# geocoder or http://geocod.io/

# Fort Worth ------------------------------------------------------------------

# A minority of offences in Fort Worth are not geocoded. It appears that some
# locations are sometimes geocoded but sometimes not. In fact, most locations
# that are not geocoded are geocoded elsewhere in the dataset. However, looking
# at a list of locations shows that some (e.g. '100 BLOCK CALHOUN ST') have
# multiple locations that are too far from one another to be the product of
# geocoding issues. E.g. in the case of Calhoun St, the co-ordinates suggest
# some offences actually occurred at South Calhoun St. This means we can't apply
# geocodes from a location to offences at the 'same' address but which have no
# geocodes. It is therefore necessary to geocode all the missing locations.

# load data
data_fw <- read_rds(here::here("temp_data/raw_fort_worth_data.Rds"))

# attempt to make sense of the address abbreviations used in FW
data_fw$geocode_address <- data_fw$block_address %>% 
  str_to_upper() %>% 
  str_replace_all('\\s+', ' ') %>%
  str_replace_all('\\bBLOCK [DGHKPRVY]\\b', '') %>%
  str_replace_all('\\b(BLOCK|RA|RAMP|NB|EB|SB|WB)\\b', '') %>%
  str_replace_all('\\bFWY\\b', 'FREEWAY') %>%
  str_replace_all('\\bIH\\b', 'INTERSTATE') %>%
  str_replace_all('\\bSR\\b', 'SERVICE ROAD') %>%
  str_replace_all("\\bWA\\b", "WAY") %>% 
  str_replace_all('\\bBELL SP\\b', 'BELL HELICOPTER BL') %>%
  str_replace_all('\\bLOOP 830\\b', 'LOOP INTERSTATE 820') %>%
  str_replace_all(
    "\\b(NORTH|EAST|SOUTH|WEST|NORTHEAST|NORTHWEST|SOUTHEAST|SOUTHWEST) LOOP\\b", 
    "\\1 LOOP INTERSTATE 820"
  ) %>%
  str_replace_all('\\d+$', '') %>%
  str_replace_all('\\s+', ' ') %>%
  str_trim()

# geocode addresses with the US Census geocoder
fw_geocoded_census <- data_fw %>% 
  filter(( is.na(latitude) | is.na(longitude) ) & geocode_address != "") %>% 
  select(geocode_address, city, state) %>% 
  unique.data.frame() %>% 
  mutate(zip_code = "", batch = ceiling(row_number() / 1000)) %>% 
  nest(data = -batch) %>% 
  mutate(
    results = map(data, cxy_geocode, street = "geocode_address", 
                  city = "city", state = "state", zip = "zip_code", 
                  output = "simple", benchmark = 9)
  ) %>% 
  select(results) %>% 
  unnest(results)

# geocode remaining addresses with Geocodio
fw_geocoded_geocodio <- fw_geocoded_census %>% 
  filter(is.na(cxy_lon)) %>% 
  mutate(
    geocode_address = 
      str_glue("{geocode_address}, {city}, {state}, {zip_code}")
  ) %>% 
  pull(geocode_address) %>% 
  rgeocodio::gio_batch_geocode() %>% 
  select(query, response_results) %>% 
  mutate(response_results = map(response_results, slice, 1)) %>% 
  unnest(response_results) %>% 
  filter(!accuracy_type %in% c("place", "state"))

# link data
