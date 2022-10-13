# Data from most cities are already geocoded. Co-ordinates for the remaining
# cities are added here, usually based on data from the US Census Bureau
# geocoder or http://geocod.io/



# Create a function to geocode in a way that doesn't stop if there is a cURL 
# error (which there often is)
geocode_safely <- safely(.f = geocode)



# Fort Worth -------------------------------------------------------------------

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
# data_fw <- read_rds(here::here("temp_data/raw_fort_worth_data.Rds"))

# attempt to make sense of the address abbreviations used in FW
# data_fw$block_address_expanded <- data_fw$block_address %>% 
#   str_to_upper() %>% 
#   str_replace_all('\\s+', ' ') %>%
#   str_replace_all('\\bBLOCK [DGHKPRVY]\\b', '') %>%
#   str_replace_all('\\b(BLOCK|RA|RAMP|NB|EB|SB|WB)\\b', '') %>%
#   str_replace_all('\\bFWY\\b', 'FREEWAY') %>%
#   str_replace_all('\\bIH\\b', 'INTERSTATE') %>%
#   str_replace_all('\\bSR\\b', 'SERVICE ROAD') %>%
#   str_replace_all("\\bWA\\b", "WAY") %>% 
#   str_replace_all('\\bBELL SP\\b', 'BELL HELICOPTER BL') %>%
#   str_replace_all('\\bLOOP 830\\b', 'LOOP INTERSTATE 820') %>%
#   str_replace_all(
#     "\\b(NORTH|EAST|SOUTH|WEST|NORTHEAST|NORTHWEST|SOUTHEAST|SOUTHWEST) LOOP\\b", 
#     "\\1 LOOP INTERSTATE 820"
#   ) %>%
#   str_replace_all('\\d+$', '') %>%
#   str_replace_all('\\s+', ' ') %>%
#   str_trim()

# attempt to geocode data using the US Census geocoding API
# data_fw <- mutate(
#   data_fw,
#   census_coords = census_geocode(block_address_expanded, "Fort Worth", "TX")
# )



# Kansas City ------------------------------------------------------------------

# All KC offenses are geocoded before 2016, but for some reason almost a third
# of the offenses in the 2017 and later files have an address but no
# co-ordinates. For 2021, the `location` field contains co-ordinates as WKT.

# read data
kc_data <- read_rds(here::here("temp_data/raw_kansas_city_data.Rds"))

# Extract points in WKT format
kc_data <- kc_data %>% 
  filter(date_year == 2021, !is.na(location)) %>% 
  select(uid, location) %>% 
  st_as_sf(wkt = "location") %>% 
  { bind_cols(st_drop_geometry(.), as_tibble(st_coordinates(.))) } %>% 
  right_join(kc_data, by = "uid") %>% 
  mutate(
    longitude = if_else(is.na(longitude) & !is.na(X), X, as.numeric(longitude)),
    latitude = if_else(is.na(latitude) & !is.na(Y), Y, as.numeric(latitude))
  ) %>% 
  select(-X, -Y)

# Harmonise the spelling of addresses as far as possible and store that in a new
# variable that we can subsequently use for a join
kc_data$address_string <- kc_data$address %>% 
  str_replace_all("\\bNA, ", ", ") %>%
  str_squish() %>% 
  str_replace("^.*\\bUNKNOWN.*$", "") %>% 
  str_replace_all("\\bAV\\b", "AVENUE") %>% 
  str_replace_all("\\bBL\\b", "BOULEVARD") %>% 
  str_replace_all("\\bCT\\b", "COURT") %>% 
  str_replace_all("\\bHW\\b", "HIGHWAY") %>% 
  str_replace_all("\\bHWY\\b", "HIGHWAY") %>% 
  str_replace_all("\\bPW\\b", "PARKWAY") %>% 
  str_replace_all("\\bRD\\b", "ROAD") %>% 
  str_replace_all("\\bST\\b", "STREET") %>% 
  str_replace_all("\\bTE\\b", "TERRACE") %>% 
  str_replace_all("\\bTR\\b", "TRAFFICWAY") %>% 
  str_replace_all("\\bWA\\b", "WAY") %>% 
  str_replace_all("\\bWEST FRONT ROAD\\b", "") %>% 
  str_replace_all("\\bE 40 HIGHWAY\\b", "E US 40 HIGHWAY") %>% 
  str_replace_all("\\bUS (\\d+) HIGHWAY\\b", "US HIGHWAY \\1") %>% 
  str_replace_all("\\bINDEPENDENCE BOULEVARD\\b", "INDEPENDENCE AVENUE") %>% 
  str_replace_all("\\bM 152\\b", "MO 152") %>% 
  str_replace_all("\\bNICHOLS PK\\b", "NICHOLS PARKWAY") %>% 
  str_replace_all("\\b\\d+ WORLDS OF FUN\\b", "100 WORLDS OF FUN") %>% 
  str_replace_all("\\b/\\b", " AND ") %>% 
  str_replace_all("\\b / \\b", " AND ") %>% 
  str_replace_all("\\b &\\b", " AND ") %>% 
  str_replace_all("(\\d+(ST|ND|RD|TH))$", "\\1 STREET") %>% 
  str_replace_all("(\\d+(ST|ND|RD|TH)) AND\\b", "\\1 STREET AND") %>% 
  str_replace("^0+\\b", "100") %>%  # replace street number 0 with 100
  str_squish()

# Create a tibble of unique addresses with missing co-ordinates and geocode them
# using the Mapbox geocoder in the first instance because it has the highest 
# rate limit and also seems the best of the free geocoders at geocoding
# intersections
kc_geocoded_mapbox <- kc_data %>% 
  filter(
    # Latitude or longitude are missing
    (is.na(longitude) | is.na(latitude)),
    # Address is not missing or blank
    !is.na(address_string), address_string != ""
  ) %>% 
  mutate(
    address_string = str_squish(str_glue("{address_string}, {city}, MO"))
  ) %>% 
  select(address_string) %>% 
  # Remove duplicate rows
  distinct() %>% 
  # Split the data into batches of 500 rows to reduce risk of time outs
  mutate(batch = ceiling(row_number() / 500)) %>% 
  nest(data = -batch) %>% 
  deframe() %>% 
  # Slow down geocoding slightly by specifying `min_time` to be slightly higher 
  # than the default of 0.1, to avoid being temporarily blocked
  map_dfr(geocode_safely, address = "address_string", method = "mapbox", min_time = 0.12)

# Geocode remaining addresses with missing co-ordinates using the US Census
# geocoder, which has no rate limit but is quite slow nonetheless
kc_geocoded_census <- kc_geocoded_mapbox$result %>% 
  filter(is.na(lat)) %>% 
  select(address_string) %>% 
  mutate(batch = ceiling(row_number() / 500)) %>% 
  nest(data = -batch) %>% 
  deframe() %>% 
  map_dfr(geocode_safely, address = "address_string", method = "census")

# Geocode remaining addresses with missing co-ordinates using the Nominatim 
# geocoder, which has a limit of 1 address per second
kc_geocoded_nominatim <- kc_geocoded_census$result %>% 
  filter(is.na(lat)) %>% 
  select(address_string) %>% 
  mutate(batch = ceiling(row_number() / 500)) %>% 
  nest(data = -batch) %>% 
  deframe() %>% 
  map_dfr(geocode_safely, address = "address_string", method = "osm")

# Combine geocoded addresses from different sources
kc_geocoded <- bind_rows(
  kc_geocoded_mapbox$result, 
  kc_geocoded_census$result, 
  kc_geocoded_nominatim$result
) %>% 
  filter(!is.na(lat) & !is.na(long)) %>% 
  rename(new_lat = lat, new_lon = long)

# Save geocoded addresses
write_rds(kc_geocoded, here::here("geocoding_data/kc_geocoded_addresses.Rds"))

# Join back to original data and remove temporary variables
kc_data_new <- kc_data %>% 
  mutate(
    address_string = str_squish(str_glue("{address_string}, {city}, MO"))
  ) %>% 
  left_join(kc_geocoded, by = "address_string") %>% 
  mutate(
    longitude = ifelse(is.na(longitude), new_lon, longitude),
    latitude = ifelse(is.na(latitude), new_lat, latitude)
  ) %>% 
  select(-address_string, -new_lat, -new_lon)

# Save data
write_rds(kc_data_new, here::here("temp_data/raw_kansas_city_data.Rds"))



# Louisville -------------------------------------------------------------------

# No Louisville offences are geocoded, so it will all have to be done manually.

# Read data
lo_data <- read_rds(here::here("temp_data/raw_louisville_data.Rds"))

# Remove quirks that might make geo-coding more difficult
lo_data$address_string <- lo_data$block_address %>% 
  str_to_upper() %>% 
  # remove 'BLOCK' from each address
  str_replace_all('\\bBLOCK\\b', '') %>% 
  # offences at a particular stadium
  str_replace_all('^@WF -\\b', 'WATERFRONT PARK') %>% 
  # offences in parks
  str_replace_all('^@PARK -\\b', '') %>%
  # offences at interstate on/off ramps
  str_replace_all('^@(\\d+\\.*\\d*)\\b(.*)$', '\\2 EXIT \\1') %>% 
  # other offences starting @, mostly in the form of cross-streets
  str_replace_all('^@', '') %>% 
  # offences on the zero block of a street
  str_replace_all('^0\\b', '1') %>% 
  str_replace_all('\\b(AT|TO)\\b', ' / ') %>% 
  str_replace_all('\\s+', ' ') %>%
  str_trim()

# Create a tibble of unique addresses with missing co-ordinates and geocode them
# using the Mapbox geocoder in the first instance because it has the highest 
# rate limit and also seems the best of the free geocoders at geocoding
# intersections
lo_geocoded_mapbox <- lo_data %>% 
  filter(
    # Address is not missing or blank
    !is.na(address_string), address_string != "",
    # Address is not the generic address used for offences at unknown locations
    address_string != "COMMUNITY / LARGE"
  ) %>% 
  mutate(
    address_string = str_squish(str_glue("{address_string}, LOUISVILLE, KY"))
  ) %>% 
  select(address_string) %>% 
  # Remove duplicate rows
  distinct() %>% 
  # Split the data into batches of 500 rows to reduce risk of time outs
  mutate(batch = ceiling(row_number() / 500)) %>% 
  nest(data = -batch) %>% 
  deframe() %>% 
  map_dfr(geocode_safely, address = "address_string", method = "mapbox", min_time = 0.12)

# Geocode remaining addresses with missing co-ordinates using the US Census
# geocoder, which has no rate limit but is quite slow nonetheless
lo_geocoded_census <- lo_geocoded_mapbox$result %>% 
  filter(is.na(lat)) %>% 
  select(address_string) %>% 
  mutate(batch = ceiling(row_number() / 500)) %>% 
  nest(data = -batch) %>% 
  deframe() %>% 
  map_dfr(geocode_safely, address = "address_string", method = "census")

# Geocode remaining addresses with missing co-ordinates using the Nominatim 
# geocoder, which has a limit of 1 address per second
lo_geocoded_nominatim <- lo_geocoded_census$result %>% 
  filter(is.na(lat)) %>% 
  select(address_string) %>% 
  mutate(batch = ceiling(row_number() / 500)) %>% 
  nest(data = -batch) %>% 
  deframe() %>% 
  map_dfr(geocode_safely, address = "address_string", method = "osm")

# Geocode remaining addresses with missing co-ordinates using the Here geocoder,
# which has a limit of 5 queries per second
lo_geocoded_here <- lo_geocoded_nominatim$result %>% 
  filter(is.na(lat)) %>% 
  select(address_string) %>% 
  mutate(batch = ceiling(row_number() / 500)) %>% 
  nest(data = -batch) %>% 
  deframe() %>% 
  map_dfr(geocode_safely, address = "address_string", method = "here", custom_query = list("apiKey" = "nVe8ugMTV8AcXX4WGR2HwCo4qTUXb42uJK-1LhVNxiQ"))

# Combine geocoded addresses from different sources
lo_geocoded <- bind_rows(
  lo_geocoded_mapbox$result, 
  lo_geocoded_census$result, 
  lo_geocoded_nominatim$result,
  lo_geocoded_here$result
) %>% 
  filter(!is.na(lat) & !is.na(long)) %>% 
  rename(latitude = lat, longitude = long)

# Save geocoded addresses
write_rds(lo_geocoded, here::here("geocoding_data/lo_geocoded_addresses.Rds"))

# Join back to original data and remove temporary variables
lo_data_new <- lo_data %>% 
  mutate(
    address_string = str_squish(str_glue("{address_string}, LOUISVILLE, KY"))
  ) %>% 
  left_join(lo_geocoded, by = "address_string") %>% 
  select(-address_string)

# Save data
write_rds(lo_data_new, here::here("temp_data/raw_louisville_data.Rds"))



# Memphis ----------------------------------------------------------------------

# Most rows in the Memphis data are geocoded and some addresses that are not 
# have consistent geocodes elsewhere in the data, so we will first match rows to
# any geocodes that exist elsewhere in the data and then geocode the remainder.

# Read data
me_data <- read_rds(here::here("temp_data/raw_memphis_data.Rds"))

# Harmonise addresses
me_data$address_string <- me_data$x100_block_address %>% 
  str_to_upper() %>% 
  # instructions left in field
  str_remove("^Enter # ") %>% 
  # offences at intersections
  str_replace_all('//', ' AND ') %>% 
  str_replace_all(' NEAR ', ' AND ') %>% 
  # Unnecessary blocks
  str_remove(" BLK ") %>% 
  str_remove(" BLOCK ") %>% 
  str_remove(" BLOCK OF ") %>%
  str_squish()

# Create tibble of co-ordinates already present in the data for addresses
me_coords <- me_data %>% 
  filter(!is.na(coord1), !is.na(coord2)) %>% 
  group_by(address_string) %>% 
  summarise(latitude = first(coord1), longitude = first(coord2))

# Join existing co-ordinates to the same addresses without co-ordinates
me_data <- me_data %>% 
  left_join(me_coords, by = "address_string") %>% 
  mutate(
    latitude = if_else(is.na(coord1), latitude, coord1),
    longitude = if_else(is.na(coord2), longitude, coord2)
  )

# Geocode remaining missing rows using the Mapbox API
me_geocoded_mapbox <- me_data %>% 
  filter(
    # Latitude and longitude are missing
    is.na(longitude), is.na(latitude),
    # Address is not missing or blank
    !is.na(address_string), address_string != ""
  ) %>% 
  mutate(
    address_string = str_squish(str_glue("{address_string}, MEMPHIS, TN"))
  ) %>% 
  select(address_string) %>% 
  # Remove duplicate rows
  distinct() %>% 
  # Split the data into batches of 500 rows to reduce risk of time outs
  mutate(batch = ceiling(row_number() / 500)) %>% 
  nest(data = -batch) %>% 
  deframe() %>% 
  map_dfr(geocode_safely, address = "address_string", method = "mapbox")

# Geocode remaining addresses with missing co-ordinates using the Here geocoder,
# which has a limit of 5 queries per second
me_geocoded_here <- me_geocoded_mapbox$result %>% 
  filter(is.na(lat)) %>% 
  select(address_string) %>% 
  mutate(batch = ceiling(row_number() / 500)) %>% 
  nest(data = -batch) %>% 
  deframe() %>% 
  map_dfr(geocode_safely, address = "address_string", method = "here", custom_query = list("apiKey" = "nVe8ugMTV8AcXX4WGR2HwCo4qTUXb42uJK-1LhVNxiQ"))

# Combine geocoded addresses from different sources
me_geocoded <- bind_rows(
  me_geocoded_mapbox$result, 
  me_geocoded_here$result
) %>% 
  filter(!is.na(lat) & !is.na(long)) %>% 
  rename(new_lat = lat, new_lon = long)

# Save geocoded addresses
write_rds(me_geocoded, here::here("geocoding_data/me_geocoded_addresses.Rds"))

# Join back to original data and remove temporary variables
me_data_new <- me_data %>% 
  mutate(
    address_string = str_squish(str_glue("{address_string}, MEMPHIS, TN"))
  ) %>% 
  left_join(me_geocoded, by = "address_string") %>% 
  mutate(
    longitude = ifelse(is.na(longitude), new_lon, longitude),
    latitude = ifelse(is.na(latitude), new_lat, latitude)
  ) %>% 
  select(-address_string, -new_lat, -new_lon)

# Save data
write_rds(me_data_new, here::here("temp_data/raw_memphis_data.Rds"))



# St Louis ---------------------------------------------------------------------

# About 98% of St Louis data are geocoded, but the remaining 2% have addresses
# but no co-ordinates.

# Read data
sl_data <- read_rds(here::here("temp_data/raw_st_louis_data.Rds"))

# Filter and geocode data with the Here API
sl_geocoded_here <- sl_data %>% 
  filter(
    is.na(latitude), is.na(longitude), 
    !ileads_street %in% c("UNKNOWN", "UNKNOWN ST LOUIS", "CITY OF ST LOUIS"), 
    !is.na(ileads_street)
  ) %>% 
  mutate(
    ileads_address = ifelse(ileads_address == "0", "", ileads_address), 
    address_string = str_glue("{ileads_address} {ileads_street}, ST LOUIS, MO")
  ) %>% 
  select(address_string) %>% 
  distinct() %>% 
  # Split the data into batches of 500 rows to reduce risk of time outs
  mutate(batch = ceiling(row_number() / 500)) %>% 
  nest(data = -batch) %>% 
  deframe() %>% 
  map_dfr(geocode_safely, address = "address_string", method = "here", custom_query = list("apiKey" = "nVe8ugMTV8AcXX4WGR2HwCo4qTUXb42uJK-1LhVNxiQ"))

# Combine geocoded addresses from different sources
sl_geocoded <- sl_geocoded_here$result %>% 
  filter(!is.na(lat) & !is.na(long)) %>% 
  rename(new_lat = lat, new_lon = long)

# Join back to original data and remove temporary variables
sl_data_new <- sl_data %>% 
  mutate(
    address_string = str_glue("{ileads_address} {ileads_street}, ST LOUIS, MO")
  ) %>% 
  left_join(sl_geocoded, by = "address_string") %>% 
  mutate(
    longitude = ifelse(is.na(longitude), new_lon, longitude),
    latitude = ifelse(is.na(latitude), new_lat, latitude)
  ) %>% 
  select(-address_string, -new_lat, -new_lon)

# Save data
write_rds(sl_data_new, here::here("temp_data/raw_st_louis_data.Rds"))



# Virginia Beach ---------------------------------------------------------------

# Virginia Beach data are geocoded until 2015 but not afterwards

# Read data
vb_data <- read_rds(here::here("temp_data/raw_virginia_beach_data.Rds"))

# Prior to 2016 addresses are contained in a single field whereas afterward they
# are split between multiple fields, so first we convert them into a consistent
# format
vb_data <- vb_data %>% 
  # Replace NAs in address components with empty strings
  replace_na(list(block_address = "", city = "", state = "", zip = "")) %>% 
  mutate(
    # Remove co-ordinates from the end of the address and add a comma before the
    # city name
    address_string = str_remove(
      str_replace_all(
        str_replace(address, " VIRGINIA BEACH, VA", ", VIRGINIA BEACH, VA"),
        ",,",
        ","
      ), 
      " \\([0-9]+\\.[0-9]+\\, \\-[0-9]+\\.[0-9]+\\)$"
    ),
    # Create addresses from multiple fields where no address is present
    address_string = ifelse(
      is.na(address_string),
      str_glue("{block_address}, {city}, {state} {zip}"),
      address_string
    ),
    # Remove empty components from address string
    address_string = str_squish(str_remove_all(
      str_remove(address_string, ", $"), 
      ", , "
    ))
  ) %>% 
  select(-block_address, -city, -state, -zip, -address)

# Because addresses prior to 2016 are geocoded, there are geocodes in the data
# for many addresses that appear ungeocoded 
vb_coords <- vb_data %>% 
  filter(!is.na(latitude), !is.na(longitude)) %>% 
  group_by(address_string) %>% 
  summarise(new_lat = first(latitude), new_lon = first(longitude))

# Join existing co-ordinates to the same addresses without co-ordinates
vb_data <- vb_data %>% 
  left_join(vb_coords, by = "address_string") %>% 
  mutate(
    latitude = ifelse(is.na(latitude), new_lat, latitude),
    longitude = ifelse(is.na(longitude), new_lon, longitude)
  ) %>% 
  select(-new_lat, -new_lon)

# Filter and geocode data with the Here API
vb_geocoded_here <- vb_data %>% 
  filter(is.na(latitude), is.na(longitude)) %>% 
  select(address_string) %>% 
  distinct() %>% 
  # Split the data into batches of 500 rows to reduce risk of time outs
  mutate(batch = ceiling(row_number() / 500)) %>% 
  nest(data = -batch) %>% 
  deframe() %>% 
  map_dfr(geocode_safely, address = "address_string", method = "here", custom_query = list("apiKey" = "nVe8ugMTV8AcXX4WGR2HwCo4qTUXb42uJK-1LhVNxiQ"))

# Combine geocoded addresses from different sources
vb_geocoded <- vb_geocoded_here$result %>% 
  filter(!is.na(lat) & !is.na(long)) %>% 
  rename(new_lat = lat, new_lon = long)

# Join back to original data and remove temporary variables
vb_data_new <- vb_data %>% 
  left_join(vb_geocoded, by = "address_string") %>% 
  mutate(
    longitude = ifelse(is.na(longitude), new_lon, longitude),
    latitude = ifelse(is.na(latitude), new_lat, latitude)
  ) %>% 
  rename(address = address_string) %>% 
  select(-new_lat, -new_lon)

# Save data
write_rds(vb_data_new, here::here("temp_data/raw_virginia_beach_data.Rds"))
