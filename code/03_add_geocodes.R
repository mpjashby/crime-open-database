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
data_fw <- data_fw %>% 
  mutate(
    geocode_address = block_address %>% 
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
      str_squish(),
    zip_code = "",
    geocode_address_full = 
      str_glue("{geocode_address}, {city}, {state}, {zip_code}")
  )

# geocode addresses with the US Census geocoder
fw_geocoded_census <- data_fw %>% 
  filter(( is.na(latitude) | is.na(longitude) ) & geocode_address != "") %>% 
  select(geocode_address_full, geocode_address, city, state, zip_code) %>% 
  unique.data.frame() %>% 
  mutate(batch = ceiling(row_number() / 1000)) %>% 
  nest(data = -batch) %>% 
  mutate(
    results = map(data, cxy_geocode, street = "geocode_address",
                  city = "city", state = "state", zip = "zip_code",
                  output = "simple", benchmark = 9)
  ) %>% 
  select(results) %>% 
  unnest(results) %>% 
  select(geocode_address_full, census_lat = cxy_lat, census_lon = cxy_lon)

# geocode remaining addresses with Geocodio
fw_geocoded_geocodio <- fw_geocoded_census %>% 
  filter(is.na(census_lat)) %>% 
  pull(geocode_address_full) %>% 
  rgeocodio::gio_batch_geocode() %>% 
  select(query, response_results) %>% 
  mutate(response_results = map(response_results, slice, 1)) %>% 
  unnest(response_results) %>% 
  filter(!accuracy_type %in% c("place", "state")) %>% 
  select(geocode_address_full = query, geo_lat = location.lat, 
         geo_lon = location.lng)

# link data
data_fw_final <- data_fw %>% 
  left_join(fw_geocoded_census, by = "geocode_address_full") %>% 
  left_join(fw_geocoded_geocodio, by = "geocode_address_full") %>% 
  mutate(
    latitude = case_when(
      is.na(latitude) & !is.na(census_lat) ~ census_lat,
      is.na(latitude) & !is.na(geo_lat) ~ geo_lat,
      TRUE ~ latitude
    ),
    longitude = case_when(
      is.na(longitude) & !is.na(census_lon) ~ census_lon,
      is.na(longitude) & !is.na(geo_lon) ~ geo_lon,
      TRUE ~ longitude
    )
  ) %>% 
  select(-census_lat, -census_lon, -geo_lat, -geo_lon, -geocode_address, 
         -geocode_address_full, -zip_code)

# report results
data_fw_final %>% 
  mutate(is_geocoded = !is.na(latitude)) %>% 
  count(is_geocoded) %>% 
  janitor::adorn_percentages(denominator = "all") %>% 
  janitor::adorn_rounding(digits = 3)

# save data
write_rds(data_fw_final, here::here("temp_data/raw_fort_worth_data.Rds"))





# Kansas City -----------------------------------------------------------------

# All KC offenses are geocoded before 2016, but for some reason almost a third
# of the offenses in the 2017 and later files have an address but no
# co-ordinates.

# read data
data_kc <- read_rds(here::here("temp_data/raw_kansas_city_data.Rds"))

# clean address string
data_kc <- data_kc %>% 
  replace_na(list(city = "", zip_code = "")) %>% 
  mutate(
    geocode_address = address %>% 
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
      str_squish(),
    city = str_squish(str_remove_all(str_remove_all(city, "[^\\w\\s]"), "\\d")),
    city = ifelse(city == "KCMO" | str_length(city) < 3, "KANSAS CITY", city),
    state = "MO",
    geocode_address_full = 
      str_glue("{geocode_address}, {city}, {state}, {zip_code}")
  )

# geocode addresses with the US Census geocoder
kc_geocoded_census <- data_kc %>% 
  filter(( is.na(latitude) | is.na(longitude) ) & geocode_address != "") %>% 
  select(geocode_address_full, geocode_address, city, state, zip_code) %>% 
  unique.data.frame() %>% 
  mutate(batch = ceiling(row_number() / 100)) %>% 
  nest(data = -batch) %>% 
  mutate(
    results = map2(data, batch, function (x, y) {
      
      message(str_glue("Processing batch {y}"))
      
      cxy_geocode(x, street = "geocode_address", city = "city", state = "state", 
                  zip = "zip_code", output = "simple", benchmark = 9)
      
    })
    # results = map(data, cxy_geocode, street = "geocode_address",
    #               city = "city", state = "state", zip = "zip_code",
    #               output = "simple", benchmark = 9)
  ) %>% 
  select(results) %>% 
  unnest(results) %>% 
  select(geocode_address_full, census_lat = cxy_lat, census_lon = cxy_lon)

# geocode remaining addresses with Geocodio
kc_geocoded_geocodio <- kc_geocoded_census %>% 
  filter(is.na(census_lat)) %>% 
  pull(geocode_address_full) %>% 
  gio_batch_geocode() %>% 
  select(query, response_results) %>% 
  mutate(response_results = map(response_results, slice, 1)) %>% 
  unnest(response_results) %>% 
  filter(!accuracy_type %in% c("place", "state")) %>% 
  select(geocode_address_full = query, geo_lat = location.lat, 
         geo_lon = location.lng)

# link data
data_kc_final <- data_kc %>% 
  left_join(kc_geocoded_census, by = "geocode_address_full") %>% 
  left_join(kc_geocoded_geocodio, by = "geocode_address_full") %>% 
  mutate_at(c("latitude", "longitude"), as.double) %>% 
  mutate(
    latitude = case_when(
      is.na(latitude) & !is.na(census_lat) ~ census_lat,
      is.na(latitude) & !is.na(geo_lat) ~ geo_lat,
      TRUE ~ latitude
    ),
    longitude = case_when(
      is.na(longitude) & !is.na(census_lon) ~ census_lon,
      is.na(longitude) & !is.na(geo_lon) ~ geo_lon,
      TRUE ~ longitude
    )
  ) %>% 
  select(-census_lat, -census_lon, -geo_lat, -geo_lon, -geocode_address, 
         -geocode_address_full, -state)

# report results
data_kc_final %>% 
  mutate(is_geocoded = !is.na(latitude)) %>% 
  count(is_geocoded) %>% 
  janitor::adorn_percentages(denominator = "all") %>% 
  janitor::adorn_rounding(digits = 3)

# save data
write_rds(data_kc_final, here::here("temp_data/raw_kansas_city_data.Rds"))





# Louisville ------------------------------------------------------------------

# load data
data_lo <- read_rds(here::here("temp_data/raw_louisville_data.Rds"))

# clean address string
data_lo <- data_lo %>% 
  replace_na(list(city = "LOUISVILLE", zip_code = "")) %>% 
  mutate(
    geocode_address = block_address %>% 
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
      str_squish(),
    # recode city abbreviations that are present in at least 0.01% of the data
    city = recode(
      str_replace_all(city, "_", " "),
      "LVIL" = "LOUISVILLE",
      "LOU" = "LOUISVILLE",
      "LOUISV" = "LOUISVILLE",
      "LOUISVIILLE" = "LOUISVILLE",
      "LOUISVILE" = "LOUISVILLE",
      "LOUSVILLE" = "LOUISVILLE",
      "LYND" = "LYNDON",
      "MTWN" = "MT WASHINGTON",
      "WB" = "WEST BUECHEL",
      "DHIL" = "DOUGLASS HILLS",
      "WTPK" = "WATTERSON PARK"
    ),
    state = "KY",
    geocode_address_full = 
      str_glue("{geocode_address}, {city}, {state}, {zip_code}")
  )
    
# geocode addresses with the US Census geocoder
lo_geocoded_census <- data_lo %>% 
  filter(geocode_address != "" & block_address != "COMMUNITY AT LARGE") %>% 
  select(geocode_address_full, geocode_address, city, state, zip_code) %>% 
  unique.data.frame() %>% 
  mutate(batch = ceiling(row_number() / 1000)) %>% 
  nest(data = -batch) %>% 
  mutate(
    results = map(data, cxy_geocode, street = "geocode_address",
                  city = "city", state = "state", zip = "zip_code",
                  output = "simple", benchmark = 9)
  ) %>% 
  select(results) %>% 
  unnest(results) %>% 
  select(geocode_address_full, census_lat = cxy_lat, census_lon = cxy_lon)

# geocode remaining addresses with Geocodio
lo_geocoded_geocodio <- lo_geocoded_census %>% 
  filter(is.na(census_lat)) %>% 
  pull(geocode_address_full) %>% 
  gio_batch_geocode() %>% 
  select(query, response_results) %>% 
  mutate(response_results = map(response_results, slice, 1)) %>% 
  unnest(response_results) %>% 
  filter(!accuracy_type %in% c("place", "state")) %>% 
  select(geocode_address_full = query, geo_lat = location.lat, 
         geo_lon = location.lng)

# link data
data_lo_final <- data_lo %>% 
  left_join(lo_geocoded_census, by = "geocode_address_full") %>% 
  left_join(lo_geocoded_geocodio, by = "geocode_address_full") %>% 
  mutate(
    latitude = case_when(
      !is.na(census_lat) ~ census_lat,
      !is.na(geo_lat) ~ geo_lat,
      TRUE ~ as.double(NA_integer_)
    ),
    longitude = case_when(
      !is.na(census_lon) ~ census_lon,
      !is.na(geo_lon) ~ geo_lon,
      TRUE ~ as.double(NA_integer_)
    )
  ) %>% 
  select(-census_lat, -census_lon, -geo_lat, -geo_lon, -geocode_address, 
         -geocode_address_full, -state)

# report results
data_lo_final %>% 
  mutate(is_geocoded = !is.na(latitude)) %>% 
  count(is_geocoded) %>% 
  janitor::adorn_percentages(denominator = "all") %>% 
  janitor::adorn_rounding(digits = 3)

# save data
write_rds(data_lo_final, here::here("temp_data/raw_louisville_data.Rds"))

