# This file takes the geocoded processed data for each city and carries out 
# spatial processing



# Convert all co-ordinates to lat/lon pairs ------------------------------------

# Data for most cities already include lat/lon pairs, but some require
# conversion from other co-ordinate systems


## Colorado Springs ----

# CO Springs data have a column of SF points, but the code below needs separate
# lat and lon columns, so we will extract these first.

here::here("temp_data/raw_colorado_springs_data.Rds") %>% 
  read_rds() %>% 
  # Remove missing co-ordinates because `st_as_sfc()` can't handle them
  filter(!is.na(location_point)) %>% 
  mutate(geometry = st_as_sfc(location_point)) %>% 
  st_as_sf(crs = 4326) %>% 
  { 
    rename(
      bind_cols(st_drop_geometry(.), st_coordinates(.)), 
      longitude = X, 
      latitude = Y
    )
  } %>% 
  select(-location_point) %>% 
  write_rds(here::here("temp_data/raw_colorado_springs_data.Rds"))


## Detroit ----

# Detroit data have lat/lon pairs, but they seem to encode NA as 999999.0
# degrees. These cases can easily be rectified by replacing any value of
# latitude that is not between -90 and +90, and any value of longitude that is
# not between -180 and +180, with NA.

here::here("temp_data/raw_detroit_data.Rds") %>% 
  read_rds() %>% 
  mutate(
    latitude = ifelse(y < -90 | y > 90, NA, y),
    longitude = ifelse(x < -180 | x > 180, NA, x)
  ) %>% 
  select(-x, -y) %>% 
  write_rds(here::here("temp_data/raw_detroit_data.Rds"))



# Strip offences outside cities and add census identifiers ---------------------

# Spatial files are from https://www.census.gov/cgi-bin/geo/shapefiles/index.php

walk(
  setdiff(
    str_remove(dir("temp_data", pattern = "^raw"), "^raw_"),
    str_remove(dir("temp_data", pattern = "^spatial"), "^spatial_")
  ),
  function (x) {
    
    # get city name
    city_name <- x %>% 
      str_match("^([a-z_]+?)\\_data.Rds") %>% 
      as.character() %>% 
      last() %>% 
      str_replace_all("\\_", " ") %>% 
      str_to_title()
    
    start_time <- now()
    rlang::inform(c(
      str_glue("\nProcessing {city_name} data "), 
      str_glue("Starting at {format(start_time, '%F %T')}")
    ))
    
    # get city metadata
    city_fips <- cities$fips[cities$name == city_name]
    
    # load data
    data <- str_glue("temp_data/raw_{x}") %>% 
      read_rds() %>% 
      mutate(across(c("latitude", "longitude"), as.numeric))
    
    # note number of rows in data
    initial_rows <- nrow(data)
    rlang::inform(str_glue("✔︎ Loaded data with {scales::comma(initial_rows)} rows"))
    
    # Update names of co-ordinate columns if needed
    if ("lat" %in% names(data) & !"latitude" %in% names(data)) {
      data <- rename(data, latitude = lat, longitude = lon)
    }
    
    # convert to SF object (removing any rows without co-ordinates)
    offences <- data %>% 
      select(uid, longitude, latitude) %>% 
      filter(!is.na(longitude) & !is.na(latitude)) %>% 
      st_as_sf(coords = c("longitude", "latitude"), crs = 4326)
    no_coords <- initial_rows - nrow(offences)
    if (nrow(offences) > initial_rows) {
      rlang::abort('✖ Rows have been added to data')
    } else {
      rlang::inform(str_glue(
        "✔︎ Converted data to an SF object, removing ", 
        "{scales::comma(no_coords)} rows without co-ordinates"
      ))
    }
    
    # create a spatial object for the city outline
    outline <- tigris::places(state = city_fips, year = 2016) %>% 
      filter(NAME == cities$census_name[cities$name == city_name]) %>% 
      st_transform(4326)
    # outline <- str_glue("spatial_data/cities/tl_2016_{city_fips}_place.shp") %>% 
    #   here::here() %>% 
    #   st_read(quiet = TRUE, stringsAsFactors = FALSE) %>% 
    #   filter(NAME == cities$census_name[cities$name == city_name]) %>% 
    #   st_transform(4326)
    rlang::inform("✔︎ Created SF object for city outline")
    
    # identify whether each offence is within the city boundary
    rlang::inform("🕣 Identifying rows outside the city")
    offences$in_city <- offences %>% 
      st_covered_by(outline, sparse = FALSE) %>% 
      as.logical()
    
    # filter out offences outside the city boundary
    outside_city <- nrow(offences) - sum(offences$in_city)
    if (nrow(offences) > initial_rows) {
      rlang::abort('✖ Rows have been added to data', call. = FALSE)
    } else {
      rlang::inform(str_glue(
        "✔ Identified {scales::comma(outside_city)} ", 
        "rows that were outside the city"
      ))
    }
    
    # create a spatial object for census blocks
    blocks <- tigris::blocks(state = city_fips, year = 2016) %>% 
    # blocks <- str_glue("spatial_data/cities/tl_2016_{city_fips}_tabblock10.shp") %>% 
    #   here::here() %>% 
    #   st_read(quiet = TRUE, stringsAsFactors = FALSE) %>% 
      select(
        fips_state = STATEFP10, # FIPS state code
        fips_county = COUNTYFP10, # FIPS county code
        tract = TRACTCE10, # Census 2010 tract ID
        block = BLOCKCE10 # Census 2010 block ID
      ) %>% 
      st_transform(4326)
    rlang::inform("✔︎ Created SF object for census blocks")
    
    # join census blocks to offences
    # Some crimes will fall on the boundary of multiple census blocks, so the
    # following code simply takes the first block in each case
    rlang::inform("🕣 Identifying census blocks")
    offences <- offences %>% 
      st_join(blocks) %>% 
      mutate(
        block_code = str_glue("{fips_state}{fips_county}{tract}{block}")
      ) %>% 
      group_by(uid) %>% 
      top_n(1, block_code) %>% 
      ungroup() %>% 
      select(-block_code)
    
    if (nrow(offences) > initial_rows) {
      rlang::abort('✖ Rows have been added to data', call. = FALSE)
    } else {
      rlang::inform(str_glue(
        "✔ ︎Identified census blocks for {scales::comma(nrow(offences))} rows"
      ))
    }
    
    # identify block groups
    offences <- mutate(offences, block_group = str_sub(block, 1, 1))
    rlang::inform(str_glue(
      "✔ ︎Identified census block groups for {scales::comma(nrow(offences))} rows"
    ))
    
    # add the fields from the spatial joins to the existing crime data
    rlang::inform("🕣 Joining census identifiers to crime data")
    data <- left_join(data, offences, by = 'uid')
    
    if (nrow(data) > initial_rows) {
      rlang::abort('✖ Rows have been added to data', call. = FALSE)
    } else {
      rlang::inform(str_glue(
        "✔ Joined census identifiers to {scales::comma(nrow(data))} rows of crime data"
      ))
    }
    
    # store data
    write_rds(
      data, 
      here::here(str_glue(
        "temp_data/spatial_", 
        str_replace_all(str_to_lower(city_name), '\\s', '_'), 
        "_data.Rds"
      ))
    )
    rlang::inform("✔ Saved data to file")
    
    duration <- difftime(now(), start_time, units = "auto")
    rlang::inform(str_glue(
      "Finished at {format(now(), '%F %T')} after ",
      "{round(duration, digits = 1)} {units(duration)}"
    ))
    
  }
)

