# This file takes the geocoded processed data for each city and carries out 
# spatial processing.





# Convert all co-ordinates to lat/lon pairs -----------------------------------

# Data for most cities already include lat/lon pairs, but some require
# conversion from other co-ordinate systems.



## Detroit -------------

# Detroit data have lat/lon pairs, but they seem to encode NA as 999999.0
# degrees. These cases can easily be rectified by replacing any value of
# latitude that is not between -90 and +90, and any value of longitude that is
# not between -180 and +180, with NA.

data_dt <- read_rds(here::here("temp_data/raw_detroit_data.Rds"))

data_dt <- mutate(
  data_dt,
  latitude = ifelse(latitude < -90 | latitude > 90, NA, latitude),
  longitude = ifelse(longitude < -180 | longitude > 180, NA, longitude)
)

write_rds(data_dt, here::here("temp_data/raw_detroit_data.Rds"))

rm(data_dt)



## St Louis -------------

# St Louis data has co-ordinates in the Missouri East State Plane projection,
# in which co-ordinates are distances in feet from a false origin.

data_sl <- read_rds(here("temp_data/raw_st_louis_data.Rds"))

data_sl <- data_sl %>% 
  st_as_sf(coords = c("x_coord", "y_coord"), crs = 102696) %>% 
  st_transform(4326) %>% 
  mutate(
    longitude = st_coordinates(.)[, 1],
    latitude = st_coordinates(.)[, 2]
  ) %>% 
  st_set_geometry(NULL)

write_rds(data_sl, here("temp_data/raw_st_louis_data.Rds"))

rm(data_sl)





# Strip offenses outside cities and add census identifiers --------------------

setdiff(
  as.character(str_extract(dir(here("temp_data"), pattern = "^raw_"), 
                           "\\_([a-z_]+?)\\_data.Rds")),
  as.character(str_extract(dir(here("temp_data"), pattern = "^spatial_"), 
                           "\\_([a-z_]+?)\\_data.Rds"))
) %>% 
  { paste0(here("temp_data/raw"), .) } %>% 
  walk(function (x) {
    
    # get city name
    city_name <- str_match(x, "raw\\_([a-z_]+?)\\_data.Rds") %>% 
      as.character() %>% 
      last() %>% 
      str_replace_all("\\_", " ") %>% 
      str_to_title()
    
    start_time <- now()
    message(str_glue("\n\nPROCESSING {str_to_upper(city_name)} DATA\n",
                     "Starting at ", format(start_time, "%F %T")))
    
    # get city metadata
    city_fips <- cities$fips[cities$name == city_name]
    
    # load data
    data <- x %>% 
      read_rds() %>% 
      mutate(
        latitude = as.numeric(latitude),
        longitude = as.numeric(longitude)
      )
    
    # note number of rows in data
    initial_rows <- nrow(data)
    message("✔︎ Loaded data with ", row_count(data), " rows")
    
    # convert to SF object (removing any rows without co-ordinates)
    offences <- data %>% 
      select(uid, longitude, latitude)  %>% 
      filter(!is.na(longitude) & !is.na(latitude)) %>% 
      st_as_sf(coords = c('longitude', 'latitude'), crs = 4326) %>% 
      st_transform(3082)
    no_coords <- initial_rows - nrow(offences)
    if (nrow(offences) > initial_rows) {
      stop('✖ Rows have been added to data', call. = FALSE)
    } else {
      message("✔︎ Converted data to an SF object, removing ", 
              scales::comma(no_coords), " rows without co-ordinates")
    }
    
    # deal with strange name of Nashville in the US Census shapefiles
    city_name_usgs <- recode(
      city_name,
      "Nashville" = "Nashville-Davidson metropolitan government (balance)",
      "St Louis" = "St. Louis"
    )

    # create a spatial object for the city outline
    outline <- str_glue("spatial_data/cities/tl_2016_{city_fips}_place.shp") %>% 
      here() %>% 
      read_sf() %>% 
      janitor::clean_names() %>% 
      filter(name == city_name_usgs) %>%
      st_transform(3082)
    message("✔︎ Created SF object for city outline")
    
    # identify whether each offense is within the city boundary
    message("🕣 Identifying rows outside the city")
    offences$in_city <- offences %>% 
      st_covered_by(outline, sparse = FALSE) %>% 
      as.logical()
    
    # filter out offenses outside the city boundary
    outside_city <- nrow(offences) - sum(offences$in_city) - no_coords

    if (nrow(offences) > initial_rows) {
      stop('✖ Rows have been added to data', call. = FALSE)
    } else {
      message("✔ Identified ", scales::comma(outside_city), 
              " rows that were outside the city")
    }
    
    # create a spatial object for census blocks
    blocks <- str_glue("spatial_data/cities/tl_2016_{city_fips}_",
                       "tabblock10.shp") %>%  
      here() %>% 
      read_sf() %>%
      janitor::clean_names() %>% 
      select(
        fips_state = statefp10, # FIPS state code
        fips_county = countyfp10, # FIPS county code
        tract = tractce10, # Census 2010 tract ID
        block = blockce10 # Census 2010 block ID
      ) %>% st_transform(3082)
    message("✔︎ Created SF object for census blocks")
    
    # join census blocks to offences
    # Some crimes will fall on the boundary of multiple census blocks, so the
    # following code simply takes the first block in each case
    message("🕣 Identifying census blocks")
    offences <- offences %>% 
      st_join(blocks) %>% 
      mutate(block_code = paste0(fips_state, fips_county, tract, block)) %>%
      group_by(uid) %>% 
      top_n(1, block_code) %>% 
      ungroup() %>% 
      select(-block_code)
    
    if (nrow(offences) > initial_rows) {
      stop('✖ Rows have been added to data', call. = FALSE)
    } else {
      message("✔ ︎Identified census blocks for ", row_count(offences), " rows")
    }
    
    # identify block groups
    offences <- mutate(offences, block_group = str_sub(block, 1, 1))
    message("✔ ︎Identified census block groups for ", row_count(offences),
            " rows")
    
    # add the fields from the spatial joins to the existing crime data
    message("🕣 Joining census identifiers to crime data")
    data <- left_join(data, offences, by = 'uid')
    
    if (nrow(data) > initial_rows) {
      stop('✖ Rows have been added to data', call. = FALSE)
    } else {
      message("✔ Joined census identifiers to ", row_count(data), 
              " rows of crime data")
    }
    
    message(
      scales::percent(sum(!is.na(data$block)) / nrow(data), accuracy = 0.01), 
      " of rows have census identifiers"
    )
    
    # store data
    write_rds(
      data,
      here(str_glue("temp_data/spatial_{slugify(city_name)}_data.Rds"))
    )
    message("✔ Saved data to file")
    
    duration <- difftime(now(), start_time, units = "auto")
    message("Finished at ", format(now(), "%F %T"), " after ", 
            round(duration, digits = 1), " ", units(duration))
    
  })





# Check data ------------------------------------------------------------------

here("temp_data") %>% 
  dir(pattern = "^spatial_") %>% 
  str_remove("^spatial_") %>% 
  str_remove("_data.Rds$") %>% 
  walk(function (city) {

    # deal with strange name of Nashville in the US Census shapefiles
    city_name <- str_to_title(str_replace_all(city, "_", " "))
    city_name_usgs <- recode(
      city_name,
      "Nashville" = "Nashville-Davidson metropolitan government (balance)",
      "St Louis" = "St. Louis"
    )
    
    message(str_glue("\nProducing map for {city_name} at ",
                     "{format(now(), '%F %T')}"))
    
    crimes <- str_glue("temp_data/spatial_{city}_data.Rds") %>% 
      here() %>% 
      read_rds() %>% 
      sample_frac(0.1) %>% 
      filter(!is.na(latitude) & !is.na(longitude)) %>% 
      st_as_sf(coords = c("longitude", "latitude"), crs = 4326)
    
    message(
      str_glue("\tSampled {scales::comma(nrow(crimes))} rows (10% of total)"))
    
    outline <- str_glue("spatial_data/cities/tl_2016_",
                        "{cities$fips[cities$name == city_name]}_place.shp") %>% 
      here() %>% 
      read_sf() %>% 
      janitor::clean_names() %>% 
      filter(name == city_name_usgs)
    
    outline_bbox <- st_bbox(outline)
    
    ggplot() +
      geom_sf(data = outline, fill = NA) +
      geom_sf(aes(shape = in_city), data = crimes, alpha = 0.5) +
      theme_minimal() +
      theme(
        legend.position = "none",
        panel.grid.major = element_line(colour = "#CCCCCC")
      ) +
      labs(title = str_glue("Offence locations in {city_name} data")) +
      xlim(outline_bbox$xmin - ((outline_bbox$xmax - outline_bbox$xmin) / 5), 
           outline_bbox$xmax + ((outline_bbox$xmax - outline_bbox$xmin) / 5)) +
      ylim(outline_bbox$ymin - ((outline_bbox$ymax - outline_bbox$ymin) / 5), 
           outline_bbox$ymax + ((outline_bbox$ymax - outline_bbox$ymin) / 5))
    
    message("\tGenerated plot")
    
    ggsave(here(str_glue("temp_data/plot_{city}.pdf")),
           width = 11.69, height = 16.53, units = "in")
    
    message("\tSaved plot")
    
  })



