# Get data from discontinued cities -------------------------------------------

# If a city is no longer included in CODE, typically because the local 
# authorities have stopped publishing usable crime data, we download the data
# from the existing CODE version so that it can be included in the updated files
# included here and have consecutive UIDs etc.

crimedata::get_crime_data(
  years = 2007 : (yearLast - 1), 
  cities = "Virginia Beach", 
  type = "extended"
) %>% 
  # restore data to the same format as produced by 05_harmonze_data.R
  rename_at(
    vars("offense_code", "offense_type", "offense_group", "offense_against"),
    ~ str_glue("nibrs_{.}")
  ) %>% 
  rename_at(vars(starts_with("vib_")), ~ str_remove(., "^vib_")) %>% 
  # rename(
  #   'nibrs_offense_code' = 'offense_code', 
  #   'nibrs_offense_type' = 'offense_type', 
  #   'nibrs_offense_category' = 'offense_group', 
  #   'nibrs_crime_against' = 'offense_against'
  # ) %>% 
  mutate(
    in_city = TRUE,
    fips_state = str_sub(census_block, 0, 2), 
    fips_county = str_sub(census_block, 3, 5), 
    tract = str_sub(census_block, 6, 11), 
    block = str_sub(census_block, -4)
  ) %>% 
  select(-uid, -census_block) %>% 
  split(.$city_name) %>% 
  walk(function (x) {
    write_rds(
      x, 
      here(
        str_glue("temp_data/final_{slugify(first(x$city_name), '_')}_data.Rds")
      )
    )
  })





# Output RDS files ------------------------------------------------------------

# This code outputs RDS files for each city for each year. These can be used to
# produce CSV files for multiple cities etc.
# 
# Three files are produced for each city for each year. The 'extended' file 
# contains all the variables, the 'core' file contains only the common variables 
# (i.e. those without the city-name prefix) and the 'sample' file contains a 1% 
# sample of data in the core file.

# remove unnecessary variables from common_vars
common_vars <- setdiff(
  common_vars, 
  c('geometry', "multiple_dates", "in_city", "address", "fips_state", 
    "fips_county", "tract", "block_group", "block", "date_year")
) %>% 
  # rename the crime category variables because they've been renamed in the 
  # dataset by the convert_names() function in file 05_harmonize_data.R
  recode(
    'nibrs_offense_code' = 'offense_code', 
    'nibrs_offense_type' = 'offense_type', 
    'nibrs_offense_category' = 'offense_group', 
    'nibrs_crime_against' = 'offense_against'
  ) %>% 
  # mapvalues(c('nibrs_offense_code', 'nibrs_offense_type', 
  #             'nibrs_offense_category', 'nibrs_crime_against'), 
  #           c('offense_code', 'offense_type', 'offense_group', 
  #             'offense_against')) %>% 
  # add census_block to common vars so it appears in the core data
  append("census_block")



# set row number, which should be continuous across cities and years so must be
# remembered between files
row_num <- 1

# for each city, load the data and output an Rds file for each year
# this must be done separately for each city to avoid the 'vector memory
# exhausted' problem on macOS described at
# http://r.789695.n4.nabble.com/R-3-5-0-vector-memory-exhausted-error-on-readBin-td4750237.html
walk(cities$name, function (city) {

  this_city <- slugify(city, "_")
  
  # load and process data
  city_data <- str_glue("temp_data/final_{this_city}_data.Rds") %>% 
    here() %>% 
    read_rds() %>% 
    mutate_at(vars(one_of(c("local_row_id", "case_number"))), as.character) %>% 
    arrange(date_single) %>%
    filter(
      offense_against != "excluded cases" 
      & !is.na(longitude) 
      & !is.na(latitude) 
      & in_city == TRUE
    ) %>% 
    mutate(
      # add a row ID
      uid = row_num:(row_num + nrow(.) - 1),
      # merge the census identifiers
      census_block = paste0(fips_state, fips_county, tract, block),
      # catch and robbery/burglary offenses have have not been recategorised
      offense_code = case_when(
        offense_code == "120" ~ "12U",
        offense_code == "220" ~ "22U",
        offense_code == "23U" ~ "23H",
        TRUE ~ offense_code
      )
    ) %>% 
    select(one_of(common_vars), everything(), -in_city, -multiple_dates,
           -fips_state, -fips_county, -tract, -block_group, -block) %>% 
    mutate(year = year(date_single))
  
  # output data by year
  city_data %>% 
    split(.$year) %>% 
    walk(function (x) {
      
      this_year <- x$year[1]
      
      x %>%
        select(-year) %>% 
        # export extended data - use write_rds() rather than saveRDS() because
        # it returns the input invisibly for use inside a pipe
        write_rds(
          here(str_glue("output_data/crime_open_database_extended_{this_city}_",
                        "{this_year}.Rds")),
          compress = "bz2"
        ) %>%
        # select core variables and export
        select(one_of(common_vars)) %>%
        write_rds(
          here(str_glue("output_data/crime_open_database_core_{this_city}_",
                        "{this_year}.Rds")),
          compress = "bz2"
        ) %>% 
        # take a 1% sample and export
        sample_frac(0.01) %>% 
        arrange(uid) %>% 
        write_rds(
          here(str_glue("output_data/crime_open_database_sample_{this_city}_",
                        "{this_year}.Rds")),
          compress = "bz2"
        )
      
      message(str_glue("\nWritten data for {city} in {this_year}"))
      
    })
  
  # use <<- to assign to the global environment
  row_num <<- row_num + nrow(city_data)
  
})





# Check that UIDs are consecutive across files --------------------------------

uid_ok <- here("output_data/") %>% 
  dir(pattern = "^crime_open_database_core_(.+?).Rds$", full.names = TRUE) %>% 
  map_dfr(function (x) {
    read_rds(x) %>% 
      summarise(city = first(city_name), year = year(first(date_single)), 
                uid_min = min(uid), uid_max = max(uid))
  }) %>% 
  mutate(uid_ok = ifelse(uid_min - 1 == lag(uid_max), "OK", "not OK"))





# Merge data by year ----------------------------------------------------------

# RDS files can only be used by R, so gzipped CSV files are also needed for 
# those using other software. Since most people will be downloading these 
# manually, the data from all cities are aggregated into one file per year.

walk(c("core", "extended", "sample"), function (file_type) {

  here("output_data/") %>% 
    dir(str_glue("^crime_open_database_{file_type}_(.+?).Rds$"), 
        full.names = TRUE) %>% 
    str_extract("\\d+") %>% 
    unique() %>% 
    walk(function (x) {
      
      here("output_data/") %>% 
        dir(str_glue("^crime_open_database_{file_type}_(.+?)_{x}.Rds$"), 
            full.names = TRUE) %>% 
        map_dfr(read_rds) %>% 
        write_rds(here(str_glue("output_data/crime_open_database_{file_type}_",
                                "all_{x}.Rds")),
                  compress = "bz2") %>% 
        write_csv(
          here(str_glue("output_data/crime_open_database_{file_type}_{x}",
                        ".csv.gz")),
          na = ""
        )
      
      message(str_glue("Merged {file_type} data for {x} into one RDS and one ",
                       "CSV file"))
      
    })
  
})
