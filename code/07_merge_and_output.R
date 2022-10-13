# This code outputs RDS files for each city for each year. These can be used to
# produce CSV files for multiple cities etc.
 
# Three files are produced for each city for each year. The 'extended' file 
# contains all the variables, the 'core' file contains only the common variables 
# (i.e. those without the city-name prefix) and the 'sample' file contains a 1% 
# sample of data in the core file.



# PREPARE ----------------------------------------------------------------------

# Remove unnecessary variables from common_vars
common_vars_final <- setdiff(
  common_vars, 
  c("geometry", "multiple_dates", "in_city", "address", "fips_state", 
    "fips_county", "tract", "block_group", "block", "date_year")
) %>% 
  # rename the crime category variables because they've been renamed in the 
  # dataset by the convert_names() function in file 05_harmonise_data.Rmd
  recode(
    "nibrs_offense_code" = "offense_code", 
    "nibrs_offense_type" = "offense_type",
    "nibrs_offense_category" = "offense_group",
    "nibrs_crime_against" = "offense_against"
  ) %>% 
  # add census_block to common vars so it appears in the core data
  append("census_block")

# set row number, which should be continuous across cities and years so must be
# remembered between files
row_num <- 1

# Find out which cities have already been processed
# cities_processed <- here::here("output_data/") %>% 
#   dir(pattern = "crime_open_database_sample_") %>% 
#   str_remove("^crime_open_database_sample_") %>% 
#   str_remove("_[0-9]{4}\\.Rds") %>% 
#   unique() %>% 
#   str_replace_all("_", " ") %>% 
#   str_to_title()



# EXPORT RDS DATA --------------------------------------------------------------

# for each city, load the data and output an Rds file for each year
# this must be done separately for each city to avoid the 'vector memory
# exhausted' problem on macOS described at
# http://r.789695.n4.nabble.com/R-3-5-0-vector-memory-exhausted-error-on-readBin-td4750237.html
# This is a for loop rather than walk() function because of the need for a
# persistent UID that changes between each iteration
for (city in cities$name) {
  
  this_city <- str_replace_all(str_to_lower(city), "\\s", "_")
  
  rlang::inform(str_glue("\n\nWriting data for {city} in …"))
  
  # load and process data
  city_data <- str_glue("temp_data/final_{this_city}_data.Rds") %>% 
    here::here() %>% 
    read_rds() %>% 
    # mutate(across(c(local_row_id, case_number), as.character)) %>% 
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
    select(one_of(common_vars_final), everything(), -in_city, -multiple_dates,
           -fips_state, -fips_county, -tract, -block_group, -block) %>% 
    mutate(year = year(date_single))
  
  # output data by year
  city_data %>% 
    split(.$year) %>% 
    walk(function (x) {
      
      this_year <- pluck(x, "year", 1)
      
      x %>%
        select(-year) %>% 
        # export extended data - use write_rds() rather than saveRDS() because
        # it returns the input invisibly for use inside a pipe
        write_rds(
          here::here(str_glue("output_data/crime_open_database_extended_{this_city}_{this_year}.Rds")),
          compress = "bz2"
        ) %>%
        # select core variables and export
        select(one_of(common_vars_final)) %>%
        write_rds(
          here::here(str_glue("output_data/crime_open_database_core_{this_city}_{this_year}.Rds")),
          compress = "bz2"
        ) %>% 
        # take a 1% sample and export
        sample_frac(0.01) %>% 
        arrange(uid) %>% 
        write_rds(
          here::here(str_glue("output_data/crime_open_database_sample_{this_city}_{this_year}.Rds")),
          compress = "bz2"
        )
      
      # message("Written data for ", city, " in ", this_year)
      rlang::inform(c("*" = as.character(this_year)))
      
    })
  
  row_num <- row_num + nrow(city_data)
  
}


## Check that UIDs are consecutive across files ----
here::here("output_data") %>% 
  dir(pattern = "^crime_open_database_core_(.+?).Rds$", full.names = TRUE) %>% 
  str_subset("_all_", negate = TRUE) %>% 
  map_dfr(function (x) {
    summarise(
      read_rds(x),
      city = first(city_name), 
      year = year(first(date_single)), 
      uid_min = min(uid), 
      uid_max = max(uid),
      rows = n()
    )
  }) %>% 
  mutate(uid_ok = ifelse(uid_min - 1 == lag(uid_max), "OK", "not OK")) %>% 
  View("uid_check")



# EXPORT CSV FILES -------------------------------------------------------------

# RDS files can only be used by R, so gzipped CSV files are also needed for
# those using other software. Since most people will be downloading these
# manually, the data from all cities are aggregated into one file per year.

walk(c("core", "extended", "sample"), function (file_type) {

  here::here("output_data") %>% 
    dir(
      pattern = str_glue("^crime_open_database_{file_type}_(.+?).Rds$"), 
      full.names = TRUE
    ) %>% 
    str_extract("\\d+") %>% 
    unique() %>% 
    walk(function (x) {
      
      csv_output_file <- here::here(
        str_glue("output_data/crime_open_database_{file_type}_{x}.csv.gz")
      )
      
      if (file.exists(csv_output_file)) {

        message(str_glue("Skipped {file_type} data for {x} because it already exists"))

      } else {
        
        rlang::inform(str_glue("Merging {file_type} data for {x}"))
        
        here::here("output_data") %>% 
          dir(
            pattern = str_glue("^crime_open_database_{file_type}_(.+?){x}.Rds$"),
            full.names = TRUE
          ) %>% 
          map_dfr(read_rds) %>%
          write_rds(
            here::here(str_glue("output_data/crime_open_database_{file_type}_all_{x}.Rds")),
            compress = "bz2"
          ) %>% 
          write.csv(
            gzfile(csv_output_file), 
            na = "", 
            row.names = FALSE, 
            fileEncoding = "UTF-8"
          )
        
        message(str_glue("Merged {file_type} data for {x} into one RDS and one CSV file"))
        
      }
      
    })
  
})

# Check there are three files for each city for each year, and six files for
# 'all' cities
dir("output_data") %>% 
  str_remove(fixed("crime_open_database_")) %>% 
  str_remove(fixed(".Rds")) %>% 
  str_remove(fixed(".csv.gz")) %>% 
  map_dfr(function(x) {
    tibble(
      city = str_sub(str_remove(x, "^(core|extended|sample)"), 2, -6),
      year = str_extract(x, "\\d{4}$")
    )
  }) %>% 
  mutate(city = if_else(city == "", "all", city)) %>% 
  count(city, year) %>% 
  pivot_wider(names_from = city, values_from = n) %>% 
  View()



# CALCULATE TYPICAL FILE SIZES -------------------------------------------------

cat(
  "\nMean size for core-data files:", 
  here::here("output_data") %>% 
    dir(pattern = "^crime_open_database_core_(.+?).csv.gz$", full.names = TRUE) %>% 
    file.size() %>% 
    mean() %>% 
    scales::number_bytes(),
  "\nMean size for extended-data files:",
  here::here("output_data") %>% 
    dir(pattern = "^crime_open_database_extended_(.+?).csv.gz$", full.names = TRUE) %>% 
    file.size() %>% 
    mean() %>% 
    scales::number_bytes(),
  "\nMean size for sample-data files:",
  here::here("output_data") %>% 
    dir(pattern = "^crime_open_database_sample_(.+?).csv.gz$", full.names = TRUE) %>% 
    file.size() %>% 
    mean() %>% 
    scales::number_bytes(),
  "\n"
)
