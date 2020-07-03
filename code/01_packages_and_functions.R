# This file loads the packages and functions necessary to run the scripts for 
# this project.



# LOAD PACKAGES

# install non-CRAN packages
# remotes::install_github("hrbrmstr/rgeocodio")
# remotes::install_github("hrbrmstr/slugify")

library("censusxy")  # US Census geocoding
library("here")      # full file paths
library("lubridate") # date handling
library("rgeocodio") # geocod.io geocoding
library('sf')        # handle spatial data
library("slackr")    # send Slack messages
library("slugify")   # make strings URL-safe
library('tidyverse') # load tidyverse last



# SET UP DIRECTORIES

if (!dir.exists(here::here("failure_records")))
  dir.create(here::here("failure_records"))
if (!dir.exists(here::here("geocoding_data")))
  dir.create(here::here("geocoding_data"))
if (!dir.exists(here::here("original_data")))
  dir.create(here::here("original_data"))
if (!dir.exists(here::here("output_data")))
  dir.create(here::here("output_data"))
if (!dir.exists(here::here("temp_data")))
  dir.create(here::here("temp_data"))



# SET PARAMETERS
slackr_setup()

# Load API keys
# source(here::here("code/api_keys.R"))

yearFirst <- 2007
yearLast <- 2019

nibrs_categories <- here::here("crime_categories/nibrs_categories.csv") %>% 
  read_csv()

cities <- tribble(
  ~name, ~fips, ~prefix,
  "Austin",         "48", "aus",
  "Boston",         "25", "bos",
  "Chicago",        "17", "chi",
  "Detroit",        "26", "dtt",
  "Fort Worth",     "48", "ftw",
  "Kansas City",    "29", "kcm",
  "Los Angeles",    "06", "lax",
  "Louisville",     "21", "lou",
  # "Memphis",        "47", "mem",
  "Mesa",           "04", "mes",
  "Nashville",      "47", "nvl",
  "New York",       "36", "nyc",
  "San Francisco",  "06", "sfo",
  "Seattle",        "53", "sea",
  "St Louis",       "29", "stl",
  "Tucson",         "04", "tus",
  "Virginia Beach", "51", "vib"
)

common_vars <- c(
  'uid',
  'city_name',
  'nibrs_offense_code',
  'nibrs_offense_type',
  'nibrs_offense_category',
  'nibrs_crime_against',
  'date_single',
  'date_start',
  'date_end',
  'date_year',
  'multiple_dates',
  'address',
  'longitude',
  'latitude',
  "location_type",
  "location_category",
  'fips_state',
  'fips_county',
  'tract',
  'block_group',
  'block',
  'in_city',
  'geometry'
)



# FUNCTIONS

# Report status within pipeline
# This function passes through an object while printing a message and optionally
# printing a summary of the object.
report_status <- function(data, message, summary = FALSE) {
  
  # if necessary, add a summary of the object to the message
  if (summary == TRUE) {
    message <- paste0(
      message, 
      " (data is ", 
      class(data), 
      ifelse(is.null(nrow(data)) | is.null(ncol(data)), 
             paste(" of length", length(data)),
             paste(" with", ncol(data), "columns and", nrow(data), "rows")), 
      ")")
  }
  
  # print message
  message(message, appendLF = TRUE)
  
  # return the object unchanged
  data
  
}



# Download crime data file
download_crime_data <- function (url, city) {
  
  http_status <- httr::GET(
    url, 
    httr::write_disk(here::here(str_glue("original_data/raw_{city}.csv"))),
    httr::progress()
  )
  
  if (http_status$status_code == 200) {
    report_status(NULL, 
                  str_glue("Downloaded file to original_data/raw_{city}.csv"))
  } else {
    warning(str_glue("Attempt to download file {url} resulted in HTTP status ",
                     "{http_status$status_code}"))
  }
  
  Sys.sleep(1)
  
}



# Read crime data into memory, cleaning file names by default
read_crime_data <- function (city, col_types = NULL, clean_names = TRUE, ...) {
  
  if (str_sub(city, end = 1) == "/") {
    file_data <- readr::read_csv(city, col_types = col_types, ...)
  } else {
    file_data <- readr::read_csv(
      here::here(str_glue("original_data/raw_{city}.csv")),
      col_types = col_types,
      ...
    )
  }
  
  if (clean_names == TRUE) {
    file_data <- janitor::clean_names(file_data)
  }
  
  report_status(NULL, str_glue("Read file from {city}"))
  
  file_data
  
}



# Add date to data
# This function extracts a date from the data, converts it to a common format
# and creates a new column showing the year (which is needed for filtering 
# later).
add_date_var <- function (data, field_name, date_format, tz) {
  if (!is_tibble(data)) {
    stop("data must be a tibble")
  }
  if (!is.character(field_name)) {
    stop("field_name must be a character string")
  }
  if (!has_name(data, field_name)) {
    stop("field_name must be the name of a column in data")
  }
  if (!is.character(date_format)) {
    stop("date_format must be a character string")
  }
  if (!is.character(tz)) {
    stop("tz must be a character string")
  }
  
  data <- data %>% mutate(
    date_temp = parse_date_time((!!as.name(field_name)), date_format, tz = tz),
    date_year = year(date_temp),
    date_single = strftime(date_temp, format = '%Y-%m-%d %H:%M', tz = tz),
    multiple_dates = FALSE
  ) %>% 
    select(-date_temp)
  
  if (sum(is.na(data$date_single)) > 0) {
    message("\n✖︎", format(sum(is.na(data$date_single)), big.mark = ","), 
            "dates could not be parsed. Sample of original field:\n")
    data %>% 
      filter(is.na(data$date_single)) %>% 
      sample_n(ifelse(sum(is.na(data$date_single)) > 10, 10, 
                      sum(is.na(data$date_single)))) %>% 
      { print(.[[field_name]]) }
  } else {
    message("✔︎ All dates parsed\n")
  }
  
  data
}



# Filter data by year
filter_by_year <- function (data, year_first, year_last) {
  if (!is_tibble(data)) {
    stop("data must be a tibble")
  }
  if (!has_name(data, "date_year")) {
    stop("data must include a column named 'date_year'")
  }
  if (!is.numeric(year_first) | !is.numeric(year_last)) {
    stop("year_first and year_last must be integers")
  }
  
  year_range <- range(data$date_year, na.rm = TRUE)
  
  message(str_glue("Original data includes {scales::comma(nrow(data))} crimes ", 
          "between {year_range[1]} and {year_range[2]}\n"))
  
  filtered_data <- data %>% 
    filter(date_year >= year_first & date_year <= year_last)
  
  year_range <- range(filtered_data$date_year)
  
  message(str_glue("{scales::comma(nrow(data) - nrow(filtered_data))} rows ",
                   "removed\nFiltered data includes ",
                   "{scales::comma(nrow(filtered_data))} crimes between ",
                   "{year_range[1]} and {year_range[2]}\n"))

  if (min(table(filtered_data$date_year)) < 1000) {
    warning("✖ Some years have fewer than 1,000 crimes\n")
    print(table(filtered_data$date_year))
  }
  
  report_status(NULL, str_glue("Filtered out data before {year_first} and ",
                         "after {year_last}"))
  
  filtered_data
  
}

# Join crime categories
join_nibrs_cats <- function (
  data, 
  file = "crime_categories/nibrs_categories.csv", 
  by = "nibrs_offense_code",
  check = TRUE
) {
  
  cats <- readr::read_csv(
    here::here(file), 
    col_types = cols(.default = col_character())
  )
  
  data <- dplyr::left_join(data, cats, by = by)
  
  if (check) check_nibrs_cats(data, file, by)
  
  report_status(NULL, "Matched cases to NIBRS categories")
  
  data
  
}



# Check if crime categories are correctly matched
check_nibrs_cats <- function (data, file, by) {
  
  failure_file_name <- str_glue("failure_records/nibrs_coding_failures_",
                                "{strftime(now(), '%Y%m%d_%H%M')}.csv")

  if (
    sum(is.na(data$nibrs_offense_code)) > 0 |
    sum(is.na(data$nibrs_offense_type)) > 0 |
    sum(is.na(data$nibrs_offense_category)) > 0 |
    sum(is.na(data$nibrs_crime_against)) > 0
  ) {
    warning(str_glue("✖ some cases could not be matched to NIBRS categories\n",
                     "see {failure_file_name} for details\n"))
    data %>% filter(
      is.na(nibrs_offense_code) |
        is.na(nibrs_offense_type) |
        is.na(nibrs_offense_category) |
        is.na(nibrs_crime_against)
    ) %>% 
      group_by_at(vars(one_of(c(by, "date_year")))) %>% 
      summarise(n = n()) %>% 
      spread(date_year, n) %>% 
      write_csv(here::here(failure_file_name))
  } else {
    message("✔︎ All cases matched to NIBRS categories\n")
  }
  
  data
  
}



# geocode addresses using the US Census Bureau geocoder
census_geocode_file <- function (data) {
  
  # check inputs
  if (!is.data.frame(data))
    stop("`data` must be a data frame")
  if (nrow(data) > 10000)
    warning("`data` contains >10,000 rows, but only the first 10,000 will be ",
            "processed")
  
  # create temporary file
  data_file <- tempfile(fileext = ".csv")
  write.table(data, file = data_file, sep = ",", col.names = FALSE)
  
  # send file to geocoder, retrying if necessary since the server is fragile
  # response <- httr::POST(
  response <- httr::RETRY(
    "POST",
    url = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch",
    body = list(
      addressFile = httr::upload_file(data_file), 
      benchmark = 9
    ),
    encode = "multipart",
    # httr::timeout(1200)
    pause_base = 5
  )
  
  # check response
  if (response$status_code == 200) {
    
    result <- dplyr::mutate_if(read.csv(
      text = httr::content(response, encoding = "UTF-8"), 
      header = FALSE, 
      col.names = c("id", "query", "match", "exact", "address", "coords", 
                    "tiger_line_id", "side")
    ), is.character, ~ ifelse(. == "", NA_character_, .))
    
    # report result
    message(glue::glue("Geocoded {sum(result$match == 'Match')} of ",
                       "{nrow(data)} records"))
    
  } else {
    
    httr::warn_for_status(response)
    
    result <- data.frame(
      "id" = character(), "query" = character(), "match" = character(), 
      "exact" = character(), "address" = character(), "coords" = character(), 
      "tiger_line_id" = character(), "side" = character()
    )
    
  }
  
  result
  
}



# manage data for census geocoding
census_geocode <- function (street, city = NULL, state = NULL, zip = NULL) {
  
  # check if inputs are as expected
  if (is.null(city) & is.null(zip))
    stop("`city` and `zip` cannot both be NULL - provide one or both")
  if (!is.character(street))
    stop("`street` must be a character vector")
  if (!is.null(city) & !is.character(city))
    stop("`city` must be either a character vector or NULL")
  if (!is.null(state) & !is.character(state))
    stop("`state` must be either a character vector or NULL")
  if (!is.null(zip) & !is.character(zip) & !is.numeric(zip))
    stop("`zip` must be either a character/numeric vector or NULL")
  
  # if any inputs are null, convert them to empty strings
  if (is.null(city)) city <- ""
  if (is.null(state)) state <- ""
  if (is.null(zip)) zip <- ""
  
  # continue checking inputs
  if (length(city) != length(street) & length(city) != 1)
    stop("`city` must be length 1 or the same length as `street`")
  if (length(state) != length(street) & length(state) != 1)
    stop("`state` must be length 1 or the same length as `street`")
  if (length(zip) != length(street) & length(zip) != 1)
    stop("`zip` must be length 1 or the same length as `street`")
  
  input <- data.frame(street = street, city = city, state = state, zip = zip)
  
  # identify unique combinations
  unique_addresses <- unique.data.frame(input)
  
  # add batch number (each batch contains 10,000 rows)
  unique_addresses <- dplyr::nest_by(dplyr::mutate(
    unique_addresses,
    batch = ceiling(dplyr::row_number() / 500)
  ), batch)
  
  # geocode addresses, one batch at a time
  results <- unnest(
    tibble(result = map(unique_addresses$data, census_geocode_file)), 
    cols = "result"
  )

  # join results to original data
  results <- dplyr::left_join(
    dplyr::mutate(input, query = as.character(
      glue::glue("{street}, {city}, {state}, {zip}")
    )), 
    results,
    by = "query"
  )
  
  # return results
  results$coords
  
}



# join location types
join_location_types <- function (data, file, by) {
  dplyr::left_join(
    data,
    readr::read_csv(here::here(file), 
                    col_types = cols(.default = col_character())),
    by = by
  ) %>% 
    report_status("Matched location categories")
}



# Write temporary data file
# This function also adds a UID field to the data and returns a count of the 
# data.
save_city_data <- function (data, name) {
  
  if (!is_tibble(data)) stop("data should be a tibble")
  if (!is.character(name)) stop("name must be a character string")
  
  data <- data %>% mutate(uid = 1:nrow(.), city_name = str_to_title(name))
  
  city_tag <- str_replace_all(str_to_lower(name), "\\s", "_")
  
  write_rds(data, here::here(str_glue("temp_data/raw_{city_tag}_data.Rds")))

  data %>% 
    nrow() %>% 
    format(big.mark = ',') %>% 
    message("Data for", str_to_title(name), "contains", ., "rows\n")
  
  data
  
}



# Convert variables names to a common format
convert_names <- function (data, common_vars, prefix) {
  
  # get existing column names
  col_names <- names(data)

  # add prefix to city-specific column names
  names(data) <- ifelse(
    col_names %in% common_vars, 
    col_names, 
    paste(prefix, col_names, sep = '_')
  )
  
  data <- data %>%
    # harmonize variable order
    select(one_of(common_vars), everything()) %>%
    select(-geometry) %>%
    # convert local ID to character, since it is alphanumeric for some cities
    mutate_at(vars(one_of(c("local_row_id", "case_number"))), as.character) %>%
    mutate(
      # remove spurious precision from co-ordinates
      longitude = round(longitude, digits = 6),
      latitude = round(latitude, digits = 6),
      # remove code from the type variable, since it is already present in the
      # code variable
      nibrs_offense_type = str_replace(nibrs_offense_type, '^\\w{3}+ ', ''),
      # shorten the crimes-against variable to remove unnecessary characters
      nibrs_crime_against = recode(nibrs_crime_against,
                                   `Crimes against person` = 'Persons',
                                   `Crimes against property` = 'Property',
                                   `Crimes against society` = 'Society',
                                   `All Other Offenses` = 'Other')
    ) %>%
    # shorten the crime category variable names, both to remove unnecessary
    # characters and because the categories are not exactly the same as the
    # NIBRS classification
    rename(offense_code = nibrs_offense_code,
           offense_type = nibrs_offense_type,
           offense_group = nibrs_offense_category,
           offense_against = nibrs_crime_against) %>%
    # convert crime categories to lower case to ease matching when the data are
    # used
    mutate_at(vars(one_of('offense_type', 'offense_group', 'offense_against')),
              'tolower')
  
  # return data
  data
  
}

# print format number of rows
row_count <- function (x) scales::comma(nrow(x))

