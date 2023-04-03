# The code in this file harmonizes the format of data from each city so that
# they can be merged into a single dataset. This is composed of:
#
# 1. renaming variables that should have the same name across all cities 
# 2. removing redundant variables 
# 3. changing the type of some variables 
# 4. adding a prefix to any other variable names while tidying up variable names
#
# The data from each city are not merged in this file because they need to be
# checked first. This checking can be done more easily once the data are in a
# common format.
#
# The final step needs a list of variables that are not specific to each city
# and so do not need a prefix.





# Austin ----------------------------------------------------------------------

here("temp_data/spatial_austin_data.Rds") %>% 
  read_rds() %>% 
  select(-occurred_date_time, -census_tract) %>% 
  rename(local_row_id = incident_number) %>% 
  convert_names(common_vars, cities$prefix[cities$name == "Austin"]) %>% 
  write_rds(here("temp_data/final_austin_data.Rds"), compress = "gz") %>%
  glimpse()





# Boston ----------------------------------------------------------------------

here("temp_data/spatial_boston_data.Rds") %>% 
  read_rds() %>% 
  rename(local_row_id = incident_number) %>% 
  convert_names(common_vars, cities$prefix[cities$name == "Boston"]) %>% 
  write_rds(here("temp_data/final_boston_data.Rds"), compress = "gz") %>%
  glimpse()





# Chicago ---------------------------------------------------------------------

here("temp_data/spatial_chicago_data.Rds") %>% 
  read_rds() %>% 
  select(-date) %>% 
  mutate(
    arrest = as.logical(arrest),
    domestic = as.logical(domestic)
  ) %>% 
  rename(local_row_id = id) %>% 
  convert_names(common_vars, cities$prefix[cities$name == "Chicago"]) %>% 
  write_rds(here("temp_data/final_chicago_data.Rds"), compress = "gz") %>% 
  glimpse()





# Detroit ---------------------------------------------------------------------

here("temp_data/spatial_detroit_data.Rds") %>% 
  read_rds() %>% 
  select(-incidentdate, -hour) %>% 
  mutate(
    latitude = ifelse(latitude < -90 | latitude > 90, NA, latitude),
    longitude = ifelse(longitude < -180 | longitude > 180, NA, longitude),
    crimeid = as.numeric(crimeid)
  ) %>% 
  rename(local_row_id = crimeid, case_number = crno) %>% 
  convert_names(common_vars, cities$prefix[cities$name == "Detroit"]) %>% 
  write_rds(here("temp_data/final_detroit_data.Rds"), compress = "gz") %>% 
  glimpse()





# Fort Worth ------------------------------------------------------------------

here("temp_data/spatial_fort_worth_data.Rds") %>% 
  read_rds() %>% 
  select(-from_date, -city, -state) %>% 
  rename(
    local_row_id = case_and_offense, 
    case_number = case_number, 
    address = block_address,
    location_type_code = location_type
  ) %>% 
  convert_names(common_vars, cities$prefix[cities$name == "Fort Worth"]) %>% 
  write_rds(here("temp_data/final_fort_worth_data.Rds"), compress = "gz") %>% 
  glimpse()





# Kansas City -----------------------------------------------------------------

here("temp_data/spatial_kansas_city_data.Rds") %>% 
  read_rds() %>% 
  select(-reported_date, -reported_time, -from_date, -from_time, -to_date, 
         -to_time, -city, -location) %>% 
  rename(
    local_row_id = report_no
  ) %>% 
  convert_names(common_vars, cities$prefix[cities$name == "Kansas City"]) %>% 
  write_rds(here("temp_data/final_kansas_city_data.Rds"), compress = "gz") %>% 
  glimpse()





# Los Angeles -----------------------------------------------------------------

here("temp_data/spatial_los_angeles_data.Rds") %>% 
  read_rds() %>% 
  select(-date_occurred, -date_occ, -time_occ) %>% 
  rename(local_row_id = dr_no) %>% 
  convert_names(common_vars, cities$prefix[cities$name == "Los Angeles"]) %>% 
  write_rds(here("temp_data/final_los_angeles_data.Rds"), compress = "gz") %>% 
  glimpse()





# Louisville ------------------------------------------------------------------

here("temp_data/spatial_louisville_data.Rds") %>% 
  read_rds() %>% 
  select(-date_occured, -nibrs_code, -ucr_hierarchy, -city) %>% 
  rename(local_row_id = incident_number, address = block_address) %>% 
  convert_names(common_vars, cities$prefix[cities$name == "Louisville"]) %>% 
  write_rds(here("temp_data/final_louisville_data.Rds"), compress = "gz") %>% 
  glimpse()





# Mesa ------------------------------------------------------------------------

here("temp_data/spatial_mesa_data.Rds") %>% 
  read_rds() %>% 
  rename(local_row_id = crime_id) %>% 
  convert_names(common_vars, cities$prefix[cities$name == "Mesa"]) %>% 
  write_rds(here("temp_data/final_mesa_data.Rds"), compress = "gz") %>% 
  glimpse()





# Nashville -------------------------------------------------------------------

here("temp_data/spatial_nashville_data.Rds") %>% 
  read_rds() %>% 
  select(-incident_occurred) %>% 
  rename(local_row_id = primary_key, case_number = incident_number) %>% 
  convert_names(common_vars, cities$prefix[cities$name == "Nashville"]) %>% 
  write_rds(here("temp_data/final_nashville_data.Rds"), compress = "gz") %>% 
  glimpse()





# New York --------------------------------------------------------------------

here("temp_data/spatial_new_york_data.Rds") %>% 
  read_rds() %>% 
  select(-cmplnt_fr_dt, -cmplnt_fr_tm, -cmplnt_to_dt, -cmplnt_to_tm, 
         -x_coord_cd, -y_coord_cd, -lat_lon) %>% 
  rename(local_row_id = `cmplnt_num`) %>% 
  convert_names(common_vars, cities$prefix[cities$name == "New York"]) %>% 
  write_rds(here("temp_data/final_new_york_data.Rds"), compress = "gz") %>% 
  glimpse()





# San Francisco ---------------------------------------------------------------

here("temp_data/spatial_san_francisco_data.Rds") %>%
  read_rds() %>% 
  select(-incident_date_time) %>% 
  rename(local_row_id = incident_number) %>% 
  convert_names(common_vars, cities$prefix[cities$name == "San Francisco"]) %>% 
  write_rds(here("temp_data/final_san_francisco_data.Rds"), compress = "gz") %>% 
  glimpse()





# Seattle ---------------------------------------------------------------------

here("temp_data/spatial_seattle_data.Rds") %>% 
  read_rds() %>% 
  select(-report_date_time) %>% 
  rename(address = block_address, local_row_id = report_number, 
         case_number = offense_id) %>% 
  convert_names(common_vars, cities$prefix[cities$name == "Seattle"]) %>% 
  write_rds(here("temp_data/final_seattle_data.Rds"), compress = "gz") %>% 
  glimpse()





# St Louis --------------------------------------------------------------------

here("temp_data/spatial_st_louis_data.Rds") %>% 
  read_rds() %>% 
  # remove cases that represent the removal of crimes from the database
  filter(count == 1) %>% 
  select(
    -month_reportedto_mshp, -new_crime_indicator, -unfounded_crime_indicator, 
    -administrative_adjustment_indicator, -count
  ) %>% 
  mutate(address = paste(ileads_address, ileads_street)) %>% 
  convert_names(common_vars, cities$prefix[cities$name == "St Louis"]) %>% 
  write_rds(here("temp_data/final_st_louis_data.Rds"), compress = "gz") %>% 
  glimpse()





# Tucson ----------------------------------------------------------------------

here("temp_data/spatial_tucson_data.Rds") %>% 
  read_rds() %>% 
  select(-objectid, -city, -state, -date_occu, -hour_occu) %>% 
  rename(local_row_id = primary_key, case_number = inci_id) %>% 
  convert_names(common_vars, cities$prefix[cities$name == "Tucson"]) %>% 
  write_rds(here("temp_data/final_tucson_data.Rds"), compress = "gz") %>% 
  glimpse()

