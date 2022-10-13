# The code in this file harmonises the format of data from each city so that
# they can be merged into a single dataset. This is composed of:
#
#   1. renaming variables that should have the same name across all cities 
#   2. removing redundant variables
#   3. changing the type of some variables 
#   4. adding a prefix to any other variable names while tidying up variable 
#      names
#
# The data from each city are not merged in this file because they need to be
# checked first. This checking can be done more easily once the data are in a
# common format.
#
# The final step needs a list of variables that are not specific to each city
# and so do not need a prefix.



# Austin -----------------------------------------------------------------------

here::here("temp_data/spatial_austin_data.Rds") %>% 
  report_status("\n\nAUSTIN") %>% 
  read_rds() %>% 
  report_status("loaded data") %>%
  select(-census_tract) %>%
  mutate(
    family_violence = recode(family_violence, "Y" = TRUE, .default = FALSE)
  ) %>%
  report_status("recoded variables") %>%
  rename(local_row_id = incident_number) %>%
  report_status("renamed variables") %>%
  convert_names(common_vars, cities$prefix[cities$name == "Austin"]) %>%
  report_status("harmonised variable names") %>%
  rename(aus_location_type = aus_location_type_raw) %>%
  write_rds(here::here("temp_data/final_austin_data.Rds"), compress = "gz") %>%
  report_status("saved data") %>%
  glimpse()



# Boston -----------------------------------------------------------------------

message("\n\nBOSTON")
here::here("temp_data/spatial_boston_data.Rds") %>% 
  read_rds() %>% 
  report_status("loaded data") %>%
  rename(local_row_id = incident_number) %>%
  report_status("renamed variables") %>%
  convert_names(common_vars, cities$prefix[cities$name == "Boston"]) %>%
  report_status("harmonised variable names") %>%
  write_rds(here::here("temp_data/final_boston_data.Rds"), compress = "gz") %>%
  report_status("saved data") %>%
  glimpse()



# Chicago ----------------------------------------------------------------------

message("\n\nCHICAGO")
here::here("temp_data/spatial_chicago_data.Rds") %>% 
  read_rds() %>% 
  report_status("loaded data") %>%
  select(-date, -x_coordinate, -y_coordinate, -year, -location) %>%
  mutate(
    arrest = as.logical(arrest),
    domestic = as.logical(domestic)
  ) %>%
  report_status("recoded variables") %>%
  rename(local_row_id = id) %>%
  report_status("renamed variables") %>%
  convert_names(common_vars, cities$prefix[cities$name == "Chicago"]) %>%
  report_status("harmonised variable names") %>%
  write_rds(here::here("temp_data/final_chicago_data.Rds"), compress = "gz") %>%
  report_status("saved data") %>%
  glimpse()



# Colorado Springs -------------------------------------------------------------

message("\n\nCOLORADO SPRINGS")
here::here("temp_data/spatial_colorado_springs_data.Rds") %>% 
  read_rds() %>% 
  report_status("loaded data") %>% 
  select(-occurred_from_date, -occurred_through_date, -city) %>% 
  rename(address = crime_location) %>%
  mutate(
    domestic_violence = as.logical(recode(domestic_violence, "Yes" = 1, "No" = 0)),
    across(where(is.instant), format, "%F %T")
  ) %>% 
  report_status("renamed variables") %>%
  convert_names(common_vars, cities$prefix[cities$name == "Colorado Springs"]) %>% 
  report_status("harmonised variable names") %>%
  write_rds(here::here("temp_data/final_colorado_springs_data.Rds"), compress = "gz") %>%
  report_status("saved data") %>%
  glimpse()



# Detroit ----------------------------------------------------------------------

message("\n\nDETROIT")
here::here("temp_data/spatial_detroit_data.Rds") %>% 
  read_rds() %>% 
  report_status("loaded data") %>%
  mutate(crime_id = as.numeric(crime_id)) %>% 
  report_status("recoded variables") %>%
  rename(
    local_row_id = crime_id, 
    case_number = report_number
  ) %>% 
  report_status("renamed variables") %>%
  convert_names(common_vars, cities$prefix[cities$name == "Detroit"]) %>% 
  report_status("harmonised variable names") %>%
  write_rds(here::here("temp_data/final_detroit_data.Rds"), compress = "gz") %>% 
  report_status("saved data") %>%
  glimpse()



# Kansas City ------------------------------------------------------------------

message("\n\nKANSAS CITY")
here::here("temp_data/spatial_kansas_city_data.Rds") %>% 
  read_rds() %>% 
  report_status("loaded data") %>%
  mutate(
    across(c(date_single, date_start, date_end), format, format = "%Y-%m-%d %H:%M")
  ) %>%
  report_status("recoded variables") %>%
  select(-reported_date, -reported_time, -from_date, -from_time, -to_date,
         -to_time, -city, -location, -ibrs) %>%
  rename(local_row_id = report_no) %>%
  report_status("renamed variables") %>%
  convert_names(common_vars, cities$prefix[cities$name == "Kansas City"]) %>%
  report_status("harmonised variable names") %>%
  write_rds(
    here::here("temp_data/final_kansas_city_data.Rds"), 
    compress = "gz"
  ) %>%
  report_status("saved data") %>%
  glimpse()



# Los Angeles ------------------------------------------------------------------

message("\n\nLOS ANGELES")
here::here("temp_data/spatial_los_angeles_data.Rds") %>% 
  read_rds() %>% 
  report_status("loaded data") %>%
  mutate(address = str_replace_all(location, "\\s+", " ")) %>%
  report_status("recoded variables") %>%
  select(-date_occurred, -date_rptd, -date_occ, -time_occ, -location) %>%
  rename(local_row_id = dr_no) %>%
  report_status("renamed variables") %>%
  convert_names(common_vars, cities$prefix[cities$name == "Los Angeles"]) %>%
  report_status("harmonised variable names") %>%
  write_rds(
    here::here("temp_data/final_los_angeles_data.Rds"), 
    compress = "gz"
  ) %>%
  report_status("saved data") %>%
  glimpse()



# Louisville -------------------------------------------------------------------

message("\n\nLOUISVILLE")
here::here("temp_data/spatial_louisville_data.Rds") %>% 
  read_rds() %>% 
  report_status("loaded data") %>%
  select(-date_occured, -nibrs_code, -ucr_hierarchy, -city) %>% 
  rename(
    local_row_id = id, 
    case_number = incident_number, 
    address = block_address
  ) %>% 
  report_status("renamed variables") %>%
  convert_names(common_vars, cities$prefix[cities$name == "Louisville"]) %>% 
  report_status("harmonised variable names") %>%
  write_rds(
    here::here("temp_data/final_louisville_data.Rds"), 
    compress = "gz"
  ) %>% 
  report_status("saved data") %>%
  glimpse()



# Memphis ----------------------------------------------------------------------

message("\n\nMEMPHIS")
here::here("temp_data/spatial_memphis_data.Rds") %>% 
  read_rds() %>% 
  report_status("loaded data") %>% 
  select(-offense_date, -coord1, -coord2, -location) %>% 
  rename(
    local_row_id = crime_id,
    address = x100_block_address
  ) %>% 
  report_status("renamed variables") %>%
  convert_names(common_vars, cities$prefix[cities$name == "Memphis"]) %>%
  report_status("harmonised variable names") %>%
  write_rds(here::here("temp_data/final_memphis_data.Rds"), compress = "gz") %>%
  report_status("saved data") %>%
  glimpse()



# Mesa -------------------------------------------------------------------------

message("\n\nMESA")
here::here("temp_data/spatial_mesa_data.Rds") %>% 
  read_rds() %>% 
  report_status("loaded data") %>%
  rename(local_row_id = crime_id) %>%
  report_status("renamed variables") %>%
  convert_names(common_vars, cities$prefix[cities$name == "Mesa"]) %>%
  report_status("harmonised variable names") %>%
  write_rds(here::here("temp_data/final_mesa_data.Rds"), compress = "gz") %>%
  report_status("saved data") %>%
  glimpse()



# Nashville --------------------------------------------------------------------

message("\n\nNASHVILLE")
here::here("temp_data/spatial_nashville_data.Rds") %>% 
  read_rds() %>% 
  report_status("loaded data") %>% 
  filter(incident_status_description != "UNFOUNDED") %>% 
  select(-incident_occurred, -incident_reported, -offense_number) %>% 
  rename(
    address = incident_location, 
    local_row_id = primary_key, 
    case_number = incident_number
  ) %>% 
  report_status("renamed variables") %>%
  convert_names(common_vars, cities$prefix[cities$name == "Nashville"]) %>%
  report_status("harmonised variable names") %>%
  write_rds(
    here::here("temp_data/final_nashville_data.Rds"), 
    compress = "gz"
  ) %>%
  report_status("saved data") %>%
  glimpse()

    

# New York ---------------------------------------------------------------------

message("\n\nNEW YORK")
here::here("temp_data/spatial_new_york_data.Rds") %>% 
  read_rds() %>% 
  report_status("loaded data") %>%
  select(-cmplnt_fr_dt, -cmplnt_fr_tm, -cmplnt_to_dt, -cmplnt_to_tm,
         -x_coord_cd, -y_coord_cd, -lat_lon) %>%
  rename(local_row_id = cmplnt_num) %>%
  report_status("renamed variables") %>%
  convert_names(common_vars, cities$prefix[cities$name == "New York"]) %>%
  report_status("harmonised variable names") %>%
  write_rds(
    here::here("temp_data/final_new_york_data.Rds"), 
    compress = "gz"
  ) %>%
  report_status("saved data") %>%
  glimpse()



# San Francisco ----------------------------------------------------------------

message("\n\nSAN FRANCISCO")
here::here("temp_data/spatial_san_francisco_data.Rds") %>% 
  read_rds() %>%
  report_status("loaded data") %>%
  rename(local_row_id = incident_number) %>%
  report_status("renamed variables") %>%
  convert_names(common_vars, cities$prefix[cities$name == "San Francisco"]) %>%
  report_status("harmonised variable names") %>%
  write_rds(
    here::here("temp_data/final_san_francisco_data.Rds"), 
    compress = "gz"
  ) %>%
  report_status("saved data") %>%
  glimpse()



# Seattle ----------------------------------------------------------------------

message("\n\nSEATTLE")
here::here("temp_data/spatial_seattle_data.Rds") %>% 
  read_rds() %>% 
  report_status("loaded data") %>%
  select(-report_date_time, -offense) %>% 
  rename(
    address = block_address, 
    case_number = report_number, 
    local_row_id = offense_id
  ) %>% 
  report_status("renamed variables") %>%
  convert_names(common_vars, cities$prefix[cities$name == "Seattle"]) %>%
  report_status("harmonised variable names") %>%
  write_rds(
    here::here("temp_data/final_seattle_data.Rds"), 
    compress = "gz"
  ) %>%
  report_status("saved data") %>%
  glimpse()



# St Louis ---------------------------------------------------------------------

# message("\n\nST LOUIS")
# here::here("temp_data/spatial_st_louis_data.Rds") %>% 
#   read_rds() %>% 
#   report_status("loaded data") %>%
#   filter(new_crime_indicator == "Y") %>% 
#   mutate(address = str_glue("{ileads_address} {ileads_street}")) %>% 
#   select(-month_reportedto_mshp, -date_occured, -flag_cleanup, -ileads_address, 
#          -ileads_street, -cad_address, -cad_street, -ycoord, -i_leads_add, 
#          -i_leads_approve, -beat, -i_leads_asg, -i_leads_type, 
#          -date_crime_coded) %>% 
#   rename(case_number = complaint) %>% 
#   report_status("renamed variables") %>%
#   convert_names(common_vars, cities$prefix[cities$name == "St Louis"]) %>%
#   report_status("harmonised variable names") %>%
#   write_rds(
#     here::here("temp_data/final_st_louis_data.Rds"), 
#     compress = "gz"
#   ) %>%
#   report_status("saved data") %>%
#   glimpse()



# Tucson -----------------------------------------------------------------------

# message("\n\nTUCSON")
# here::here("temp_data/spatial_tucson_data.Rds") %>% 
#   read_rds() %>% 
#   report_status("loaded data") %>%
#   select(-objectid, -date_occu, -hour_occu) %>%
#   rename(
#     local_row_id = primary_key, 
#     case_number = inci_id,
#     address = address_public
#   ) %>%
#   report_status("renamed variables") %>%
#   convert_names(common_vars, cities$prefix[cities$name == "Tucson"]) %>%
#   report_status("harmonised variable names") %>%
#   write_rds(here::here("temp_data/final_tucson_data.Rds"), compress = "gz") %>%
#   report_status("saved data") %>%
#   glimpse()



# Virginia Beach ---------------------------------------------------------------

message("\n\nVIRGINIA BEACH")
here::here("temp_data/spatial_virginia_beach_data.Rds") %>% 
  read_rds() %>% 
  report_status("loaded data") %>%
  select(-date_occured) %>%
  rename(local_row_id = police_case_number) %>%
  report_status("renamed variables") %>%
  convert_names(common_vars, cities$prefix[cities$name == "Virginia Beach"]) %>%
  report_status("harmonised variable names") %>%
  write_rds(
    here::here("temp_data/final_virginia_beach_data.Rds"), 
    compress = "gz"
  ) %>%
  report_status("saved data") %>%
  glimpse()
