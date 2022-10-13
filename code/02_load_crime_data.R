# The code in this file loads crime data from different cities in whatever
# format is provided, and converts it into a common format that can be joined
# together. Once processed, data from each city are saved to a temporary file to
# minimize memory usage. Temporary files are in .Rds format because it is likely
# to be smaller than a zipped CSV

# The processing done here is done via custom functions where possible, but with
# some aspects done using custom code to account for the data formats used by
# specific cities. The general process for each city is

#  1. download the data, mostly using custom function download_crime_data()
#  2. read the data using custom function read_crime_data()
#  3. harmonize date formats and add a year variable using custom function 
#     add_date_var()
#  4. filter data to keep only the correct years using custom function 
#     filter_by_year()
#  5. categorize offenses into NIBRS categories, mostly manually but sometimes
#     using custom function join_nibrs_cats()
#  6. check the categorization using custom function check_nibrs_cats()
#  7. categorize location types, mostly manually
#  8. save the data using custom function save_city_data()

# Progress is reported at the end of each step using custom function 
# report_status()





# Austin ----------------------------------------------------------------------

# Austin data are in one large CSV file from 2003 onward

# save data from website
# Source: 
download_crime_data(
  "https://data.austintexas.gov/api/views/fdj4-gpfu/rows.csv?accessType=DOWNLOAD", 
  "austin"
)

# process data
read_crime_data(
  "austin", 
  cols(
    .default = col_character(),
    `Highest Offense Code` = col_integer(),
    Latitude = col_double(),
    Longitude = col_double()
  )
) %>% 
  # add date variable
  add_date_var("occurred_date_time", "mdY IMS p", "America/Chicago") %>% 
  # add year variable and remove offenses before start date
  filter_by_year(yearFirst, yearLast) %>%
  # remove variables containing duplicate information
  select(-occurred_date, -occurred_time, -report_date, -report_time, 
         -x_coordinate, -y_coordinate, -location) %>% 
  # categorize crimes
  mutate(
    nibrs_offense_code = case_when(
      ucr_category == "120" ~ "12U",
      ucr_category == "220" ~ "22U",
      !is.na(ucr_category) ~ ucr_category,
      highest_offense_code %in% c(100, 102, 108) ~ "09A", # non-negligent homicide
      highest_offense_code == 103 ~ "09B", # negligent homicide
      highest_offense_code == 208 ~ "36B", # statutory rape
      highest_offense_code == 500 ~ "22A", # residential burglary
      highest_offense_code %in% c(608, 615, 621, 1102, 1198) ~ "26A", # false pretenses
      highest_offense_code %in% c(800, 801, 802) ~ "200", # arson
      highest_offense_code %in% c(900, 909, 2000, 2011, 2012, 2013) ~ "13A", # agg assault
      highest_offense_code == 902 & 
        highest_offense_description %in% c(
          "ASSAULT BY CONTACT", "ASSAULT BY CONTACT FAM/DATING") ~ 
        "13B", # simple assault
      highest_offense_code == 902 & 
        highest_offense_description == "ASSAULT CONTACT-SEXUAL NATURE" ~ 
        "11D", # fondling
      highest_offense_code %in% c(903, 906) ~ "13B", # simple assault
      highest_offense_code %in% c(901, 2701, 2702, 2704) ~ "13C", # intimidation
      highest_offense_code %in% c(902, 910, 1709) ~ "11D", # fondling
      highest_offense_code %in% c(1000, 1001, 1002, 1003, 1006, 1007, 1008, 
                                  1099, 1111) ~ "250", # forgery
      highest_offense_code %in% c(1005, 1103, 1106, 1108, 1112) ~ "26B", # credit card fraud
      highest_offense_code %in% c(1009, 1010, 1011) ~ "26A", # false pretenses
      highest_offense_code %in% c(1004, 4022, 4027) ~ "26C", # impersonation
      highest_offense_code %in% c(1100, 1101, 1104) ~ "90A", # bad checks
      highest_offense_code %in% c(1105) ~ "26A", # false pretenses
      highest_offense_code %in% c(1199) ~ "26U", # bad checks
      highest_offense_code %in% c(1200, 1202) ~ "270", # embezzlement
      highest_offense_code == 1300 ~ "280", # stolen property offenses
      highest_offense_code %in% c(1400, 1401, 1402, 3817, 3832) ~ "290", # destruction of property
      highest_offense_code %in% c(1500, 1501, 1502, 1503, 1504, 1506, 1507, 
                                  1509, 2408, 2409, 2410, 3304) ~ "520", # weapon offenses
      highest_offense_code == 1600 ~ "40A", # prostitution
      highest_offense_code %in% c(1601, 1602) ~ "40B", # assisting prostitution
      highest_offense_code == 1604 ~ "40C", # purchasing prostitution
      highest_offense_code %in% c(1603, 4199) ~ "64A", # human trafficking - sex
      highest_offense_code %in% c(1705, 1706, 2401, 2402, 2403, 2404, 2405, 
                                  2406, 2407, 2411, 2415, 3201, 3207, 3212, 
                                  3215, 3218, 3297, 3301, 3303, 3305, 3401) ~ 
        "90C", # disorderly conduct
      highest_offense_code %in% c(1710, 2001, 2003, 2004, 2005, 2006, 2008, 
                                  2009, 2010) ~ "90F", # family, nonviolent
      highest_offense_code == 1715 ~ "36A", # incest
      highest_offense_code %in% c(1800, 1801, 1802, 1803, 1804, 1805, 1806, 
                                  1807, 1808, 1809, 1810, 1811, 1812, 1813, 
                                  1814, 1815, 1819, 1820, 1821, 1822, 1823, 
                                  1825, 1826) ~ "35A", # drug violations
      highest_offense_code == 1818 ~ "35B", # drug equipment
      highest_offense_code %in% c(1900) ~ "39A", # betting
      highest_offense_code %in% c(1901, 1902, 1903) ~ "39B", # betting promotion
      highest_offense_code %in% c(1904, 1905) ~ "39C", # betting equipment
      highest_offense_code %in% c(2100, 2102, 2103, 2104, 2105, 2106, 2107, 
                                  2108, 2109, 2110, 2111) ~ "90D", # DUI
      highest_offense_code %in% c(2200, 2201, 2202, 2203, 2205, 2206, 2207, 
                                  2208, 2209, 2210, 3210, 3211) ~ 
        "90G", # liquor violations
      highest_offense_code %in% c(2300, 2302) ~ "90E", # drunkenness
      highest_offense_code %in% c(2416, 2500, 2501, 3203, 3213, 3214, 3217, 
                                  3295, 3302) ~ "90B", # curfew/vagrancy
      highest_offense_code %in% c(2417, 2609, 3414) ~ "90H", # peeping tom
      highest_offense_code %in% c(2600, 2601, 2602, 2603, 2605, 2610, 2611) ~ 
        "370", # porn
      highest_offense_code %in% c(2716, 2712, 2722, 2727, 2721) ~ 
        "90J", # trespass
      highest_offense_code %in% c(2718) ~ "210", # extortion/blackmail
      highest_offense_code %in% c(2800, 2801, 2802, 2803, 2805) ~ 
        "100", # kidnapping
      highest_offense_code %in% c(3102, 3111, 3116) ~ "510", # bribery
      highest_offense_code %in% c(616, 702, 904, 905, 907, 1110, 1301, 1711, 
                                  1799, 2099, 2412, 2413, 2606, 2607, 2608, 
                                  2612, 2700, 2703, 2705, 2706, 2707, 2708, 
                                  2709, 2710, 2711, 2712, 2715, 2717, 2719, 
                                  2720, 2723, 2724, 2728, 2731, 2732, 2733, 
                                  2735, 2899, 2900, 2901, 2902, 2903, 2904, 
                                  2905, 2907, 2908, 3001, 3002, 3003, 3006, 
                                  3007, 3008, 3009, 3010, 3020, 3021, 3103, 
                                  3104, 3105, 3106, 3107, 3108, 3109, 3112, 
                                  3114, 3115, 3117, 3118, 3200, 3205, 3206, 
                                  3209, 3216, 3294, 3296, 3298, 3299, 3300, 
                                  3306, 3310, 3311, 3312, 3313, 3395, 3396, 
                                  3397, 3398, 3399, 3402, 3604, 3720, 3724, 
                                  4111, 8905) ~ "90Z", # all other offenses
      highest_offense_code %in% c(107, 1505, 1824, 2002, 2400, 3722, 3400, 3442, 
                                  3458, 3459, 4100, 3403, 3113, 3829, 4003, 
                                  4200, 4202, 4203, 4204, 4205) ~ 
        "99Z" # non-criminal incidents
    )
  ) %>% 
  join_nibrs_cats() %>% 
  select(-ucr_category, -category_description) %>% 
  # identify location types
  rename(location_type_raw = location_type) %>% 
  join_location_types(
    file = "crime_categories/location_types_austin.csv", 
    by = "location_type_raw"
  ) %>% 
  # save data
  save_city_data("Austin") %>% 
  glimpse()





# Boston ----------------------------------------------------------------------

# save data from website
tribble(
  ~year, ~url,
  "2016", "https://data.boston.gov/dataset/6220d948-eae2-4e4b-8723-2dc8e67722a3/resource/b6c4e2c3-7b1e-4f4a-b019-bef8c6a0e882/download/crime-incident-reports-2016.csv",
  "2017", "https://data.boston.gov/dataset/6220d948-eae2-4e4b-8723-2dc8e67722a3/resource/64ad0053-842c-459b-9833-ff53d568f2e3/download/crime-incident-reports-2017.csv",
  "2018", "https://data.boston.gov/dataset/6220d948-eae2-4e4b-8723-2dc8e67722a3/resource/e86f8e38-a23c-4c1a-8455-c8f94210a8f1/download/crime-incident-reports-2018.csv",
  "2019", "https://data.boston.gov/dataset/6220d948-eae2-4e4b-8723-2dc8e67722a3/resource/34e0ae6b-8c94-4998-ae9e-1b51551fe9ba/download/script_113631134_20210423192813_combine.csv",
  "2020", "https://data.boston.gov/dataset/6220d948-eae2-4e4b-8723-2dc8e67722a3/resource/be047094-85fe-4104-a480-4fa3d03f9623/download/script_113631134_20210423193017_combine.csv",
  "2021", "https://data.boston.gov/dataset/6220d948-eae2-4e4b-8723-2dc8e67722a3/resource/f4495ee9-c42c-4019-82c1-d067f07e45d2/download/tmp7_f32p54.csv"
) %>% 
  mutate(year = str_glue("boston_{year}")) %>% 
  pwalk(~ download_crime_data(..2, ..1))

# process data
here::here("original_data") %>% 
  dir(pattern = "^raw_boston_", full.names = TRUE) %>% 
  map_dfr(read_crime_data, col_types = cols(.default = col_character())) %>% 
  rename(latitude = lat, longitude = long) %>% 
  # add date variable
  add_date_var("occurred_on_date", "Ymd T", "America/New_York") %>% 
  # filter by year
  filter_by_year(2016, yearLast) %>% 
  # remove variables containing duplicate information
  select(-year, -month, -day_of_week, -hour, -location) %>% 
  # categorize crimes
  mutate(
    # Pad `offense_code` to be five figures since some years are padded and some
    # are not
    offense_code = str_pad(offense_code, 5, side = "left", pad = "0"),
    nibrs_offense_code = case_when(
      offense_code_group == "Arson" | offense_code == "00900" ~ "200",
      offense_code_group == "Burglary - No Property Taken" ~ "22U",
      offense_code %in% c("00121", "00123") ~ "09B", # negligent manslaughter
      # this line must be after the lines above otherwise some incidents listed
      # as being UCR Part 3 but actually being within Part 2 will be incorrectly
      # excluded
      ucr_part %in% c("Other", "Part Three") ~ "99Z", # non-criminal incidents
      offense_code %in% c("00423", "00400", "00402", "00403", "00404", "00413") ~ "13A", # aggravated assault
      offense_code_group == "Simple Assault" | offense_code %in% c("00800", "00801") ~ "13B", # simple assault
      offense_code_group == "Biological Threat" |
        offense_code %in% c("02648", "03170") ~ "13C", # intimidation
      offense_code_group == "Auto Theft" | offense_code %in% c("00706", "00724", "00727") ~ "240",
      offense_code_group == "Ballistics" ~ "99Z",
      offense_code_group == "Commercial Burglary" ~ "22B",
      offense_code == "00111" ~ "09A",
      offense_code == "00611" ~ "23A", # Pocket-picking
      offense_code == "00612" ~ "23B", # Purse-snatching
      offense_code %in% c("00613", "00633") ~ "23C", # Shoplifting
      offense_code %in% c("00617", "00617", "00627", "00637") ~ "23D", # Theft From Building
      offense_code == "00618" ~ "23E", # Theft From Coin-Operated Machine
      offense_code %in% c("00614", "00634", "00531", "00641") ~ "23F", # Theft From Motor Vehicle except Parts
      offense_code == "00615" ~ "23G", # Theft of Motor Vehicle Parts
      offense_code %in% c("00616", "00619", "00629", "00600", "02632") ~ "23H", # All Other Larceny
      offense_code_group == "Other Burglary" | offense_code == "00500" ~ "22U",
      offense_code_group == "Residential Burglary" | offense_code == "00520" ~ "22A",
      offense_code == "00540" ~ "22B",
      offense_code %in% c("00301", "00371", "00381", "00338", "00339") ~ "12A", # personal robbery
      offense_code %in% c("00311", "00351", "00335") ~ "12B", # commercial robbery
      offense_code %in% c("00361", "00300") ~ "12U", # other robbery
      offense_code_group == "Bomb Hoax" ~ "13C", 
      offense_code_group == "Confidence Games" ~ "26A", 
      offense_code_group == "Counterfeiting" | offense_code == "01001" ~ "250", 
      offense_code_group == "Criminal Harassment" | 
        offense_code %in% c("02670", "02671") ~ "90Z", 
      offense_code_group == "Disorderly Conduct" | 
        offense_code %in% c("02401", "02403", "03304") ~ "90C", 
      offense_code_group == "Drug Violation" | 
        offense_code %in% c("01800", "01810", "01825") ~ "35A", 
      offense_code_group == "Evading Fare" ~ "23H",
      offense_code_group == "Embezzlement" | offense_code == "01201" ~ "270",
      offense_code_group %in% c("Explosives", "Firearm Violations") | 
        offense_code == "02618" ~ "520", 
      offense_code %in% c("01102") ~ "26A", 
      offense_code == "01106" ~ "26B",
      offense_code == "01107" ~ "26C", 
      offense_code == "01108" ~ "26D", 
      offense_code == "01109" ~ "26E", 
      offense_code_group == "Gambling" ~ "39A", 
      offense_code_group == "Operating Under the Influence" |
        offense_code %in% c("02101", "02102") ~ "90D", 
      offense_code == "03305" ~ "90E",
      offense_code_group == "Offenses Against Child / Family" | 
        offense_code %in% c("02006", "02007") ~ "90F", 
      offense_code_group == "Liquor Violation" | 
        offense_code %in% c("02204", "02646", "03111") ~ "90G", 
      offense_code_group == "Harassment" ~ "90Z", 
      offense_code %in% c("01500", "01501", "01504") ~ "520",
      offense_code %in% c("02511", "02611", "02622", "02664") ~ "100",
      offense_code == "02604" ~ "210",
      offense_code == "02610" ~ "90J",
      offense_code %in% c("02905", "02907", "02914") ~ "99Y",
      offense_code %in% c("02613", "02616", "02641", "02657", "02660", "02663", 
                          "02900", "02628") ~ "90Z",
      offense_code == "02623" ~ "370",
      offense_code == "02647" ~ "13C",
      offense_code_group == "Prisoner Related Incidents" |
        offense_code %in% c("02600", "02612", "02619") ~ "90Z", # all other offenses
      offense_code %in% c("01601", "01602", "01605") ~ "40A",
      offense_code == "01603" ~ "40B",
      offense_code_group == "Recovered Stolen Property" | offense_code %in% c("01300", "01304") ~ "280",
      offense_code_group == "Restraining Order Violations" ~ "90Z",
      offense_code_group == "Vandalism" | 
        offense_code %in% c("01400", "01415") |
        offense_description %in% c("M/V - LEAVING SCENE - PROPERTY DAMAGE", "VANDALISM") ~ "290",
      offense_code_group == "Violations" ~ "90Z",
      offense_code_group == "HOME INVASION" ~ "12A",
      offense_code_group == "HUMAN TRAFFICKING" ~ "64A",
      offense_code_group == "HUMAN TRAFFICKING - INVOLUNTARY SERVITUDE" ~ "64B",
      offense_code_group %in% c("INVESTIGATE PERSON", "Missing Person Reported", 
                                "Fire Related Reports") ~ "99Z",
      offense_code %in% c(
        "00735", "00736", "00990", "01832", "02608", "02642", "02950", "03001", 
        "03004", "02662", "03007", "03008", "03018", "03029", "03100", "03106", 
        "03108", "03110", "03112", "03116", "03119", "03122", "03123", "03125",
        "03126", "03130", "03200", "03202", "03203", "03205", "03207", "03300",
        "03402", "03403", "03501", "03502", "03503", "03625", "99999"
      ) | str_detect(offense_code, "^038") ~ "99Z", # non-criminal matters
      offense_description %in% c(
        "INVESTIGATE PERSON", "INVESTIGATE PROPERTY", "SICK ASSIST",
        "TOWED MOTOR VEHICLE", "SICK/INJURED/MEDICAL - PERSON",
        "PROPERTY - LOST/ MISSING", "M/V ACCIDENT - PROPERTY DAMAGE",
        "VERBAL DISPUTE"
      ) ~ "99Z",
      TRUE ~ NA_character_
    )
  ) %>% 
  join_nibrs_cats() %>%
  # save data
  save_city_data("Boston") %>%
  glimpse()





# Chicago ----------------------------------------------------------------------

# save data from website
# Source: https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2
download_crime_data(
  "https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD", 
  "chicago"
)

# process data
read_crime_data("chicago") %>% 
  rename(address = block) %>% 
  # add date variable
  add_date_var("date", "mdY T", "America/Chicago") %>% 
  # filter by year
  filter_by_year(yearFirst, yearLast) %>% 
  # add crime categories
  join_nibrs_cats("crime_categories/categories_chicago.csv",
                  by = c("primary_type", "description")) %>% 
  # separate burglaries and robberies into sub-categories
  mutate(nibrs_offense_type = case_when(
    # residential burglary
    # CONSIDER ADDING "HOTEL/MOTEL", "RESIDENTIAL YARD (FRONT/BACK)", 
    # "BOAT/WATERCRAFT", "DRIVEWAY - RESIDENTIAL"
    nibrs_offense_code == "220" & location_description %in% c(
      "RESIDENCE", "APARTMENT", "RESIDENCE-GARAGE", "CHA APARTMENT", 
      "RESIDENCE PORCH/HALLWAY", "NURSING HOME/RETIREMENT HOME", 
      "COLLEGE/UNIVERSITY RESIDENCE HALL"
    ) ~ "22A Residential Burglary/Breaking & Entering",
    nibrs_offense_code == "220" ~ 
      "22B Non-residential Burglary/Breaking & Entering",
    nibrs_offense_code == "120" & location_description %in% 
      c("SMALL RETAIL STORE", "GAS STATION", "RESTAURANT", 
        "GROCERY FOOD STORE", "BANK", "CONVENIENCE STORE", "DEPARTMENT STORE", 
        "DRUG STORE", "TAVERN/LIQUOR STORE", "BARBERSHOP", "CURRENCY EXCHANGE", 
        "CLEANING STORE", "BAR OR TAVERN", "COMMERCIAL / BUSINESS OFFICE", 
        "CAR WASH", "VEHICLE-COMMERCIAL", "CONSTRUCTION SITE", 
        "POLICE FACILITY/VEH PARKING LOT", "CHURCH/SYNAGOGUE/PLACE OF WORSHIP", 
        "WAREHOUSE", "DELIVERY TRUCK", "FACTORY/MANUFACTURING BUILDING", 
        "GOVERNMENT BUILDING/PROPERTY", "APPLIANCE STORE", 
        "MEDICAL/DENTAL OFFICE", "MOVIE HOUSE/THEATER", "PAWN SHOP", 
        "SAVINGS AND LOAN", "NEWSSTAND", "POOL ROOM", 
        "AIRPORT BUILDING NON-TERMINAL - NON-SECURE AREA", "CREDIT UNION", 
        "BOWLING ALLEY", "VEHICLE - DELIVERY TRUCK") ~ "12B Commercial Robbery",
    nibrs_offense_code == "120" & location_description == "OTHER" ~ 
      "12U Other Robbery",
    nibrs_offense_code == "120" ~ "12A Personal Robbery",
    TRUE ~ nibrs_offense_type
  ),
  # where necessary, update the NIBRS code to match the new type
  nibrs_offense_code = case_when(
    nibrs_offense_type == "22A Residential Burglary/Breaking & Entering" ~ "22A",
    nibrs_offense_type == "22B Non-residential Burglary/Breaking & Entering" ~ 
      "22B",
    nibrs_offense_type == "12A Personal Robbery" ~ "12A",
    nibrs_offense_type == "12B Commercial Robbery" ~ "12B",
    nibrs_offense_type == "12U Other Robbery" ~ "12U",
    TRUE ~ nibrs_offense_code
  )) %>% 
  # identify location types
  join_location_types(
    file = "crime_categories/location_types_chicago.csv", 
    by = c("location_description" = "Location Description")
  ) %>%
  # save data
  save_city_data("Chicago") %>%
  glimpse()





# Colorado Springs -------------------------------------------------------------

# Save data from website
# Source: https://policedata.coloradosprings.gov/Crime/Crime-Level-Data/bc88-hemr
download_crime_data(
  "https://policedata.coloradosprings.gov/api/views/bc88-hemr/rows.csv?accessType=DOWNLOAD",
  "colorado_springs"
)

# Process data
read_crime_data("colorado_springs") %>% 
  # Since Colorado Springs includes both start and end dates, so we will process 
  # the dates manually rather than using add_date_var()
  mutate(
    date_start = parse_date_time(occurred_from_date, "mdY IMS p"),
    date_end = parse_date_time(occurred_through_date, "mdY IMS p"),
    date_single = date_start + ((date_end - date_start) / 2),
    date_year = year(date_single),
    multiple_dates = TRUE
  ) %>% 
  # Filter by year
  filter_by_year(2016, yearLast) %>% 
  # Add crime categories
  rename(nibrs_offense_code = crime_code) %>% 
  mutate(
    # Replace codes that aren't used in CODE
    nibrs_offense_code = recode(
      nibrs_offense_code,
      "09C" = "99Z",
      "26F" = "26C",
      "26G" = "26E",
      "720" = "90Z",
      "90I" = "99Z",
      "99" = "99Z"
    ),
    # Split burglaries into res/non-res and robberies into personal/business
    nibrs_offense_code = case_when(
      nibrs_offense_code == "120" & str_detect(ncic_code_description, "Carjacking|Purse|Residence|Street") ~ "12A",
      nibrs_offense_code == "120" & str_detect(ncic_code_description, "Banking|Business") ~ "12B",
      nibrs_offense_code == "120" ~ "12U",
      nibrs_offense_code == "220" & str_detect(ncic_code_description, "Residence") ~ "22A",
      nibrs_offense_code == "220" ~ "22B",
      TRUE ~ nibrs_offense_code
    )
  ) %>%
  join_nibrs_cats() %>% 
  # Remove cases that were unfounded, which are left in the data because of
  # repeated public requests to the city
  filter(disposition != "Unfounded") %>% 
  # save data
  save_city_data("Colorado Springs") %>%
  glimpse()





# Detroit ---------------------------------------------------------------------

# Data prior to 2017 is no longer available on the city data portal. Instead we
# combine existing data in CODE with new data from the city portal.

# Detroit data changed format at the end of 2016. Although the column names
# changed, the information provided in the two files is largely the same.
# Offenses occurring on 6 December 2016 can appear in either file, but do not
# appear to be duplicated. The newer file contains far fewer records overall,
# but this is because of a substantial reduction in non-criminal incidents
# recorded in the later file. There are, however, some differences in the
# categorizations:
#
# * 11A Rape (except Statutory Rape), 11C Sexual Assault With An Object, 11D
#   Fondling, 36B Statutory Rape and 40A Prostitution only appear in the new 
#   file.
# * There is a large decrease in crimes in 13C Intimidation and 210
#   Extortion/Blackmail in 2017, for which there is no obvious cause. 
# * In the new file, commercial and personal robbery can no longer be 
#   distinguished, so almost all robberies are coded as 12U Other Robbery. 
#   Similarly it is no longer possible to distinguish between residential and 
#   non-residential burglary, so all burglaries from 2017 onward are classified 
#   as 22U Other Burglary/Breaking & Entering. 
# * There are large increases in 26B Credit Card/Automated Teller Machine Fraud 
#   and 26E Wire Fraud together with an even larger decreases in 26A False 
#   Pretenses/Swindle/Confidence Game and 270 Embezzlement. 
# * There is a large increase in 90F Family Offenses, Nonviolent offenses.

# save data from website
# download_crime_data(
#   "https://data.detroitmi.gov/api/views/invm-th67/rows.csv?accessType=DOWNLOAD", 
#   "detroit_early"
# )
download_crime_data(
  "https://opendata.arcgis.com/api/v3/datasets/871c66cb1faf458b9d80b5dba1fc08c8_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1", 
  "detroit_late"
)

# There is (as of 2022) a bug in `get_crime_data()` that means data for Detroit
# for 2010 is missing when downloaded separately, so this has to be extracted 
# from the all-cities file for 2010
detroit_2010 <- crimedata::get_crime_data(years = 2010, type = "extended") %>% 
  filter(city_name == "Detroit") %>% 
  discard(~ all(is.na(.x)))

# Download existing data from CODE and format it to look like the raw data
detroit_old <- crimedata::get_crime_data(
  years = 2009:2016, 
  cities = "Detroit", 
  type = "extended"
) %>% 
  bind_rows(detroit_2010) %>% 
  rename_all(~ str_remove(., "^dtt_")) %>% 
  select(
    x = longitude, 
    y = latitude, 
    crime_id = local_row_id, 
    report_number = case_number,
    address,
    offense_description,
    offense_category,
    arrest_charge,
    charge_description,
    incident_timestamp,
    scout_car_area,
    precinct,
    neighborhood,
    council_district,
    zip_code,
    oid
  ) %>% 
  mutate(across(everything(), as.character))

# process data
read_crime_data("detroit_late", col_types = cols(.default = col_character())) %>% 
  mutate(
    incident_timestamp = str_glue("{str_sub(incident_timestamp, 1, 10)} {incident_time}"),
    neighborhood = str_to_upper(neighborhood)
  ) %>% 
  bind_rows(detroit_old) %>% 
  # add data var
  add_date_var("incident_timestamp", "Ymd T", "America/Detroit") %>%
  # filter by year
  filter_by_year(2009, yearLast) %>% 
  # categorize crimes
  join_nibrs_cats(
    "crime_categories/categories_detroit.csv", 
    by = c("offense_category" = "category", "offense_description" = "offensedescription")
  ) %>%
  # save data
  save_city_data("Detroit") %>%
  glimpse()





# Fort Worth ------------------------------------------------------------------

# Data are from 
# https://data.fortworthtexas.gov/Public-Safety/Crime-Data/k6ic-7kp7
# 
# The `Offense` column seems to contain NIBRS codes, so it's just necessary to 
# match the codes to the offense categories. This can be done directly by using 
# the look-up table, so there is no need to create a mapping between local 
# offense codes and NIBRS codes as there is for other cities.

# save data from website
# download_crime_data(
#   "https://data.fortworthtexas.gov/api/views/k6ic-7kp7/rows.csv?accessType=DOWNLOAD", 
#   "fort_worth"
# )

# process data
# read_crime_data(
#   "fort_worth",
#   col_types = cols(
#     .default = col_character(),
#     `Case Number` = col_integer(),
#     `Council District` = col_integer(),
#     `Location Type` = col_integer()
#   )
# ) %>% 
#   mutate(location = str_replace(location, ".*\\((.+)\\)", "\\1")) %>% 
#   separate(location, c("latitude", "longitude"), ", ") %>% 
#   mutate_at(vars(c("latitude", "longitude")), as.double) %>% 
#   rename(nibrs_offense_code = offense, location_type_raw = location_type) %>% 
#   # add date variable
#   add_date_var("from_date", 'mdY T', "America/Chicago") %>% 
#   # filter by year
#   filter_by_year(yearFirst, yearLast) %>% 
#   # categorize crimes
#   mutate(
#     nibrs_offense_code = recode(
#       nibrs_offense_code, 
#       "09C" = "99Z", 
#       "26F" = "26C", 
#       "26G" = "26E", 
#       "720" = "90Z",
#       "90I" = "99Z"
#       "TRC" = "99Z"
#       "WAR" = "99Z"
#     )
#   ) %>% 
#   join_nibrs_cats() %>% 
#   mutate(nibrs_offense_type = case_when(
#     nibrs_offense_code == "220" & location_description == "Residence, home" ~ 
#       "22A Residential Burglary/Breaking & Entering",
#     nibrs_offense_code == "220" ~ 
#       "22B Non-residential Burglary/Breaking & Entering",
#     nibrs_offense_code == "120" & location_description %in% c(
#       "Highway, street, roadway, alley", "Parking lot, garage", 
#       "Residence, home", "Field, woods", "Park/Playground", 
#       "Airplane, bus, train terminal", "School, college", "Shopping Mall",
#       "Lake, waterway", "School-Elementary/Secondary", "ATM separate from Bank",
#       "Rest Area", "School-College/University") ~ "12A Personal Robbery",
#     nibrs_offense_code == "120" & location_description == "Other, unknown" ~ 
#       "12U Other Robbery",
#     nibrs_offense_code == "120" ~ "12B Commercial Robbery",
#     TRUE ~ nibrs_offense_type
#   ),
#   # where necessary, update the NIBRS code to match the new type
#   nibrs_offense_code = case_when(
#     nibrs_offense_type == "22A Residential Burglary/Breaking & Entering" ~ 
#       "22A",
#     nibrs_offense_type == "22B Non-residential Burglary/Breaking & Entering" ~ 
#       "22B",
#     nibrs_offense_type == "12A Personal Robbery" ~ "12A",
#     nibrs_offense_type == "12B Commercial Robbery" ~ "12B",
#     nibrs_offense_type == "12U Other Robbery" ~ "12U",
#     TRUE ~ nibrs_offense_code
#   )) %>% 
#   # identify location types
#   join_location_types(
#     file = "crime_categories/location_types_fort_worth.csv",
#     by = "location_description"
#   ) %>% 
#   save_city_data("Fort Worth") %>% 
#   glimpse()





# Kansas City -----------------------------------------------------------------

# Data are from 
# https://data.kcmo.org/browse?category=Crime&limitTo=datasets&sortBy=alpha&utf8=✓&page=2
# in annual files from 2009 onward, with slight differences in the variable 
# names and formats across years. All variables will be loaded as characters and 
# then changed later.

# download data from website
tribble(
  ~year, ~url,
  "2009", "https://data.kcmo.org/api/views/6efz-664k/rows.csv?accessType=DOWNLOAD",
  "2010", "https://data.kcmo.org/api/views/tk79-kf9y/rows.csv?accessType=DOWNLOAD",
  "2011", "https://data.kcmo.org/api/views/nt2f-uxvx/rows.csv?accessType=DOWNLOAD",
  "2012", "https://data.kcmo.org/api/views/csec-aghy/rows.csv?accessType=DOWNLOAD",
  "2013", "https://data.kcmo.org/api/views/m3xd-e7tp/rows.csv?accessType=DOWNLOAD",
  "2014", "https://data.kcmo.org/api/views/yu5f-iqbp/rows.csv?accessType=DOWNLOAD",
  "2015", "https://data.kcmo.org/api/views/kbzx-7ehe/rows.csv?accessType=DOWNLOAD",
  "2016", "https://data.kcmo.org/api/views/wbz8-pdv7/rows.csv?accessType=DOWNLOAD",
  "2017", "https://data.kcmo.org/api/views/98is-shjt/rows.csv?accessType=DOWNLOAD",
  "2018", "https://data.kcmo.org/api/views/dmjw-d28i/rows.csv?accessType=DOWNLOAD",
  "2019", "https://data.kcmo.org/api/views/pxaa-ahcm/rows.csv?accessType=DOWNLOAD",
  "2020", "https://data.kcmo.org/api/views/vsgj-uufz/rows.csv?accessType=DOWNLOAD",
  "2021", "https://data.kcmo.org/api/views/w795-ffu6/rows.csv?accessType=DOWNLOAD"
) %>% 
  mutate(year = str_glue("kansas_city_{year}")) %>% 
  pwalk(~ download_crime_data(..2, ..1))
  
# process data
here::here("original_data") %>% 
  dir(pattern = "^raw_kansas_city_", full.names = TRUE) %>% 
  map_dfr(function (x) {
    
    # load data
    kc_data <- read_crime_data(x, col_types = cols(.default = col_character()))
    
    # harmonize names
    if ("location_1" %in% names(kc_data)) {
      kc_data <- rename(kc_data, location = location_1)
    }
    
    # return data
    kc_data
    
  }) %>% 
  # extract address from location
  mutate(
    location_string = str_extract(location, "\\n\\(.+?\\)$") %>% 
      trimws() %>% 
      str_sub(2, -2),
  ) %>% 
  separate(location_string, into = c("latitude", "longitude"), sep = ", ") %>% 
  # add date variable
  # since KC data has multiple dates, and some are sometimes missing, this is
  # done manually rather than using add_date_var()
  mutate_at(vars(reported_date, from_date, to_date), ~ str_sub(., end = 10)) %>% 
  mutate_at(vars(reported_time, from_time, to_time), 
            ~ str_pad(., 5, pad = "0")) %>% 
  mutate(
    # create date/time objects
    reported_date_time = parse_date_time(
      paste(reported_date, reported_time), 
      orders = "mdY HM", 
      tz = "America/Chicago"
    ),
    date_start = parse_date_time(
      paste(from_date, from_time), 
      orders = "mdY HM", 
      tz = "America/Chicago"
    ),
    date_end = parse_date_time(
      paste(str_extract(to_date, "^[0-9\\/]+"), to_time), 
      orders = "mdY HM", 
      tz = "America/Chicago"
    ),
    # when start and end times are missing, use reported time
    date_start = as.POSIXct(
      ifelse(is.na(date_start), reported_date_time, date_start),
      origin = "1970-01-01",
      tz = "America/Chicago"
    ),
    # when end time is missing, use start time
    date_end = as.POSIXct(
      ifelse(is.na(date_end), date_start, date_end),
      origin = "1970-01-01",
      tz = "America/Chicago"
    ),
    # calculate mid-point
    date_single = date_start + ((date_end - date_start) / 2),
    date_year = year(date_single),
    multiple_dates = TRUE
  ) %>% 
  # dates are stored as strings, for consistent formatting (this is normally
  # done by add_date_var())
  mutate_if(is.POSIXct, strftime, format = '%Y-%m-%d %H:%M', 
            tz = "America/Chicago") %>% 
  filter_by_year(2009, yearLast) %>% 
  # categorize crimes
  mutate(
    # replace some NIBRS codes
    nibrs_offense_code = case_when(
      ibrs %in% c("09C", "09D", "90I") ~ "99Z", # justifiable homicide
      ibrs == "26F" ~ "26C", # identity theft
      ibrs == "26G" ~ "26E", # computer hacking
      ibrs == "720" ~ "90Z", # animal cruelty
      ibrs == "220" & description %in% c("Burglary - Resid", 
                                         "Burglary - Residence", "burglary res", 
                                         "burglary-res") ~ "22A",
      ibrs == "220" & description %in% c("Burglary - Non Resid", 
                                         "BURGLARY NON RES") ~ "22B",
      ibrs == "220" ~ "22U",
      is.na(ibrs) ~ "99Z", # non-criminal matters
      TRUE ~ ibrs
    )
  ) %>% 
  join_nibrs_cats() %>% 
  # save data
  save_city_data("Kansas City") %>%
  glimpse()





# Los Angeles -----------------------------------------------------------------


# LA data include multiple offenses occurring during the same incident in a
# single row, so to make them equivalent to the other cities these data should
# be converted from long to wide.
#
# Most categories can be extracted from the `Crime Code Description` field, but
# for some offenses it may be necessary to look at the MO used, as coded in the
# `MO Codes` field. The `MO Codes` field is a space-separated list of codes,
# meaning a look-up table can't be created as for other cities. As such, to
# generate the NIBRS for LA data, we need to:
# 
#   1. use the `Crime Code Description` field and -- where necessary -- the 
#      `MO Codes` field to allocate a NIBRS category to each record, then
#   2. use that NIBRS category to match all the other NIBRS columns.

# save data from website
download_crime_data(
  "https://data.lacity.org/api/views/63jg-8b9z/rows.csv?accessType=DOWNLOAD", 
  "los_angeles_old"
)
download_crime_data(
  "https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD",
  "los_angeles_new"
)

# process data
la_data <- here::here("original_data") %>% 
  dir(pattern = "^raw_los_angeles_", full.names = TRUE) %>% 
  map_dfr(read_crime_data, col_types = cols(.default = col_character())) %>% 
  pivot_longer(
    cols = c(crm_cd_1, crm_cd_2, crm_cd_3, crm_cd_4), 
    names_to = NULL, 
    values_to = "crime_type", 
    values_drop_na = TRUE
  ) %>% 
  mutate(
    date_occurred = paste(str_sub(date_occ, 1, 10), time_occ),
    location = str_replace_all(location, "\\s+?", " "),
    across(c(lon, lat), as.numeric)
  ) %>% 
  # add date var
  add_date_var("date_occurred", "mdY HM", "America/Los_Angeles") %>% 
  # filter by year
  filter_by_year(yearFirst, yearLast) %>% 
  # Rename some variables for use in the code below, since the variable names in
  # the data have changed since the code was written and it is safer to rename
  # the columns than change all the code and track-down side effects
  rename(
    crime_code_description = crm_cd_desc, 
    mo_codes = mocodes,
    premise_description = premis_desc
  ) %>% 
  # categorize crimes
  mutate(
    # convert empty `MO Codes` fields from NA to an empty string, which makes it 
    # easier to search that field later
    mo_codes = if_else(is.na(mo_codes), "", mo_codes),
    nibrs_offense_type = case_when(
      
      # Arson
      crime_code_description %in% c('ARSON') ~ '200 Arson',
      
      # Assault
      crime_code_description %in% c(
        'ASSAULT WITH DEADLY WEAPON ON POLICE OFFICER', 
        'ASSAULT WITH DEADLY WEAPON, AGGRAVATED ASSAULT', 
        'INTIMATE PARTNER - AGGRAVATED ASSAULT', 
        'CHILD ABUSE (PHYSICAL) - AGGRAVATED ASSAULT') ~ 
        '13A Aggravated assault',
      crime_code_description %in% c(
        'BATTERY - SIMPLE ASSAULT', 'BATTERY ON A FIREFIGHTER', 
        'BATTERY POLICE (SIMPLE)', 'INTIMATE PARTNER - SIMPLE ASSAULT', 
        'OTHER ASSAULT', 'CHILD ABUSE (PHYSICAL) - SIMPLE ASSAULT', 
        'RESISTING ARREST') ~ '13B Simple assault',
      crime_code_description %in% c('CRIMINAL THREATS - NO WEAPON DISPLAYED', 
                                    'STALKING', 'BOMB SCARE', 'THREATENING PHONE CALLS/LETTERS',
                                    "LETTERS, LEWD  -  TELEPHONE CALLS, LEWD") ~ 
        '13C Intimidation',
      
      # Bribery
      crime_code_description %in% c('BRIBERY') ~ '510 Bribery',
      
      # Burglary/Breaking & Entering
      crime_code_description %in% c('BURGLARY', 'BURGLARY, ATTEMPTED') &
        premise_description %in% c("SINGLE FAMILY DWELLING", 
                                   "MULTI-UNIT DWELLING (APARTMENT, DUPLEX, ETC)", "GARAGE/CARPORT", 
                                   "OTHER RESIDENCE", "CONDOMINIUM/TOWNHOUSE", 
                                   "MOBILE HOME/TRAILERS/CONSTRUCTION TRAILERS/RV'S/MOTORHOME", 
                                   "NURSING/CONVALESCENT/RETIREMENT HOME", 
                                   "FRAT HOUSE/SORORITY/DORMITORY", "PROJECT/TENEMENT/PUBLIC HOUSING", 
                                   "GROUP HOME", "PORCH, RESIDENTIAL", "FOSTER HOME BOYS OR GIRLS*") ~ 
        "22A Residential Burglary/Breaking & Entering",
      crime_code_description %in% c('BURGLARY', 'BURGLARY, ATTEMPTED') ~ 
        "22B Non-residential Burglary/Breaking & Entering",
      
      # Counterfeiting/Forgery
      crime_code_description %in% c('COUNTERFEIT', 
                                    'DOCUMENT FORGERY / STOLEN FELONY') ~ '250 Counterfeiting/Forgery',
      
      # Destruction/Damage/Vandalism of Property (except Arson)
      crime_code_description %in% c(
        'VANDALISM - FELONY ($400 & OVER, ALL CHURCH VANDALISMS) 0114', 
        'VANDALISM - MISDEAMEANOR ($399 OR UNDER)', 
        'TELEPHONE PROPERTY - DAMAGE', "TRAIN WRECKING", 
        "VANDALISM - FELONY ($400 & OVER, ALL CHURCH VANDALISMS)") ~ 
        '290 Destruction/Damage/Vandalism of Property (except Arson)',
      
      # Embezzlement
      crime_code_description %in% c(
        'EMBEZZLEMENT, GRAND THEFT ($950.01 & OVER)', 
        'EMBEZZLEMENT, PETTY THEFT ($950 & UNDER)', 
        'DISHONEST EMPLOYEE - GRAND THEFT', 'DISHONEST EMPLOYEE - PETTY THEFT', 
        'DISHONEST EMPLOYEE ATTEMPTED THEFT') ~ '270 Embezzlement',
      
      # Extortion/Blackmail
      crime_code_description %in% c('EXTORTION') ~ '210 Extortion/Blackmail',
      
      # Fraud Offenses (except Counterfeiting/Forgery and Bad Checks)
      crime_code_description %in% c(
        'DEFRAUDING INNKEEPER/THEFT OF SERVICES, $400 & UNDER', 
        'DEFRAUDING INNKEEPER/THEFT OF SERVICES, OVER $400', 'BUNCO, ATTEMPT', 
        'BUNCO, GRAND THEFT', 'BUNCO, PETTY THEFT', 
        'GRAND THEFT / INSURANCE FRAUD') ~ 
        '26A False Pretenses/Swindle/Confidence Game',
      crime_code_description %in% c('CREDIT CARDS, FRAUD USE ($950 & UNDER', 
                                    'CREDIT CARDS, FRAUD USE ($950.01 & OVER)') ~ 
        '26B Credit Card/Automated Teller Machine Fraud',
      crime_code_description %in% c('THEFT OF IDENTITY') ~ 
        '26C Impersonation',
      crime_code_description %in% c('UNAUTHORIZED COMPUTER ACCESS') ~ 
        '26E Wire Fraud',
      
      # Homicide Offenses
      crime_code_description %in% c('CRIMINAL HOMICIDE') ~ 
        '09A Murder and Nonnegligent Manslaughter',
      crime_code_description %in% c('MANSLAUGHTER, NEGLIGENT') ~ 
        '09B Negligent Manslaughter',
      
      # Kidnapping/Abduction
      crime_code_description %in% c('CHILD STEALING', 'FALSE IMPRISONMENT', 
                                    'KIDNAPPING', 'KIDNAPPING - GRAND ATTEMPT') ~ 
        '100 Kidnapping/Abduction',
      
      # Larceny/Theft Offenses
      crime_code_description %in% c('PICKPOCKET', 'PICKPOCKET, ATTEMPT', 
                                    'DRUNK ROLL', 'DRUNK ROLL - ATTEMPT') ~ '23A Pocket-picking',
      crime_code_description %in% c('PURSE SNATCHING', 
                                    'PURSE SNATCHING - ATTEMPT') ~ '23B Purse-snatching',
      crime_code_description %in% c('SHOPLIFTING - ATTEMPT', 
                                    'SHOPLIFTING - PETTY THEFT ($950 & UNDER)', 
                                    'SHOPLIFTING-GRAND THEFT ($950.01 & OVER)') ~ '23C Shoplifting',
      crime_code_description %in% c('TILL TAP - ATTEMPT', 
                                    'TILL TAP - GRAND THEFT ($950.01 & OVER)', 
                                    'TILL TAP - PETTY ($950 & UNDER)') ~ '23D Theft From Building',
      crime_code_description %in% c('THEFT, COIN MACHINE - ATTEMPT', 
                                    'THEFT, COIN MACHINE - GRAND ($950.01 & OVER)', 
                                    'THEFT, COIN MACHINE - PETTY ($950 & UNDER)') ~ 
        '23E Theft From Coin-Operated Machine or Device',
      crime_code_description %in% c('THEFT FROM MOTOR VEHICLE - ATTEMPT', 
                                    'THEFT FROM MOTOR VEHICLE - GRAND ($400 AND OVER)', 
                                    'THEFT FROM MOTOR VEHICLE - PETTY ($950 & UNDER)', 
                                    'BURGLARY FROM VEHICLE', 'BURGLARY FROM VEHICLE, ATTEMPTED') ~ 
        '23F Theft From Motor Vehicle (except Theft of Motor Vehicle Parts or Accessories)',
      crime_code_description %in% c('THEFT PLAIN - ATTEMPT', 
                                    'THEFT PLAIN - PETTY ($950 & UNDER)', 
                                    'THEFT-GRAND ($950.01 & OVER)EXCPT,GUNS,FOWL,LIVESTK,PROD0036', 
                                    'BIKE - ATTEMPTED STOLEN', 'BIKE - STOLEN', 'BOAT - STOLEN', 
                                    'GRAND THEFT / AUTO REPAIR', 'PETTY THEFT - AUTO REPAIR', 
                                    'THEFT, PERSON', 'THEFT FROM PERSON - ATTEMPT', 
                                    "THEFT-GRAND ($950.01 & OVER)EXCPT,GUNS,FOWL,LIVESTK,PROD") ~ 
        '23H All Other Larceny',
      # some 23A and 23B offences are coded using a combination of crime codes 
      # and MO codes, so have to be done after the other larceny categories are 
      # created
      crime_code_description %in% c('THEFT, PERSON', 
                                    'THEFT FROM PERSON - ATTEMPT') & 
        str_count(mo_codes, pattern = '0357') > 0 ~ '23A Pocket-picking',
      crime_code_description %in% c('THEFT, PERSON', 
                                    'THEFT FROM PERSON - ATTEMPT') & str_count(mo_codes, 
                                                                               pattern = '0336|0337|0341|0342|0343|0346|0355') > 0 ~ 
        '23B Purse-snatching',
      # 23G offences are identified using an MO code that can be present in 
      # several crime categories, so this assignment has to be done after all 
      # the other theft categories are created
      crime_code_description %in% c('BURGLARY FROM VEHICLE', 
                                    'BURGLARY FROM VEHICLE, ATTEMPTED', 'THEFT FROM MOTOR VEHICLE - ATTEMPT', 
                                    'THEFT FROM MOTOR VEHICLE - GRAND ($400 AND OVER)', 
                                    'THEFT FROM MOTOR VEHICLE - PETTY ($950 & UNDER)', 
                                    'THEFT PLAIN - ATTEMPT', 'THEFT PLAIN - PETTY ($950 & UNDER)', 
                                    'THEFT-GRAND ($950.01 & OVER)EXCPT,GUNS,FOWL,LIVESTK,PROD0036') & 
        str_count(mo_codes, pattern = '0385') > 0 ~ 
        '23G Theft of Motor Vehicle Parts or Accessories',
      
      # Motor Vehicle Theft
      crime_code_description %in% c('VEHICLE - STOLEN', 
                                    'VEHICLE - ATTEMPT STOLEN', 'DRIVING WITHOUT OWNER CONSENT (DWOC)') ~ 
        '240 Motor Vehicle Theft',
      
      # Prostitution Offenses
      crime_code_description %in% c('PIMPING') ~ 
        '40B Assisting or Promoting Prostitution',
      crime_code_description %in% c('PANDERING') ~ 
        '40C Purchasing Prostitution',
      
      # Robbery
      crime_code_description %in% c('ROBBERY', 'ATTEMPTED ROBBERY') & 
        premise_description %in% c("STREET", "SIDEWALK", "PARKING LOT", 
                                   "MULTI-UNIT DWELLING (APARTMENT, DUPLEX, ETC)", 
                                   "SINGLE FAMILY DWELLING", "MARKET", "ALLEY", "BUS STOP", 
                                   "PARK/PLAYGROUND", "DRIVEWAY", "HIGH SCHOOL", 
                                   "YARD (RESIDENTIAL/BUSINESS)", "VEHICLE, PASSENGER/TRUCK", 
                                   "BUS STOP OR LAYOVER", "GARAGE/CARPORT", "JUNIOR HIGH SCHOOL",
                                   "PARKING UNDERGROUND/BUILDING", "AUTOMATED TELLER MACHINE (ATM)",
                                   "BEACH", "PROJECT/TENEMENT/PUBLIC HOUSING", "OTHER RESIDENCE",
                                   "ELEMENTARY SCHOOL", "PORCH, RESIDENTIAL", 
                                   "CHARTER BUS AND PRIVATELY OWNED BUS", "FREEWAY", 
                                   "PUBLIC RESTROOM/OUTSIDE*", "STAIRWELL*", "MTA BUS", 
                                   "PUBLIC RESTROOM(INDOORS-INSIDE)", "RIVER BED*", 
                                   "CONDOMINIUM/TOWNHOUSE", "SKATEBOARD FACILITY/SKATEBOARD PARK*",
                                   "COLLEGE/JUNIOR COLLEGE/UNIVERSITY", "TRAIN", 
                                   "MOBILE HOME/TRAILERS/CONSTRUCTION TRAILERS/RV'S/MOTORHOME",
                                   "MTA PROPERTY OR PARKING LOT", "UNDERPASS/BRIDGE*", "ELEVATOR",
                                   "GROUP HOME", "PATIO*", "PEDESTRIAN OVERCROSSING", 
                                   "SPECIALTY SCHOOL/OTHER", "METROLINK TRAIN", "VACANT LOT", 
                                   "ABANDONED BUILDING ABANDONED HOUSE", "REDLINE ENTRANCE/EXIT",
                                   "TRANSPORTATION FACILITY (AIRPORT)", "BUS, SCHOOL, CHURCH",
                                   "TRAIN TRACKS", "TUNNEL", "OTHER RR TRAIN (UNION PAC, SANTE FE ETC",
                                   "PAY PHONE", "REDLINE SUBWAY PLATFORM", "TERMINAL", 
                                   "MUNICIPAL BUS LINE INCLUDES LADOT/DASH", 
                                   "POOL-PUBLIC/OUTDOOR OR INDOOR*", "PRIVATE SCHOOL/PRESCHOOL",
                                   "REDLINE (SUBWAY TRAIN)", "SLIPS/DOCK/MARINA/BOAT", 
                                   "BLUE LINE (ABOVE GROUND SURFACE TRAIN)", "FOSTER HOME BOYS OR GIRLS*",
                                   "FRAT HOUSE/SORORITY/DORMITORY", "GOLF COURSE*", 
                                   "GREEN LINE (I-105 FWY LEVEL TRAIN)") ~ "12A Personal Robbery",
      crime_code_description %in% c('ROBBERY', 'ATTEMPTED ROBBERY') & 
        (premise_description %in% c("OTHER PREMISE", "OTHER/OUTSIDE") | 
           is.na(premise_description)) ~ "12U Other Robbery",
      crime_code_description %in% c('ROBBERY', 'ATTEMPTED ROBBERY') ~ 
        "12B Commercial Robbery",
      
      # Sex Offenses
      crime_code_description %in% c('RAPE, ATTEMPTED', 'RAPE, FORCIBLE') ~ 
        '11A Rape (except Statutory Rape)',
      crime_code_description %in% c('SODOMY/SEXUAL CONTACT B/W PENIS OF ONE PERS TO ANUS OTH 0007=02',
                                    'ORAL COPULATION') ~ '11B Sodomy',
      crime_code_description %in% c('SEXUAL PENTRATION WITH A FOREIGN OBJECT') ~
        '11C Sexual Assault With An Object',
      crime_code_description %in% c('BATTERY WITH SEXUAL CONTACT') ~ 
        '11D Fondling',
      
      # Sex Offenses, Nonforcible
      crime_code_description %in% c('INCEST (SEXUAL ACTS BETWEEN BLOOD RELATIVES)') ~ 
        '36A Incest',
      crime_code_description %in% c('SEX, UNLAWFUL') ~ '36B Statutory Rape',
      # The offence of 'CRM AGNST CHLD (13 OR UNDER) (14-15 & SUSP 10 YRS OLDER)0060'
      # is problematic because it covers a wide array of behaviour. Offenses
      # matching specific criteria are allocated to particular categories, with
      # all remaining offences being allocated to category 90Z. The specific 
      # offences are ordered by decreasing severity, since an offence may match 
      # multiple MO codes.
      crime_code_description %in% c('CRM AGNST CHLD (13 OR UNDER) (14-15 & SUSP 10 YRS OLDER)0060') 
      & str_count(mo_codes, pattern = '0527|0533') > 0 ~ '36B Statutory Rape',
      crime_code_description %in% c('CRM AGNST CHLD (13 OR UNDER) (14-15 & SUSP 10 YRS OLDER)0060') 
      & str_count(mo_codes, pattern = '0507|0512|0519|0521|0539|0540|0541|0548|0549') > 0
      ~ '11B Sodomy',
      crime_code_description == "SODOMY/SEXUAL CONTACT B/W PENIS OF ONE PERS TO ANUS OTH" 
      ~ "11C Sexual Assault With An Object",
      crime_code_description %in% c('CRM AGNST CHLD (13 OR UNDER) (14-15 & SUSP 10 YRS OLDER)0060') 
      & str_count(mo_codes, pattern = '0515') > 0 ~ 
        '11C Sexual Assault With An Object',
      crime_code_description %in% c("SEX,UNLAWFUL(INC MUTUAL CONSENT, PENETRATION W/ FRGN OBJ", 
                                    "SEX,UNLAWFUL(INC MUTUAL CONSENT, PENETRATION W/ FRGN OBJ0059",
                                    "SEXUAL PENETRATION W/FOREIGN OBJECT") ~ 
        "11C Sexual Assault With An Object",
      crime_code_description %in% c('CRM AGNST CHLD (13 OR UNDER) (14-15 & SUSP 10 YRS OLDER)0060') 
      & str_count(mo_codes, pattern = '0500|0501|0502|0503|0504|0505|0506|0509|0510|0511|0517|0518|0522|0528|0532|0537|0543|0544|0550|0551') > 0 
      ~ '11D Fondling',
      crime_code_description %in% c('CRM AGNST CHLD (13 OR UNDER) (14-15 & SUSP 10 YRS OLDER)0060') 
      & str_count(mo_codes, pattern = '0529|0538') > 0 ~ 
        '90C Disorderly Conduct',
      crime_code_description %in% c('CRM AGNST CHLD (13 OR UNDER) (14-15 & SUSP 10 YRS OLDER)0060') 
      ~ '90Z All Other Offenses',
      
      # Pornography/Obscene Material
      crime_code_description == "CHILD PORNOGRAPHY" ~ 
        "370 Pornography/Obscene Material",
      
      # Weapon Law Violations
      crime_code_description %in% c('BRANDISH WEAPON', 
                                    'WEAPONS POSSESSION/BOMBING', 
                                    'FIREARMS EMERGENCY PROTECTIVE ORDER (FIREARMS EPO)', 
                                    'DISCHARGE FIREARMS/SHOTS FIRED', 
                                    'REPLICA FIREARMS(SALE,DISPLAY,MANUFACTURE OR DISTRIBUTE)0132', 
                                    'SHOTS FIRED AT INHABITED DWELLING', 
                                    'SHOTS FIRED AT MOVING VEHICLE, TRAIN OR AIRCRAFT', 
                                    "FIREARMS RESTRAINING ORDER (FIREARMS RO)",
                                    "REPLICA FIREARMS(SALE,DISPLAY,MANUFACTURE OR DISTRIBUTE)") ~ 
        '520 Weapon Law Violations',
      
      # Human Trafficking
      crime_code_description == "HUMAN TRAFFICKING - COMMERCIAL SEX ACTS" ~ 
        "64A Human Trafficking, Commercial Sex Acts",
      crime_code_description == "HUMAN TRAFFICKING - INVOLUNTARY SERVITUDE" ~ 
        "64B Human Trafficking, Involuntary Servitude",
      
      # Bad Checks (except Counterfeit Checks or Forged Checks)
      crime_code_description %in% c('DOCUMENT WORTHLESS ($200 & UNDER)', 
                                    'DOCUMENT WORTHLESS ($200.01 & OVER)') ~ 
        '90A Bad Checks (except Counterfeit Checks or Forged Checks)',
      
      # Curfew/Loitering/Vagrancy Violations
      crime_code_description %in% c('FAILURE TO DISPERSE', 
                                    'BLOCKING DOOR INDUCTION CENTER') ~ 
        '90B Curfew/Loitering/Vagrancy Violations',
      
      # Disorderly Conduct
      crime_code_description %in% c('DISTURBING THE PEACE', 'DISRUPT SCHOOL', 
                                    'INCITING A RIOT', 'INDECENT EXPOSURE', 'LEWD CONDUCT', 
                                    'THROWING OBJECT AT MOVING VEHICLE') ~ '90C Disorderly Conduct',
      
      # Family Offenses, Nonviolent
      crime_code_description %in% c('CHILD ABANDONMENT', 
                                    'CHILD NEGLECT (SEE 300 W.I.C.)', 
                                    "CRM AGNST CHLD (13 OR UNDER) (14-15 & SUSP 10 YRS OLDER)",
                                    "LEWD/LASCIVIOUS ACTS WITH CHILD") ~ 
        '90F Family Offenses, Nonviolent',
      
      # Peeping Tom
      crime_code_description %in% c('PEEPING TOM') ~ '90H Peeping Tom',
      
      # Trespass of Real Property
      crime_code_description %in% c('PROWLER', 'TRESPASSING') ~ 
        '90J Trespass of Real Property',
      
      # All Other Offenses
      crime_code_description %in% c('ABORTION/ILLEGAL', 
                                    'BEASTIALITY, CRIME AGAINST NATURE SEXUAL ASSLT WITH ANIM0065', 'BIGAMY', 
                                    'CHILD ANNOYING (17YRS & UNDER)', 'CONSPIRACY', 'CONTEMPT OF COURT', 
                                    'CONTRIBUTING', 'CRUELTY TO ANIMALS', 'FALSE POLICE REPORT', 
                                    'ILLEGAL DUMPING', 'LYNCHING', 'LYNCHING - ATTEMPTED', 
                                    'VIOLATION OF COURT ORDER', 'VIOLATION OF RESTRAINING ORDER', 
                                    'VIOLATION OF TEMPORARY RESTRAINING ORDER', 'LETTERS, LEWD', 
                                    'OTHER MISCELLANEOUS CRIME') ~ '90Z All Other Offenses',
      is.na(crime_code_description) ~ "90Z All Other Offenses",
      
      # Not-categorisable offences
      # Where it is clear that not all the offences that should be present are 
      # present in the data, it would be misleading to populate those 
      # categories. Offences within such categories that are present are 
      # categorised here so that they can be stripped from the analysis.
      crime_code_description %in% c('DRUGS, TO A MINOR') ~ 
        '99X Non-Categorisable Offenses',
      
      # Non-reportable crimes
      crime_code_description %in% c('FAILURE TO YIELD', 'RECKLESS DRIVING') ~
        '99Y Non-Reportable Crimes'
      
    )
  ) %>% 
  left_join(nibrs_categories, by = "nibrs_offense_type") %>%
  check_nibrs_cats(
    file = "crime_categories/categories_LA",
    by = "crime_code_description"
  ) %>% 
  # where necessary, update the NIBRS code to match the new type
  mutate(nibrs_offense_code = case_when(
    nibrs_offense_type == "22A Residential Burglary/Breaking & Entering" ~ 
      "22A",
    nibrs_offense_type == "22B Non-residential Burglary/Breaking & Entering" ~
      "22B",
    nibrs_offense_type == "12A Personal Robbery" ~ "12A",
    nibrs_offense_type == "12B Commercial Robbery" ~ "12B",
    nibrs_offense_type == "12U Other Robbery" ~ "12U",
    TRUE ~ nibrs_offense_code
  )) %>%
  # identify location types
  join_location_types(
    file = "crime_categories/location_types_los_angeles.csv",
    by = "premise_description"
  ) %>% 
  # Rename variables that need consistent names in the next stage of processing
  rename(longitude = lon, latitude = lat) %>% 
  # save data
  save_city_data("Los Angeles") %>%
  glimpse()





# Louisville ------------------------------------------------------------------

# Louisville has a field called `NIBRS_CODE` that can be used to match the
# various levels of the NIBRS hierarchy. However, some incidents that Louisville
# categorizes as 90Z should actually be in other categories, e.g. because they
# are not criminal or are criminal but are not NIBRS reportable. Other crimes
# are recorded in category 999, in many cases despite a NIBRS code existing for
# that offense. These need to be reallocated manually.

# save data from website
# Source: https://data.louisvilleky.gov/search?tags=Category%2Clmpd
tribble(
  ~year, ~url,
  "2007", "https://opendata.arcgis.com/api/v3/datasets/68a6bffe2f4849a2bb43e716de0b3109_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1",
  "2008", "https://opendata.arcgis.com/api/v3/datasets/2ae5ed2569b34dca887e3aa78bd393e6_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1",
  "2009", "https://opendata.arcgis.com/api/v3/datasets/d9900fb6c88a4bab891b6f33abe5ca5e_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1",
  "2010", "https://opendata.arcgis.com/api/v3/datasets/cf42137603c84880a29561b98ac5d77d_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1",
  "2011", "https://opendata.arcgis.com/api/v3/datasets/fc45166a9c794b4090dfca6551f1f07a_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1",
  "2012", "https://opendata.arcgis.com/api/v3/datasets/459df206b0a04283831bf1ad7436b262_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1",
  "2013", "https://opendata.arcgis.com/api/v3/datasets/ca0d48cd2b2f43dc810e4af2f94c27d6_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1",
  "2014", "https://opendata.arcgis.com/api/v3/datasets/bc88775238fc4b1db84beff34ef43c77_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1",
  "2015", "https://opendata.arcgis.com/api/v3/datasets/1c94ff45cfde43db999c0dc3fdfe8cd8_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1",
  "2016", "https://opendata.arcgis.com/api/v3/datasets/3a84cb35fc9a4f209732ceff6d8ae7ea_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1",
  "2017", "https://opendata.arcgis.com/api/v3/datasets/2bb69cd2c40444738c46110c03411439_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1",
  "2018", "https://opendata.arcgis.com/api/v3/datasets/497405bfa80047ee95690107ca4ae003_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1",
  "2019", "https://opendata.arcgis.com/api/v3/datasets/34a5c3d4a4c54dbb80a467645ddf242f_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1",
  "2020", "https://opendata.arcgis.com/api/v3/datasets/43cf1d2647aa4b1c9b98579b83da4ec5_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1",
  "2021", "https://opendata.arcgis.com/api/v3/datasets/99c95d815ee24ed19c15ad408efb7a76_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1"
) %>% 
  mutate(year = str_glue("louisville_{year}")) %>% 
  pwalk(~ download_crime_data(..2, ..1))

# process data
louisville_data <- here::here("original_data") %>% 
  dir(pattern = "^raw_louisville_", full.names = TRUE) %>% 
  map_dfr(
    read_crime_data, 
    col_types= cols(
      .default = col_character(),
      ID = col_integer()
    )
  ) %>% 
  # add date var
  add_date_var("date_occured", c("Ymd T", "mdy IM p"), "America/Kentucky/Louisville") %>% 
  # filter by year
  filter_by_year(yearFirst, yearLast) %>% 
  # categorize crimes
  mutate(nibrs_offense_type = case_when(
    
    # homicide
    nibrs_code == "999" & uor_desc %in% c("FETAL HOMICIDE - 1ST DEGREE",
                                          "MURDER - DOMESTIC VIOLENCE") ~ 
      "09A Murder and Nonnegligent Manslaughter",
    nibrs_code == "999" & uor_desc %in% c("RECKLESS HOMICIDE") ~ 
      "09B Negligent Manslaughter",
    
    # kidnapping
    nibrs_code == "999" & uor_desc %in% c("UNLAWFUL IMPRISONMENT-1ST DEGREE",
                                          "UNLAWFUL IMPRISONMENT-2ND DEGREE", 
                                          "KIDNAPPING-WITH SERIOUS PHYSICAL INJURY") ~ "100 Kidnapping/Abduction",
    
    # sex offences
    # it seems to be safe to search on the word 'rape', since there aren't any
    # crimes coded as 999 and described as statutory rape
    nibrs_code == "999" & str_detect(uor_desc, "\\bRAPE\\b") == TRUE ~ 
      "11A Rape (except Statutory Rape)",
    nibrs_code == "999" & uor_desc %in% c("SODOMY - 1ST DEGREE",
                                          "SODOMY - 1ST DEGREE - DOMESTIC VIOLENCE", 
                                          "SODOMY - 1ST DEGREE - VICTIM U/12 YEARS OF AGE") ~ "11B Sodomy",
    nibrs_code == "999" & uor_desc %in% c("SEXUAL ABUSE - 1ST DEGREE",
                                          "SEXUAL ABUSE - 1ST DEGREE- VICTIM U/12 YOA", 
                                          "SEXUAL ABUSE - 3RD DEGREE") ~ "11D Fondling",
    
    # robbery
    (nibrs_code == "120" | str_detect(uor_desc, "\\bROBBERY\\b")) & 
      premise_type %in% c("HIGHWAY / ROAD / ALLEY", 
                          "RESIDENCE / HOME", "PARKING LOT / GARAGE", 
                          "OTHER RESIDENCE (APARTMENT/CONDO)", "SCHOOL - ELEMENTARY / SECONDARY", 
                          "FIELD / WOODS", "AIR / BUS / TRAIN TERMINAL", "ATM SEPARATE FROM BANK",
                          "SCHOOL - COLLEGE / UNIVERSITY") ~ "12A Personal Robbery",
    (nibrs_code == "120" | str_detect(uor_desc, "\\bROBBERY\\b")) & 
      premise_type == "OTHER / UNKNOWN" ~ 
      "12U Other Robbery",
    (nibrs_code == "120" | str_detect(uor_desc, "\\bROBBERY\\b")) ~ 
      "12B Commercial Robbery",
    
    # Assault offences
    # Some assault offences are listed under nibrs_code 999, so must be
    # categorised according to the degree of assault.
    str_detect(uor_desc, "ASSAULT.+?(1ST|2ND)") == TRUE | uor_desc %in% c(
      "CRIMINAL ABUSE-1ST DEGREE", 
      "CRIMINAL ABUSE-1ST DEGREE-CHILD 12 OR UNDER", 
      "CRIMINAL ABUSE-3RD DEGREE", "MURDER - DOMESTIC VIOLENCE ATTEMPTED",
      "MURDER - POLICE OFFICER ATTEMPTED", "MURDER ATTEMPTED") ~ 
      "13A Aggravated assault",
    str_detect(uor_desc, "ASSAULT.+?(3RD|4TH)") == TRUE ~ "13B Simple assault",
    nibrs_code == "999" & uor_desc %in% c("ABUSE OF A TEACHER PROHIBITED",
                                          "MENACING", "TERRORISTIC THREATENING 1ST DEGREE", 
                                          "TERRORISTIC THREATENING 2ND DEGREE", 
                                          "TERRORISTIC THREATENING 3RD DEGREE", 
                                          "INTIMIDATING A PARTICIPANT IN LEGAL PROCESS", 
                                          "HARASSMENT (NO PHYSICAL CONTACT)", 
                                          "HARASSMENT - PHYSICAL CONTACT - NO INJURY", "HARASSING COMMUNICATIONS",
                                          "RETALIATING AGAINST PARTICIPANT IN LEGAL PROCESS",
                                          "STALKING-1ST DEGREE", "STALKING-2ND DEGREE") ~
      "13C Intimidation",
    
    # burglary
    (nibrs_code == "220" | str_detect(uor_desc, "\\bBURGLARY\\b") == TRUE) & 
      premise_type %in% c("RESIDENCE / HOME", 
                          "PARKING LOT / GARAGE", "OTHER RESIDENCE (APARTMENT/CONDO)", 
                          "ATTACHED RESIDENTIAL GARAGE", "PARKING LOT / GARAGE") ~ 
      "22A Residential Burglary/Breaking & Entering",
    (nibrs_code == "220" | str_detect(uor_desc, "\\bBURGLARY\\b") == TRUE) ~ 
      "22B Non-residential Burglary/Breaking & Entering",
    
    # theft
    nibrs_code == "999" & uor_desc %in% c(
      "THEFT BY UNLAWFUL TAKING/DISP-PURSESNATCHING  FELONY CLASS D") ~ 
      "23B Purse-snatching",
    nibrs_code == "999" & uor_desc %in% c(
      "THEFT BY UNLAWFUL TAKING/DISP-SHOPLIFTING - FELONY CLASS D",
      "THEFT BY UNLAWFUL TAKING/DISP-SHOPLIFTING -MISD") ~ "23C Shoplifting",
    nibrs_code == "999" & uor_desc %in% c(
      "THEFT BY UNLAWFUL TAKING/DISP - FROM BUILDING - MISD", 
      "THEFT BY UNLAWFUL TAKING/DISP-FROM BUILDING - $10,000 OR >",
      "THEFT BY UNLAWFUL TAKING/DISP-FROM BUILDING - FELONY D") ~ "23D Theft From Building",
    nibrs_code == "999" & uor_desc %in% c(
      "THEFT BY UNLAWFUL TAKING/DISP-CONTENTS FROM AUTO - FELONY", 
      "THEFT BY UNLAWFUL TAKING/DISP-CONTENTS FROM AUTO - MISD") ~ 
      "23F Theft From Motor Vehicle (except Theft of Motor Vehicle Parts or Accessories)",
    nibrs_code == "999" & uor_desc %in% c(
      "THEFT BY UNLAW TAKING/DISP-PARTS FROM VEHICLE $10,000 OR >",
      "THEFT BY UNLAWFUL TAKING/DISP-PARTS FROM VEHICLE - MISD",
      "THEFT OF MOTOR VEHICLE REGISTRATION PLATE/DECAL") ~ "23G Theft of Motor Vehicle Parts or Accessories",
    nibrs_code == "999" & uor_desc %in% c(
      "THEFT BY UNLAWFUL TAKING - GASOLINE - 1ST OFFENSE", 
      "THEFT BY UNLAWFUL TAKING - GASOLINE - U/$10,000", 
      "THEFT BY UNLAWFUL TAKING - GASOLINE - U/$500 MISD", 
      "THEFT BY UNLAWFUL TAKING-ALL OTHERS $10,000 OR MORE", 
      "THEFT BY UNLAWFUL TAKING/DISP-ALL OTHERS - FELONY CLASS D", 
      "THEFT BY UNLAWFUL TAKING/DISP-ALL OTHERS -MISD", "THEFT OF CARGO", 
      "THEFT OF CONTROLLED SUBSTANCE", 
      "THEFT OF CONTROLLED SUBSTANCE, 1ST OFFENSE OR UNDER $300", 
      "THEFT OF CONTROLLED SUBSTANCE, 2ND OR  > OFFENSE OR OVER $30", 
      "THEFT OF ID PRIOR TO JULY 2000", 
      "THEFT OF IDENTITY OF ANOTHER WITHOUT CONSENT", "THEFT OF MAIL MATTER", 
      "THEFT OF PROP LOST/MISLAID/DELIVERED BY MISTAKE FELONY D", 
      "THEFT OF PROPERTY LOST/MISLAID/DELIVERED BY MISTAKE MISD",
      "THEFT OF SERVICES - FELONY CLASS D", "THEFT OF SERVICES - MISD",
      "LESSER INCLUDED THEFT", "TBUT OR DISP FIREARM") ~ 
      "23H All Other Larceny",
    
    # motor vehicle theft
    nibrs_code == "999" & uor_desc %in% c(
      "THEFT BY UNLAWFUL TAKING/DISP-AUTO - FELONY CLASS D",
      "THEFT BY UNLAWFUL TAKING/DISP-AUTO - MISD",
      "THEFT BY UNLAWFUL TAKING/DISP-AUTO $10,000 OR MORE") ~ 
      "240 Motor Vehicle Theft",
    
    # forgery
    nibrs_code == "999" & uor_desc %in% c(
      "CRIMINAL POSS OF A FORGED PERSCRIPTION, 1ST OFFENSE",
      "CRIMINAL POSSESSION FORGED INSTRUMENT-1ST DEGREE-IDENTIFY",
      "CRIMINAL POSSESSION FORGED INSTRUMENT-2ND DEGREE-IDENTIFY",
      "CRIMINAL POSSESSION OF FORGED INSTRUMENT-3RD DEGREE",
      "FORGERY - 1ST DEGREE", "FORGERY - 2ND DEGREE", 
      "FORGERY OF A PRESCRIPTION - 1ST OFFENSE", "FORGERY-3RD DEGREE",
      "POSSESSION OF FORGED PRESCRIPTION FOR LEGEND DRUG 1ST",
      "UTTER FALSE/FORGED PRESCRIPTION", 
      "FALSE MAKING/EMBOSSING OF CREDIT CARD", 
      "POSSESSION OF A FORGERY DEVICE-IDENTIFY",
      "PROHIBIT COMMERCE COUNTERFEIT GOODS/SERVICES >25 ITMS 1ST OF") ~ 
      "250 Counterfeiting/Forgery",
    
    # fraud
    nibrs_code == "999" & (str_detect(uor_desc, "\\bFRAUD.+?\\bCARD") == TRUE |
                             uor_desc %in% c("MISUSE OF ELECTRONIC INFO-AUTOMATED BANKING DEVICE, ETC",
                                             "RECEIPT OF CREDIT CARD IN VIOLATION KRS 434.570, 434.610")) ~
      "26B Credit Card/Automated Teller Machine Fraud",
    nibrs_code == "999" & uor_desc %in% c("IMPERSONATING A PEACE OFFICER") ~
      "26C Impersonation",
    nibrs_code == "999" & uor_desc %in% c(
      "UNLAWFUL ACCESS TO COMPUTER-1ST DEGREE") ~ "26E Wire Fraud",
    
    # criminal damage
    nibrs_code == "999" & uor_desc %in% c("CRIMINAL MISCHIEF - 1ST DEGREE",
                                          "CRIMINAL MISCHIEF-2ND DEGREE", "CRIMINAL MISCHIEF-3RD DEGREE",
                                          "CRIMINAL USE OF NOXIOUS SUBSTANCE") ~ 
      "290 Destruction/Damage/Vandalism of Property (except Arson)",
    
    # Drugs
    # There are lots of drug offences listed under nibrs_code 999 (which does
    # not exist) and for which uor_desc involves variations on the phrase
    # possession of a controlled substance (typically abbreivated)
    str_detect(uor_desc, "\\bPOS.+?\\bCON.+?\\bSUB") == TRUE | 
      str_detect(uor_desc, "\\bTRAF.+?\\bCON.+?\\bSUB") == TRUE | 
      str_detect(uor_desc, "\\bTRF.+?\\bCON.+?\\bSUB") == TRUE |
      str_detect(uor_desc, "\\bTRAF.+?\\bMARIJUANA") == TRUE |
      (nibrs_code == "999" & uor_desc %in% c(
        "ASSUME FALSE TITLE TO OBTAIN CONT SUB-1ST OFFENSE",
        "ATT/OBTAIN CONTSUB BY FRAUD/FLS STMT/FORGERY",
        "ATTEMPT/OBTAIN CONT SUB BY FRAUD/FALSE STMT/FORGERY-1ST OFF",
        "CULTIVATE IN MARIJUANA-< 5 PLANTS-1ST OFFENSE",
        "CULTIVATE IN MARIJUANA-< 5 PLANTS-2ND OR > OFFENSE",
        "CULTIVATE IN MARIJUANA-5 PLANTS OR >-1ST OFFENSE",
        "CULTIVATE IN MARIJUANA-5 PLANTS OR >-2ND OR > OFFENSE",
        "MANUFACTURING METHAMPHETAMINE 1ST OFFENSE",
        "MANUFACTURING METHAMPHETAMINE 2ND OR > OFFENSE",
        "POSS OF MARIJUANA", 
        "PRESCRIPTION CONT SUB NOT IN ORIGINAL CONTAINER-1ST OFFENSE",
        "UNLAWFUL DISTRIBUTION OF A METH PRECURSOR - 1ST OFFENSE",
        "UNLAWFUL DISTRIBUTION OF A METH PRECURSOR - 2ND OR > OFFENSE",
        "UNLAWFUL POSSESSION OF METH PRECURSOR 1ST OFFENSE",
        "UNLAWFUL POSSESSION OF METH PRECURSOR 2ND OR > OFFENSE",
        "TRAF IN SYNTHETIC CANNABINOIL AGOINTS OR PIPEAZINES",
        "TRAFFICKING IN A LEGEND DRUG, 1ST OFFENSE", "TRAFFICKING IN SALVIA",
        "ILLEGAL POSSESSION OF A LEGEND DRUG 1ST OFFENSE", 
        "ILLEGAL POSSESSION OF LEGEND DRUG", "POSSESSION OF SALVIA",
        "POSSESSION OF SYNTHETIC CANNABINOID AGONISTS OR PIPEAZINES",
        "SELL CONTROL SUBSTANCE TO A MINOR-1ST OFFENSE",
        "VOLATILE SUBSTANCE ABUSE")) ~ 
      "35A Drug/Narcotic Violations",
    nibrs_code == "999" & uor_desc %in% c("DRUG PARAPHERNALIA - BUY/POSSESS",
                                          "DRUG PARAPHERNALIA-BUY/POSSESS-1ST OFFENSE", 
                                          "DRUG PARAPHERNALIA-BUY/POSSESS-2ND OR > OFFENSE") ~ 
      "35B Drug Equipment Violations",
    
    # non-violent sex offences
    nibrs_code == "999" & uor_desc %in% c(
      "INCEST - VICTIM U/12 YOA OR SERIOUS PHYSICAL INJURY") ~ "36A Incest",
    
    # stolen property
    nibrs_code == "999" & (str_detect(uor_desc, "STOLEN.+?PROPERTY") == TRUE |
                             uor_desc %in% c("THEFT-RECEIPT OF STOLEN CREDIT/DEBIT CARD-1 CARD",
                                             "THEFT-RECEIPT OF STOLEN CREDIT/DEBIT CARD-2 OR MORE CARDS",
                                             "TRAFFICKING IN STOLEN IDENTITIES", 
                                             "OBSCURING THE IDENTITY OF A MACHINE U/$10000",
                                             "OBSCURING THE IDENTITY OF A MACHINE U/$500")) ~ 
      "280 Stolen Property Offenses",
    
    # obscene material
    nibrs_code == "999" & uor_desc %in% c(
      "DIST OF MATTER PORTRAYING SEXUAL PERFORM BY MINOR 2ND > OFF",
      "POSSESSION OF MATTER PORTRAYING SEX PERFORMANCE BY MINOR") ~ 
      "370 Pornography/Obscene Material",
    
    # gambling
    nibrs_code == "999" & uor_desc %in% c(
      "POSSESS OF GAMBLING RECORD-1ST DEGREE", 
      "POSSESSION OF A GAMBLING DEVICE") ~ "39C Gambling Equipment Violations",
    nibrs_code == "999" & uor_desc %in% c("PROMOTING GAMBLING - 2ND DEGREE") ~
      "39B Operating/Promoting/Assisting Gambling",
    
    # prostitution
    nibrs_code == "999" & uor_desc %in% c(
      "LOITERING FOR PROSTITUTION PURPOSES - 1ST OFFENSE",
      "LOITERING FOR PROSTITUTION PURPOSES - 2ND > OFFENSE", "PROSTITUTION") ~ 
      "40A Prostitution",
    
    # weapons violations
    nibrs_code == "999" & uor_desc %in% c("CARRYING A CONCEALED DEADLY WEAPON",
                                          "DEFACING A FIREARM", "POSSESSION OF DEFACED FIREARM", 
                                          "POSSESSION OF FIREARM BY CONVICTED FELON", 
                                          "POSSESSION OF HANDGUN BY CONVICTED FELON",
                                          "UNLAWFUL POSSESSION OF WEAPON ON SCHOOL PROPERTY",
                                          "UNLAWFULLY PROV/PERMIT MINOR TO POSSESS HANDGUN",
                                          "USING RESTRICTED AMMO DURING A FELONY (NO SHOTS)", 
                                          "POSS HANDGUN BY MINOR 1ST OFFENSE", 
                                          "POSS,MANUF,TRANSPRT HANDGUN WITH EXC FOR UNLAWFUL",
                                          "POSS,MANUF,TRANSPRT HANDGUN WITH EXC FOR UNLAWFUL 2ND OFFENS") ~ 
      "520 Weapon Law Violations",
    
    # bad checks
    nibrs_code == "999" & uor_desc %in% c("THEFT BY DECEPTION-INCL COLD CHECKS",
                                          "THEFT BY DECEPTION-INCL COLD CHECKS $10,000 OR MORE",
                                          "THEFT BY DECEPTION-INCL COLD CHECKS U/$10,000",
                                          "THEFT BY DECEPTION-INCL COLD CHECKS U/$500") ~ 
      "90A Bad Checks (except Counterfeit Checks or Forged Checks)",
    
    # loitering
    nibrs_code == "999" & uor_desc %in% c("JUVENILE CURFEW ORD 17 & UNDER") ~ 
      "90B Curfew/Loitering/Vagrancy Violations",
    
    # Disorderly conduct
    uor_desc %in% c('INDECENT EXPOSURE - 1ST DEGREE - 1ST OFFENSE',
                    'INDECENT EXPOSURE - 1ST DEGREE - 2ND OFFENSE', 
                    'INDECENT EXPOSURE - 1ST DEGREE - 3RD OFFENSE', 
                    'INDECENT EXPOSURE - 1ST DEGREE - 4TH OR > OFFENSE', 
                    'INDECENT EXPOSURE - 2ND DEGREE') ~ '90C Disorderly Conduct',
    
    # Child abuse (non-violent)
    nibrs_code == "999" & uor_desc %in% c(
      "CONTROLLED SUBSTANCE ENDANGERMENT TO CHILD 2ND DEGREE",
      "CONTROLLED SUBSTANCE ENDANGERMENT TO CHILD 4TH DEGREE") ~ 
      "90F Family Offenses, Nonviolent",
    
    # trespass
    nibrs_code == "999" & uor_desc %in% c("CRIMINAL TRESPASSING-3RD DEGREE") ~ 
      "90J Trespass of Real Property",
    
    # All other offences
    uor_desc %in% c("ANY FELONY CATEGORY NOT LISTED", 
                    "ADVERTISING MATTER PORTRAY SEX PERFORMANCE BY MINOR 1ST OFF",
                    "CONTEMPT OF COURT - VIOL EMERGENCY PROTECTIVE ORDER",
                    "CRUELTY TO ANIMALS - 2ND DEGREE", "DANGEROUS ANIMAL",
                    "DANGEROUS DOG-CONTROL OF DOG", "DISARMING A PEACE OFFICER",
                    "FAILURE TO COMPLY W/SEX OFFENDER REGISTRATION - 1ST OFF",
                    "FLEEING OR EVADING POLICE - 1ST DEGREE (MOTOR VEHICLE)",
                    "FLEEING OR EVADING POLICE - 1ST DEGREE (ON FOOT)",
                    "FLEEING OR EVADING POLICE - 2ND DEGREE (MOTOR VEHICLE)",
                    "ILLEGAL DUMPING LOUISVILLE METRO ORDINANCE",
                    "LEAVING SCENE OF ACCIDENT - HIT & RUN",
                    "LEAVING SCENE OF ACCIDENT/FAILURE TO RENDER AID OR ASSISTANC",
                    "OBSTRUCTING GOVERNMENTAL OPERATIONS",
                    "WANTON ENDANGERMENT-1ST DEGREE", 
                    "WANTON ENDANGERMENT-1ST DEGREE-POLICE OFFICER", 
                    "WANTON ENDANGERMENT-2ND DEGREE",
                    "WANTON ENDANGERMENT-2ND DEGREE-POLICE OFFICER",
                    "VIOLATING GRAVES", "VIOLATION UNKNOWN", 
                    "WANTON ABUSE/NEGLECT OF ADULT BY PERON", 
                    "INTERFERE W/ENFORCEMENT PROHI", "INTERMEDIATE LICENSING VIOLATIONS",
                    "LICENSEE PERMIT NUDE PERFORMA", "OFFENSES AGAINST PUBLIC PEACE",
                    "OWNER TO CONTROL ANIMAL", "PROBATION VIOLATION (FOR FELONY OFFENSE)",
                    "PROBATION VIOLATION (FOR MISDEMEANOR OFFENSE)", 
                    "PROBATION VIOLATION (JUVENILE PUBLIC OFFENSE)",
                    "VIOLATION OF KENTUCKY EPO/DVO") ~ 
      "90Z All Other Offenses",
    
    # Non-categorisable offences
    uor_desc %in% c("DOMESTIC VIOLENCE INVOLVED INCIDENT") ~ 
      "99X Non-Categorisable Offenses",
    
    # Non-reportable crimes
    uor_desc %in% c('DISPLAY OF ILLEGAL/ALTERED REGISTRATION PLATE',
                    'FAIL OF NON-OWNER/OPER TO MAINTAIN REQ INS/SECURITY 1ST OFF',
                    'FAIL OF TRANSFEREE OF VEH TO PROMPTLY APPLY FOR NEW TITLE',
                    'FAILURE OF OWNER TO MAINTAIN REQUIRED INS/SECURITY 1ST OFF',
                    'FAILURE OF OWNER TO MAINTAIN REQUIRED INS/SECURITY 2ND OFF',
                    'FAILURE OF SELLER TO DELIVER REGISTRATION W/ASSIGNMENT FORM',
                    'IMPROPER EMERGENCY/SAFETY LIGHTS',
                    'LEAV SCENE OF ACCIDENT/FAIL TO RNDR AID/ASSIST W/DEATH OR SJ',
                    'MOTOR VEH DEALER REQ TO GIVE TITLE TO PURCHASER',
                    'MOTOR VEHICLE DEALER LICENSE REQUIRED',
                    'POSSESSING LICENSE WHEN PRIVILEGES ARE REVOKED', 
                    "OPERATING ON SUSPENDED/REVOKED OPERATORS LICENSE", "RECKLESS DRIVING",
                    "FAILURE TO OR IMPROPER SIGNAL", "NO OR EXPIRED REGISTRATION PLATES",
                    "NO OPERATORS/MOPED LICENSE", "FAILURE TO WEAR SEAT BELTS",
                    "FAILURE TO REGISTER TRANSFER OF MOTOR VEHICLE", 
                    "FAILURE TO NOTIFY ADDRESS CHANGE TO DEPT OF TRANSPORTATION",
                    "DISREGARDING STOP SIGN", "REAR LICENSE NOT ILLUMINATED",
                    "DISREGARDING TRAFFIC CONTROL DEVICE, TRAFFIC LIGHT", "CARELESS DRIVING",
                    "EXCESSIVE WINDSHIELD/ WINDOW TINTING", "FAILURE TO ILLUMINATE HEAD LAMPS",
                    "IMPROPER DISPLAY OF REGISTRATION PLATES", "NO CARRIER PERMIT",
                    "FAIL OF TRANSFEROR OF VEH TO PROP EXECUTE TITLE",
                    "FAILURE TO NOTIFY OWNER OF DAMAGE TO UNATTENDED VEHICLE",
                    "UNAUTHORIZED USE OF VEHICLE UNDER HARDSHIP DRIVERS LICENSE",
                    "FAILURE TO PRODUCE INSURANCE CARD", "IMPROPER REGISTRATION PLATE",
                    "HITCHHIKING/DISREGARDING TRAFFIC REGULATIONS BY PEDESTRIAN",
                    "IMPROPER TURNING", "OBSTRUCTED VISION AND/OR WINDSHIELD",
                    "ONE HEADLIGHT", "OPERATING VEHICLE WITH EXPIRED OPERATORS LICENSE",
                    "SPEEDING 15 MPH OVER LIMIT", "ABANDONMENT OF VEHICLE ON PUBLIC ROAD",
                    "LOCAL VIOLATION CODES 80000 - 80999", "SPEEDING - SPEED NO SPECIFIED",
                    "SPEEDING 12 MPH OVER LIMIT", "SPEEDING 14 MPH OVER LIMIT",
                    "SPEEDING 20 MPH OVER LIMIT", "SPEEDING 26 MPH OR GREATER OVER LIMIT",
                    "SPEEDING 7 MPH OVER LIMIT - LIMITED ACCESS HWY",
                    "DISPLAY/POSSESSOIN OF CANCELLED/FICTITIOUS OPERATOR LICENSE",
                    "DRIVING TOO FAST FOR TRAFFIC CONDITIONS", 
                    "FAIL TO GIVE RT OF WY TO VESS APPR SHORELINE", 
                    "FOLLOWING ANOTHER VEHICLE TOO CLOSELY",
                    "FAILURE TO GIVE RIGHT OF WAY TO EMERGENCY VEHICLE",
                    "FAILURE TO REPORT TRAFFIC ACCIDENT",
                    "FAILURE TO USE CHILD RESTRAINT DEVICE IN VEHICLE",
                    "IMPROPER SOUND DEVICE (WHISTLE, BELL, SIREN)",
                    "IMPROPER STOPPING AT FLASHING RED LIGHT", "IMPROPER USE OF BLUE LIGHTS",
                    "IMPROPER USE OF RED LIGHTS", "IMPROPERLY ON LEFT SIDE OF ROAD",
                    "HITCHHIKING ON LIMITED ACCESS FACILITIES", 
                    "INSTRUCTIONAL PERMIT VIOLATIONS", "LICENSE TO BE IN POSSESSION",
                    "NO PERSON SHALL HAVE MORE THAN ONE OPERATORS LICENSE", 
                    "NO REAR VIEW MIRROR", "NO/EXPIRED KENTUCKY REGISTRATION RECEIPT",
                    "PERMIT UNLICENSED OPERATOR TO OPERATE MOTOR VEHICLE",
                    "REPRESENTING AS ONES OWN ANOTHERS OPER LIC",
                    "RIM OR FRAME OBSCURING LETTERING OR DECAL ON PLATE",
                    "SEAT BELT ANCHORS,CHILD RESTRAINTS") ~ 
      '99Y Non-Reportable Crimes',
    
    # Non-criminal matters
    uor_desc %in% c('ACCIDENTAL DEATH (DROWNING)', 
                    'ACCIDENTAL DEATH (OTHER THAN DROWNING AND HUNTING)',
                    'ACCIDENTAL SHOOTING (OTHER THAN HUNTING)',
                    'DOMESTIC ABUSE DUTIES OF LAW ENFORCEMENT',
                    'EMERGENCY ADMIT,MENTAL HEALTH HOSP.-UNIFIED JUV CO',
                    'EMERGENCY CUSTODY ORDERS UNIFIED JUVENILE CODE',
                    'INVOLUN ADMIT MENTAL HEALTH HOSP - UNIFIED JUV CODE',
                    'INVOLUNTARY HOSPITALIZATION OF MENTALLY ILL - 60/360 DAYS',
                    'NON-CRIMINAL DEATH (NATURAL CAUSES)',
                    'PROPERTY LOST OR ABANDONED NON CRIMINAL',
                    'RECOVERY OF STOLEN PROPERTY',
                    'SERVING BENCH WARRANT FOR COURT',
                    'SERVING WARRANT (FOR OTHER POLICE AGENCY)', "SEE ACCIDENT MODULE", 
                    "PRELIMINARY REPORT NUMBER", "REPORT NUMBER NOT REQUIRED", 
                    "VOIDED REPORT NUMBER", "FINANCIAL CRIME/OUT OF JURISDICTION",
                    "RECOVERY OF STOLEN VEHICLE-OUT OF JURISDICTION", 
                    "NARC INVESTIGATION PENDING", "INJURED PERSON REQUIRING POLICE REPORT",
                    "OVERDOSE", "CIT FORM COMPLETED", "SUICIDE", "UNSUBSTANTIATED RAPE",
                    "CACU INVESTIGATION PENDING", "CACU UNSUBSTANTIATED CASES", 
                    "SVU INVESTIGATION PENDING", "COLD CASE REPORT NUMBER", 
                    "9TH MOBILE PENDING", "RUNAWAY (STATUS OFFENDERS-UNIFIED JUVENILE CODE)",
                    "CACU ASSISTING OTHER AGENCY", "UNSUBSTANTIATED SODOMY",
                    "SUSPICIOUS ACTIVITY/POSSIBLE ABDUCTOR", "JUSTIFIABLE HOMICIDE",
                    "UNSUBSTANTIATED SEXUAL ABUSE", "DV WAITING ON CHARGE",
                    "DEATH INVESTIGATION - LMPD INVOLVED",
                    "DEATH INVESTIGATION - OTHER POLICE AGENCY INVOLVED",
                    "SHOOTING INVESTIGATION - LMPD INVOLVED", 
                    "SHOOTING INVESTIGATION - OTH POLICE AGENCY INVOLVED",
                    "IN-CUSTODY DEATH - CORRECTIONS - IN FACILITY",
                    "IN-CUSTODY DEATH - CORRECTIONS - NOT IN FACILITY",
                    "IN-CUSTODY DEATH - OTHER POLICE AGENCY INVOLVED",
                    "OFFICER NEEDS TO COMPLETE INCIDENT REPORT", 
                    "(NO OFFENSE COUNT) BURGLARY 1A",
                    "(NO OFFENSE COUNT) BURGLARY 2A", "(NO OFFENSE COUNT) BURGLARY 3A", 
                    "ABANDONED ON PRIVATE PROPERTY",
                    "ANY NON CRIMINAL CHARGE NOT COVERED BY THESE CODES", "MISC PENDING",
                    "UNIDENTIFIED PERSON OR REMAINS", "CYBER CRIME PENDING",
                    "SEX OFFENDER REGISTRANT, OUT-OF-STATE, MOVE TO KY",
                    "SUICIDE INVESTIGATION (POLICE ON SCENE)", 
                    "SUICIDE INVESTIGTION - CORRECTIONS - IN FACILITY", 
                    "SUICIDE INVESTIGATION (IN POLICE CUSTODY -OTHER AGENCY)",
                    "GARBAGE AND RUBBISH", "EMA INCIDENT", "FUGITIVE - WARRANT NOT REQUIRED",
                    "FUGITIVE FROM ANOTHER STATE - WARRANT REQUIRED",
                    "INJURED PERSON (LMPD INVOLVED)", "NARC UNSUBSTANTIATED CASES",
                    "NCIC NOT NOTIFIED", "SEE KRS ENTRY") ~ 
      '99Z Non-Criminal Matters',
    
    # all other crimes keep their existing code
    TRUE ~ nibrs_code
    
  )) %>% 
  # the above mutate() call, which is old, creates a variable called 
  # nibrs_offense_type when what we actually need to join via join_nibrs_cats()
  # is nibrs_offense_code, which is the first three characters of 
  # nibrs_offense_type, so we extract that
  mutate(nibrs_offense_code = str_sub(nibrs_offense_type, end = 3)) %>% 
  select(-nibrs_offense_type) %>% 
  join_nibrs_cats() %>%
  # identify location types
  join_location_types(
    file = "crime_categories/location_types_louisville.csv",
    by = "premise_type"
  ) %>%
  # save data
  save_city_data("Louisville") %>%
  glimpse()





# Memphis ---------------------------------------------------------------------

# save data from website
download_crime_data(
  "https://memphisinternal.data.socrata.com/api/views/ybsi-jur4/rows.csv?accessType=DOWNLOAD", 
  "memphis"
)

# process data
read_crime_data("memphis", col_types = cols(.default = col_character())) %>% 
  # add date variable
  add_date_var("offense_date", "mdY IMS p", "America/Chicago") %>% 
  # add year variable and remove offenses before start date
  filter_by_year(yearFirst, yearLast) %>% 
  # remove variables containing duplicate information
  select(-city, -state) %>% 
  # categorize crimes
  mutate(
    nibrs_offense_type = recode(
      agency_crimetype_id,
      "Aggravated Assault" = "13A Aggravated assault",
      "Aggravated Assault/DV" = "13A Aggravated assault",
      "Arson" = "200 Arson",
      "Burglary/Business" = "22B Non-residential Burglary/Breaking & Entering",
      "Burglary/Non-residential" = "22B Non-residential Burglary/Breaking & Entering",
      "Burglary/Non-Residential" = "22B Non-residential Burglary/Breaking & Entering",
      "Burglary/Residential" = "22A Residential Burglary/Breaking & Entering",
      "Carjacking" = "12A Personal Robbery",
      "Disorderly Conduct" = "90C Disorderly Conduct",
      "Driving Under the Influence" = "90D Driving Under the Influence",
      "Drug Equipment Violation" = "35B Drug Equipment Violations",
      "Drugs/Narcotics Violation/Felony" = "35A Drug/Narcotic Violations",
      "Drugs/Narcotics Violation/Misdemeanor" = "35A Drug/Narcotic Violations",
      "Drunkenness" = "90E Drunkenness (except Driving Under the Influence)",
      "Embezzlement" = "270 Embezzlement",
      "False Pretenses/Swindle/Confidence Game" = "26A False Pretenses/Swindle/Confidence Game",
      "Identity Theft" = "26C Impersonation",
      "Intimidation" = "13C Intimidation",
      "Intimidation/DV" = "13C Intimidation",
      "Justifiable Homicide" = "99Z Non-Criminal Matters",
      "Kidnapping/Abduction" = "100 Kidnapping/Abduction",
      "Liquor Law Violations" = "90G Liquor Law Violations (except Driving Under the Influence and Drunkenness)",
      "Murder" = "09A Murder and Nonnegligent Manslaughter",
      "MVT/Bus" = "240 Motor Vehicle Theft",
      "MVT/ISU/Passenger Vehicle" = "240 Motor Vehicle Theft",
      "MVT/Motorcycle" = "240 Motor Vehicle Theft",
      "MVT/Other" = "240 Motor Vehicle Theft",
      "MVT/Passenger Vehicle" = "240 Motor Vehicle Theft",
      "MVT/Tractor Truck" = "240 Motor Vehicle Theft",
      "Negligent Manslaughter" = "09B Negligent Manslaughter",
      "Negligent Vehicular Manslaughter" = "09B Negligent Manslaughter",
      "Other Larceny/Access Device" = "26A False Pretenses/Swindle/Confidence Game",
      "Other Theft/Non-Specific" = "23H All Other Larceny",
      "Other Theft/Scrap Metal" = "23H All Other Larceny",
      "Pocket-Picking" = "23A Pocket-picking",
      "Possible Stolen MVT" = "99Z Non-Criminal Matters",
      "Prescription Forgery" = "250 Counterfeiting/Forgery",
      "Purse-Snatching" = "23B Purse-snatching",
      "Robbery/Business" = "12B Commercial Robbery",
      "Robbery/Individual" = "12A Personal Robbery",
      "Shoplifting/Felony" = "23C Shoplifting",
      "Shoplifting/Misdemeanor" = "23C Shoplifting",
      "Simple Assault" = "13B Simple assault",
      "Simple Assault/DV" = "13B Simple assault",
      "Stolen Property" = "280 Stolen Property Offenses",
      "Stolen Property Offense" = "280 Stolen Property Offenses",
      "Theft & Recovery/Other" = "240 Motor Vehicle Theft",
      "Theft & Recovery/Passenger Vehicle" = "240 Motor Vehicle Theft",
      "Theft from Building" = "23D Theft From Building",
      "Theft from Building/Access Device" = "23D Theft From Building",
      "Theft from Motor Vehicle" = "23F Theft From Motor Vehicle (except Theft of Motor Vehicle Parts or Accessories)",
      "Theft from Semi-trailer" = "23F Theft From Motor Vehicle (except Theft of Motor Vehicle Parts or Accessories)",
      "Theft from Semi-Trailer" = "23F Theft From Motor Vehicle (except Theft of Motor Vehicle Parts or Accessories)",
      "Theft of Construction/Farm Equipment" = "23H All Other Larceny",
      "Theft of Other Trailer" = "23F Theft From Motor Vehicle (except Theft of Motor Vehicle Parts or Accessories)",
      "Theft of Semi-trailer" = "23F Theft From Motor Vehicle (except Theft of Motor Vehicle Parts or Accessories)",
      "Theft of Vehicle Parts/Accessories" = "23G Theft of Motor Vehicle Parts or Accessories",
      "Theft of Watercraft" = "23H All Other Larceny",
      "Threatening Phone Call" = "13C Intimidation",
      "Threatening Phone Call/DV" = "13C Intimidation",
      "Traffic/Misc." = "99Y Non-Reportable Crimes",
      "Trespass of Real Property" = "90J Trespass of Real Property",
      "Vandalism/Felony" = "290 Destruction/Damage/Vandalism of Property (except Arson)",
      "Vandalism/Misdemeanor" = "290 Destruction/Damage/Vandalism of Property (except Arson)",
      "Violation of Protection Order" = "90Z All Other Offenses",
      "Weapon Law Violations/Felony" = "520 Weapon Law Violations",
      "Weapon Law Violations/Misdemeanor" = "520 Weapon Law Violations",
      "Welfare Fraud" = "26D Welfare Fraud",
      "Wire Fraud" = "26E Wire Fraud"
    )
  ) %>% 
  # the above mutate() call, which is old, creates a variable called 
  # nibrs_offense_type when what we actually need to join via join_nibrs_cats()
  # is nibrs_offense_code, which is the first three characters of 
  # nibrs_offense_type, so we extract that
  mutate(nibrs_offense_code = str_sub(nibrs_offense_type, end = 3)) %>% 
  select(-nibrs_offense_type) %>% 
  join_nibrs_cats() %>% 
  # save data
  save_city_data("Memphis") %>% 
  glimpse()





# Mesa ------------------------------------------------------------------------

# save data from website
download_crime_data(
  "https://data.mesaaz.gov/api/views/39rt-2rfj/rows.csv?accessType=DOWNLOAD", 
  "mesa"
)

# process data
read_crime_data(
  "mesa",
  col_types = cols(
    .default = col_character(),
    Latitude = col_double(),
    Longitude = col_double()
  )
) %>% 
  # add date variable
  add_date_var("occurred_date", "mdY", "America/Phoenix") %>% 
  # add year variable and remove offenses before start date
  filter_by_year(2016, yearLast) %>%
  # remove variables containing duplicate information
  select(-report_month, -report_year, -street_number, -street_direction, 
         -street_name, -street_type, -city, -state, -occurred_date, 
         -location_1, -row_id) %>% 
  rename(nibrs_offense_code = 
           national_incident_based_crime_reporting_system_code) %>% 
  # Data for 2019 onwards sometimes has multiple offenses in one row, so we have
  # to separate those into multiple rows where every variable is the same except
  # the offense code. To do this …
  # Separate the codes into multiple columns, with the number of columns set as
  # the maximum number of ', ' separators in the data, plus 1
  separate(
    nibrs_offense_code, 
    into = str_glue(
      "nibrs_code_", 
      "{1:(max(str_count(.$nibrs_offense_code, ', '), na.rm = TRUE) + 1)}"
    ), 
    fill = "right"
  ) %>% 
  # Pivot longer so that each offense in a row is now on a separate row
  pivot_longer(
    starts_with("nibrs_code_"), 
    names_to = "nibrs_code_num", 
    values_to = "nibrs_offense_code", 
    values_drop_na = TRUE
  ) %>% 
  # Remove the variable that contains the column names, since are superfluous
  select(-nibrs_code_num) %>% 
  mutate(
    nibrs_offense_code = case_when(
      nibrs_offense_code %in% c(
        "720", # animal cruelty
        "0908", # accident
        "1038", # warrant arrest
        "2000", # assist other agency
        "3002", "3003", "3004", "3005", "3006", # sudden death
        "3031", "3425", # info
        "4044", # injunction
        "4045", # "OOP SERVICE"
        "4046", # mental health
        "4010", "4015", "4020", "4025", "4030", "4040", "4050", "4055", # info
        "4070", "4080", "4090", # info
        "5000", "5010", "5020", "5025", "5030", # lost/found/seized property
        "5040", "5050", "5060", "5070", "5080", # lost/found/seized property
        "5081", "5082", "5090", "5091", "5092", # lost/found/seized property
        "5093", "5094", # lost/found/seized property
        "5499", # accident
        "6001", "6002", # missing
        "7477", "7478", "7500", # skeleton cases
        "8001", # officer-involved shooting
        "9900", "9905", "9906", "9909", "9910", "9912" # minor violations
      ) ~ "99Z", # non-criminal matters
      nibrs_offense_code %in% c("09C", "90I") ~ "99Z",
      nibrs_offense_code == "120" ~ "12U",
      nibrs_offense_code == "220" ~ "22U",
      nibrs_offense_code == "26F" ~ "26C",
      nibrs_offense_code == "26G" ~ "26E",
      national_incident_based_crime_reporting_system_description == 
        "AGGRAVATED ROBBERY" ~ "12U",
      national_incident_based_crime_reporting_system_description == 
        "DESTRUCTION/DAMAGE/VANDALISM OF PROPERTY" ~ "290",
      national_incident_based_crime_reporting_system_description == 
        "MURDER & NONNEGLIGENT MANSLAUGHTER" ~ "09A",
      national_incident_based_crime_reporting_system_description == 
        "STOLEN PROPERTY OFFENSE" ~ "280",
      national_incident_based_crime_reporting_system_description %in% 
        c("NOT REPORTABLE", "NOT REPOTABLE") ~ "99Z",
      nibrs_offense_code == "90" ~ "99X", # a single case with multiple descriptions, so not possible to identify what this code means
      is.na(nibrs_offense_code) ~ "99X",
      TRUE ~ nibrs_offense_code
    )
  ) %>% 
  join_nibrs_cats() %>% 
  select(-national_incident_based_crime_reporting_system_description) %>%
  # save data
  save_city_data("Mesa") %>%
  glimpse()





# Nashville -------------------------------------------------------------------

# It appears that Nashville is progressively deleting old data from the data
# file, so we will download the old data from CODE itself and then re-upload it
# later

# save data from website
# Source: https://data.nashville.gov/Police/Metro-Nashville-Police-Department-Incidents/2u6v-ujjs
download_crime_data(
  "https://data.nashville.gov/api/views/2u6v-ujjs/rows.csv?accessType=DOWNLOAD", 
  "nashville"
)
# tribble(
#   ~year, ~url,
#   "2013", "https://data.nashville.gov/api/views/hsz5-g34u/rows.csv?accessType=DOWNLOAD",
#   "2014", "https://data.nashville.gov/api/views/sguy-xf8k/rows.csv?accessType=DOWNLOAD",
#   "2015", "https://data.nashville.gov/api/views/ce74-dvvv/rows.csv?accessType=DOWNLOAD",
#   "2016", "https://data.nashville.gov/api/views/tpvn-3k6v/rows.csv?accessType=DOWNLOAD",
#   "2017", "https://data.nashville.gov/api/views/ei8z-vngg/rows.csv?accessType=DOWNLOAD",
#   "2018", "https://data.nashville.gov/api/views/we5n-wkcf/rows.csv?accessType=DOWNLOAD",
#   "2019", "https://data.nashville.gov/api/views/a88c-cc2y/rows.csv?accessType=DOWNLOAD"
# ) %>% 
#   mutate(year = str_glue("nashville_{year}")) %>% 
#   pmap(~ download_crime_data(..2, ..1))

nashville_old <- crimedata::get_crime_data(
  years = 2013:2014, 
  cities = "Nashville", 
  type = "extended"
) %>% 
  rename_all(~ str_remove(., "^nvl_")) %>% 
  select(
    -uid,
    -city_name,
    -offense_type,
    -offense_group,
    -offense_against,
    -location_type,
    -location_category,
    -census_block,
    -date_year,
    -address,
    -zip
  ) %>% 
  rename(
    offense_nibrs = offense_code,
    incident_occurred = date_single,
    primary_key = local_row_id,
    incident_number = case_number
  ) %>% 
  mutate(
    across(everything(), as.character),
    across(c(longitude, latitude), as.double)
  )


# process data
nashville_data <- read_crime_data(
  "nashville",
  col_types = cols(
    .default = col_character(),
    Latitude = col_double(),
    Longitude = col_double()
  )
) %>% 
  bind_rows(nashville_old) %>% 
  # add date variable
  add_date_var("incident_occurred", c("Ymd T", "mdY IMS p"), "America/Chicago") %>% 
  # add year variable and remove offenses before start date
  filter_by_year(2013, yearLast) %>%
  # remove variables containing duplicate information
  select(-report_type, -incident_status_code, -rpa, -zone, -location_code,
         -victim_number, -victim_type, -mapped_location) %>% 
  # categorize offenses
  rename(nibrs_offense_code = offense_nibrs) %>% 
  mutate(
    nibrs_offense_code = recode(
      str_to_upper(nibrs_offense_code),
      "09C" = "99Z",
      "13D" = "13C",
      "26F" = "26C",
      "26G" = "26E",
      "720" = "90Z",
      "730" = "90Z",
      "850" = "90Z"
    ),
    nibrs_offense_code = ifelse(
      nibrs_offense_code %in% c(
        "620", # accident
        "680", "685", "690", "695", # sudden death,
        "700", # escape
        "715", "780", "810", # lost/found property
        "735", # civil case
        "740", # admin,
        "760", # overdose
        "90I" # runaway
      ),
      "99Z",
      nibrs_offense_code
    ),
    nibrs_offense_code = ifelse(
      is.na(nibrs_offense_code), 
      "99X", 
      nibrs_offense_code
    )
  ) %>% 
  join_nibrs_cats() %>% 
  # identify location types
  join_location_types(
    file = "crime_categories/location_types_nashville.csv",
    by = "location_description"
  ) %>% 
  # save data
  save_city_data("Nashville") %>%
  glimpse()





# New York ------

# save data from website
download_crime_data(
  "https://data.cityofnewyork.us/api/views/qgea-i56i/rows.csv?accessType=DOWNLOAD", 
  "new_york_early"
)
# Source: https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Current-Year-To-Date-/5uac-w243
download_crime_data(
  "https://data.cityofnewyork.us/api/views/5uac-w243/rows.csv?accessType=DOWNLOAD", 
  "new_york_late"
)

# process data
here::here("original_data") %>% 
  dir(pattern = "^raw_new_york_", full.names = TRUE) %>% 
  map_dfr(
    read_crime_data, 
    col_types = cols(.default = col_character()),
    na = c("", "NA", "(null)")
  ) %>% 
  # since NYC includes both start and end dates, so we will process the dates
  # manually rather than using add_date_var()
  mutate(
    # Some early NYC offences have times equal to 24:00:00, which cannot be 
    # parsed by R date functions
    cmplnt_fr_tm = if_else(cmplnt_fr_tm == "24:00:00", "23:59:59", cmplnt_fr_tm),
    cmplnt_to_tm = if_else(cmplnt_to_tm == "24:00:00", "23:59:59", cmplnt_to_tm),
    # For offenses with only one date, there is some inconsistency in which date
    # is included. In almost all cases the start date is the date provided, but
    # occasionally only the end date is present. The next line copies the single
    # date from the end date to the start date and (separately) the single time
    # to end date to the start date.
    cmplnt_fr_dt = ifelse(is.na(cmplnt_fr_dt), cmplnt_to_dt, cmplnt_fr_dt),
    cmplnt_fr_tm = ifelse(is.na(cmplnt_fr_tm), cmplnt_to_tm, cmplnt_fr_tm),
    # For offenses with a time but no date in either column, use the reported 
    # date as the start date
    cmplnt_fr_dt = ifelse(is.na(cmplnt_fr_dt) & !is.na(cmplnt_fr_tm), rpt_dt, 
                          cmplnt_fr_dt)
  ) %>% 
  report_status("Impossible times changed") %>% 
  # Now we can convert the date strings to date objects
  mutate(
    Date.Start.Temp = parse_date_time(paste(cmplnt_fr_dt, cmplnt_fr_tm), 'mdY T', 
                                      tz = 'America/New_York'),
    Date.End.Temp = parse_date_time(paste(cmplnt_to_dt, cmplnt_to_tm), 'mdY T', 
                                    tz = 'America/New_York'),
    Date.Temp = strftime(Date.Start.Temp + ((Date.End.Temp - Date.Start.Temp)/2), 
                         format = '%Y-%m-%d %H:%M', tz = 'America/New_York'),
    Date.Temp = ifelse(is.na(Date.Temp), 
                       strftime(Date.Start.Temp, format = "%Y-%m-%d %H:%M", 
                                tz = 'America/New_York'), 
                       Date.Temp),
    date_year = year(Date.Temp),
    date_start = strftime(Date.Start.Temp, format = '%Y-%m-%d %H:%M', 
                          tz = 'America/New_York'),
    date_end = strftime(Date.End.Temp, format = '%Y-%m-%d %H:%M', 
                        tz = 'America/New_York'),
    date_single = strftime(Date.Temp, format = '%Y-%m-%d %H:%M', 
                           tz = 'America/New_York'),
    multiple_dates = TRUE
  ) %>% 
  select(-Date.Start.Temp, -Date.End.Temp, -Date.Temp) %>% 
  report_status("Strings converted to dates") %>% 
  {
    cat("  Failures:  Date.Start:", sum(is.na(.$date_start)), " Date.End:", 
        sum(is.na(.$date_end)), " Date.Single:", sum(is.na(.$date_single)), 
        "\n")
    .
  } %>% 
  # filter data by year
  filter_by_year(2007, yearLast) %>% 
  # categorize crimes
  join_nibrs_cats("crime_categories/categories_new_york.csv",
                  by = c('ofns_desc', 'pd_desc')) %>% 
  mutate(nibrs_offense_type = case_when(
    # burglary
    nibrs_offense_code == "220" & pd_desc %in% c(
      "BURGLARY,RESIDENCE,DAY", 
      "BURGLARY,RESIDENCE,NIGHT", 
      "BURGLARY,RESIDENCE,UNKNOWN TIM"
    ) ~ "22A Residential Burglary/Breaking & Entering",
    nibrs_offense_code == "220" ~ 
      "22B Non-residential Burglary/Breaking & Entering",
    
    # robbery
    nibrs_offense_code == "120" & pd_desc %in% c(
      "ROBBERY,PERSONAL ELECTRONIC DEVICE",
      "ROBBERY,OPEN AREA UNCLASSIFIED", "ROBBERY,RESIDENTIAL COMMON AREA",
      "ROBBERY,DWELLING", "ROBBERY,POCKETBOOK/CARRIED BAG", 
      "ROBBERY,BEGIN AS SHOPLIFTING", "ROBBERY,HOME INVASION", 
      "ROBBERY,NECKCHAIN/JEWELRY", "ROBBERY,PUBLIC PLACE INSIDE", 
      "ROBBERY,BICYCLE", "ROBBERY,CLOTHING", "ROBBERY,CAR JACKING", 
      "ROBBERY,ATM LOCATION") ~ "12A Personal Robbery",
    nibrs_offense_code == "120" ~ "12B Commercial Robbery",
    
    # all other crimes
    TRUE ~ nibrs_offense_type
  ),
  # where necessary, update the NIBRS code to match the new type
  nibrs_offense_code = case_when(
    nibrs_offense_type == "22A Residential Burglary/Breaking & Entering" ~ 
      "22A",
    nibrs_offense_type == "22B Non-residential Burglary/Breaking & Entering" ~ 
      "22B",
    nibrs_offense_type == "12A Personal Robbery" ~ "12A",
    nibrs_offense_type == "12B Commercial Robbery" ~ "12B",
    nibrs_offense_type == "12U Other Robbery" ~ "12U",
    TRUE ~ nibrs_offense_code
  )) %>% 
  # identify location types
  join_location_types(
    file = "crime_categories/location_types_new_york.csv",
    by = "prem_typ_desc"
  ) %>% 
  # save data
  save_city_data("New York") %>% 
  glimpse()





# St Louis --------------------------------------------------------------------

# St Louis has data back to 2008 in *monthly* CSV files, but without NIBRS or
# location-type codes and hidden behind a web form. Fortunately, they left
# directory listing turned on until 2019. Unfortunately, they've now turned off
# directory listing so the 2020 data have to be downloaded manually and added to
# the existing directory of files previously scraped from the website.

# There is a useful FAQ at http://www.slmpd.org/crime/CrimeReferenceDocument.pdf

# since monthly reporting means 100+ files, create a sub-directory for them
if (!dir.exists(here::here("original_data/raw_st_louis/")))
  dir.create(here::here("original_data/raw_st_louis/"))

# save data from website
xml2::read_html("https://slmpd.org/CrimeReport.aspx") %>%
  rvest::html_nodes("a") %>%
  rvest::html_attr("href") %>%
  str_subset("(CSV|csv)$") %>% 
  {
    str_glue("http://www.slmpd.org{.}")
  } %>% 
  enframe() %>% 
  mutate(name = str_glue("st_louis/st_louis_{str_pad(name, 3, pad = '0')}")) %>% 
  pwalk(~ download_crime_data(..2, ..1))

# process data
here::here("original_data/raw_st_louis") %>% 
  dir(full.names = TRUE) %>% 
  # read data and rename columns that have different names in some data files
  map_dfr(function (file) {
    
    x <- read_crime_data(
      file, 
      col_types = cols(
        .default = col_character(),
        Count = col_number(),
        District = col_number()
      )
    )
    
    if ("date_occur" %in% names(x)) 
      x <- rename(x, date_occured = date_occur)
    if ("flag_crime" %in% names(x)) 
      x <- rename(x, new_crime_indicator = flag_crime)
    if ("flag_unfounded" %in% names(x)) 
      x <- rename(x, unfounded_crime_indicator = flag_unfounded)
    if ("flag_administrative" %in% names(x)) 
      x <- rename(x, administrative_adjustment_indicator = flag_administrative)
    if ("coded_month" %in% names(x)) 
      x <- rename(x, month_reportedto_mshp = coded_month)
    
    x
    
  }) %>% 
  # remove unfounded crimes
  filter(is.na(unfounded_crime_indicator)) %>% 
  # add date var 
  add_date_var("date_occured", "mdY HM", "America/Chicago") %>% 
  filter_by_year(2008, yearLast) %>% 
  # pad crime codes with leading zeros to simplify categorization, also 
  # extracting the first three digits since this is typically all we need
  mutate(
    crime = str_pad(crime, 6, pad = "0"),
    crime2 = str_sub(crime, end = 2),
    crime3 = str_sub(crime, end = 3)
  ) %>% 
  # categorize crimes
  # some of this code is adapted from 
  # https://github.com/slu-openGIS/compstatr/blob/master/R/categorize.R
  mutate(nibrs_offense_code = case_when(
    crime3 == "010" ~ "09A", # homicide
    crime3 %in% c("021", "022", "023") ~ "11A", # rape
    crime3 %in% c("031", "032", "038") ~ "12A", # personal robbery
    crime3 %in% c("033", "034", "035", "036") ~ "12B", # commercial robbery
    crime3 == "037" ~ "12U", # other robbery,
    crime2 == "04" ~ "13A", # aggravated assault
    crime3 == "051" ~ "22A", # non-residential burglary
    crime3 %in% c("052", "053") ~ "22B", # non-residential burglary
    crime3 == "061" ~ "23A", # pick-pocketing
    crime3 == "062" ~ "23B", # purse-snatching
    crime3 == "063" ~ "23C", # shoplifting
    crime3 == "067" ~ "23D", # theft from building
    crime3 == "068" ~ "23E", # theft from coin-operated machine
    crime3 == "064" ~ "23F", # theft from motor vehicle
    crime3 == "065" ~ "23G", # theft of motor-vehicle parts
    crime3 %in% c("066", "069") ~ "23H", # all other larceny
    crime2 == "07" ~ "240", # motor-vehicle theft
    crime2 == "08" ~ "200", # arson
    crime3 == "091" ~ "13B", # simple assault
    crime3 %in% c("101", "102", "103", "104", "109") ~ "250", # forgery
    crime3 == "111" ~ "90A", # bad checks
    crime2 == "11" ~ "26U", # all fraud
    crime2 == "12" ~ "270", # embezzlement
    crime3 == "130" ~ "280", # stolen property offenses
    crime2 == "14" ~ "290", # property destruction
    crime2 == "15" ~ "520", # weapons offenses
    crime2 == "16" ~ "40A", # prostitution
    crime3 == "172" ~ "11B", # sodomy
    crime3 %in% c("173", "174", "175", "176", "179") ~ "11D", # fondling
    crime3 == "177" ~ "36A", # incest
    crime3 == "171" ~ "36B", # statutory rape
    crime2 == "18" ~ "35A", # drug offenses
    crime3 %in% c("191", "199") ~ "39A", # gambling
    crime3 == "192" ~ "39B", # promoting gambling
    crime2 == "20" ~ "90F", # family offenses
    crime2 == "21" ~ "90D", # drink driving
    crime2 == "22" ~ "90G", # liquor violations
    crime2 == "24" | crime3 %in% c("178") ~ "90C", # disorderly conduct
    crime2 == "25" ~ "90B", # curfew/loitering violations
    crime == "261200" ~ "100", # kidnapping
    crime == "261310" ~ "13C", # intimidation
    crime3 == "262" ~ "370", # pornography
    crime %in% c("266510", "266520") ~ "90J", # trespassing
    crime3 %in% c("092", "095", "263", "264", "265", "266") | 
      crime %in% c("261100", "261210", "261320", "261330", "261400", "261500") ~ 
      "90Z", # all other offenses
    TRUE ~ "99X" # uncategorizable offenses
  )) %>% 
  select(-crime2, -crime3) %>% 
  join_nibrs_cats() %>% 
  # St Louis data have co-ordinates in a local CRS, so convert these to lat/long
  # First convert missing co-ordinates to a fixed value, since NAs aren't 
  # allowed in SF object co-ordinates.
  replace_na(list(x_coord = 0, y_coord = 0)) %>% 
  # Set a flag for the crimes with missing co-ordinates so we can extract them
  # later for geocoding, since the zero co-ordinates are about to be converted
  # to lat/lon pairs
  mutate(needs_geocoding = x_coord == 0) %>% 
  # This proj string is from https://stackoverflow.com/a/57559562/8222654 and
  # has been checked against the addresses in the data
  st_as_sf(
    coords = c("x_coord", "y_coord"), 
    crs = "+proj=tmerc +lat_0=35.83333333333334 +lon_0=-90.5 +k=0.9999333333333333 +x_0=250000 +y_0=0 +ellps=GRS80 +datum=NAD83 +to_meter=0.3048006096012192 +no_defs"
  ) %>% 
  st_transform(4326) %>% 
  mutate(
    longitude = sapply(st_geometry(.), function (x) x[[1]]), 
    latitude = sapply(st_geometry(.), function (x) x[[2]]),
    # Replace co-ordinates with NAs for cases that need geocoding because the
    # co-ordinates have been transformed from zeros
    longitude = ifelse(needs_geocoding, NA, longitude),
    latitude = ifelse(needs_geocoding, NA, latitude)
  ) %>% 
  select(-needs_geocoding) %>% 
  st_drop_geometry() %>% 
  save_city_data("St Louis") %>%
  glimpse()





# San Francisco ---------------------------------------------------------------

# save data from website
download_crime_data(
  "https://data.sfgov.org/api/views/tmnf-yvry/rows.csv?accessType=DOWNLOAD", 
  "san_francisco_early"
)
download_crime_data(
  "https://data.sfgov.org/api/views/wg3w-h783/rows.csv?accessType=DOWNLOAD", 
  "san_francisco_late"
)

# process data
list(
  read_crime_data(
    "san_francisco_early",
    col_types = cols(.default = col_character())
  ) %>% 
    rename(
      incident_number = incidnt_num,
      incident_category = category,
      incident_description = descript,
      incident_day_of_week = day_of_week,
      latitude = y,
      longitude = x,
      police_district = pd_district
    ) %>% 
    mutate(
      incident_date_time = parse_date_time(paste(date, time), "mdY HM"),
      file = "early"
    ) %>% 
    filter(incident_date_time < ymd("2018-01-01")),
  read_crime_data(
    "san_francisco_late",
    col_types = cols(.default = col_character())
  ) %>% 
    rename(
      address = intersection,
      location = point
    ) %>% 
    mutate(
      incident_date_time = 
        parse_date_time(paste(incident_date, incident_time), "Ymd HM"),
      file = "late"
    ) %>% 
    filter(incident_date_time >= ymd("2018-01-01"))
) %>% 
  bind_rows() %>% 
  select(-incident_date, -incident_time, -date, -time, -incident_datetime, 
         -incident_year, -incident_day_of_week, -location) %>% 
  mutate_at(vars(one_of("incident_category", "incident_description")), 
            str_to_upper) %>% 
  mutate(
    incident_category = recode(
      incident_category,
      "LARCENY THEFT" = "LARCENY/THEFT",
      "FORGERY AND COUNTERFEITING" = "FORGERY/COUNTERFEITING",
      "HUMAN TRAFFICKING, COMMERCIAL SEX ACTS" = 
        "HUMAN TRAFFICKING (A), COMMERCIAL SEX ACTS",
      "MOTOR VEHICLE THEFT?" = "MOTOR VEHICLE THEFT",
      "OTHER" = "OTHER OFFENSES",
      "SUSPICIOUS" = "SUSPICIOUS OCC",
      "MOTOR VEHICLE THEFT" = "VEHICLE THEFT",
      "WARRANT" = "WARRANTS",
      "WEAPON LAWS" = "WEAPONS CARRYING ETC",
      "WEAPONS OFFENCE" = "WEAPONS OFFENSE"
    ),
    incident_date_time = strftime(incident_date_time, format = "%F %R")
  ) %>% 
  add_date_var("incident_date_time", "Ymd HM", "America/Los_Angeles") %>% 
  filter_by_year(yearFirst, yearLast) %>% 
  join_nibrs_cats("crime_categories/categories_SF.csv",
                  by = c('incident_category', 'incident_description')) %>% 
  save_city_data("San Francisco") %>%
  glimpse()





# Seattle ---------------------------------------------------------------------

# Seattle data are from 
# https://data.seattle.gov/Public-Safety/SPD-Crime-Data-2008-Present/tazs-3rd5

# save data from website
download_crime_data(
  "https://data.seattle.gov/api/views/tazs-3rd5/rows.csv?accessType=DOWNLOAD",
  "seattle"
)

# process data
read_crime_data("seattle") %>% 
  select(-group_a_b, -crime_against_category, -offense_parent_group) %>% 
  rename(
    block_address = x100_block_address, 
    nibrs_offense_code = offense_code
  ) %>% 
  # Seattle data has start and end dates, so we process the dates manually
  mutate(
    # if start date is missing, use end date
    # if end date is missing, use start date
    # if start and end date are missing, use reported date for both
    date_start_chr = case_when(
      is.na(offense_start_date_time) & is.na(offense_end_date_time) ~ report_date_time,
      is.na(offense_start_date_time) ~ as.character(offense_end_date_time),
      TRUE ~ as.character(offense_start_date_time)
    ),
    date_end_chr = case_when(
      is.na(offense_start_date_time) & is.na(offense_end_date_time) ~ report_date_time,
      is.na(offense_end_date_time) ~ as.character(offense_start_date_time),
      TRUE ~ offense_end_date_time
    ),
    # Process the dates into date-time objects to validate them
    across(
      c(date_start_chr, date_end_chr),
      as_datetime,
      format = "%m/%d/%Y %I:%M:%S %p",
      tz = "America/Los_Angeles",
      .names = "{str_remove(.col, '_chr')}"
    ),
    # Calculate the mid-point and year
    date_single = date_start + ((date_end - date_start) / 2),
    date_year = year(date_single),
    # Convert dates back into strings in ISO format so they can be stored 
    # robustly
    across(c(date_start, date_single, date_end), as.character),
    # Flag that there are multiple dates
    multiple_dates = TRUE
  ) %>% 
  select(
    -offense_start_date_time, 
    -offense_end_date_time, 
    -date_start_chr, 
    -date_end_chr
  ) %>%
  # Filter by year
  filter_by_year(2008, yearLast) %>%
  # Categorize crimes
  mutate(
    nibrs_offense_code = recode(
      nibrs_offense_code, 
      "09C" = "99Z", 
      "26F" = "26C", 
      "26G" = "26E", 
      "720" = "90Z"
    )
  ) %>% 
  join_nibrs_cats() %>%
  save_city_data("Seattle") %>%
  glimpse()





# Tucson ----------------------------------------------------------------------

# Data are from
# http://gisdata.tucsonaz.gov/datasets?group_ids=b6a49faa168647d8b56e1a06bd53600f&page=2&sort=name&t=police
# in individual files for each year from 2009. The variable names are not
# consistent across years, so some adjustment is required before rows are bound
# together. Columns are in a different order in some files, so `row_bind()` is
# used to bind by column names.

# save data from website
tribble(
  ~year, ~url,
  "2009", "https://opendata.arcgis.com/datasets/67498f323aaa481d9f579089da0e2793_25.csv?outSR=%7B%22latestWkid%22%3A2868%2C%22wkid%22%3A2868%7D",
  "2010", "https://opendata.arcgis.com/datasets/d967085e54314a02a1b104bb54b457d5_26.csv?outSR=%7B%22latestWkid%22%3A2868%2C%22wkid%22%3A2868%7D",
  "2011", "https://opendata.arcgis.com/datasets/047a71ecb92c424d946abeb80d75b5de_27.csv?outSR=%7B%22latestWkid%22%3A2868%2C%22wkid%22%3A2868%7D",
  "2012", "https://opendata.arcgis.com/datasets/a2365412f9624f79afa09efd7138ea37_28.csv?outSR=%7B%22latestWkid%22%3A2868%2C%22wkid%22%3A2868%7D",
  "2013", "https://opendata.arcgis.com/datasets/292605340f6846459aa2b1bf3a7b478a_29.csv?outSR=%7B%22latestWkid%22%3A2868%2C%22wkid%22%3A2868%7D",
  "2014", "https://opendata.arcgis.com/datasets/771c37cda6ab4df28b15d685361d4aa5_30.csv?outSR=%7B%22latestWkid%22%3A2868%2C%22wkid%22%3A2868%7D",
  # "2015", "https://opendata.arcgis.com/datasets/705cd0b16c0141259a73c12266e28792_31.csv?outSR=%7B%22latestWkid%22%3A2868%2C%22wkid%22%3A2868%7D",
  "2016", "https://opendata.arcgis.com/datasets/ff59ac036cc14689923596abf88f3e24_32.csv?outSR=%7B%22latestWkid%22%3A2868%2C%22wkid%22%3A2868%7D",
  "2017", "https://opendata.arcgis.com/datasets/ef95666c825645868d0f9db6770af969_33.csv?outSR=%7B%22latestWkid%22%3A2868%2C%22wkid%22%3A2868%7D",
  "2018", "https://opendata.arcgis.com/datasets/6a11fe12a2f9444fa16e7b7ac810727e_40.csv?outSR=%7B%22latestWkid%22%3A2868%2C%22wkid%22%3A2868%7D",
  "2019", "https://opendata.arcgis.com/api/v3/datasets/9205a32aeab34091b1cd9bcea08eccfe_48/downloads/data?format=csv&spatialRefId=2868",
  "2020", "https://opendata.arcgis.com/api/v3/datasets/0cd8b23211b84cdb9334a6b548916623_54/downloads/data?format=csv&spatialRefId=2868"
) %>% 
  mutate(year = str_glue("tucson_{year}")) %>% 
  pwalk(~ download_crime_data(..2, ..1))

# process data
tucson_data <- here::here("original_data") %>% 
  dir(pattern = '^raw_tucson_', full.names = TRUE) %>% 
  map_dfr(read_crime_data, col_types = cols(.default = col_character())) %>% 
  rename(longitude = x, latitude = y) %>% 
  mutate(
    # In some files, `address` is mis-spelled in the variable names, which 
    # results in multiple variables after binding. Deal with this by putting all
    # variables into the correctly spelled one.
    address_public = ifelse(is.na(address_public), addrress_public,
                            address_public),
    # Occasionally `hour_occu` starts with a `-`, so replace these with a zero
    hour_occu = str_replace(hour_occu, "^\\-", "0"),
    # When `date_occu` or `hour_occu` is missing, use `date_fnd` or `hour_fnd` 
    # instead. If those as missing, too, use `date_rept` or `hour_rept`.
    date_occu = case_when(
      is.na(date_occu) & is.na(date_fnd) ~ date_rept,
      is.na(date_occu) ~ date_fnd,
      TRUE ~ date_occu
    ),
    hour_occu = case_when(
      is.na(hour_occu) & is.na(hour_fnd) ~ hour_rept,
      is.na(hour_occu) ~ hour_fnd,
      TRUE ~ hour_occu
    ),
    # Merge the date and time strings into a single date-time object
    date_occurred = paste(str_sub(date_occu, 1, 10), hour_occu),
    # The field statutdesc has triple quotes around it in the CSV file, only two
    # pairs of which are removed by read_csv, so the remainder are removed here
    statutdesc = str_remove_all(statutdesc, "\""),
    loc_method = ifelse(is.na(loc_method), loc_status, loc_method),
  ) %>% 
  add_date_var("date_occurred", "Ymd HM", "America/Phoenix") %>% 
  filter_by_year(2009, yearLast) %>% 
  # A few crimes have no offense category listed. We will remove these now to
  # make joining the NIBRS variables easier. This also removes the large number
  # of cases in the 2017 file that do not relate to crime (e.g. traffic stops)
  # and for which the statutdesc field is set to the string 'N/A'
  filter(!is.na(statutdesc) & statutdesc != "N/A") %>% 
  # There are many variables that are either duplicates or are NA for the vast
  # majority of rows, so we'll remove these to save space
  select(
    -addrress_public, -point_x, -point_y, -month_rept, -year_rept, -dow_rept, 
    -time_rept, -loc_method, -loc_status, -date_occurred, -city, -state,
    -datetime_occu, -datetime_rept, -lat, -long, -city_geo, -secure, -replace,
    -month_occu, -month_occu_string, -year_occu, -dow_occu, -dow_occu_string,
    -time_occu, -censustract, -censusblock, -datasource, -emunit, 
    -address_100blk
  ) %>% 
  # Transform co-ordinates if not in WGS84 (i.e. if the co-ordinates are so much
  # larger than is possible for Lat/Lon pairs that it can't be due to rounding
  # errors or similar). This code creates an SF object from the existing tibble,
  # transforms it, extracts the transformed co-ordinates and converts the SF
  # object back to a tibble. The custom CRS is taken from the shapefile used to
  # obtain the 2016 data (see above). sf_as_sf() doesn't allow missing values,
  # so we must first remove any rows that don't have co-ordinates
  drop_na(longitude, latitude) %>% 
  st_as_sf(coords = c("longitude", "latitude"), 
           crs = paste("+proj=tmerc +lat_0=31 +lon_0=-111.9166666666667", 
                       "+k=0.9999 +x_0=213360 +y_0=0 +ellps=GRS80",
                       "+towgs84=0,0,0,0,0,0,0 +units=ft +no_defs")) %>% 
  st_transform(4326) %>% 
  mutate(
    longitude = sapply(st_geometry(.), function (x) x[[1]]), 
    latitude = sapply(st_geometry(.), function (x) x[[2]])
  ) %>% 
  st_set_geometry(NULL) %>% 
  # join NIBRS categories
  join_nibrs_cats(
    "crime_categories/categories_Tucson.csv", 
    by = "statutdesc"
  ) %>% 
  save_city_data("Tucson") %>%
  glimpse()





# Virginia Beach --------------------------------------------------------------

# Virginia Beach moved to a new data file in 2017, and appear to have deleted
# the old file from their open data website. New data are from
# https://data.vbgov.com/dataset/police-incident-reports

# save data from website
download_crime_data(
  "https://s3.amazonaws.com/vbgov-ckan-open-data/Police+Incident+Reports.csv", 
  "virginia_beach_late"
)

# process data
list(
  filter(
    read_crime_data("virginia_beach_early"),
    year(parse_date_time(date_occured, "mdY T", tz = "America/New_York")) < 2016
  ),
  filter(
    read_crime_data("virginia_beach_mid"),
    between(year(date_occured), 2016, 2019)
  ),
  filter(
    read_crime_data("virginia_beach_late"),
    year(date_occured) >= 2020
  )
) %>% 
  map(
    mutate, 
    across(
      any_of(c("date_reported", "date_occured", "date_found", "zip")), 
      as.character
    )
  ) %>% 
  bind_rows() %>% 
  mutate(
    address = str_replace_all(location, "\\n", " ") %>% trimws(),
    location = str_extract(location, "\\(.+?\\)$") %>% 
      str_replace("\\((.+?)\\)", "\\1")
  ) %>%
  separate(location, c("latitude", "longitude"), ", ") %>%
  mutate_at(vars(c("latitude", "longitude")), as.double) %>%
  add_date_var("date_occured", c("mdY T", "Ymd T"), "America/New_York") %>%
  filter_by_year(2013, yearLast) %>% 
  join_nibrs_cats(
    here::here("crime_categories/categories_VB.csv"),
    by = "offense_code"
  ) %>% 
  save_city_data("Virginia Beach") %>%
  glimpse()

