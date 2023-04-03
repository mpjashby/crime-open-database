# This code creates a single geo-package containing the boundaries of all the
# cities in the dataset

# Load packages
library(sf)
library(tidyverse)
library(tigris)

# Extract city boundaries and save as file
cities |> 
  select(name, fips, census_name) |> 
  pmap(function(name, fips, census_name) {
    
    places(state = fips, year = 2016) |> 
      filter(NAME == census_name) |> 
      st_transform("EPSG:4326") |> 
      mutate(city_name = name) |> 
      select(city_name, geometry)
    
  }) |> 
  bind_rows() |> 
  write_sf(here::here("spatial_data/city_outlines.gpkg")) |> 
  print()
