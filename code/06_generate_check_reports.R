# This file generates a report for each city summarising the data available

here::here("temp_data/") %>% 
  dir(pattern = "^final_(.+?)_data.Rds") %>% 
  str_remove("^final_") %>% 
  str_remove("_data.Rds$") %>% 
  walk(function (x) {
    rmarkdown::render(
      input = here::here("code/06_check_data.Rmd"),
      output_file = here::here(str_glue("code/crime_summary_{x}.pdf")),
      params = list(this_city = str_to_title(str_replace_all(x, "_", " ")))
    )    
  })

