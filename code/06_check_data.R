walk(
  c(
    # "Austin", "Boston", "Chicago", "Detroit", "Fort Worth", "Kansas City", 
    # "Los Angeles", "Louisville", "Mesa", "Nashville", "New York",
    # "San Francisco", "Seattle", "St Louis", "Tucson"
    "Virginia Beach"
  ),
  function (city) {
    rmarkdown::render(
      input = here::here("code/06_check_data_template.Rmd"),
      output_format = "pdf_document",
      output_file = str_glue("crime_summary_{slugify(city, '_')}.pdf"),
      output_dir = here::here("crime_summaries/"),
      params = list(city = city)
    )
  }
)
