#' Get Path to Sample Hansard Data
#'
#' Returns the path to sample Hansard CSV files included with the package
#' for testing and demonstration purposes.
#'
#' @param year Character vector of years to get (e.g., "2024", "2025").
#'   If NULL, returns the base houseCSV directory.
#' @return Character vector of paths to sample data directories
#' @export
#' @examples
#' # Get all sample data directory
#' sample_path <- hansard_sample_data()
#'
#' # Get specific years
#' sample_2024 <- hansard_sample_data("2024")
#' sample_files <- list.files(sample_2024, pattern = "*.csv", full.names = TRUE)
hansard_sample_data <- function(year = NULL) {
  base_path <- system.file("extdata", "houseCSV", package = "hansardR")

  if (is.null(year)) {
    return(base_path)
  }

  # Return paths for specific years
  year_paths <- file.path(base_path, year)

  # Check which paths exist
  existing_paths <- year_paths[dir.exists(year_paths)]

  if (length(existing_paths) == 0) {
    available_years <- list.dirs(base_path, recursive = FALSE, full.names = FALSE)
    available_years <- available_years[grepl("^\\d{4}$", available_years)]

    stop("No sample data found for year(s): ", paste(year, collapse = ", "),
         "\nAvailable years: ", paste(available_years, collapse = ", "))
  }

  return(existing_paths)
}

#' List Available Sample Data
#'
#' @return Data frame with information about available sample files
#' @export
#' @examples
#' # See what sample data is available
#' hansard_sample_info()
hansard_sample_info <- function() {
  base_path <- hansard_sample_data()

  if (!dir.exists(base_path)) {
    return(data.frame(
      year = character(0),
      files = integer(0),
      total_size_kb = numeric(0),
      sample_files = character(0)
    ))
  }

  year_dirs <- list.dirs(base_path, recursive = FALSE, full.names = TRUE)
  year_dirs <- year_dirs[grepl("^\\d{4}$", basename(year_dirs))]

  if (length(year_dirs) == 0) {
    return(data.frame(
      year = character(0),
      files = integer(0),
      total_size_kb = numeric(0),
      sample_files = character(0)
    ))
  }

  info <- purrr::map_dfr(year_dirs, ~ {
    year <- basename(.x)
    files <- list.files(.x, pattern = "*.csv", full.names = TRUE)

    if (length(files) > 0) {
      sizes <- file.size(files)
      total_size_kb <- round(sum(sizes, na.rm = TRUE) / 1024, 1)
      file_names <- basename(files)  # Extract basenames safely
    } else {
      total_size_kb <- 0
      file_names <- character(0)
    }

    tibble::tibble(
      year = year,
      files = length(files),
      total_size_kb = total_size_kb,
      sample_files = if(length(file_names) > 0) paste(file_names, collapse = ", ") else ""
    )
  })

  return(info)
}
