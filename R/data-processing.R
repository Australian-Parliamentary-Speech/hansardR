#' Load and Clean CSV File
#'
#' Loads a CSV file and applies standard cleaning operations
#'
#' @param file_path Path to CSV file
#' @param validate Should file structure be validated first?
#' @return Cleaned data frame or NULL if validation fails
#' @export
load_hansard_csv <- function(file_path, validate = TRUE) {

  if (validate) {
    validation <- validate_csv_structure(file_path)
    if (!validation$valid) {
      warning("File validation failed for: ", file_path)
      return(NULL)
    }
  }

  tryCatch({
    df <- readr::read_csv(
      file_path,
      col_types = readr::cols(
        name.id = readr::col_character(),
        page.no = readr::col_double(),
        speaker_no = readr::col_integer(),
        .default = readr::col_character()
      ),
      na = c("", "N/A"),
      show_col_types = FALSE
    )

    # Add metadata
    df$source_file <- basename(file_path)
    df$session_date <- parse_session_date(basename(file_path))
    # df$file_hash <- digest::digest(file_path, file = TRUE)  # Optional: remove if not needed

    return(df)

  }, error = function(e) {
    warning("Error loading file ", file_path, ": ", conditionMessage(e))
    return(NULL)
  })
}

#' Parse Session Date from Filename
#'
#' @param filename Filename to parse
#' @return Date object or NA
parse_session_date <- function(filename) {
  date_match <- stringr::str_extract(filename, "\\d{4}-\\d{2}-\\d{2}")
  if (!is.na(date_match)) {
    return(as.Date(date_match))
  }
  return(as.Date(NA))
}

#' Clean Data Values
#'
#' @param x Vector to clean
#' @return Cleaned vector
clean_value <- function(x) {
  dplyr::case_when(
    is.na(x) ~ NA_character_,
    x == "N/A" ~ NA_character_,
    TRUE ~ as.character(x)
  )
}

#' Parse Time Strings
#'
#' @param time_str Time string like "(12:04):"
#' @return Cleaned time string or NA
parse_time <- function(time_str) {
  if (is.na(time_str) || time_str == "N/A") return(NA)

  time_match <- stringr::str_extract(time_str, "\\d{1,2}:\\d{2}")
  if (!is.na(time_match)) {
    return(paste0(time_match, ":00"))
  }
  return(NA)
}
