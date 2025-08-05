#' Validate CSV File Structure
#'
#' Checks if a CSV file has the expected structure for Hansard data
#'
#' @param file_path Path to CSV file
#' @param expected_cols Expected column names (NULL for standard set)
#' @return List with validation results
#' @export
validate_csv_structure <- function(file_path, expected_cols = NULL) {

  if (is.null(expected_cols)) {
    expected_cols <- c(
      "question_flag", "answer_flag", "interjection_flag", "speech_flag",
      "chamber_flag", "stage_direction_flag", "speaker_no", "name", "name.id",
      "electorate", "party", "role", "page.no", "content", "subdebateinfo",
      "debateinfo", "path", "Speaker", "Time"
    )
  }

  # Check file exists
  if (!file.exists(file_path)) {
    return(list(
      valid = FALSE,
      error = "File not found",
      file_path = file_path
    ))
  }

  tryCatch({
    # Read just the header
    sample_df <- readr::read_csv(file_path, n_max = 5, show_col_types = FALSE)

    actual_cols <- names(sample_df)
    missing_cols <- setdiff(expected_cols, actual_cols)
    extra_cols <- setdiff(actual_cols, expected_cols)

    # Basic data checks
    session_date <- parse_session_date(basename(file_path))
    row_count <- nrow(sample_df)

    # Check for critical issues
    critical_issues <- c()

    if (length(missing_cols) > 0) {
      critical_issues <- c(critical_issues, paste("Missing columns:", paste(missing_cols, collapse = ", ")))
    }

    if (is.na(session_date)) {
      critical_issues <- c(critical_issues, "Cannot parse session date from filename")
    }

    if (row_count == 0) {
      critical_issues <- c(critical_issues, "File appears to be empty")
    }

    return(list(
      valid = length(critical_issues) == 0,
      file_path = file_path,
      session_date = session_date,
      row_count = row_count,
      actual_columns = actual_cols,
      missing_columns = missing_cols,
      extra_columns = extra_cols,
      critical_issues = critical_issues,
      warnings = if (length(extra_cols) > 0) paste("Extra columns found:", paste(extra_cols, collapse = ", ")) else NULL
    ))

  }, error = function(e) {
    return(list(
      valid = FALSE,
      error = paste("Error reading file:", conditionMessage(e)),
      file_path = file_path
    ))
  })
}

#' Validate Multiple Files
#'
#' @param file_paths Vector of file paths
#' @return Data frame with validation results
#' @export
validate_csv_batch <- function(file_paths) {

  results <- purrr::map_dfr(file_paths, ~ {
    result <- validate_csv_structure(.x)

    tibble::tibble(
      file_path = result$file_path,
      filename = basename(result$file_path),
      valid = result$valid,
      session_date = result$session_date %||% NA,
      row_count = result$row_count %||% NA,
      n_missing_cols = length(result$missing_columns %||% character()),
      n_extra_cols = length(result$extra_columns %||% character()),
      issues = paste(c(result$critical_issues, result$warnings), collapse = "; "),
      error = result$error %||% NA_character_
    )
  })

  return(results)
}
