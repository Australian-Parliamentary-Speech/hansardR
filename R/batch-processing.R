#' Import Multiple Files
#'
#' @param file_paths Vector of file paths
#' @param con Database connection
#' @param chamber Chamber type: "house" or "senate"
#' @param validate Should files be validated?
#' @param force_reimport Should existing sessions be overwritten?
#' @param progress Show progress bar?
#' @return Summary of import results
#' @export
import_hansard_batch <- function(file_paths, con, chamber = c("house", "senate"), validate = TRUE, force_reimport = FALSE, progress = TRUE) {
  
  # Validate chamber parameter
  chamber <- match.arg(chamber)

  if (progress && requireNamespace("progress", quietly = TRUE)) {
    pb <- progress::progress_bar$new(
      format = "Processing [:bar] :percent (:current/:total) ETA: :eta",
      total = length(file_paths)
    )
  }

  results <- purrr::map_dfr(file_paths, ~ {
    if (progress && exists("pb")) pb$tick()

    success <- import_hansard_file(.x, con, chamber, validate, force_reimport)

    tibble::tibble(
      file_path = .x,
      filename = basename(.x),
      success = success,
      processed_at = Sys.time()
    )
  })

  # Summary
  n_success <- sum(results$success)
  n_total <- nrow(results)

  message("\n=== Import Summary ===")
  message("Successful: ", n_success, "/", n_total, " (", round(100 * n_success / n_total, 1), "%)")

  if (n_success < n_total) {
    failed_files <- results$filename[!results$success]
    message("Failed files: ", paste(failed_files, collapse = ", "))
  }

  return(results)
}

#' Import Year Directory
#'
#' @param year_dir Path to year directory
#' @param con Database connection
#' @param chamber Chamber type: "house" or "senate"
#' @param pattern File pattern to match
#' @param ... Additional arguments passed to import_hansard_batch
#' @return Import results
#' @export
import_hansard_year <- function(year_dir, con, chamber = c("house", "senate"), pattern = "*_edit_step7.csv", ...) {
  
  # Validate chamber parameter
  chamber <- match.arg(chamber)

  csv_files <- list.files(year_dir, pattern = utils::glob2rx(pattern), full.names = TRUE)

  if (length(csv_files) == 0) {
    message("No files found in ", year_dir)
    return(tibble::tibble())
  }

  message("Processing ", basename(year_dir), " (", length(csv_files), " files)")

  results <- import_hansard_batch(csv_files, con, chamber, ...)

  return(results)
}
