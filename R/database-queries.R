#' Get Database Table References
#'
#' Returns dplyr table references for easy querying
#'
#' @param con Database connection
#' @return List of table references
#' @export
get_hansard_tables <- function(con) {
  list(
    sessions = dplyr::tbl(con, "sessions"),
    members = dplyr::tbl(con, "members"),
    debates = dplyr::tbl(con, "debates"),
    speeches = dplyr::tbl(con, "speeches")
  )
}

#' Get Database Statistics
#'
#' @param con Database connection
#' @return List with database statistics
#' @export
get_database_stats <- function(con) {

  tbls <- get_hansard_tables(con)

  stats <- list(
    sessions = tbls$sessions |> dplyr::count() |> dplyr::collect() |> dplyr::pull(n),
    members = tbls$members |> dplyr::count() |> dplyr::collect() |> dplyr::pull(n),
    debates = tbls$debates |> dplyr::count() |> dplyr::collect() |> dplyr::pull(n),
    speeches = tbls$speeches |> dplyr::count() |> dplyr::collect() |> dplyr::pull(n)
  )

  # Date range
  date_range <- tbls$sessions |>
    dplyr::summarise(
      min_date = min(session_date, na.rm = TRUE),
      max_date = max(session_date, na.rm = TRUE)
    ) |>
    dplyr::collect()

  stats$date_range <- c(date_range$min_date, date_range$max_date)

  # Speech types
  speech_types <- tbls$speeches |>
    dplyr::summarise(
      questions = sum(is_question, na.rm = TRUE),
      answers = sum(is_answer, na.rm = TRUE),
      speeches = sum(is_speech, na.rm = TRUE),
      interjections = sum(is_interjection, na.rm = TRUE),
      avg_length = mean(content_length, na.rm = TRUE)
    ) |>
    dplyr::collect()

  stats <- c(stats, as.list(speech_types))

  return(stats)
}

#' Get Top Speakers
#'
#' @param con Database connection
#' @param limit Number of speakers to return
#' @return Data frame with speaker statistics
#' @importFrom utils head
#' @export
get_top_speakers <- function(con, limit = 10) {

  tbls <- get_hansard_tables(con)

  result <- tbls$speeches |>
    dplyr::left_join(tbls$members, by = "member_id") |>
    dplyr::group_by(full_name, party, electorate) |>
    dplyr::summarise(
      total_speeches = dplyr::n(),
      total_words = sum(content_length, na.rm = TRUE),
      avg_speech_length = mean(content_length, na.rm = TRUE),
      questions_asked = sum(is_question, na.rm = TRUE),
      answers_given = sum(is_answer, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::arrange(dplyr::desc(total_speeches)) |>
    dplyr::collect()

  # Apply limit after collecting data
  return(head(result, limit))
}
