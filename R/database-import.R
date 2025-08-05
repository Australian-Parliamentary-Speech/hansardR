#' Import Single CSV File to Database
#'
#' Complete pipeline: validate, load, process, and import a single CSV file
#'
#' @param file_path Path to CSV file
#' @param con Database connection
#' @param validate Should file be validated first?
#' @param force_reimport Should existing session be overwritten?
#' @return Boolean indicating success
#' @export
import_hansard_file <- function(file_path, con, validate = TRUE, force_reimport = FALSE) {

  message("Processing: ", basename(file_path))

  # Load and validate
  df <- load_hansard_csv(file_path, validate = validate)
  if (is.null(df)) {
    message("  x Failed to load file")
    return(FALSE)
  }

  session_date <- df$session_date[1]
  if (is.na(session_date)) {
    message("  x Cannot determine session date")
    return(FALSE)
  }

  # Check if session already exists
  existing_session <- DBI::dbGetQuery(con,
                                      "SELECT session_id FROM sessions WHERE session_date = ?",
                                      params = list(session_date)
  )

  if (nrow(existing_session) > 0 && !force_reimport) {
    message("  ! Session already exists (use force_reimport = TRUE to overwrite)")
    return(FALSE)
  }

  # Import the data
  success <- tryCatch({
    import_session_data(df, con, force_reimport)
    TRUE
  }, error = function(e) {
    message("  x Import failed: ", conditionMessage(e))
    FALSE
  })

  if (success) {
    message("  v Successfully imported ", nrow(df), " records")
  }

  return(success)
}

#' Import Session Data
#'
#' Internal function to handle the actual database import
#'
#' @param df Data frame with session data
#' @param con Database connection
#' @param force_reimport Should existing session be replaced?
import_session_data <- function(df, con, force_reimport = FALSE) {

  session_date <- df$session_date[1]
  filename <- df$source_file[1]
  # file_hash <- df$file_hash[1]  # Optional: remove if not using digest

  DBI::dbBegin(con)

  tryCatch({
    # Handle existing session
    if (force_reimport) {
      existing_session <- DBI::dbGetQuery(con,
                                          "SELECT session_id FROM sessions WHERE session_date = ?",
                                          params = list(session_date)
      )

      if (nrow(existing_session) > 0) {
        session_id <- existing_session$session_id[1]
        # Delete existing data
        DBI::dbExecute(con, "DELETE FROM speeches WHERE session_id = ?", params = list(session_id))
        DBI::dbExecute(con, "DELETE FROM debates WHERE session_id = ?", params = list(session_id))
        DBI::dbExecute(con, "UPDATE sessions SET source_file = ? WHERE session_id = ?",
                       params = list(filename, session_id))
      }
    } else {
      # Create new session
      DBI::dbExecute(con,
                     "INSERT INTO sessions (session_date, chamber_type, source_file) VALUES (?, ?, ?)",
                     params = list(session_date, df$chamber_flag[1], filename)
      )
      session_id <- DBI::dbGetQuery(con, "SELECT last_insert_rowid() as id")$id
    }

    # Process members
    member_map <- process_members(df, con, session_date)

    # Process debates
    debate_map <- process_debates(df, con, session_id)

    # Process speeches
    process_speeches(df, con, session_id, member_map, debate_map)

    DBI::dbCommit(con)

  }, error = function(e) {
    DBI::dbRollback(con)
    stop(e)
  })
}

#' Process Members Data
#'
#' @param df Session data frame
#' @param con Database connection
#' @param session_date Date of session
#' @return Tibble mapping name_id to member_id
process_members <- function(df, con, session_date) {

  member_rows <- df |>
    dplyr::filter(!is.na(name), name != "N/A", !is.na(name.id), name.id != "N/A") |>
    dplyr::distinct(name.id, .keep_all = TRUE)

  member_map <- tibble::tibble()

  for (i in seq_len(nrow(member_rows))) {
    row <- member_rows[i, ]
    name_id <- as.character(row$name.id)

    tryCatch({
      existing_member <- DBI::dbGetQuery(con,
                                         "SELECT member_id FROM members WHERE name_id = ?",
                                         params = list(name_id)
      )

      if (nrow(existing_member) > 0) {
        member_id <- existing_member$member_id[1]
        DBI::dbExecute(con,
                       "UPDATE members SET last_seen_date = ? WHERE name_id = ?",
                       params = list(as.character(session_date), name_id)
        )
      } else {
        DBI::dbExecute(con,
                       "INSERT INTO members (name_id, full_name, electorate, party, role, first_seen_date, last_seen_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
                       params = list(
                         name_id,
                         clean_value(row$name),
                         clean_value(row$electorate),
                         clean_value(row$party),
                         clean_value(row$role),
                         as.character(session_date),
                         as.character(session_date)
                       )
        )
        member_id <- DBI::dbGetQuery(con, "SELECT last_insert_rowid() as id")$id
      }

      member_map <- dplyr::bind_rows(member_map, tibble::tibble(name_id = name_id, member_id = member_id))

    }, error = function(e) {
      message("Error processing member ", name_id, ": ", conditionMessage(e))
      stop(e)
    })
  }

  return(member_map)
}

#' Process Debates Data
#'
#' @param df Session data frame
#' @param con Database connection
#' @param session_id Session ID
#' @return Tibble mapping debateinfo to debate_id
process_debates <- function(df, con, session_id) {

  debates <- df |>
    dplyr::filter(!is.na(debateinfo), debateinfo != "N/A") |>
    dplyr::pull(debateinfo) |>
    unique()

  debate_map <- tibble::tibble()

  for (i in seq_along(debates)) {
    debate_title <- debates[i]

    DBI::dbExecute(con,
                   "INSERT INTO debates (session_id, debate_title, debate_order) VALUES (?, ?, ?)",
                   params = list(session_id, debate_title, i)
    )

    debate_id <- DBI::dbGetQuery(con, "SELECT last_insert_rowid() as id")$id
    debate_map <- dplyr::bind_rows(debate_map, tibble::tibble(debateinfo = debate_title, debate_id = debate_id))
  }

  return(debate_map)
}

#' Process Speeches Data
#'
#' @param df Session data frame
#' @param con Database connection
#' @param session_id Session ID
#' @param member_map Member mapping
#' @param debate_map Debate mapping
process_speeches <- function(df, con, session_id, member_map, debate_map) {

  speeches_data <- df |>
    dplyr::mutate(
      name_id_clean = dplyr::if_else(is.na(name.id) | name.id == "N/A", NA_character_, as.character(name.id)),
      debateinfo_clean = dplyr::if_else(is.na(debateinfo) | debateinfo == "N/A", NA_character_, as.character(debateinfo))
    ) |>
    dplyr::left_join(member_map |> dplyr::mutate(name.id = as.character(name_id)),
                     by = c("name_id_clean" = "name.id"),
                     relationship = "many-to-one") |>
    dplyr::left_join(debate_map |> dplyr::mutate(debateinfo = as.character(debateinfo)),
                     by = c("debateinfo_clean" = "debateinfo"),
                     relationship = "many-to-one") |>
    dplyr::mutate(
      session_id = session_id,
      speech_time = purrr::map_chr(Time, parse_time),
      page_no = as.numeric(page.no),
      content = clean_value(content),
      subdebate_info = clean_value(subdebateinfo),
      xml_path = clean_value(path),
      is_question = as.logical(question_flag),
      is_answer = as.logical(answer_flag),
      is_interjection = as.logical(interjection_flag),
      is_speech = as.logical(speech_flag),
      is_stage_direction = as.logical(stage_direction_flag)
    ) |>
    dplyr::select(session_id, debate_id, member_id, speaker_no, speech_time, page_no,
                  content, subdebate_info, xml_path, is_question, is_answer,
                  is_interjection, is_speech, is_stage_direction)

  DBI::dbWriteTable(con, "speeches", speeches_data, append = TRUE, row.names = FALSE)
}
