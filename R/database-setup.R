#' Create Hansard Database
#'
#' Creates a new SQLite database with the standard Hansard schema
#'
#' @param db_path Path to the database file
#' @param overwrite Should existing database be overwritten?
#' @return DBI connection object
#' @export
create_hansard_database <- function(db_path, overwrite = FALSE) {

  if (file.exists(db_path) && !overwrite) {
    stop("Database already exists. Use overwrite = TRUE to replace it.")
  }

  if (overwrite && file.exists(db_path)) {
    file.remove(db_path)
  }

  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)

  # Create standard schema
  create_standard_schema(con)

  message("Database created successfully at: ", db_path)
  return(con)
}

#' Connect to Hansard Database
#'
#' @param db_path Path to the database file
#' @return DBI connection object
#' @export
connect_hansard_database <- function(db_path) {
  if (!file.exists(db_path)) {
    stop("Database not found at: ", db_path)
  }

  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  return(con)
}

#' Create Standard Schema
#'
#' @param con Database connection
create_standard_schema <- function(con) {

  schema_statements <- list(
    sessions = "
      CREATE TABLE IF NOT EXISTS sessions (
        session_id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_date DATE NOT NULL UNIQUE,
        year INTEGER GENERATED ALWAYS AS (CAST(strftime('%Y', session_date) AS INTEGER)) STORED,
        chamber_type INTEGER,
        source_file TEXT NOT NULL,
        file_hash TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
      CREATE INDEX IF NOT EXISTS idx_sessions_date ON sessions(session_date);
      CREATE INDEX IF NOT EXISTS idx_sessions_year ON sessions(year);
    ",

    members = "
      CREATE TABLE IF NOT EXISTS members (
        member_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name_id TEXT NOT NULL UNIQUE,
        full_name TEXT NOT NULL,
        electorate TEXT,
        party TEXT,
        role TEXT,
        first_seen_date DATE,
        last_seen_date DATE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
      CREATE INDEX IF NOT EXISTS idx_members_party ON members(party);
      CREATE INDEX IF NOT EXISTS idx_members_electorate ON members(electorate);
    ",

    debates = "
      CREATE TABLE IF NOT EXISTS debates (
        debate_id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER REFERENCES sessions(session_id),
        debate_title TEXT NOT NULL,
        debate_order INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
      CREATE INDEX IF NOT EXISTS idx_debates_session ON debates(session_id);
    ",

    speeches = "
      CREATE TABLE IF NOT EXISTS speeches (
        speech_id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER REFERENCES sessions(session_id),
        debate_id INTEGER REFERENCES debates(debate_id),
        member_id INTEGER REFERENCES members(member_id),
        speaker_no INTEGER,
        speech_time TIME,
        page_no REAL,
        content TEXT,
        subdebate_info TEXT,
        xml_path TEXT,
        is_question BOOLEAN DEFAULT FALSE,
        is_answer BOOLEAN DEFAULT FALSE,
        is_interjection BOOLEAN DEFAULT FALSE,
        is_speech BOOLEAN DEFAULT FALSE,
        is_stage_direction BOOLEAN DEFAULT FALSE,
        content_length INTEGER GENERATED ALWAYS AS (LENGTH(content)) STORED,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
      CREATE INDEX IF NOT EXISTS idx_speeches_session ON speeches(session_id);
      CREATE INDEX IF NOT EXISTS idx_speeches_member ON speeches(member_id);
      CREATE INDEX IF NOT EXISTS idx_speeches_time ON speeches(speech_time);
    "
  )

  # Execute schema creation
  purrr::walk(schema_statements, ~ {
    statements <- stringr::str_split(.x, ";")[[1]] %>%
      stringr::str_trim() %>%
      purrr::keep(~ nchar(.x) > 0)

    purrr::walk(statements, ~ DBI::dbExecute(con, .x))
  })

  message("Standard schema created")
}
