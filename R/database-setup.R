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
        status TEXT CHECK(status IN ('active', 'inactive', 'unknown')) DEFAULT 'unknown',
        first_seen_date DATE,
        last_seen_date DATE,
        first_parliament_date DATE,
        last_parliament_date DATE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
      CREATE INDEX IF NOT EXISTS idx_members_party ON members(party);
      CREATE INDEX IF NOT EXISTS idx_members_electorate ON members(electorate);
      CREATE INDEX IF NOT EXISTS idx_members_party_electorate ON members(party, electorate);
      CREATE INDEX IF NOT EXISTS idx_members_status ON members(status);
    ",

    debates = "
      CREATE TABLE IF NOT EXISTS debates (
        debate_id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id INTEGER REFERENCES sessions(session_id),
        parent_debate_id INTEGER REFERENCES debates(debate_id),
        debate_title TEXT NOT NULL,
        debate_order INTEGER,
        debate_level INTEGER DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
      CREATE INDEX IF NOT EXISTS idx_debates_session ON debates(session_id);
      CREATE INDEX IF NOT EXISTS idx_debates_hierarchy ON debates(parent_debate_id, debate_level);
      CREATE INDEX IF NOT EXISTS idx_debates_session_order ON debates(session_id, debate_order);
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
      CREATE INDEX IF NOT EXISTS idx_speeches_time_member ON speeches(speech_time, member_id);
      CREATE INDEX IF NOT EXISTS idx_speeches_debate_time ON speeches(debate_id, speech_time);
      CREATE INDEX IF NOT EXISTS idx_speeches_content_length ON speeches(content_length);
      CREATE INDEX IF NOT EXISTS idx_speeches_session_member_time ON speeches(session_id, member_id, speech_time);
      CREATE INDEX IF NOT EXISTS idx_speeches_xml_path ON speeches(xml_path);
    ",
    
    speeches_fts = "
      CREATE VIRTUAL TABLE IF NOT EXISTS speeches_fts USING fts5(
        content, 
        content=speeches, 
        content_rowid=speech_id
      );
    "
  )

  # Execute schema creation
  purrr::walk(schema_statements, ~ {
    statements <- stringr::str_split(.x, ";")[[1]] %>%
      stringr::str_trim() %>%
      purrr::keep(~ nchar(.x) > 0)

    purrr::walk(statements, ~ DBI::dbExecute(con, .x))
  })

  # Create triggers for data validation and maintenance
  create_database_triggers(con)
  
  message("Standard schema created with optimizations")
}

#' Create Database Triggers
#'
#' @param con Database connection
create_database_triggers <- function(con) {
  
  trigger_statements <- list(
    # Trigger to update member status based on activity
    update_member_status = "
      CREATE TRIGGER IF NOT EXISTS tr_update_member_status
      AFTER INSERT ON speeches
      BEGIN
        UPDATE members 
        SET status = 'active',
            last_seen_date = (
              SELECT session_date 
              FROM sessions 
              WHERE session_id = NEW.session_id
            )
        WHERE member_id = NEW.member_id;
      END;
    ",
    
    # Trigger to maintain speeches_fts table
    update_speeches_fts_insert = "
      CREATE TRIGGER IF NOT EXISTS tr_speeches_fts_insert
      AFTER INSERT ON speeches
      BEGIN
        INSERT INTO speeches_fts(rowid, content) VALUES (NEW.speech_id, NEW.content);
      END;
    ",
    
    update_speeches_fts_delete = "
      CREATE TRIGGER IF NOT EXISTS tr_speeches_fts_delete
      AFTER DELETE ON speeches
      BEGIN
        DELETE FROM speeches_fts WHERE rowid = OLD.speech_id;
      END;
    ",
    
    update_speeches_fts_update = "
      CREATE TRIGGER IF NOT EXISTS tr_speeches_fts_update
      AFTER UPDATE OF content ON speeches
      BEGIN
        UPDATE speeches_fts SET content = NEW.content WHERE rowid = NEW.speech_id;
      END;
    ",
    
    # Trigger to validate speech_time format
    validate_speech_time = "
      CREATE TRIGGER IF NOT EXISTS tr_validate_speech_time
      BEFORE INSERT ON speeches
      WHEN NEW.speech_time IS NOT NULL AND NEW.speech_time NOT GLOB '[0-9][0-9]:[0-9][0-9]*'
      BEGIN
        SELECT RAISE(ABORT, 'Invalid speech_time format. Use HH:MM or HH:MM:SS');
      END;
    "
  )
  
  purrr::walk(trigger_statements, ~ DBI::dbExecute(con, .x))
  message("Database triggers created")
}

#' Optimize Existing Database
#'
#' Apply optimizations to an existing database
#'
#' @param con Database connection
#' @export
optimize_hansard_database <- function(con) {
  
  # Add new columns if they don't exist
  add_columns_if_missing(con)
  
  # Create new indexes
  create_optimization_indexes(con)
  
  # Create FTS table if it doesn't exist
  create_fts_table_if_missing(con)
  
  # Create triggers
  create_database_triggers(con)
  
  # Analyze tables for better query planning
  DBI::dbExecute(con, "ANALYZE;")
  
  message("Database optimization complete")
}

#' Add Missing Columns
#'
#' @param con Database connection
add_columns_if_missing <- function(con) {
  
  # Check and add columns to members table
  members_columns <- DBI::dbListFields(con, "members")
  
  if (!"status" %in% members_columns) {
    DBI::dbExecute(con, "ALTER TABLE members ADD COLUMN status TEXT CHECK(status IN ('active', 'inactive', 'unknown')) DEFAULT 'unknown';")
  }
  
  if (!"first_parliament_date" %in% members_columns) {
    DBI::dbExecute(con, "ALTER TABLE members ADD COLUMN first_parliament_date DATE;")
  }
  
  if (!"last_parliament_date" %in% members_columns) {
    DBI::dbExecute(con, "ALTER TABLE members ADD COLUMN last_parliament_date DATE;")
  }
  
  # Check and add columns to debates table
  debates_columns <- DBI::dbListFields(con, "debates")
  
  if (!"parent_debate_id" %in% debates_columns) {
    DBI::dbExecute(con, "ALTER TABLE debates ADD COLUMN parent_debate_id INTEGER REFERENCES debates(debate_id);")
  }
  
  if (!"debate_level" %in% debates_columns) {
    DBI::dbExecute(con, "ALTER TABLE debates ADD COLUMN debate_level INTEGER DEFAULT 1;")
  }
}

#' Create Optimization Indexes
#'
#' @param con Database connection
create_optimization_indexes <- function(con) {
  
  index_statements <- c(
    "CREATE INDEX IF NOT EXISTS idx_members_party_electorate ON members(party, electorate);",
    "CREATE INDEX IF NOT EXISTS idx_members_status ON members(status);",
    "CREATE INDEX IF NOT EXISTS idx_debates_hierarchy ON debates(parent_debate_id, debate_level);",
    "CREATE INDEX IF NOT EXISTS idx_debates_session_order ON debates(session_id, debate_order);",
    "CREATE INDEX IF NOT EXISTS idx_speeches_time_member ON speeches(speech_time, member_id);",
    "CREATE INDEX IF NOT EXISTS idx_speeches_debate_time ON speeches(debate_id, speech_time);",
    "CREATE INDEX IF NOT EXISTS idx_speeches_content_length ON speeches(content_length);",
    "CREATE INDEX IF NOT EXISTS idx_speeches_session_member_time ON speeches(session_id, member_id, speech_time);",
    "CREATE INDEX IF NOT EXISTS idx_speeches_xml_path ON speeches(xml_path);"
  )
  
  purrr::walk(index_statements, ~ DBI::dbExecute(con, .x))
}

#' Create FTS Table If Missing
#'
#' @param con Database connection
create_fts_table_if_missing <- function(con) {
  
  # Check if FTS table exists
  fts_exists <- DBI::dbExistsTable(con, "speeches_fts")
  
  if (!fts_exists) {
    DBI::dbExecute(con, "
      CREATE VIRTUAL TABLE speeches_fts USING fts5(
        content, 
        content=speeches, 
        content_rowid=speech_id
      );
    ")
    
    # Populate FTS table with existing data
    DBI::dbExecute(con, "INSERT INTO speeches_fts(rowid, content) SELECT speech_id, content FROM speeches WHERE content IS NOT NULL;")
  }
}
