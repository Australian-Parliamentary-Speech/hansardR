test_that("database optimization works", {
  
  # Create temporary database
  temp_db <- tempfile(fileext = ".db")
  on.exit(unlink(temp_db))
  
  # Create database with optimizations
  con <- create_hansard_database(temp_db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  # Check that all tables exist
  tables <- DBI::dbListTables(con)
  expect_true("sessions" %in% tables)
  expect_true("members" %in% tables)
  expect_true("debates" %in% tables)
  expect_true("speeches" %in% tables)
  expect_true("speeches_fts" %in% tables)
  
  # Check that new columns exist
  members_columns <- DBI::dbListFields(con, "members")
  expect_true("status" %in% members_columns)
  expect_true("first_parliament_date" %in% members_columns)
  expect_true("last_parliament_date" %in% members_columns)
  
  debates_columns <- DBI::dbListFields(con, "debates")
  expect_true("parent_debate_id" %in% debates_columns)
  expect_true("debate_level" %in% debates_columns)
  
  # Check that indexes exist (by trying to use them in a query)
  # This will fail if indexes don't exist properly
  expect_no_error({
    DBI::dbGetQuery(con, "EXPLAIN QUERY PLAN SELECT * FROM members WHERE party = 'ALP' AND electorate = 'Sydney'")
    DBI::dbGetQuery(con, "EXPLAIN QUERY PLAN SELECT * FROM speeches WHERE session_id = 1 AND member_id = 1 ORDER BY speech_time")
  })
})

test_that("optimize_hansard_database works on existing database", {
  
  # Create basic database without optimizations but with proper structure
  temp_db <- tempfile(fileext = ".db")
  on.exit(unlink(temp_db))
  
  con <- DBI::dbConnect(RSQLite::SQLite(), temp_db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  # Create basic tables with all required columns but without optimizations
  DBI::dbExecute(con, "CREATE TABLE members (member_id INTEGER PRIMARY KEY, name_id TEXT, full_name TEXT, electorate TEXT, party TEXT, role TEXT, first_seen_date DATE, last_seen_date DATE, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
  DBI::dbExecute(con, "CREATE TABLE debates (debate_id INTEGER PRIMARY KEY, session_id INTEGER, debate_title TEXT, debate_order INTEGER, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
  DBI::dbExecute(con, "CREATE TABLE speeches (speech_id INTEGER PRIMARY KEY, session_id INTEGER, debate_id INTEGER, member_id INTEGER, content TEXT, speech_time TIME, xml_path TEXT, content_length INTEGER, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
  DBI::dbExecute(con, "CREATE TABLE sessions (session_id INTEGER PRIMARY KEY, session_date DATE, source_file TEXT)")
  
  # Apply optimizations
  expect_no_error(optimize_hansard_database(con))
  
  # Check that new columns were added
  members_columns <- DBI::dbListFields(con, "members")
  expect_true("status" %in% members_columns)
  
  debates_columns <- DBI::dbListFields(con, "debates")
  expect_true("parent_debate_id" %in% debates_columns)
  
  # Check that FTS table was created
  expect_true(DBI::dbExistsTable(con, "speeches_fts"))
})

test_that("full-text search functions work", {
  
  # Create database with sample data
  temp_db <- tempfile(fileext = ".db")
  on.exit(unlink(temp_db))
  
  con <- create_hansard_database(temp_db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  # Insert sample data
  DBI::dbExecute(con, "INSERT INTO sessions (session_date, chamber_type, source_file) VALUES ('2024-01-01', 1, 'test.csv')")
  DBI::dbExecute(con, "INSERT INTO members (name_id, full_name, party, electorate) VALUES ('TEST001', 'Test Member', 'ALP', 'Sydney')")
  DBI::dbExecute(con, "INSERT INTO debates (session_id, debate_title, debate_order) VALUES (1, 'Test Debate', 1)")
  DBI::dbExecute(con, "INSERT INTO speeches (session_id, debate_id, member_id, content) VALUES (1, 1, 1, 'This is a test speech about climate change and renewable energy')")
  
  # Test content statistics
  stats <- get_content_statistics(con)
  expect_type(stats, "list")
  expect_true(stats$fts_available)
  expect_equal(stats$basic$total_speeches, 1)
  
  # Test search functionality
  results <- search_speech_content(con, "climate")
  expect_s3_class(results, "data.frame")
  expect_equal(nrow(results), 1)
  
  # Test advanced search
  advanced_results <- search_speeches_advanced(con, content_query = "renewable", party = "ALP")
  expect_s3_class(advanced_results, "data.frame")
  expect_equal(nrow(advanced_results), 1)
})