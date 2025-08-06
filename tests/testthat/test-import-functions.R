test_that("import_hansard_file() works with chamber parameter and date extraction", {
  library(hansardR)
  
  extdata_path <- system.file("extdata", "houseCSV", "2024", "2024-02-08_edit_step7.csv", package = "hansardR")
  skip_if_not(file.exists(extdata_path), "Test CSV file not available")
  
  temp_db <- tempfile(fileext = ".db")
  on.exit(unlink(temp_db), add = TRUE)
  
  con <- create_hansard_database(temp_db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  # Test import with chamber = "house"
  result <- import_hansard_file(extdata_path, con, chamber = "house", validate = TRUE)
  
  expect_true(result, "File import should succeed")
  
  # Check that session was created with correct chamber and date
  sessions <- DBI::dbGetQuery(con, "SELECT session_date, chamber, source_file FROM sessions")
  
  expect_equal(nrow(sessions), 1)
  expect_equal(sessions$chamber, "house")
  expect_equal(as.Date(sessions$session_date, origin = "1970-01-01"), as.Date("2024-02-08"))
  expect_equal(basename(sessions$source_file), "2024-02-08_edit_step7.csv")
  
  # Check that data was imported
  stats <- get_database_stats(con)
  expect_gt(stats$sessions, 0, "Should have at least one session")
  expect_gt(stats$speeches, 0, "Should have imported speeches")
  expect_gt(stats$members, 0, "Should have imported members")
  
  # Test that duplicate import is prevented
  result2 <- import_hansard_file(extdata_path, con, chamber = "house", validate = TRUE, force_reimport = FALSE)
  expect_false(result2, "Duplicate import should be prevented")
  
  # Verify still only one session
  sessions_after <- DBI::dbGetQuery(con, "SELECT COUNT(*) as count FROM sessions")
  expect_equal(sessions_after$count, 1)
})

test_that("import_hansard_file() handles different chambers correctly", {
  library(hansardR)
  
  extdata_path <- system.file("extdata", "houseCSV", "2024", "2024-02-08_edit_step7.csv", package = "hansardR")
  skip_if_not(file.exists(extdata_path), "Test CSV file not available")
  
  temp_db <- tempfile(fileext = ".db")
  on.exit(unlink(temp_db), add = TRUE)
  
  con <- create_hansard_database(temp_db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  # Import same date for different chambers
  result1 <- import_hansard_file(extdata_path, con, chamber = "house", validate = TRUE)
  result2 <- import_hansard_file(extdata_path, con, chamber = "senate", validate = TRUE)
  
  expect_true(result1, "House import should succeed")
  expect_true(result2, "Senate import should succeed (different chamber)")
  
  # Check that we have two sessions with same date but different chambers
  sessions <- DBI::dbGetQuery(con, "SELECT session_date, chamber FROM sessions ORDER BY chamber")
  
  expect_equal(nrow(sessions), 2)
  expect_equal(as.Date(sessions$session_date, origin = "1970-01-01"), rep(as.Date("2024-02-08"), 2))
  expect_equal(sessions$chamber, c("house", "senate"))
})

test_that("import_hansard_file() validates chamber parameter", {
  library(hansardR)
  
  extdata_path <- system.file("extdata", "houseCSV", "2024", "2024-02-08_edit_step7.csv", package = "hansardR")
  skip_if_not(file.exists(extdata_path), "Test CSV file not available")
  
  temp_db <- tempfile(fileext = ".db")
  on.exit(unlink(temp_db), add = TRUE)
  
  con <- create_hansard_database(temp_db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  # Test invalid chamber value
  expect_error(
    import_hansard_file(extdata_path, con, chamber = "invalid"),
    "should be one of"
  )
})

test_that("import_hansard_file() extracts date correctly from different filename formats", {
  library(hansardR)
  
  # Test with actual file first
  extdata_path <- system.file("extdata", "houseCSV", "2025", "2025-02-06_edit_step7.csv", package = "hansardR")
  skip_if_not(file.exists(extdata_path), "Test CSV file not available")
  
  temp_db <- tempfile(fileext = ".db")
  on.exit(unlink(temp_db), add = TRUE)
  
  con <- create_hansard_database(temp_db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  # Import file from 2025
  result <- import_hansard_file(extdata_path, con, chamber = "house", validate = TRUE)
  expect_true(result, "2025 file import should succeed")
  
  # Check date extraction
  sessions <- DBI::dbGetQuery(con, "SELECT session_date FROM sessions")
  expect_equal(as.Date(sessions$session_date, origin = "1970-01-01"), as.Date("2025-02-06"))
})

test_that("import_hansard_file() handles force_reimport correctly", {
  library(hansardR)
  
  extdata_path <- system.file("extdata", "houseCSV", "2024", "2024-02-08_edit_step7.csv", package = "hansardR")
  skip_if_not(file.exists(extdata_path), "Test CSV file not available")
  
  temp_db <- tempfile(fileext = ".db")
  on.exit(unlink(temp_db), add = TRUE)
  
  con <- create_hansard_database(temp_db)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  # Import file first time
  result1 <- import_hansard_file(extdata_path, con, chamber = "house", validate = TRUE)
  expect_true(result1, "Initial import should succeed")
  
  # Get initial speech count
  initial_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as count FROM speeches")$count
  
  # Try reimport without force - should fail
  result2 <- import_hansard_file(extdata_path, con, chamber = "house", validate = TRUE, force_reimport = FALSE)
  expect_false(result2, "Reimport without force should fail")
  
  # Try with force_reimport = TRUE - should succeed
  result3 <- import_hansard_file(extdata_path, con, chamber = "house", validate = TRUE, force_reimport = TRUE)
  expect_true(result3, "Reimport with force should succeed")
  
  # Should still have only one session
  session_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as count FROM sessions")$count
  expect_equal(session_count, 1)
  
  # Speech count should be same (data replaced, not duplicated)
  final_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as count FROM speeches")$count
  expect_equal(final_count, initial_count)
})