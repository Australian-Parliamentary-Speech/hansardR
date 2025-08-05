#' Search Speech Content
#'
#' Full-text search of speech content using SQLite FTS5
#'
#' @param con Database connection
#' @param query Search query string
#' @param limit Maximum number of results to return
#' @param highlight Whether to include highlighted snippets
#' @return Data frame with search results
#' @export
search_speech_content <- function(con, query, limit = 100, highlight = TRUE) {
  
  if (!DBI::dbExistsTable(con, "speeches_fts")) {
    stop("Full-text search table not available. Run optimize_hansard_database() first.")
  }
  
  base_query <- "
    SELECT 
      s.speech_id,
      s.session_id,
      s.debate_id,
      s.member_id,
      m.full_name,
      m.party,
      m.electorate,
      sess.session_date,
      d.debate_title,
      s.speech_time,
      s.content_length"
  
  if (highlight) {
    base_query <- paste0(base_query, ",
      highlight(speeches_fts, 0, '<mark>', '</mark>') as highlighted_content")
  } else {
    base_query <- paste0(base_query, ",
      s.content")
  }
  
  full_query <- paste0(base_query, "
    FROM speeches_fts fts
    JOIN speeches s ON s.speech_id = fts.rowid
    LEFT JOIN members m ON s.member_id = m.member_id
    LEFT JOIN sessions sess ON s.session_id = sess.session_id
    LEFT JOIN debates d ON s.debate_id = d.debate_id
    WHERE speeches_fts MATCH ?
    ORDER BY bm25(speeches_fts)
    LIMIT ?")
  
  result <- DBI::dbGetQuery(con, full_query, params = list(query, limit))
  
  if (nrow(result) == 0) {
    message("No results found for query: ", query)
  } else {
    message("Found ", nrow(result), " results for query: ", query)
  }
  
  return(result)
}

#' Search Speeches by Member and Content
#'
#' Combined search by member characteristics and content
#'
#' @param con Database connection
#' @param content_query Full-text search query (optional)
#' @param member_name Member name to filter by (optional)
#' @param party Party to filter by (optional)
#' @param electorate Electorate to filter by (optional)
#' @param date_from Start date for filtering (optional)
#' @param date_to End date for filtering (optional)
#' @param limit Maximum number of results
#' @return Data frame with search results
#' @export
search_speeches_advanced <- function(con, 
                                   content_query = NULL,
                                   member_name = NULL,
                                   party = NULL,
                                   electorate = NULL,
                                   date_from = NULL,
                                   date_to = NULL,
                                   limit = 100) {
  
  # Build WHERE conditions
  where_conditions <- c()
  params <- list()
  param_count <- 0
  
  # Content search condition
  if (!is.null(content_query)) {
    if (!DBI::dbExistsTable(con, "speeches_fts")) {
      stop("Full-text search table not available. Run optimize_hansard_database() first.")
    }
    where_conditions <- c(where_conditions, "speeches_fts MATCH ?")
    param_count <- param_count + 1
    params[[param_count]] <- content_query
  }
  
  # Member name condition
  if (!is.null(member_name)) {
    where_conditions <- c(where_conditions, "m.full_name LIKE ?")
    param_count <- param_count + 1
    params[[param_count]] <- paste0("%", member_name, "%")
  }
  
  # Party condition
  if (!is.null(party)) {
    where_conditions <- c(where_conditions, "m.party = ?")
    param_count <- param_count + 1
    params[[param_count]] <- party
  }
  
  # Electorate condition
  if (!is.null(electorate)) {
    where_conditions <- c(where_conditions, "m.electorate = ?")
    param_count <- param_count + 1
    params[[param_count]] <- electorate
  }
  
  # Date conditions
  if (!is.null(date_from)) {
    where_conditions <- c(where_conditions, "sess.session_date >= ?")
    param_count <- param_count + 1
    params[[param_count]] <- date_from
  }
  
  if (!is.null(date_to)) {
    where_conditions <- c(where_conditions, "sess.session_date <= ?")
    param_count <- param_count + 1
    params[[param_count]] <- date_to
  }
  
  # Build query
  base_query <- "
    SELECT 
      s.speech_id,
      s.session_id,
      s.debate_id,
      s.member_id,
      m.full_name,
      m.party,
      m.electorate,
      sess.session_date,
      d.debate_title,
      s.speech_time,
      s.content_length,
      s.content
    FROM speeches s"
  
  # Add FTS join if needed
  if (!is.null(content_query)) {
    base_query <- paste0(base_query, "
    JOIN speeches_fts ON s.speech_id = speeches_fts.rowid")
  }
  
  base_query <- paste0(base_query, "
    LEFT JOIN members m ON s.member_id = m.member_id
    LEFT JOIN sessions sess ON s.session_id = sess.session_id
    LEFT JOIN debates d ON s.debate_id = d.debate_id")
  
  # Add WHERE clause if conditions exist
  if (length(where_conditions) > 0) {
    base_query <- paste0(base_query, "
    WHERE ", paste(where_conditions, collapse = " AND "))
  }
  
  # Add ordering and limit
  if (!is.null(content_query)) {
    base_query <- paste0(base_query, "
    ORDER BY bm25(speeches_fts)")
  } else {
    base_query <- paste0(base_query, "
    ORDER BY sess.session_date DESC, s.speech_time")
  }
  
  base_query <- paste0(base_query, "
    LIMIT ", limit)
  
  # Execute query
  if (length(params) > 0) {
    result <- DBI::dbGetQuery(con, base_query, params = params)
  } else {
    result <- DBI::dbGetQuery(con, base_query)
  }
  
  message("Found ", nrow(result), " results")
  return(result)
}

#' Get Content Statistics
#'
#' Get statistics about speech content and search capabilities
#'
#' @param con Database connection
#' @return List with content statistics
#' @export
get_content_statistics <- function(con) {
  
  stats <- list()
  
  # Basic content stats
  basic_stats <- DBI::dbGetQuery(con, "
    SELECT 
      COUNT(*) as total_speeches,
      COUNT(CASE WHEN content IS NOT NULL AND LENGTH(TRIM(content)) > 0 THEN 1 END) as speeches_with_content,
      AVG(content_length) as avg_content_length,
      MAX(content_length) as max_content_length,
      MIN(content_length) as min_content_length
    FROM speeches
  ")
  
  stats$basic <- basic_stats
  
  # FTS availability
  stats$fts_available <- DBI::dbExistsTable(con, "speeches_fts")
  
  if (stats$fts_available) {
    fts_stats <- DBI::dbGetQuery(con, "SELECT COUNT(*) as indexed_speeches FROM speeches_fts")
    stats$fts_indexed <- fts_stats$indexed_speeches
  }
  
  # Content by type
  content_by_type <- DBI::dbGetQuery(con, "
    SELECT 
      CASE 
        WHEN is_question = 1 THEN 'Question'
        WHEN is_answer = 1 THEN 'Answer'
        WHEN is_interjection = 1 THEN 'Interjection'
        WHEN is_speech = 1 THEN 'Speech'
        WHEN is_stage_direction = 1 THEN 'Stage Direction'
        ELSE 'Other'
      END as speech_type,
      COUNT(*) as count,
      AVG(content_length) as avg_length
    FROM speeches
    WHERE content IS NOT NULL
    GROUP BY speech_type
    ORDER BY count DESC
  ")
  
  stats$by_type <- content_by_type
  
  return(stats)
}