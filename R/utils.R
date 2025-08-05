# Utility operator for handling NULL values
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

# Suppress R CMD CHECK notes for variables that are column names in dplyr operations
utils::globalVariables(c(
  # Database column names
  "n", "session_date", "is_question", "is_answer", "is_speech", "is_interjection",
  "content_length", "member_id", "full_name", "party", "electorate", "name.id",
  "debateinfo", "name", "subdebateinfo", "question_flag", "answer_flag",
  "interjection_flag", "speech_flag", "stage_direction_flag", "Time", "page.no",
  "content", "path", "chamber_flag", "speaker_no",

  # Additional computed/transformed variables
  "total_speeches", "name_id", "debate_id", "speech_time", "subdebate_info",
  "xml_path", "is_stage_direction",
  "page_no"
))
