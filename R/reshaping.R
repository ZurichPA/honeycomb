# Copyright 2017 Zurich
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Gather columns into key-value pairs
#'
#' \code{gather_hive} operates on a Hive table, taking multiple columns and gathering them into key-value pairs
#' @param hive_tbl A tbl_Hive object
#' @param key Bare unquoted name of the key column to create
#' @param value Bare unquoted name of the value column to create
#' @param ... Specification of the columns to gather, using dplyr \code{\link[dplyr]{select}} syntax
#' @param temporary Whether the resulting table should be a temporary table or not
#' @param name Name of the resulting Hive table
#' @export
gather_hive <- function(hive_tbl, key, value, ..., temporary = TRUE, 
                        name = dplyr:::random_table_name()) {
  if (!inherits(hive_tbl, "tbl_Hive")) stop("hive_tbl must be a Hive table")
  hive_tbl <- compute(ungroup(hive_tbl))
  sel_vars <- unname(dplyr::select_vars(colnames(hive_tbl), ...))
  other_vars <- setdiff(colnames(hive_tbl), sel_vars)
  key <- lazyeval::expr_text(key)
  value <- lazyeval::expr_text(value)
  temp_name_1 <- dplyr:::random_table_name()
  temp_name_2 <- dplyr:::random_table_name()
  
  dbSendUpdate(
    hive_tbl$src$con,
    paste("CREATE",
          if_else(temporary, "TEMPORARY TABLE", "TABLE"),
          "IF NOT EXISTS",
          sql_escape_ident(hive_tbl$src$con, name),
          "AS SELECT",
          paste(paste0(sql_escape_ident(hive_tbl$src$con, temp_name_1), ".",
                       sql_escape_ident(hive_tbl$src$con, other_vars)),
                collapse = ", "),
          ",", paste(sql_escape_ident(hive_tbl$src$con, temp_name_2), "key",
                     sep = "."), "as",
          sql_escape_ident(hive_tbl$src$con, key),
          ",", paste(sql_escape_ident(hive_tbl$src$con, temp_name_2), "value",
                     sep = "."), "as",
          sql_escape_ident(hive_tbl$src$con, value),
          "FROM", sql_escape_ident(hive_tbl$src$con, hive_tbl$ops$x$x),
          sql_escape_ident(hive_tbl$src$con, temp_name_1),
          "LATERAL VIEW explode (map(",
          paste(paste(paste0("'", sel_vars, "'"), sel_vars, sep = ", "), collapse = ",\n"),
          "))", sql_escape_ident(hive_tbl$src$con, temp_name_2), "as key, value"
    )
  )
  
  tbl(hive_tbl$src, sql_escape_ident(hive_tbl$src$con, name))
}


#' Spread a key-value pair across multiple columns
#'
#' \code{spread_hive} operates on a Hive table, taking a key and value column and spreading them to multiple columns
#' @param hive_tbl A tbl_Hive object
#' @param key Bare unquoted name of the key column, whose values will be column names
#' @param value Bare unquoted name of the value column, whose values will populate the cells
#' @param temporary Whether the resulting table should be a temporary table or not
#' @param name Name of the resulting Hive table
#' @export
spread_hive <- function(hive_tbl, key, value, temporary = TRUE, 
                        name = dplyr:::random_table_name()) {
  if (!inherits(hive_tbl, "tbl_Hive")) stop("hive_tbl must be a Hive table")
  hive_tbl <- compute(ungroup(hive_tbl))
  key <- lazyeval::expr_text(key)
  value <- lazyeval::expr_text(value)
  temp_name <- dplyr:::random_table_name()
  other_vars <- setdiff(colnames(hive_tbl), c(key, value))
  unique_keys <- hive_tbl %>%
    distinct_(.dots = key) %>% 
    collect
  unique_keys <- sort(unique_keys[[key]])
  
  dbSendUpdate(
    hive_tbl$src$con,
    paste("CREATE",
          if_else(temporary, "TEMPORARY TABLE", "TABLE"),
          "IF NOT EXISTS",
          sql_escape_ident(hive_tbl$src$con, name),
          "AS SELECT", 
          paste(sql_escape_ident(hive_tbl$src$con, other_vars), collapse = ", "), ",",
          paste(paste0("collect_list(kv['", unique_keys, "'])[0] AS ", 
                       sql_escape_ident(hive_tbl$src$con, unique_keys)), collapse = ",\n"),
          "FROM (SELECT",
          paste(sql_escape_ident(hive_tbl$src$con, other_vars), collapse = ", "),
          ", map(", sql_escape_ident(hive_tbl$src$con, key), ",", 
          sql_escape_ident(hive_tbl$src$con, value), ")", "kv",
          "FROM", sql_escape_ident(hive_tbl$src$con, hive_tbl$ops$x$x),
          ")", sql_escape_ident(hive_tbl$src$con, name),
          "GROUP BY", 
          paste(sql_escape_ident(hive_tbl$src$con, other_vars), collapse = ", ")
    )
  )
  
  tbl(hive_tbl$src, sql_escape_ident(hive_tbl$src$con, name))
}

