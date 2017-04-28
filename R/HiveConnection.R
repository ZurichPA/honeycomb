# Copyright 2015 Revolution Analytics
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


#' @import dplyr
#' @import methods
#' @import RJDBC
NULL

#' @export
db_list_tables.HiveConnection <- function(con){
  dbGetQuery(con, "show tables")$tab_name
}

#' @export
db_has_table.HiveConnection <- function(con, table){
  table %in% db_list_tables.HiveConnection(con)
}

#' @export
db_query_fields.HiveConnection <- function(con, sql){
  if (is.ident(sql)) {
    sql_query <- build_sql("SELECT * FROM ", sql, " LIMIT 0", con = con)
  } else {
    sql_query <- build_sql(sql, " LIMIT 0", con = con)
  }

  var_result <- dbSendQuery(con, sql_query)
  fields <- names(fetch(var_result, n = 10))
  as.character(fields)
}

#' @export
db_explain.HiveConnection = dplyr:::db_explain.DBIConnection

#' @export
db_data_type.HiveConnection = dplyr:::db_data_type.DBIConnection

#' @export
db_drop_table.HiveConnection =
  function (con, table, force = FALSE, ...) {
    sql =
      build_sql(
        "DROP TABLE ",
        if(force) sql("IF EXISTS "),
        ident(table),
        con = con)
    dbSendUpdate(con, sql)}

#' @export
db_begin.HiveConnection =
  function(con, ...) TRUE

#' @export
db_commit.HiveConnection =
  function(con, ...) TRUE

#' @export
db_rollback.HiveConnection =
  function(con, ...) TRUE

#' @export
db_create_table.HiveConnection =
  function(con, table, types, temporary = TRUE, url = NULL, using = NULL, ...) {
    external = !is.null(url)
    table = tolower(table)
    stopifnot(is.character(table) && length(table) == 1)
    stopifnot(is.character(types) || is.null(types))
    if(!is.null(types)) {
      field_names =
        escape(
          ident(names(types)),
          collapse = NULL,
          con = con)
      fields =
        dplyr:::sql_vector(
          paste0(field_names, " ", types),
          parens = TRUE,
          collapse = ", ",
          con = con)}
    else
      fields = NULL
    sql =
      build_sql(
        "CREATE ",
        if(external) sql("EXTERNAL "),
        if(temporary) sql("TEMPORARY "),
        "TABLE ", ident(table), " ",
        fields,
        if(external & !is.null(url)) build_sql(sql(" LOCATION "), encodeString(url)),
        if(!is.null(using))
          build_sql(
            sql(paste0("USING ", using$parser, " ")),
            "OPTIONS (",
            sql(
              paste0(
                names(using$options), " '",
                as.character(using$options), "'",
                collapse = ", ")), ")"),
        con = con)
    dbSendUpdate(con, sql)}

#' @export
db_insert_into.HiveConnection =
  function (con, table, values, ...) {
    cols = lapply(values, escape, collapse = NULL, parens = FALSE,
                  con = con)
    col_mat = matrix(unlist(cols, use.names = FALSE), nrow = nrow(values))
    rows = apply(col_mat, 1, paste0, collapse = ", ")
    values = paste0("(", rows, ")", collapse = "\n, ")
    sql = build_sql("INSERT INTO TABLE", ident(table), " VALUES ",
                    sql(values),con = con)
    dbSendUpdate(con, sql)}

#' @export
db_analyze.HiveConnection =
  function(con, table, ...) TRUE

#' @export
db_create_index.HiveConnection =
  function(con, table, columns, name = NULL, ...)
    TRUE

#' @export
sql_escape_string.HiveConnection =
  function(con, x)
    sql_quote(x, "'")

#' @export
sql_escape_ident.HiveConnection =
  function(con, x)
    sql_quote(x, "`")

#' @export
db_save_query.HiveConnection =
  function(con, sql, name, temporary = TRUE, ...){
    name = tolower(name)
    sql =
      build_sql(
        "CREATE ",
        if(temporary) sql("TEMPORARY "),
        "TABLE ",
        ident(name),
        " AS ", sql,
        con = con)
    dbSendUpdate(con, sql)}

#' @export
db_explain.HiveConnection =
  function(con, sql, ...)
    dbGetQuery(
      con,
      build_sql("EXPLAIN ", sql))




