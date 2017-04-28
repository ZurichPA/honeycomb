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

#' Create Hive connection
#'
#' \code{src_Hive} creates a connection to Hive
#'
#' This is the connection that is used by dplyr to use Hive
#' as a backend.
#' 
#' @param db Name of the Hive database
#' @param ... Additional arguments passed to \code{\link[orpheus]{hive_connect}}
#' @export
src_Hive <- function(db, ...) {
  con <- orpheus::hive_connect(db, ...)
  con <- new("HiveConnection", con)
  info <- paste("Hive at:", db)
  src_sql(
    "Hive",
    con, 
    info = info,
    db = db
  )
}

.hive_type <- function(x) {
  case_when(inherits(x, "numeric") ~ "double",
            inherits(x, "integer") ~ "int",
            inherits(x, "character") ~ "string",
            inherits(x, "logical") ~ "boolean",
            inherits(x, "Date") ~ "date",
            inherits(x, "POSIXct") ~ "timestamp",
            TRUE ~ NA_character_)
}


#' Copy a local data frame to Hive
#' 
#' \code{copy_to} copies a local data frame to 
#' a table in the active Hive database.
#' 
#' By default, a temporary table in the Hive database will be created, 
#' unless you specify that it should be a permanent table.
#' 
#' @param src A \code{src_Hive} connection object
#' @param df The local data frame to copy
#' @param name What to name the table in Hive
#' @param temporary Whether to create a temporary or permanent table
#' @param overwrite Whether to overwrite an existing table with the
#' same name if one exists in the Hive database
#' 
#' @return A \code{tbl_Hive} object reference to the Hive table created
#' 
#' @name copy_to
#' @export
copy_to.src_Hive <- function(src, df, name = lazyeval::expr_text(df), temporary = TRUE, 
                             indexes = NULL, overwrite = FALSE) {
  if (overwrite) {
    db_drop_table(src$con, name)
  } else if (db_has_table(src$con, name)) {
    stop("Table already exists - to overwrite, use the argument overwrite = TRUE")
  }
  
  var_types <- purrr::map_chr(df, .hive_type)

  if (any(is.na(var_types))) {
    invalid_vars <- purrr::keep(var_types, is.na) %>%
      names %>%
      paste(collapse = ", ")
    stop(paste("Variables with unsupported types:", invalid_vars))
  }

  if (any(var_types == "timestamp")) {
    df <- purrr::dmap_if(df, ~ inherits(.x, "POSIXct"), strftime)
  }

  var_string <- paste(sql_escape_ident(src$con, names(var_types)), var_types) %>%
    paste(collapse = ", ")

  hdfs_tmp <- tempfile(tmpdir = paste0("/user/", Sys.info()[["user"]]))
  orpheus::write_hdfs(df, hdfs_tmp, col_names = FALSE, na = "", delim = "\001")

  dbSendUpdate(
    src$con, paste("CREATE",
                   if_else(temporary, "TEMPORARY TABLE", "TABLE"),
                   "IF NOT EXISTS",
                   name,
                   "(",
                   var_string,
                   ")",
                   "ROW FORMAT DELIMITED",
                   "FIELDS TERMINATED BY '\001'",
                   "LINES TERMINATED BY '\n'",
                   "STORED AS TEXTFILE",
                   "TBLPROPERTIES('serialization.null.format' = '')")
  )

  dbSendUpdate(
    src$con, paste0("LOAD DATA INPATH '",
                    hdfs_tmp,
                    "' OVERWRITE INTO TABLE ",
                    name)
  )

  if (system(paste("hadoop fs -test -e", hdfs_tmp)) == 0) {
    system(paste("hadoop fs -rm", hdfs_tmp))
  }

  tbl(src, name)
}


#' Establish a connection to a Hive table
#' 
#' \code{tbl} is a method for the generic \code{\link[dplyr]{tbl}}
#' 
#' To create a connection to a Hive table in a database other than the one 
#' connected to in \code{src_Hive}, simply add the database name when 
#' specifying the table name.
#' 
#' In addition to specifying table names in a \code{tbl} statement, you can use 
#' arbitrary SQL itself to refer to a table. In place of the table name 
#' in the \code{tbl} statement, simply wrap the desired SQL in 
#' a \code{sql} call.
#' 
#' @param src A src_Hive connection object
#' @param from Information to create the table connection (either a string of
#' the table name or a query wrapped in \code{sql})
#' @name tbl
#' @export
tbl.src_Hive <- function(src, from, ...) {
  dplyr::tbl_sql("Hive", src = src, from = from, ...)
}

#' @export
src_desc.src_Hive <- function(x) {
  x$info
}


