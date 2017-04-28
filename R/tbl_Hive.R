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

#' Pull a Hive table to a local data frame
#' 
#' \code{collect} forces computation of a query and pulls the result 
#' down into a local data frame.
#' 
#' By default, collect will pull down the 100,000 rows of the result table.
#' If the result table pulls back exactly 100,000 rows, 
#' a warning message will be printed.
#' 
#' To pull down all of the rows, \code{n = Inf} can be specified. 
#' When using \code{collect}, make sure to keep in mind how large the data 
#' set you're pulling down is, in regard to both the number of rows as well 
#' as the number of columns.
#' 
#' \code{collect} works by calling \code{hive_query}, so if necessary, you can specify 
#' the batch argument like you would in hive_query to avoid out-of-memory
#' errors, and you can specify the \code{quiet} argument for whether to print 
#' update messages as the data is being pulled. In contrast to \code{hive_query}, 
#' for \code{collect} the \code{quiet} parameter is TRUE by default. 
#' You can set \code{quiet = FALSE} if you want messages to be printed.
#' 
#' To the extent that you can leave the data in Hive, it is best to do so. 
#' \code{collect} should only be called once you have a data set that has been 
#' filtered, aggregated, and narrowed down to the columns you need, 
#' such as a modeling data set.
#' 
#' @param x A \code{tbl_Hive} object
#' @param n The number of rows to pull down
#' @param batch Size of batches in which to fetch the data (passed to 
#' \code{\link[orpheus]{hive_query}})
#' @param quiet Whether to print progress updates as the data is being fetched
#' @param ... Additional arguments passed to \code{\link[orpheus]{hive_query}}
#' 
#' @return A tibble of the result
#' 
#' @name collect
#' @export
collect.tbl_Hive <- function(x, n, batch = 100000, quiet = TRUE, ...) {
  num_cols <- ncol(x)
  if (num_cols <= 1000) {
    max_n <- 100000
  } else if (num_cols <= 5000) {
    max_n <- 10000
  } else {
    max_n <- 1000
  }
  if (missing(n)) {
    n <- max_n
  }

  sql <- sql_render(x)
  df <- orpheus::hive_query(x$src$con, sql, batch = batch, max_rows = n, quiet = quiet, ...)
  df <- grouped_df(df, groups(x))

  if (nrow(df) == max_n) {
    warning(paste("Only first", formatC(max_n, digits = 0, format = "f", big.mark = ","), 
                  "results retrieved. Use n = Inf to retrieve all."),
            call. = FALSE)
  }
  
  df
}

#' Compute a Hive table
#' 
#' \code{compute} forces computation of a Hive table, saving the result in
#' a Hive database. 
#' 
#' By default, a temporary table in the Hive database will 
#' be created, unless you specify that it should be a permanent table.
#' 
#' As a general rule, it is good practice to call compute after every few steps.
#' Doing so helps make troubleshooting easier, as you don't need to wade 
#' through deeply nested statements to figure out what is going on, making 
#' it simpler to track down the point at something is going awry.
#' 
#' @param x A \code{tbl_Hive} object
#' @param name The name of the Hive table to create
#' @param temporary Whether to create a temporary or permanent table
#' @param overwrite Whether to overwrite an existing table with the
#' same name if one exists in the Hive database
#' 
#' @return A \code{tbl_Hive} object pointing to the resulting table
#' @name compute
#' @export
compute.tbl_Hive <- function(x, name = dplyr:::random_table_name(), 
                             temporary = TRUE, ..., overwrite = FALSE) {
  if (overwrite) {
    db_drop_table(x$src$con, name)
  }
  dplyr:::compute.tbl_sql(x, name = name, temporary = temporary, ...)
}

#' Export a Hive table to a txt file in HDFS
#' 
#' \code{hive_to_hdfs_txt} will export a Hive table to a delimited text file
#' in HDFS (tab-delimited by default)
#' 
#' @param hive_tbl A \code{tbl_Hive} object
#' @param hdfs_path HDFS path for the resulting file
#' @param delim Delimiter for the exported text file
#' 
#' @export
hive_to_hdfs_txt <- function(hive_tbl, hdfs_path, delim = "\t") {
  hive_tbl <- compute(hive_tbl)
  hdfs_tmp <- tempfile(tmpdir = paste0("/user/", Sys.info()[["user"]]))
  
  on.exit({
    if (system(paste("hadoop fs -test -e", hdfs_tmp)) == 0) {
      system(paste("hadoop fs -rm -f -r", hdfs_tmp), ignore.stdout = TRUE, ignore.stderr = TRUE)
    }
  })
  
  dbSendUpdate(
    hive_tbl$src$con,
    paste("INSERT OVERWRITE DIRECTORY", paste0("'", hdfs_tmp, "'"),
          "ROW FORMAT DELIMITED",
          "FIELDS TERMINATED BY", paste0("'", delim, "'"),
          "LINES TERMINATED BY '\n'", 
          "NULL DEFINED AS ''",
          "SELECT * FROM", 
          hive_tbl$ops$x$x
    )
  )
  
  colnames(hive_tbl) %>%
    paste(collapse = delim) %>%
    paste0("\n") %>%
    orpheus::write_hdfs(file.path(hdfs_tmp, "0.txt"), readr::write_file)
  
  system(paste("hadoop fs -cat", file.path(hdfs_tmp, "*"), "| hadoop fs -put -f -", hdfs_path))

  invisible(hdfs_path)
}

#' Pull rows from a Hive table into a local data.frame
#' 
#' \code{as.data.frame.tbl_Hive} collects rows from a 
#' Hive table and coerces it to a data.frame
#' 
#' The reason this function was created was to fix a problem
#' when printing Hive tables with a large number of columns
#' 
#' @param x A tbl_Hive object
#' @param n Number of rows to collect
#' @export
as.data.frame.tbl_Hive <- function(x, n = 100, ...) {
  as.data.frame(collect(x, n = n, ...))
}

