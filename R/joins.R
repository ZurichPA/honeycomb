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

#' @export
sql_join.HiveConnection <- function(con, x, y, type = "inner", by = NULL,
                                    x_names, y_names, x_tbl_nm, y_tbl_nm, ...) {
  y_names_retain <- setdiff(y_names, by$y)
  
  if (length(y_names_retain) > 0) {
    select_y <- paste0(sql_escape_ident(con, y_tbl_nm), ".", 
                       sql_escape_ident(con, y_names_retain))
  } else {
    select_y <- character(0)
  }
  
  select_cols <- paste(c(paste0(sql_escape_ident(con, x_tbl_nm), ".", sql_escape_ident(con, x_names)),
                         select_y),
                       collapse = ", ") %>%
    sql_vector
  
  join <- switch(type, left = sql("LEFT"), inner = sql("INNER"),
                 right = sql("RIGHT"), full = sql("FULL"), stop("Unknown join type:",
                                                                type, call. = FALSE))
  
  on <- sql_vector(paste0(sql_escape_ident(con, x_tbl_nm), ".", sql_escape_ident(con, by$x),
                          " = ", sql_escape_ident(con, y_tbl_nm), ".", 
                          sql_escape_ident(con, by$y)), collapse = " AND ",
                   parens = TRUE)
  cond <- build_sql("ON ", on, con = con)
  
  build_sql("SELECT ", select_cols, " FROM ", x, "\n\n", join, " JOIN\n\n",
            y, "\n\n", cond, con = con)
}

.change_join_class <- function(x) {
  if (is.null(x)) return(NULL)
  if (is.list(x)) {
    orig_class <- class(x)
    x <- purrr::map(x, .change_join_class)
    class(x) <- orig_class
  }
  if (inherits(x, "op_join")) {
    class(x) <- c("op_join_hive", class(x))
  }
  x
}

#' @export
sql_render.tbl_Hive <- function(query, con = NULL, ...) {
  ops <- query$ops
  
  orig_class <- class(ops)
  ops <- purrr::map(ops, .change_join_class)
  class(ops) <- orig_class
  
  if (inherits(ops, "op_join")) {
    class(ops) <- c("op_join_hive", class(ops))
  }
  
  sql_render(sql_build(ops, query$src$con, ...), con = query$src$con, ...)
}

#' @export
sql_build.op_join_hive <- function(op, con, ...) {
  class(op) <- class(op)[-1]
  hive_join <- sql_build(op, con)
  class(hive_join) <- c("hive_join_query", class(hive_join))
  hive_join
}

#' @export
sql_render.hive_join_query <- function(query, con = NULL, ..., root = FALSE) {
  x_names <- colnames(query$x)
  y_names <- colnames(query$y)
  class(query) <- class(query)[-1]
  x_tbl_nm <- dplyr:::random_table_name()
  y_tbl_nm <- dplyr:::random_table_name()
  from_x <- sql_subquery(con, sql_render(query$x, con, ...,
                                         root = root), name = x_tbl_nm)
  from_y <- sql_subquery(con, sql_render(query$y, con, ...,
                                         root = root), name = y_tbl_nm)
  sql_join(con, from_x, from_y, type = query$type, by = query$by,
           x_names = x_names, y_names = y_names,
           x_tbl_nm = x_tbl_nm, y_tbl_nm = y_tbl_nm)
}



prepare_join <- function(x, y, by) {
  by_vars <- common_by(by, x, y)
  ykey_clash <- setdiff(intersect(colnames(x), by_vars$y), by_vars$x)
  if (length(ykey_clash) > 0) {
    old_ykey <- by_vars$y
    by_vars$y <- paste0(old_ykey, "_ykeyzzz")
    y <- y %>%
      rename_(.dots = setNames(old_ykey, by_vars$y))
  }
  list(y_to_join = y, by_vars = by_vars)
}

after_join <- function(joined_tbl, by_vars) {
  extraneous_cols <- setdiff(intersect(colnames(joined_tbl), by_vars$y), by_vars$x)
  if (length(extraneous_cols) > 0) {
    joined_tbl <- joined_tbl %>%
      select_(.dots = setdiff(colnames(.), extraneous_cols))
  }
  joined_tbl
}

#' Join two Hive tables
#' 
#' join functions in honeycomb are just like those in dplyr as documented 
#' in \code{\link[dplyr]{join}}. The only difference is the default \code{suffix} 
#' values, which use underscores rather than dots for compatibility with Hive.
#' 
#' @name inner_join
#' @export
inner_join.tbl_Hive <- function(x, y, by = NULL, copy = FALSE, suffix = c("_x", "_y"), ...) {
  join_info <- prepare_join(x, y, by)
  joined_tbl <- dplyr:::inner_join.tbl_lazy(x, join_info$y_to_join, by = join_info$by_vars, 
                                            copy = copy, suffix = suffix, ...)
  after_join(joined_tbl, join_info$by_vars)
}

#' Join two Hive tables
#' 
#' join functions in honeycomb are just like those in dplyr as documented 
#' in \code{\link[dplyr]{join}}. The only difference is the default \code{suffix} 
#' values, which use underscores rather than dots for compatibility with Hive.
#' 
#' @name left_join
#' @export
left_join.tbl_Hive <- function(x, y, by = NULL, copy = FALSE, suffix = c("_x", "_y"), ...) {
  join_info <- prepare_join(x, y, by)
  joined_tbl <- dplyr:::left_join.tbl_lazy(x, join_info$y_to_join, by = join_info$by_vars, 
                                           copy = copy, suffix = suffix, ...)
  after_join(joined_tbl, join_info$by_vars)
}

#' Join two Hive tables
#' 
#' join functions in honeycomb are just like those in dplyr as documented 
#' in \code{\link[dplyr]{join}}. The only difference is the default \code{suffix} 
#' values, which use underscores rather than dots for compatibility with Hive.
#' 
#' @name right_join
#' @export
right_join.tbl_Hive <- function(x, y, by = NULL, copy = FALSE, suffix = c("_x", "_y"), ...) {
  by_vars <- common_by(by, x, y)
  dplyr:::left_join.tbl_lazy(y, x, by = setNames(by_vars$x, by_vars$y), copy = copy, suffix = suffix, ...)
}

#' Join two Hive tables
#' 
#' join functions in honeycomb are just like those in dplyr as documented 
#' in \code{\link[dplyr]{join}}. The only difference is the default \code{suffix} 
#' values, which use underscores rather than dots for compatibility with Hive.
#' 
#' @name full_join
#' @export
full_join.tbl_Hive <- function(x, y, by = NULL, copy = FALSE, suffix = c("_x", "_y"), ...) {
  join_info <- .prepare_join(x, y, by)
  xkey <- join_info$by_vars$x
  ykey <- join_info$by_vars$y
  ykey_copy <- paste0(ykey, "_ykey_copy")
  joined_tbl <- dplyr:::full_join.tbl_lazy(
    x, y = mutate_(join_info$y_to_join, .dots = setNames(as.list(ykey), ykey_copy)), 
    by = join_info$by_vars, copy = copy, suffix = suffix, ...
  ) %>%
    select_(.dots = setdiff(colnames(.), setdiff(ykey, xkey))) %>%
    mutate_(.dots = setNames(paste0("coalesce(", xkey, ", ", ykey_copy, ")"), xkey)) %>%
    select_(.dots = setdiff(colnames(.), ykey_copy))
  after_join(joined_tbl, join_info$by_vars)
}

