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

make.win.fun =
  function(f)
    function(...) {
      dplyr:::over(
        dplyr::build_sql(
          dplyr::sql(f),
          list(...)),
        dplyr:::partition_group(),
        NULL,
        frame = c(-Inf, Inf))}



parallel_op <- function(..., operator) {
  vars <- list(...)
  conditions <- vector(mode = "list", length = length(vars))
  for (i in 1:(length(vars) - 1)) {
    condition_txt <- paste(vars[[i]], vars[(i + 1):length(vars)], sep = operator)
    conditions[[i]] <- paste("when", paste(condition_txt, collapse = " and "), "then", vars[[i]])
  }
  conditions[[length(vars)]] <- paste("else", vars[[length(vars)]])
  sql(paste("case", paste(conditions, collapse = "\n"), "end"))
}

#' @export
sql_translate_env.HiveConnection =
  function(x)
    sql_variant(
      scalar =
        sql_translator(
          .parent = base_scalar,
          rand =
            function(seed = NULL)
              build_sql(sql("rand"), if(!is.null(seed)) list(as.integer(seed))),
          randn =
            function(seed =  NULL)
              build_sql(sql("randn"), if(!is.null(seed)) list(as.integer(seed))),
          round =
            function(x, d = 0) build_sql(sql("round"), list(x, as.integer(d))),
          paste =
            function(..., sep = " ") build_sql(sql("CONCAT_WS"), list(sep, ...)),
          paste0 =
            function(...) build_sql(sql("CONCAT_WS"), list("", ...)),
          str_length =
            function(x) build_sql(sql("LENGTH"), list(x)),
          substr =
            function(x, s, n) build_sql(sql("substr"), list(x, as.integer(s), as.integer(n))),
          str_detect = function(x, pattern) build_sql(x, " RLIKE ", list(pattern)),
          str_replace_all = function(x, pattern, replacement) {
            build_sql(sql("REGEXP_REPLACE"), list(x, pattern, replacement))
          },
          `%in%` = function(x, y) {
            if (is.sql(y) && substr(y, 1, 1) != "(") {
              y <- sql(paste0("(", y, ")"))
            }
            if (!is.sql(y) && length(y) == 1) {
              code <- build_sql(x, " in ", "(", y, ")")
            } else {
              code <- build_sql(x, " in ", y)
            }
            code
          },
          case_when = function(...) {
            conditions <- lazyeval::lazy_dots(...) %>%
              purrr::map(function(x) {
                rhs <- lazyeval::f_rhs(as.formula(x$expr))
                if (is.character(rhs)) {
                  rhs <- paste0("'", rhs, "'")
                }
                if (is.language(rhs)) {
                  rhs <- translate_sql_(rhs, con = new("HiveConnection"))
                }
                lhs <- lazyeval::f_lhs(as.formula(x$expr))
                if (!isTRUE(lhs)) {
                  lhs <- translate_sql_(lhs, con = new("HiveConnection"))
                }
                list(lhs = lhs, rhs = rhs)
              })
            default <- purrr::keep(conditions, function(x) isTRUE(x$lhs))
            if (length(default) > 0) {
              default_elt <- paste("else", default[[1]]$rhs)
            } else {
              default_elt <- ""
            }
            conditions <- conditions %>%
              purrr::discard(function(x) isTRUE(x$lhs)) %>%
              purrr::map_chr(function(x) paste("when", x$lhs, "then", x$rhs))
            sql(paste("case", paste(conditions, collapse = " "), default_elt,
                      "end"))
          },
          bround =
            function(x, d = 0) build_sql(sql("bround"), list(x, as.integer(d))),
          as.Date = function (x) build_sql("CAST(", x, " AS DATE)"),
          ymd = function(x) build_sql("CAST(CONCAT_WS('-', substr(", x,
                                      ", 1, 4), substr(", x, ", 5, 2), substr(",
                                      x, ", 7, 2)) AS DATE)"),
          dayofyear = function(x) build_sql("CAST(date_format(", x, ", 'D') AS INT)"),
          today = function() build_sql("CURRENT_DATE()"),
          subtract_days = function(x,n_days) build_sql("CAST(DATE_SUB(", x, ", CAST(", n_days, " AS INT)) AS DATE)"),
          add_days = function(x,n_days) build_sql("CAST(DATE_ADD(", x, ", CAST(", n_days, " AS INT)) AS DATE)"),
          datediff = function(x,y) build_sql("DATEDIFF(", x, ", ", y, ")"),
          as.character = function (x) build_sql("CAST(", x, " AS STRING)"),
          as.numeric = function (x) build_sql("CAST(", x, " AS DOUBLE)"),
          as.integer = function(x) build_sql("CAST(", x, " AS INT)"),
          as.timestamp = function(x) build_sql("CAST(", x, " AS TIMESTAMP)"),
          `[` = function(x, y) build_sql(x, "[", y, "]"),
          pmin = function(...) parallel_op(..., operator = " <= "),
          pmax = function(...) parallel_op(..., operator = " >= "),
          get = function(x, y) build_sql(x, ".", y)),
      aggregate =
        sql_translator(
          .parent = base_agg,
          n = function() sql("COUNT(*)"),
          sd =  sql_prefix("STDDEV_SAMP"),
          var = sql_prefix("VAR_SAMP"),
          covar = sql_prefix("COVAR_POP"),
          cor = sql_prefix("CORR"),
          quantile = sql_prefix("PERCENTILE_APPROX")),
      window =
        sql_translator(
          .parent = base_win,
          sd =  make.win.fun("STDDEV_SAMP"),
          var = make.win.fun("VAR_SAMP"),
          covar = make.win.fun("COVAR_POP"),
          cor = make.win.fun("COR_POP"),
          quantile = make.win.fun("PERCENTILE_APPROX"),
          lag = function(x, n = 1L, default = NA, order = NULL) base_win$lag(x, as.integer(n), default, order),
          lead = function(x, n = 1L, default = NA, order = NULL) base_win$lead(x, as.integer(n), default, order),
          ntile = function (order_by, n) base_win$ntile(order_by, as.integer(n))
        ))


