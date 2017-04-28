
op_sort.op_arrange.hive <- function(op) {
  order_vars <- translate_sql_(op$dots, dplyr:::partition_con(), op_vars(op))
  dplyr:::c.sql(op_sort(op$x), order_vars, drop_null = TRUE)
}

.onLoad <- function(libname, pkgname) {
  dplyr_ns <- getNamespace("dplyr")
  unlockBinding("op_sort.op_arrange", env = dplyr_ns)
  assignInNamespace("op_sort.op_arrange", op_sort.op_arrange.hive, ns = dplyr_ns)
  lockBinding("op_sort.op_arrange", env = dplyr_ns)
  invisible()
}


