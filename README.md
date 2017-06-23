# honeycomb

honeycomb is a dplyr backend for Hive. honeycomb developed out of [dplyr.spark.hive](https://github.com/rzilla/dplyr.spark.hive), which served as the launching point from which honeycomb was built. 

honeycomb is focused only on Hive, and it was adapted for compatibility with dplyr v0.5.

## Installation
`honeycomb` requires `dplyr` `v0.5.0`.
The easiest way to verify that the correct set of dependencies are installed is to use a fixed snapshot of MRAN (Microsoft) snapshot of CRAN.
Add the following lines to your `Rprofile.site` and `Renviron.site` files. If you have a fresh installation of `R` you will need to create the files.

__`$R_HOME/etc/Rprofile.site`__
```
options(repos=structure(c(CRAN='https://mran.revolutionanalytics.com/snapshot/2017-04-01/')))
```

__`$R_HOME/etc/Renviron.site`__
```
HADOOP_CMD=/usr/bin/hadoop
USE_KERBEROS=0
HIVE_SERVER_HOST=localhost
HIVE_SERVER_PORT=10000
HIVE_JAR_FOLDERS=/usr/lib/hive/lib
```

Now install the `honeycomb` package using the `devtools` library within `R`.

```{r}
devtools::install_github('ZurichPA/rhdfs', subdir='pkg')
devtools::install_github('ZurichPA/orpheus')
devtools::install_github('ZurichPA/honeycomb')
```
