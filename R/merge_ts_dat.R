#' Merge time series data files
#'
#' @param master_df A data frame of the master data. This can be
#' created with the output from \code{check_input}.
#' @param ts_list A list with a data frame for each ts data file. This can be
#' created with the output from \code{check_input}.
#' @return A data frame of the merged time series data.

merge_ts_dat <- function(master_df, ts_list) {
  require(plyr)
  ts_out <- ts_list[[1]] # initial value
  if(length(ts_list) > 1) { # merge ts data
    for(i in 2:length(ts_list)) {
      ts_out <- join(ts_out, ts_list[[i]], type = "full")
    }
  }
  ts_out$ref <- unique(master_df$ref) 
  return(ts_out)
}

#merge_ts_dat(x$master_dat, x$ts_dat)

