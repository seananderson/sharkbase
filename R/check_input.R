#' Check input data files for errors and consistency
#'
#' @param ref The ref code to check. E.g. "Author.2009"
#' @param con A RPostgreSQL connection object from dbConnect()
#' @param data_folder The folder in which the .csv files are stored
#'

check_input  <- function(ref, con, data_folder = "data") {

  data1 <- dbSendQuery(con, statement = paste("select column_name from
      INFORMATION_SCHEMA.COLUMNS where table_name =
      'timeseries';",sep=""))  
  data1<- fetch(data1, n = -1)  
  timeseries_db_cols <- as.character(data1[,1])

  data1 <- dbSendQuery(con, statement = paste("select column_name from
      INFORMATION_SCHEMA.COLUMNS where table_name =
      'master';",sep=""))  
  data1<- fetch(data1, n = -1)  
  master_db_cols <- as.character(data1[,1])

  files <- list.files(path = data_folder, pattern = paste(ref, "*", sep = ""))

  # is there a master file?
  master_t_or_f <- sapply(files, function(i) grepl("Master", i))
  if(sum(master_t_or_f) != 1) 
    stop("Missing a Master file")

  # get master file:
  master_file <- files[master_t_or_f] 
  ts_files <- files[-master_t_or_f] 

  master_dat <- read.csv(paste(data_folder, "/", master_file, sep =
      ""), strip.white = TRUE, stringsAsFactors = FALSE)
  ts_dat <- list()
  for(i in 1:length(ts_files)) {
    ts_dat[[i]] <- read.csv(paste(data_folder, "/", ts_files[i], sep =
        ""), stringsAsFactors = FALSE, strip.white = TRUE)
  }

  # one ref code listed?
  if(length(unique(master_dat$ref)) > 1)
    stop("More than one ref code.")

  # does the ref match the ref in master?
  if(ref != master_dat$ref[1])
    stop("Ref code doesn't match master file ref code.")

  # is there a species and year listed in ts files?
  for(i in 1:length(ts_files)) {
    if(!"species" %in% names(ts_dat[[i]])) 
      stop(paste("No species in", ts_files[i]))
    if(!"year" %in% names(ts_dat[[i]]))
      stop(paste("No year in", ts_files[i])) 
  }

  # is there a species listed in the master file?
  if(!"species" %in% names(master_dat))
    stop(paste("No species in", master_file)) 

  # are all species in ts files also in master file?
  require(plyr)
  ts_spp <- laply(ts_dat, function(x) unique(x$species))
  ms_spp <- unique(master_dat$species)
  if(length(setdiff(ts_spp, ms_spp)) > 0)
    stop("Species in ts and master don't match")

  # do all cols match ts or master cols in db?
  for(i in 1:length(ts_files)) {
    ts_names_check <- names(ts_dat[[i]]) %in% timeseries_db_cols
    if(FALSE %in% ts_names_check)
      stop(paste(names(ts_dat[[i]])[!ts_names_check], "was found in",
          ts_files[i], "but is not in timeseries table of database"))
  }
  master_names_check <- names(master_dat) %in% master_db_cols
  if(FALSE %in% master_names_check)
    stop(paste(names(master_dat)[!master_names_check], "not in master
        database", collapse = "; "))

  # are there the same number of rows in master as time series files?
  if(nrow(master_dat) != length(ts_files))
    stop("Number of rows in Master file don't match number of ts .csv files")

  # are the years and species filled out without gaps?
  for(i in 1:length(ts_files)) {
    if(sum(is.na(ts_dat[[i]]$species)) > 0)
      stop(paste("Gaps in species column in", ts_files[i]))
    if(sum(is.na(ts_dat[[i]]$year)) > 0)
      stop(paste("Gaps in year column in", ts_files[i]))
  }

  # do the species already exist in the db?
  check_if_vals_in_db(vals = ts_spp, con = con, col = "species")


  # do the regions already exist in the db?
  if(sum(laply(ts_dat, function(x) "region" %in% names(x))) > 0) {# there are regions in ts files
    ts_reg <- laply(ts_dat, function(x) unique(x$region))
    check_if_vals_in_db(vals = ts_reg, con = con, col = "region")
  }
  ms_reg <- unique(master_dat$region)
  check_if_vals_in_db(vals = ms_reg, con = con, col = "region", tab = "master")

  invisible(list(master_dat = master_dat, ts_dat = ts_dat))
}

#require("RPostgreSQL")
#con <- dbConnect(PostgreSQL(), user= "postgres", password="", dbname="pelagic")
#check_input("Andrews.Brown.2009", con = con)

