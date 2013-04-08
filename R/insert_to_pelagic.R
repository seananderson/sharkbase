#' Insert data from csv files into the database
#' 
#' @param ref The reference to insert (refcode). Must match file name.
#' @param Type of data to insert. E.g. "Master" or "tab1". Must match
#' file name.
#' @param File extension for the data file. Defaults to csv.
#' @param base_folder Absolute path in which all files are located.
#' Could be left as "" if you want to use relative paths.
#' @param data_folder Folder with the .csv files
#' @param sql_folder Folder to store SQL commands
#' @param inserted_data_folder Folder to move used .csv files to
#' @param log_file Log file to store output messages

insert_to_pelagic = function(ref, material, extension = "csv",
  base_folder = "/home/pelagicDB/", data_folder = "data", 
  sql_folder = "sql", inserted_data_folder = "data/datainDB", 
  log_file = "insertToPelagicLog.txt"){

  # this file is generally to insert the master file in master and then move master in the datainDB
  #material could be 'Master' or any of the tab and figures: "Tab1" or "Fig1" and so on
  #source("/home/pelagicDB/R/connectPelagic.R")

  con <- connectPelagic()

  # set up folder and file locations:
  data_folder <- paste(base_folder, "/", data_folder, sep = "")
  sql_folder <- paste(base_folder, "/", sql_folder, sep = "")
  temp_csv <- paste(data_folder, "/temporary.csv", sep = "")
  inserted_data_folder <-  paste(base_folder, "/", inserted_data_folder, sep = "") 

  if (material=='Master') pelagic.table <- 'master' else pelagic.table <- 'timeseries' 

  # check whether there is already data:
  datacheck <- dbSendQuery(con, statement = paste("select * from ",
      pelagic.table," where ref = '",ref,"';",sep=""))  
  datacheck <- fetch(datacheck, n = -1)

  if (nrow(datacheck)!=0) stop (paste(ref," is already in ",pelagic.table,sep = '')) 

  # extract the fields from the palgic table 
  data1 <- dbSendQuery(con, statement = paste("select * from ", 
      pelagic.table," where ref = 'dummy';",sep=""))  
  data1<- fetch(data1, n = -1)  

  if (extension == "csv") separator = "," else separator = "\t"

  # reads the input table:
  todata <- read.table(paste("../data/", ref, material,".", extension, sep=""),
    header = TRUE, sep = separator) 

  # find common names between database table and input table:
  fields <- intersect(names(todata),names(data1)) 

  # identifies the fields not included in the table:
  notin <- setdiff(names(todata),names(data1)) 

  # writes in a log file fields not included:
  write(paste("these fields (",paste(notin,collapse=" "),") of
      ",ref,material,".",extension," were not included in ",
      pelagic.table,"on ",date(),sep=""), log_file, append = TRUE) 

  # select only these fields from input table:
  todata <- todata[,fields] 

  # add the reference key to the data if they do not have it:
  if (!"ref" %in% names(todata)) {
    todata = data.frame(todata,ref = ref) 
    fields <- c(fields,"ref")
  }
  todata <- unique(todata)

  # I have to use ; instead of , because there are fields with internal comas:
  write.table(todata, file = temp_csv, row.names = F,sep=";",quote = F,na = "") 

  # to format the field names in a proper sql statement format:
  fields <- paste(fields,collapse=",") 

  # write an sql query in a sql file:
  write(paste("copy ",pelagic.table," (",fields,") from '", temp_csv,
      "' with delimiter ';' CSV HEADER;",sep=""),
    file = paste("../sql/",ref,material,".sql",sep="")) 

  # runs the sql command from system:
  system(paste("psql pelagic -U postgres -f ", sql_folder, ref,
      material, ".sql", sep="")) 

  # writes another message in insertToPelagic:
  write(paste("file ", ref, material,".csv",
      " no longer useful because tab delimited txt was created and used on ", 
      date(), sep=""), file = log_file, append = TRUE) 

  # clean folder:
  system(paste("mv ", data_folder, ref ,material, ".", extension," ", 
      inserted_data_folder, sep="")) 
}	
