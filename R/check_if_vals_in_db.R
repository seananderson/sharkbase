#' Check if a set of values are contained in a database table
#'
#' @param vals  The values you want to check
#' @param con A RPostgreSQL connection object from dbConnect()
#' @param col The name of the column in the database table
#' @param tab The name of the table in the database

check_if_vals_in_db <- function(vals, con, col, tab = "timeseries") {
  data1 <- dbSendQuery(con, statement = paste("select ", col, " from ", tab, ";",sep=""))  
  data1<- fetch(data1, n = -1)  
  uni_db <- unique(data1[,1])
  not_in_db <- vals[!vals %in% uni_db]
  if(length(not_in_db) > 0) 
    warning(paste(not_int_db, "is not in the", tab, "database table", collapse = "; "))
}  

