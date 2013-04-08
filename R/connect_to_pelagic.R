#' Connect to pelagic database
#' 
#' @param dbname Database name

connect_to_pelagic = function(dbname = "pelagic"){
	require(RpgSQL)
	# it does not work from loval 
 
	con <- dbConnect(pgSQL(),host='baseline.stanford.edu',user = "postgres", password = "DELETED", dbname = dbname)	# this works in remote R 

 con

	#iccat = dbSendQuery(con, statement = paste("select lat,lon,gearcode FROM iccatt2ce WHERE Eff1Type = 'NO.HOOKS' AND GearGrpCode = 'LL' and Lat <= 46 AND Lat>30 AND (QuadID = 1 OR (QuadID = 4 AND Lon>0 AND Lon<=5))",sep=""))
	#iccat<- fetch(iccat, n = -1)
	
}
