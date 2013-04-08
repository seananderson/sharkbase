\name{insert_to_pelagic}
\alias{insert_to_pelagic}
\title{Insert data from csv files into the database}
\usage{
  insert_to_pelagic(ref, material, extension = "csv",
    base_folder = "/home/pelagicDB/", data_folder = "data",
    sql_folder = "sql",
    inserted_data_folder = "data/datainDB",
    log_file = "insertToPelagicLog.txt")
}
\arguments{
  \item{ref}{The reference to insert (refcode). Must match
  file name.}

  \item{Type}{of data to insert. E.g. "Master" or "tab1".
  Must match file name.}

  \item{File}{extension for the data file. Defaults to
  csv.}

  \item{base_folder}{Absolute path in which all files are
  located. Could be left as "" if you want to use relative
  paths.}

  \item{data_folder}{Folder with the .csv files}

  \item{sql_folder}{Folder to store SQL commands}

  \item{inserted_data_folder}{Folder to move used .csv
  files to}

  \item{log_file}{Log file to store output messages}
}
\description{
  Insert data from csv files into the database
}

