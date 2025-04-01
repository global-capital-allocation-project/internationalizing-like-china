####################
###PACKAGES#########
####################

dir.create("~/R/x86_64-pc-linux-gnu-library/3.5.1", showWarnings = FALSE)

install.packages("httr", repos = "http://cran.us.r-project.org", lib = "~/R/x86_64-pc-linux-gnu-library/3.5.1")
install.packages("jsonlite", repos="http://cran.us.r-project.org", lib = "~/R/x86_64-pc-linux-gnu-library/3.5.1")
install.packages("data.table", repos="http://cran.us.r-project.org", lib = "~/R/x86_64-pc-linux-gnu-library/3.5.1")
install.packages("curl", repos="http://cran.us.r-project.org", lib = "~/R/x86_64-pc-linux-gnu-library/3.5.1")

require(httr, lib.loc = "~/R/x86_64-pc-linux-gnu-library/3.5.1")
require(jsonlite, lib.loc = "~/R/x86_64-pc-linux-gnu-library/3.5.1")
require(data.table, lib.loc = "~/R/x86_64-pc-linux-gnu-library/3.5.1")
require(curl, lib.loc = "~/R/x86_64-pc-linux-gnu-library/3.5.1")


library(httr, lib.loc = "~/R/x86_64-pc-linux-gnu-library/3.5.1")
library(jsonlite, lib.loc = "~/R/x86_64-pc-linux-gnu-library/3.5.1")
library(data.table, lib.loc = "~/R/x86_64-pc-linux-gnu-library/3.5.1")
library(curl, lib.loc = "~/R/x86_64-pc-linux-gnu-library/3.5.1")


####################
###FUNCTIONS########
####################

#Intelligently ascribe the code to all potential formats based on length
CodesToJSONInput <- function(x) {
  xDf <- data.frame(idValue = x[, 1], Length = nchar(as.character(x[, 1])))
  key <- data.frame(idType = c("ID_SEDOL",
                               "ID_CUSIP_8_CHR",
                               "ID_COMMON",
                               "ID_CUSIP",
                               "ID_CINS",
                               "ID_BB",
                               "COMPOSITE_ID_BB_GLOBAL",
                               "ID_BB_GLOBAL_SHARE_CLASS_LEVEL",
                               "ID_BB_GLOBAL",
                               "ID_ISIN",
                               "ID_BB_UNIQUE"),
                    Length = c(7, 8, rep(9, 4), rep(12, 4), 18))
  return(merge(xDf, key, by = 'Length')[, c('idType', 'idValue')])
}

ChunkJSONToList <- function(jsonobj, chunksize){
  n <- nrow(jsonobj)
  idx <- rep(1:ceiling(n/chunksize),each=chunksize)[1:n]
  return(split(jsonobj, idx))
}

OpenFIGIFn <- function(input, apikey = NULL, openfigiurl = "https://api.openfigi.com/v1/mapping", 
                       preferdf = FALSE) 
{
  # Set headers
  headers <- add_headers(`Content-Type` = "application/json", `X-OPENFIGI-APIKEY` = apikey)
  
  # Convert input to JSON
  myjson <- toJSON(input, auto_unbox = TRUE)
  
  # Send API request
  req <- POST(openfigiurl, headers, body = myjson)
  
  # Check response status
  if (as.integer(req$status_code) != 200L) {
    warning(paste0("Got return code ", req$status_code, " when POST json request.\n"))
    return(NULL)
  }
  
  # Parse response content
  jsonrst <- content(req, as = "text")
  jsonrst <- fromJSON(jsonrst)
  
  # Append the input to the result
  jsonrst$input <- input
  
  # Return results
  return(jsonrst)
}


####################
###SETUP############
####################

apikey = '< INSERT KEY HERE >'
apiLimitPerRequest <- 100
apiRequestsPerMinute <- 250

# read raw and temp directory from arguments. it should be something like:


args = commandArgs(trailingOnly=TRUE)

if (length(args)!=2) {
  stop("Please supply the working directory for the externalids_to_api file, and the working directory to the do_not_search file, after calling the rscript.\n", call.=FALSE)
} else if (length(args)==2) {
  for (i in 1:length(args)) {
    eval (parse (text = args[[i]] ))
  }
  print(tempdir)
  print(rawdir)
  setwd(tempdir)
  dirRaw <- rawdir
}

externalids_to_api <- read.csv('externalids_to_api.csv', stringsAsFactors = FALSE)
# Following document contains previously queried externalids that will not be queried again for time saving purposes
# These externalids are later added to the externalid_keyfile.dta file in externalid_postbloomberg.do part of the build
donotsearch <- read.csv(paste0(dirRaw,'/donotsearch.csv'), stringsAsFactors = FALSE)
print("Head of infile:")
print(head(externalids_to_api))

print("Head of donotsearch:")
print(head(donotsearch))

codes <- externalids_to_api[!(externalids_to_api$externalid_mns %in% donotsearch$donotsearch), ]
print(paste('Loaded',nrow(codes),'unique externalids to send to API.'))

####################
###MAIN#############
####################

inputJSON <- CodesToJSONInput(codes)
print(paste('Made JSON for',nrow(inputJSON),'queries.'))
jsonBlockList <- ChunkJSONToList(inputJSON, apiLimitPerRequest)
# Calculate the total number of requests and estimate time for processing
totalBlocks <- length(jsonBlockList)
estimatedMinutes <- ceiling((totalBlocks / apiRequestsPerMinute) * 1.5)
estimatedHours <- ceiling((estimatedMinutes) / 60)

# Print the details about the JSON block list and estimated time
print(paste('Made JSON Block List, sending to openfigi.org. Total request will take approximately',
            estimatedMinutes, 'minutes, i.e. approx', estimatedHours, 'hours.'))


for(i in 1:length(jsonBlockList)) {
  # Print update every 100 blocks
  if (i %% 100 == 0) {
    print(paste('Processing block', i, 'of', totalBlocks))
  }
  resultsList <- list()
  result <- try(OpenFIGIFn(jsonBlockList[[i]], apikey = apikey), silent = FALSE)
  if (length(result) == 3) {
    for (j in 1:length(result$data)){
      if (is.null(result$data[[j]])) {
        # Ignore null values
      }
      else {
        # Append data and input to the results list
        resultsList[[length(resultsList) + 1]] <- list(data = result$data[[j]], input = result$input[j,])
      }
    }
    
    # Turn the cleaned results into a data frame
    resultsDf <- do.call(rbind, lapply(resultsList, function(x) {
      data.frame(input = x$input, data = x$data, stringsAsFactors = FALSE)
    }))
    # Remove data. and input. from column names
    colnames(resultsDf) <- gsub("data\\.|input\\.", "", colnames(resultsDf))
    # Rename idType and idValue columns to externalid_mns	idformat
    colnames(resultsDf)[colnames(resultsDf) == "idType"] <- "idformat"
    colnames(resultsDf)[colnames(resultsDf) == "idValue"] <- "externalid_mns"
    # Rename all the columns to lowercase
    colnames(resultsDf) <- tolower(colnames(resultsDf))
    
    resultsFound <- resultsDf[!is.na(resultsDf$externalid_mns),]
    
    if(nrow(resultsFound) > 0){
      # Check if the file exists
      if (file.exists("externalid_keyfile.csv")) {
        # Append to the existing file
        write.table(resultsFound, file = "externalid_keyfile.csv", row.names = FALSE, col.names = FALSE, sep = ",", append = TRUE)
      } else {
        # Create a new file and write the data
        write.table(resultsFound, file = "externalid_keyfile.csv", row.names = FALSE, col.names = TRUE, sep = ",")
      }
      # Revert resultsFound to NULL dataframe
      resultsFound <- data.frame()
    }
  }
  Sys.sleep(0.25)
}

print("Finished processing all the blocks.")
