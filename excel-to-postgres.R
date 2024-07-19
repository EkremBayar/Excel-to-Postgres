# Packages ----------------------------------------------------------------
library(DBI)
library(RPostgres)

# Root Path ---------------------------------------------------------------
root_path <- getwd()

# Connect Postgres --------------------------------------------------------
con <- dbConnect(
  RPostgres::Postgres(), 
  port = portnumber, 
  dbname ='dbname', 
  user ='username', 
  password ='password', 
  host ='host'
  )

# Create Table ------------------------------------------------------------
query <- 'CREATE TABLE excel_files (
  id SERIAL PRIMARY KEY,
  file_name VARCHAR(255) NOT NULL,
  file_data BYTEA NOT NULL,
  version INTEGER DEFAULT 1
);'
dbSendStatement(con, query)


# Check Table -------------------------------------------------------------
dbReadTable(con, "excel_files")


# Insert File -------------------------------------------------------------
insert_file <- function(con, file_path, disconnect = FALSE) {
  
  binary_data <- readBin(file_path, "raw", file.info(file_path)$size)
  file_name <- basename(file_path)
  
  # Check Excel File if it exists on DB
  query_check <- "SELECT id FROM excel_files WHERE file_name = $1"
  result <- dbGetQuery(con, query_check, params = list(file_name))
  
  if (nrow(result) > 0) {
    # If it exists, then update it
    file_id <- result$id[1]
    query_update <- "UPDATE excel_files SET file_data = $2, version = version + 1 WHERE id = $1"
    dbExecute(con, query_update, params = list(file_id, list(binary_data)))
    cat("File updated with ID:", file_id, "\n")
  } else {
    # If does not exist, insert fiel to DB
    query_insert <- "INSERT INTO excel_files (file_name, file_data) VALUES ($1, $2)"
    dbExecute(con, query_insert, params = list(file_name, list(binary_data)))
    cat("New file inserted:", file_name, "\n")
  }
  
  if(disconnect == TRUE){dbDisconnect(con)}
}


# Retrive Excel File ------------------------------------------------------
retrieve_file <- function(file_name, output_path, disconnect = FALSE) {

  query <- paste0("SELECT file_name, file_data FROM ",table_name," WHERE file_name = '", file_name, "'")
  result <- dbGetQuery(con, query)
  
  if (nrow(result) > 0) {
    file_name <- result$file_name[1]
    binary_data <- result$file_data[[1]]
    
    output_file <- file.path(output_path, file_name)
    writeBin(binary_data, output_file)
    
    cat("File", file_name, "has been saved to", output_path, "\n")
  } else {
    cat("File not found\n")
  }
  
  if(disconnect == TRUE){dbDisconnect(con)}
}


# Example -----------------------------------------------------------------
file <- paste0(root_path, "/test.xlsx")
insert_file(con, file_path = file)

dbReadTable(con, "excel_files")

retrieve_file("test.xlsx", root_path)

