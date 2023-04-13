# NOT SUPPORTED:
#  not standard tables names
tables <- function(query) {
  WORDS <- c("SELECT","FROM","WHERE","GROUP","HAVING","ORDER","LIMIT")

# Clean and split query string --------------------------------------------

  s <- query |>
    str_replace_all("/\\*.*?\\*/"," ") |>         # Multi line comments
    str_replace_all("--[^\\n]*\\n"," ") |>        # EOL comments
    str_replace_all("'(''|[^'])*'","_STRING_") |>
    str_replace_all("([(),])"," \\1 ") |>
    str_replace_all("[ \\n]+"," ") |>
    str_split(' ')
  v <- s[[1]]
  v <- v[v!='']

# Transform to list -------------------------------------------------------

  i <- 0; n <- length(v)
  f <- function() {
    l <- list(); j <- 0
    while (i<n) {
      i <<- i+1
      if (v[i]==")") return(l)
      else if (v[i]=="(") l[[j<-j+1]] <- f()
      else                      l[[j<-j+1]] <- v[i]
    }
    l
  }
  l <- f()

# Find table names --------------------------------------------------------
# Just after a FROM or
# within a FROM group, just after a JOIN or a ','

  r <- character(0); n <- 0
  g <- function (theList) {
    b <- ''; p <- ''
    for (i in seq_along(theList)) {
      e <- theList[[i]]
      if (length(e)>1) g(e)
      else if (e %in% WORDS) b <- e
      else if (p=="FROM") r[n<<-n+1]<<- e
      else if ((b=="FROM")&&(p %in% c("JOIN",","))) r[n<<-n+1]<<- e
      p <- if (length(e)==1) e else ''
    } }
  g(l)

  r[r!="_STRING_"]
}

SQL <- function(query,path) {
  on.exit(dbDisconnect(con,shutdown=TRUE))
  con <- dbConnect(duckdb())

  Map(\(x) duckdb_register(con,x,get(x,mode="list")), tables(query))

  if (missing(path))
      dbGetQuery(con,query)
  else {
    reader <-
      duckdb_fetch_record_batch(
        dbSendQuery(con,query,arrow=TRUE),
        chunk_size=1e6)

    file <- FileOutputStream$create(path)
    batch <- reader$read_next_batch()
    if (!is.null(batch)) {
      s <- batch$schema
      writer <-
        ParquetFileWriter$create(s,file,
          properties = ParquetWriterProperties$create(names(s)))

      while (!is.null(batch)) {
        writer$WriteTable(arrow_table(batch),chunk_size=1e6)
        batch <- NULL; gc()
        batch <- reader$read_next_batch()
      }

      writer$Close()
    }
    file$close()
  }
}
