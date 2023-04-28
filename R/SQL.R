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
