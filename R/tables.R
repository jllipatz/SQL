#
# Get a list of possible table names from a SQL query string
#
# NB: not standard tables names (inside quotes) are not supported
#
tables <- function(query) {
  WORDS <- c("SELECT","FROM","WHERE","GROUP","HAVING","ORDER","LIMIT",";")

  # Clean and split query string --------------------------------------------

  s <- query |>
    str_replace_all("/\\*.*?\\*/"," ") |>         # Multi line comments
    str_replace_all("--[^\\n]*\\n"," ") |>        # EOL comments
    str_replace_all("'(''|[^'])*'","_STRING_") |>
    str_replace_all("([(),;])"," \\1 ") |>
    str_replace_all("[ \\n]+"," ") |>
    str_split(' ')
  v <- s[[1]]
  v <- v[v!='']

  # Transform to list -------------------------------------------------------

  i <- 0  # (1) position in the vector of strings, will be incremented within f()
  n <- length(v)
  f <- function() {
    l <- list(); j <- 0
    while (i<n) {
      i <<- i+1         # Increments the position initialized outside recursion (1)
      if (v[i]==")") return(l)
      else if (v[i]=="(") l[[j<-j+1]] <- f()
      else                l[[j<-j+1]] <- v[i]
    }
    l
  }
  l <- f()

  # Find table names --------------------------------------------------------
  # within a FROM group
  #  just after a FROM or just after a JOIN or a ','
  # and before something indicating the end of the FROM group

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
