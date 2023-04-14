\name{SQL}
\alias{SQL}
\title{Run duckdb SQL statements on data frames}
\usage{
SQL(query,path)
}
\description{
Runs Duckdb SQL statements on in-memory data frames without registering nor copying them.
Optionnaly sends the result of a query to a parquet file in an efficient way.
Using the 'path' argument may be be twenty times faster than using the COPY ... TO SQL statement.
}
\arguments{
\item{query}{
A character string containing SQL statements undertandable by Duckdb.
}
\item{path}{
A path to a parquet file to be created.
}}
\value{
If 'path' is not given a value, returns the result of the query as a data frame, else returns an invisible NULL.
}
\examples{
SQL("SELECT * FROM mtcars LIMIT 1")

# Temporary tables may be created but must be quoted when used
SQL("
  CREATE TABLE cyls AS SELECT DISTINCT cyl FROM mtcars;
  SELECT * FROM 'cyls'
")

\dontrun{
# Mixing data frames and parquet files, then writing to parquet
dep <- rio::import('V:/PALETTES/IGoR/data/dep2014.dbf')

SQL(
  "SELECT a.*,b.REGION
   FROM 'V:/PALETTES/parquet/rp68a19.parquet' a
   LEFT JOIN dep b
   ON a.DR=b.DEP",
  'V:/PALETTES/tmp/rp68a19s.parquet'
)
 }
}
