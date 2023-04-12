\name{SQL}
\alias{SQL}
\title{Run duckdb SQL statements on data frames}
\usage{
SQL(query,path)
}
\description{
Runs duckdb SQL statements on in-memory data frames without registering them.
Optionnaly sends the result of a query to a parquet file in an efficient way.
Using the 'path' argument may be be twenty times faster than using the COPY ... TO SQL statement.
}
\arguments{
\item{query}{
A character string with SQL statements.
}
\item{path}{
A path to a parquet file to be created.
}}
\examples{
SQL("SELECT * FROM mtcars LIMIT 1")
}
