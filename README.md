# SQL
Runs SQL statements on in-memory data frames and more.

The package offers an unique function `SQL` to run SQL statements within a temporary Duckdb data base. The function automatically detects the use of data frames being in the R global environment and registers them for use by Duckdb. So the SQL function is a small substitute to `sqldf::sqldf` with the double bonus of an extended SQL language and no need to copy data frames contents in the data base.

The SQL function is also designed to write the results to a 'parquet' file, like the `COPY TO` SQL statement but considerably faster.
