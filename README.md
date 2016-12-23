# CW_pipeline_SQL
A SQLite database is very appropriate for continuous gravitational waves searches!

For the particular kind of data analysis involved in looking for quasi-periodic gravitational waves, a SQLite database can be a very good fit. This is because the database is portable, easy to set up, and the concurrency is <i>very</i> low. We'd take a slight hit from network lag, so a client/server DB may not be as efficient. Plus you can easily <a href="https://www.sqlite.org/cli.html">interactively query the DB</a>.

This project uses the Python script <a href="https://github.com/NotAFakeRa/CW_pipeline_SQL/blob/master/xml2sql.py">xml2sql.py</a> to translate search results and veto regions into an SQLite database, and then apply the vetoing procedures. 

This is currently a work in progress, as it's not pretty and automated yet. 
