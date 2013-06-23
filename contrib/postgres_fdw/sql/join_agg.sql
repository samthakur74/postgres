CREATE EXTENSION postgres_fdw;

CREATE SERVER loopback FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (dbname 'contrib_regression');

CREATE USER MAPPING FOR CURRENT_USER SERVER loopback;

CREATE TABLE t1 (
       c0 int
);

CREATE TABLE t2 (
       c1 int
);

CREATE FOREIGN TABLE ft1 (
	c0 int
)
SERVER loopback
OPTIONS (schema_name 'public', table_name 't1');

CREATE FOREIGN TABLE ft2 (
	c1 int
)
SERVER loopback
OPTIONS (schema_name 'public', table_name 't2');

ANALYZE t1;
ANALYZE t2;
ANALYZE ft1;
ANALYZE ft2;

EXPLAIN (VERBOSE, COSTS TRUE)
  SELECT * FROM ft1 JOIN ft2 ON c0 = c1;
