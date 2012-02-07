#!/usr/bin/env python
# Regression tests for pg_stat_statements normalization
# Peter Geoghegan

# Any sort of concurrency can be expected to break these
# regression tests, as they rely on the number of tuples
# in the pg_stat_statements view not changing due to
# external factors

# I assume that the dellstore 2 dump has been restored
# to the postgres database
# http://pgfoundry.org/forum/forum.php?forum_id=603

import psycopg2

test_no = 1

def do_post_mortem(conn):
	print "Post-mortem:"
	cur = conn.cursor()
	cur.execute("select * from pg_stat_statements();")
	for i in cur:
		print i

	print "\n"

def print_queries(conn):
	print "Queries that were found in pg_stat_statements: "
	cur = conn.cursor()
	cur.execute("select query from pg_stat_statements;")
	for i in cur:
		print i[0]

def demonstrate_buffer_limitation(conn):
	# It's expected that comparing a number of sufficiently large queries will result
	# in their incorrectly being considered equivalent provided the differences occur
	# after we run out of space to selectively serialize to in our buffer.

	set_operations = ['union', 'union all', 'except' ]
	for it, i in enumerate(["select 1,2,3,4",
				"select upper(lower(upper(lower(initcap(lower('Foo'))))))",
				"select count(*) from orders o group by orderid" ]):
		long_query = ""
		long_long_query = ""
		for j in range(0,100):
			long_query += i + (' ' + set_operations[it] + ' \n' if j != 99 else " ")
		for j in range(0,1000):
			long_long_query += i + (' ' + set_operations[it] + ' \n' if j != 999 else " ")

		print long_query
		verify_statement_differs(long_query, long_long_query, conn, "Differences out of range (iteration {0})".format(it + 1))

def verify_statement_equivalency(sql, equiv, conn, test_name = None, cleanup_sql = None):
	# Run both queries in isolation and verify that there
	# is only a single tuple
	global test_no
	cur = conn.cursor()
	cur.execute("select pg_stat_statements_reset();")
	cur.execute(sql)
	if cleanup_sql is not None:
		cur.execute(cleanup_sql)

	cur.execute(equiv)
	if cleanup_sql is not None:
		cur.execute(cleanup_sql)

	cur.execute(
	# Exclude both pg_stat_statements_reset() calls and foreign key enforcement queries
	"""select count(*) from pg_stat_statements
		where query not like '%pg_stat_statements_reset%'
		and
		query not like 'SELECT 1 FROM ONLY%'
		{0};""".format(
		"" if cleanup_sql is None else "and query != '{0}'".format(cleanup_sql))
			)

	for i in cur:
		tuple_n = i[0]

	if tuple_n != 1:
		do_post_mortem(conn)
		raise SystemExit("""The SQL statements \n'{0}'\n and \n'{1}'\n do not appear to be equivalent!
		Test {2} failed.""".format(sql, equiv, test_no if test_name is None else "'{0}' ({1})".format( test_name, test_no)) )

	print """The statements \n'{0}'\n and \n'{1}'\n are equivalent, as expected.
		Test {2} passed.\n\n""".format(sql, equiv, test_no if test_name is None else "'{0}' ({1})".format( test_name, test_no))
	test_no +=1

def verify_statement_differs(sql, diff, conn, test_name = None, cleanup_sql = None):
	# Run both queries in isolation and verify that there are
	# two tuples
	global test_no
	cur = conn.cursor()
	cur.execute("select pg_stat_statements_reset();")
	cur.execute(sql)
	if cleanup_sql is not None:
		cur.execute(cleanup_sql)
	cur.execute(diff)
	if cleanup_sql is not None:
		cur.execute(cleanup_sql)

	cur.execute(
	# Exclude both pg_stat_statements_reset() calls and foreign key enforcement queries
	"""select count(*) from pg_stat_statements
		where query not like '%pg_stat_statements_reset%'
		and
		query not like 'SELECT 1 FROM ONLY%'
		{0};""".format(
		"" if cleanup_sql is None else "and query != '{0}'".format(cleanup_sql))
			)
	for i in cur:
		tuple_n = i[0]

	if tuple_n != 2:
		do_post_mortem(conn)
		raise SystemExit("""The SQL statements \n'{0}'\n and \n'{1}'\n do not appear to be different!
				Test {2} failed.""".format(sql, diff, test_no if test_name is None else "'{0}' ({1})".format( test_name, test_no)))

	print """The statements \n'{0}'\n and \n'{1}'\n are not equivalent, as expected.
		Test {2} passed.\n\n """.format(sql, diff, test_no if test_name is None else "'{0}' ({1})".format( test_name, test_no))
	test_no +=1

def verify_normalizes_correctly(sql, norm_sql, conn, test_name = None):
	global test_no
	cur = conn.cursor()
	cur.execute("select pg_stat_statements_reset();")
	cur.execute(sql)
	ver_exists = "select exists(select 1 from pg_stat_statements where query = %s);"
	cur.execute(ver_exists, (norm_sql, ) )
	for i in cur:
		exists = i[0]

	if exists:
		print """The SQL \n'{0}'\n normalizes to \n'{1}'\n , as expected.
			Test {2} passed.\n\n """.format(sql, norm_sql, test_no if test_name is None else "'{0}' ({1})".format( test_name, test_no))
	else:
		do_post_mortem(conn)
		raise SystemExit("""The SQL statement \n'{0}'\n does not normalize to \n  '{1}'\n , which is not expected!
				Test {2} failed.""".format(sql, norm_sql, test_no if test_name is None else "'{0}' ({1})".format( test_name, test_no)))
	test_no +=1

# Test for bugs in synchronisation between planner and executor
def test_sync_issues(conn, count=5):
	global test_no
	cur = conn.cursor()
	cur.execute("select pg_stat_statements_reset();")


	# Note that the SQL in this plpgsql function is not paramaterized, so
	# detecting that and hashing on string doesn't get the implementation
	# off-the-hook:
	spi_query = "update orders set orderid = 5 where orderid = 5"
	cur.execute(""" create or replace function test() returns void as
					$$begin {0};
					end;$$ language plpgsql""".format(spi_query))

	sql = "select test();"

	for i in range(0, count):
		cur.execute(sql)

	# Plan caching will make the first call of the function above plan the sql statement after it plans the function call.
	# This caused the assumption about their synchronisation to fall down in earlier revisions of the patch.
	# Another concern is that we don't overwrite the jumble that is needed in the executor for an unnested
	# query with a nested query's jumble

	# pg_stat_statements.track is set to 'all', so we expect to see both the function call and the spi-executed query `count` times
	cur.execute("select calls from pg_stat_statements where query = '{0}' order by calls;".format(sql));
	for i in cur:
		 function_calls = i[0]

	cur.execute("select calls from pg_stat_statements where query = '{0}' order by calls;".format(spi_query));
	for i in cur:
		 spi_query_calls = i[0]

	for i in (function_calls, spi_query_calls):
		name = "function_calls" if i is function_calls else "spi_query_calls"

		if i == count:
				print "Planner/ Executor sync issue test (test {0}, '{1}') passed. actual calls: {2}".format(test_no, name, i)
		else:
			do_post_mortem(conn)
			raise SystemExit("Planner/ Executor sync issue detected! Test {0} ('{1}') failed! (actual calls: {2}, reported: {3}".format(test_no, name, i, count))

		test_no +=1

def main():
	conn = psycopg2.connect("")

	# Run all tests while tracking all statements (i.e. nested ones too, within
	# functions). This is necessary for the test_sync_issues() test, but shouldn't
	# otherwise matter. I suspect that it may usefully increase test coverage
	# in some cases at some point in the code's development.
#	cur = conn.cursor()
#	cur.execute("set pg_stat_statements.track = 'all';")

	## Start tests...just let exceptions propagate
#	test_sync_issues(conn, 25)

	verify_statement_equivalency("select '5'::integer;", "select  '17'::integer;", conn)
	verify_statement_equivalency("select 1;", "select      5   ;", conn)
	verify_statement_equivalency("select 'foo'::text;", "select  'bar'::text;", conn)
	# Date constant normalization
	verify_statement_equivalency("select * from orders where orderdate = '2001-01-01';" ,"select * from orders where orderdate = '1960-01-01'", conn)
	verify_statement_equivalency("select '5'::integer;","select  '17'::integer;", conn)
	# Test equivalency of cast syntaxes:
	verify_statement_equivalency("select '5'::integer;","select integer '5'", conn)

	# We don't care about whitespace or differences in constants:
	verify_statement_equivalency(
	"select o.orderid from orders o join orderlines ol on o.orderid = ol.orderid     where    customerid        =  12345  ;",
	"select o.orderid from orders o join orderlines ol on o.orderid = ol.orderid where customerid = 6789;", conn)

	# using clause matters:
	verify_statement_differs(
	"select o.orderid from orders o join orders oo using (orderid);",
	"select o.orderid from orders o join orders oo using (customerid);", conn)

	# "select * from " and "select <enumerate columns> from " equivalency:
	verify_statement_equivalency("select * from orders", "select orderid, orderdate, customerid, netamount, tax, totalamount from orders;", conn)
	# The equivalency only holds if you happen to enumerate columns in the exact same order though:
	verify_statement_differs("select * from orders", "select orderid, orderdate, customerid, tax, netamount, totalamount from orders", conn)
	# columns must match:
	verify_statement_differs("select  customerid from orders", "select orderid from orders", conn)
	# We haven't really resolved the types of these values, so they're actually equivalent:
	verify_statement_equivalency("select 1;", "select 1000000000;", conn)
	# However, these are not:
	verify_statement_differs("select 1::integer", "select 1000000000::bigint", conn)
	# Types must match - types aren't resolved here, but they are partially resolved:
	verify_statement_differs("select 5", "select 'foo'", conn)
	# Join Qual differences matter:
	verify_statement_differs("select * from orders o join orderlines ol on o.orderid = ol.orderid;",
				 "select * from orders o join orderlines ol on o.orderid = 5;", conn)
	# Although differences in Join Qual constants do not:
	verify_statement_equivalency("select * from orders o join orderlines ol on o.orderid = 66;",
				 "select * from orders o join orderlines ol on o.orderid = 5;", conn)
	# Constants may vary here:
	verify_statement_equivalency("select orderid from orders where orderid = 5 and 1=1;",
				 "select orderid from orders where orderid = 7 and 3=3;", conn)
	# Different logical operator means different query though:
	verify_statement_differs("select orderid from orders where orderid = 5 and 1=1;",
				 "select orderid from orders where orderid = 7 or 3=3;", conn)

	# don't mistake the same column number (MyVar->varno) from different tables:
	verify_statement_differs("select a.orderid    from orders a join orderlines b on a.orderid = b.orderlineid",
				 "select b.orderlineid from orders a join orderlines b on a.orderid = b.orderlineid", conn)

	# Note that these queries are considered equivalent, though you could argue that they shouldn't be.
	# This is because they have the same range table entry. I could look at aliases and differentiate
	# them that way, but I'm reasonably convinced that to do so would be a mistake. This is a feature,
	# not a bug.
	verify_statement_equivalency("select a.orderid from orders a join orders b on a.orderid = b.orderid",
				 "select b.orderid from orders a join orders b on a.orderid = b.orderid", conn)

	# Boolean Test node:
	verify_statement_differs(
	"select orderid from orders where orderid = 5 and (1=1) is true;",
	"select orderid from orders where orderid = 5 and (1=1) is not true;",
	conn)

	verify_statement_equivalency(
		"values(1, 2, 3);",
		"values(4, 5, 6);", conn)

	verify_statement_differs(
		"values(1, 2, 3);",
		"values(4, 5, 6, 7);", conn)


	verify_statement_equivalency(
		"select * from (values(1, 2, 3)) as v;",
		"select * from (values(4, 5, 6)) as v;", conn)

	verify_statement_differs(
		"select * from (values(1, 2, 3)) as v;",
		"select * from (values(4, 5, 6, 7)) as v;", conn)

	# Coalesce
	verify_statement_equivalency("select coalesce(orderid, null, null) from orders where orderid = 5 and 1=1;",
				 "select coalesce(orderid, 5, 5) from orders where orderid = 7 and 3=3;", conn)
	verify_statement_differs("select coalesce(orderid, 5, 5, 6 ) from orders where orderid = 5 and 1=1;",
				 "select coalesce(orderid, 5, 5) from orders where orderid = 7 and 3=3;", conn)


	# Observe what we can do with noise words (no "outer" in later statement, plus we use AS in the second query):
	verify_statement_equivalency("select * from orders o left outer join orderlines ol on o.orderid = ol.orderid;",
					"select * from orders AS o left join orderlines AS ol on o.orderid = ol.orderid;", conn)

	# Join order in statement matters:
	verify_statement_differs("select * from orderlines ol join orders o on o.orderid = ol.orderid;",
				 "select * from orders o join orderlines ol on o.orderid = ol.orderid;", conn)

	# Join strength/type matters:
	verify_statement_differs("select * from orderlines ol inner join orders o on o.orderid = ol.orderid;",
				 "select * from orderlines ol left outer join orders o on o.orderid = ol.orderid;", conn)

	# ExprNodes can be processed recursively:
	verify_statement_differs(
	"select upper(lower(upper(lower  (initcap(lower('Foo'))))));",
	"select upper(lower(upper(initcap(initcap(lower('Foo'))))));",
				conn)

	verify_statement_equivalency(
	"select upper(lower(upper(lower(initcap(lower('Foo'))))));",
	"select upper(lower(upper(lower(initcap(lower('Bar'))))));",
				conn)
	# Do same again, but put function in FROM
	verify_statement_differs(
	"select * from upper(lower(upper(lower  (initcap(lower('Foo'))))));",
	"select * from upper(lower(upper(initcap(initcap(lower('Foo'))))));",
				conn)

	verify_statement_equivalency(
	"select * from upper(lower(upper(lower(initcap(lower('Foo'))))));",
	"select * from upper(lower(upper(lower(initcap(lower('Bar'))))));",
				conn)

	# In the where clause too:
	verify_statement_equivalency(
	"select 1 from orders where 'foo' = upper(lower(upper(lower(initcap(lower('Foo'))))));",
	"select 1 from orders where 'foo' = upper(lower(upper(lower(initcap(lower('FOOFofofo'))))));",
				conn)

	verify_statement_differs(
	"select 1 from orders where 'foo' = upper(lower(upper(initcap(initcap(lower('Foo'))))));",
	"select 1 from orders where 'foo' = upper(lower(upper(lower(initcap(lower('FOOFofofo'))))));",
				conn)

	verify_statement_equivalency(
	"select 1 from orders where 1=55 and 'foo' = 'fi' or 'foo' = upper(lower(upper(initcap(initcap(lower('Foo'))))));",
	"select 1 from orders where 1=2 and 'feew' = 'fi' or 'bar' = upper(lower(upper(initcap(initcap(lower('Foo'))))));",
				conn)

	verify_statement_differs(
	"select 1 from orders where 1=55 and 'foo' = 'fi' or 'foo' = upper(lower(upper(initcap(upper  (lower('Foo'))))));",
	"select 1 from orders where 1=2 and 'feew' = 'fi' or 'bar' = upper(lower(upper(initcap(initcap(lower('Foo'))))));",
				conn)

	verify_statement_differs(
	"select 1 from orders where 1=55 and 'foo' = 'fi' or upper(lower(upper(initcap(upper(lower('Foo')))))) is null;",
	"select 1 from orders where 1=55 and 'foo' = 'fi' or upper(lower(upper(initcap(upper(lower('Foo')))))) is not null;",
				conn)

	verify_statement_differs(
	"select 1 from orders where 1=55 and 'foo' = 'fi' or     upper(lower(upper(initcap(upper(lower('Foo')))))) is null;",
	"select 1 from orders where 1=55 and 'foo' = 'fi' or not upper(lower(upper(initcap(upper(lower('Foo')))))) is null;",
				conn)

	# Nested BoolExpr is a differentiator:
	verify_statement_differs(
	"select 1 from orders where 1=55 and 'foo' = 'fi' or  'foo' = upper(lower(upper(initcap(upper (lower('Foo'))))));",
	"select 1 from orders where 1=55 and 'foo' = 'fi' and 'foo' = upper(lower(upper(initcap(upper (lower('Foo'))))));",
				conn)

	# For aggregates too
	verify_statement_differs(
	"select array_agg(lower(upper(initcap(initcap(lower('Foo')))))) from orders;",
	"select array_agg(lower(upper(lower(initcap(lower('Bar')))))) from orders;",
				conn)

	verify_statement_equivalency(
	"select array_agg(lower(upper(lower(initcap(lower('Baz')))))) from orders;",
	"select array_agg(lower(upper(lower(initcap(lower('Bar')))))) from orders;",
				conn)
	# Row-wise comparison
	verify_statement_differs(
	"select (1, 2) < (3, 4);",
	"select ('a', 'b') < ('c', 'd');",
				conn)
	verify_statement_differs(
	"select (1, 2, 3) < (3, 4, 5);",
	"select (1, 2) < (3, 4);",
				conn)
	verify_statement_equivalency(
	"select (1, 2, 3) < (3, 4, 5);",
	"select (3, 4, 5) < (1, 2, 3);",
				conn)

	# Use lots of different operators:
	verify_statement_differs(
	"select 1 < 2;",
	"select 1 <= 2;",
				conn)

	verify_statement_differs(
	"select ARRAY[1,2,3]::integer[] && ARRAY[1]::integer[];",
	"select ARRAY[1,2,3]::integer[] <@ ARRAY[1]::integer[];",
				conn)
	# Number of elements in ARRAY[] expression is a differentiator:
	verify_statement_differs(
	"select ARRAY[1,2,3]::integer[] <@ ARRAY[1]::integer[];",
	"select ARRAY[999]::integer[]   <@ ARRAY[342, 543, 634 ,753]::integer[];",
				conn)

	# Array coercion
	verify_statement_equivalency(
	"select '{1,2,3}'::oid[]::integer[] from orders;",
	"select '{4,5,6}'::oid[]::integer[] from orders;",
				conn)

	# array subscripting operations
	verify_statement_equivalency(
	"select (array_agg(lower(upper(lower(initcap(lower('Baz')))))))[5:5] from orders;",
	"select (array_agg(lower(upper(lower(initcap(lower('Bar')))))))[6:6] from orders;",
				conn)
	verify_statement_differs(
	"select (array_agg(lower(upper(lower(initcap(lower('Baz')))))))[5:5] from orders;",
	"select (array_agg(lower(upper(lower(initcap(lower('Bar')))))))[6] from orders;",
				conn)


	# nullif, represented as a distinct node but actually just a typedef
	verify_statement_differs(
	"select *, (select customerid from orders limit 1), nullif(5,10) from orderlines ol join orders o on o.orderid = ol.orderid;",
	"select *, (select customerid from orders limit 1), nullif('a','b') from orderlines ol join orders o on o.orderid = ol.orderid;",
	conn)

	verify_statement_equivalency(
	"select *, (select customerid from orders limit 1), nullif(5,10) from orderlines ol join orders o on o.orderid = ol.orderid;",
	"select *, (select customerid from orders limit 1), nullif(10,15) from orderlines ol join orders o on o.orderid = ol.orderid;",
	conn)

	# Row constructor
	verify_statement_differs(
	"select row(1, 2,'this is a test');",
	"select row(1, 2.5,'this is a test');",
	conn)

	# XML Stuff
	verify_statement_differs(
	"""
	select xmlagg
	(
		xmlelement
		(	name database,
			xmlattributes (d.datname as "name"),
			xmlforest(
				pg_database_size(d.datname) as size,
				xact_commit,
				xact_rollback,
				blks_read,
				blks_hit,
				tup_fetched,
				tup_returned,
				tup_inserted,
				tup_updated,
				tup_deleted
				)
		)
	)
	from		pg_stat_database d
	right join	pg_database
	on		d.datname = pg_database.datname
	where		not datistemplate;
	"""
	,
	"""
	select xmlagg
	(
		xmlelement
		(	name database,
			xmlattributes (d.datname as "name"),
			xmlforest(
				pg_database_size(d.datname) as size,
				xact_commit,
				xact_rollback,
				blks_read,
				blks_hit,
				tup_fetched,
				tup_returned,
				tup_updated,
				tup_deleted
				)
		)
	)
	from		pg_stat_database d
	right join	pg_database
	on		d.datname = pg_database.datname
	where		not datistemplate;
	""",
	conn)

	verify_statement_differs(
	"""
	select xmlagg
	(
		xmlelement
		(	name database,
			xmlattributes (d.blks_hit as "name"),
			xmlforest(
				pg_database_size(d.datname) as size
				)
		)
	)
	from		pg_stat_database d
	right join	pg_database
	on		d.datname = pg_database.datname
	where		not datistemplate;
	"""
	,
	"""
	select xmlagg
	(
		xmlelement
		(	name database,
			xmlattributes (d.blks_read as "name"),
			xmlforest(
				pg_database_size(d.datname) as size
				)
		)
	)
	from		pg_stat_database d
	right join	pg_database
	on		d.datname = pg_database.datname
	where		not datistemplate;
	""",
	conn)



	# subqueries

	# in select list
	verify_statement_differs("select *, (select customerid from orders limit 1) from orderlines ol join orders o on o.orderid = ol.orderid;",
				 "select *, (select orderid    from orders limit 1) from orderlines ol join orders o on o.orderid = ol.orderid;", conn)

	# select list nested subselection - inner most types differ
	verify_statement_differs(
	"select *, (select (select 1::integer from customers limit 1) from orders limit 1) from orderlines ol join orders o on o.orderid = ol.orderid;",
	"select *, (select (select 1::bigint  from customers limit 1) from orders limit 1) from orderlines ol join orders o on o.orderid = ol.orderid;", conn)
	# This time, they're the same
	verify_statement_equivalency(
	"select *, (select (select 1::integer from customers limit 1) from orders limit 1) from orderlines ol join orders o on o.orderid = ol.orderid;",
	"select *, (select (select 1::integer from customers limit 1) from orders limit 1) from orderlines ol join orders o on o.orderid = ol.orderid;", conn)

	# in from - this particular set of queries entail recursive JoinExprNode() calls
	verify_statement_differs(
	"select * from orderlines ol join orders o on o.orderid = ol.orderid join (select orderid a from orders) as t on ol.orderid=t.a;",
	"select * from orderlines ol join orders o on o.orderid = ol.orderid join (select orderid, customerid a from orders) as t on ol.orderid = t.a;", conn)
	# another in from - entails recursive JoinExprNode() call
	verify_statement_equivalency(
	"select * from orderlines ol join orders o on o.orderid = ol.orderid join (select orderid a from orders where orderid = 77) as t on ol.orderid=t.a;",
	"select * from orderlines ol join orders o on o.orderid = ol.orderid join (select orderid b from orders where orderid = 5) as t on ol.orderid = t.b;", conn)
	# Even though these two queries will result in the same plan, they are not yet equivalent - they are not
	# semantically equivalent, as least by our standard. We could probably figure out a way of having the
	# equivalency recognized, but I highly doubt it's worth bothering, since recognizing most kinds of semantic
	# equivalence is generally more of a neat consequence of our implementation than a practical feature:
	verify_statement_differs("select * from orderlines ol inner join orders o on o.orderid = ol.orderid;",
				 "select * from orderlines ol, orders o where o.orderid = ol.orderid;", conn)

	# Aggregate group by normalisation:
	verify_statement_differs("select count(*) from orders o group by orderid;",
				 "select count(*) from orders o group by customerid;", conn)

	# Which aggregate was called matters
	verify_statement_differs("select sum(customerid) from orders o group by orderid;",
				 "select avg(customerid) from orders o group by orderid;", conn)

	# As does the order of aggregates
	verify_statement_differs("select sum(customerid), avg(customerid) from orders o group by orderid;",
				 "select avg(customerid), sum(customerid) from orders o group by orderid;", conn)

	# As does the having clause
	verify_statement_equivalency(
	"select avg(customerid), sum(customerid) from orders o group by orderid having sum(customerid) > 50;",
	"select avg(customerid), sum(customerid) from orders o group by orderid having sum(customerid) > 1000;",
	conn)

	# operator in having clause matters
	verify_statement_differs(
	"select avg(customerid), sum(customerid) from orders o group by orderid having sum(customerid) > 100;",
	"select avg(customerid), sum(customerid) from orders o group by orderid having sum(customerid) = 100;",
	conn)

	# as does datatype
	verify_statement_differs(
	"select avg(customerid), sum(customerid) from orders o group by orderid having sum(customerid) > 100;",
	"select avg(customerid), sum(customerid) from orders o group by orderid having sum(customerid) > 100::bigint;",
	conn)
	verify_statement_equivalency(
	"select avg(customerid), sum(customerid::bigint) from orders o group by orderid having sum(customerid::bigint) > 100;",
	"select avg(customerid), sum(customerid::bigint) from orders o group by orderid having sum(customerid::bigint) > 15450;",
	conn)
	verify_statement_differs(
	"select avg(customerid), sum(customerid) from orders o group by orderid having sum(customerid) > 100;",
	"select avg(customerid), sum(customerid::bigint) from orders o group by orderid having sum(customerid::bigint) > 100;",
	conn)

	# columns order in "order by" matters:
	verify_statement_differs(
	"select * from orders o join orderlines ol on o.orderid = ol.orderid order by o.orderdate, o.orderid, customerid, netamount, totalamount, tax ;",
	"select * from orders o join orderlines ol on o.orderid = ol.orderid order by o.orderdate, o.orderid, customerid, netamount, tax, totalamount;", conn)

	# distinct on is a differentiator:
	verify_statement_differs("select distinct on(customerid) customerid, o.orderid from orderlines ol join orders o on o.orderid = ol.orderid;",
				 "select distinct on(o.orderid)    customerid, o.orderid from orderlines ol join orders o on o.orderid = ol.orderid;", conn)
	# both the "on" value and the absence or presence of the clause:
	verify_statement_differs("select distinct on(customerid) customerid, o.orderid from orderlines ol join orders o on o.orderid = ol.orderid;",
				 "select			 customerid, o.orderid from orderlines ol join orders o on o.orderid = ol.orderid;", conn)

	# select "all" is generally just a noise word:
	verify_statement_equivalency("select all customerid, o.orderid from orderlines ol join orders o on o.orderid = ol.orderid;",
				     "select                 customerid, o.orderid from orderlines ol join orders o on o.orderid = ol.orderid;", conn)


	# Updates are similarly normalised, and their internal representations shares much with select statements
	verify_statement_equivalency("update orders set orderdate='2011-01-06' where orderid=3;",
				     "update orders set orderdate='1992-01-06' where orderid     =     10;", conn)
	# updates with different columns are not equivalent:
	verify_statement_differs("update orders set orderdate='2011-01-06' where orderid = 3;",
				 "update orders set orderdate='1992-01-06' where customerid     =     10;", conn)

	# default within update statement
	verify_statement_equivalency(
	"update products set special=default where prod_id = 7;",
	"update products set special=default where prod_id = 10;",
	conn)

	verify_statement_equivalency(
	"insert into products(category, title, actor, price, special, common_prod_id) values (1,2,3,4,5,6);",
	"insert into products(category, title, actor, price, special, common_prod_id) values (1,2,3,4,5,6);",
	conn)

	verify_statement_differs(
	"insert into products(category, title, actor, price, special, common_prod_id) values (1,2,3,4,5,6), (1,2,3,4,5,6);",
	"insert into products(category, title, actor, price, special, common_prod_id) values (1,2,3,4,5,6);",
	conn)

	# select into
	verify_statement_equivalency(
	"select * into orders_recent FROM orders WHERE orderdate >= '2002-01-01';",
	"select * into orders_recent FROM orders WHERE orderdate >= '2010-01-01';",
	cleanup_sql = "drop table if exists orders_recent;", conn = conn)

	verify_statement_differs(
	"select * into orders_recent FROM orders WHERE orderdate >= '2002-01-01';",
	"select * into orders_recent FROM orders WHERE orderdate >  '2002-01-01';",
	cleanup_sql = "drop table if exists orders_recent;", conn = conn)

	verify_statement_differs(
	"select orderdate into orders_recent FROM orders WHERE orderdate > '2002-01-01';",
	"select orderid   into orders_recent FROM orders WHERE orderdate > '2002-01-01';",
	cleanup_sql = "drop table if exists orders_recent;", conn = conn)

	# Here, name of new relation matters:
	verify_statement_differs(
	"select * into orders_recent  FROM orders WHERE orderdate > '2002-01-01';",
	"select * into orders_recent2 FROM orders WHERE orderdate > '2002-01-01';",
	cleanup_sql = "drop table if exists orders_recent; drop table if exists orders_recent2;", conn = conn)

	# CTE
	verify_statement_differs(
	"with a as (select customerid from orders ), b as (select 'foo') select orderid from orders",
	"with a as (select customerid from orders ), b as (select 1)     select orderid from orders",
	conn)


	# temporary column name within recursive CTEs doesn't differentiate
	verify_statement_equivalency(
	"""
	with recursive j(n) AS (
	values (1)
	union all
	select n + 1 from j where n < 100
	)
	select avg(n) from j;
	""",
	"""
	with recursive k(n) AS (
	values (1)
	union all
	select n + 1 from k where n < 50
	)
	select avg(n) from k;
	""",
	conn)

	# set operation normalization occurs by walking the query tree recursively.
	verify_statement_differs( "select orderid from orders union all select customerid from orders", "select customerid from orders union all select orderid from orders", conn)
	verify_statement_equivalency(
	"select 1, 2, 3 except select orderid, 6, 7 from orders",
	"select 4, 5, 6 except select orderid, 55, 33 from orders",
	conn)

	verify_statement_differs(
	"select orderid, 6       from orders union select 1,2 ",
	"select 55,      orderid from orders union select 4,5 ",
	conn,
	"column order differs (left child)")

	verify_statement_differs(
	"select 1, 2 union select orderid,   6       from orders",
	"select 4, 5 union select 55,        orderid from orderlines",
	conn,
	"column order differs (right child easy)")

	verify_statement_differs(
	"select 1, 2 union select orderid,   6       from orders",
	"select 4, 5 union select 55,        orderid from orders",
	conn,
	"column order differs (right child)")

	# union != union all
	verify_statement_differs(
	"select customerid from orders union all select customerid from orders",
	"select customerid from orders union     select customerid from orders",
	conn, "union != union all")

	verify_statement_equivalency(
					"""
					select 1,2,3
					union all
					select 5,6,7 from orders
					except
					select orderid, 934, 194 from orderlines
					intersect
					select 66,67,68;
					""",
					"""
					select 535,8437,366
					union all
					select 1,1,1 from orders
					except
					select orderid, 73737, 4235 from orderlines
					intersect
					select 5432, 6667,6337;
					""",
					conn)

	verify_statement_differs(
	"select orderid    from orders union all select customerid from orders",
	"select customerid from orders union all select orderid    from orders",
	conn)

	verify_statement_differs(
	"select 1::integer union all select orderid from orders union all select customerid from orders",
	"select customerid from orders union all select orderid from orders", conn)

	# My attempt to isolate the set operation problem.

	# test passes if there is one less select,
	# even if it'si an int, or if there's
	# one more "select 1"
	verify_statement_differs(
				"""
				select 1
				union
				select (current_date - orderdate + 5 + 6 + 7 + 8) from orderlines
				union
				select 1;
				""",
				"""
				select 1
				union
				select orderid from orders
				union
				select 1;
				""",
				conn, "isolate set operations problem")

	# Same as above, but the one table is different
	verify_statement_differs(
					"""
					select 1,2,3
					union all
					select 5,6,7 from orders
					except
					select orderid, 934, 194 from orderlines
					intersect
					select 66,67,68;
					""",
					"""
					select 535,8437,366
					union all
					select 1,1,1 from orders
					except
					select orderid, 73737, 4235 from orders
					intersect
					select 5432, 6667,6337;
					""",
					conn, "one table is different")



	# Same as original, but I've switched a column reference with a constant in
	# the second query
	verify_statement_differs(
					"""
					select 1,2,3
					union all
					select 5,6,7 from orders
					except
					select orderid, 934, 194 from orderlines
					intersect
					select 66,67,68
					""",
					"""
					select 535,8437,366
					union all
					select 1,1,1 from orders
					except
					select 73737, orderid, 4235 from orderlines
					intersect
					select 5432, 6667,6337
					""",
					conn)



	# The left node in the set operation tree matches for before and after; we should still catch that
	verify_statement_differs(
					"""
						select orderid from orders
						union all
						select customerid from orders
						union all
						select customerid from orders
					""",
					"""
						select orderid from orders
						union all
						select orderid from orders
						union all
						select customerid from orders
					""", conn)

	verify_statement_differs(
					"""
						select orderid, customerid from orders
						union
						select orderid, customerid from orders
						order by orderid
					""",
					"""
						select orderid, customerid from orders
						union
						select orderid, customerid from orders
						order by customerid
					""", conn)

	verify_statement_differs(
	"""
	with recursive j(n) AS (
	values (1)
	union all
	select n + 1 from j where n < 100
	)
	select avg(n) from j;
	""",
	"""
	with recursive j(n) AS (
	values (1)
	union all
	select 1 from orders
	)
	select avg(n) from j;
	""",
	conn)

	# Excercise some less frequently used Expr nodes
	verify_statement_equivalency(
					"""
						select
						case orderid
						when 0 then 'zero'
						when 1 then 'one'
						else 'some other number' end
						from orders
					""",
					"""	select
						case orderid
						when 5 then 'five'
						when 6 then 'six'
						else 'some other number' end
						from orders

					""", conn)

	verify_statement_differs(
					"""
						select
						case orderid
						when 0 then 'zero'
						else 'some other number' end
						from orders
					""",
					"""	select
						case orderid
						when 5 then 'five'
						when 6 then 'six'
						else 'some other number' end
						from orders

					""", conn)

	# Counter-intuitively, these two statements are equivalent...
	verify_statement_equivalency(
					"""
						select
						case when orderid = 0
						then 'zero'
						when orderid = 1
						then 'one'
						else 'other number' end
						from orders
					""",
					"""
						select
						case when orderid = 0
						then 'zero'
						when orderid = 1
						then 'one' end
						from orders
					""", conn, "Case when test")

	# ...this is because no else clause is actually equivalent to "else NULL".

	verify_statement_equivalency(
					"""
						select
						case when orderid = 0
						then 'zero'
						when orderid = 1
						then 'one'
						else 'other number' end
						from orders
					""",
					"""
						select
						case when orderid = 5
						then 'five'
						when orderid = 6
						then 'six'
						else 'some other number' end
						from orders
					""", conn, "second case when test")

	verify_statement_differs( "select min(orderid) from orders", "select max(orderid) from orders", conn, "min not max check")

	# The parser uses a dedicated Expr node	to handle greatest()/least()
	verify_statement_differs( "select greatest(1,2,3) from orders", "select least(1,2,3) from orders", conn, "greatest/least differ check")

	# Window functions
	verify_statement_differs(
	"select sum(netamount) over () from orders",
	"select sum(tax) over () from orders",
	conn, "window function column check")

	verify_statement_differs(
	"select sum(netamount) over (partition by customerid) from orders",
	"select sum(netamount) over (partition by orderdate) from orders",
	conn, "window function over column differs check")

	verify_statement_differs(
	"select sum(netamount) over (partition by customerid order by orderid) from orders",
	"select sum(netamount) over (partition by customerid order by tax) from orders",
	conn, "window function over column differs check")

	verify_statement_differs(
		"""
			 SELECT c.oid AS relid,
				n.nspname AS schemaname,
				c.relname
			   FROM pg_class c
			   LEFT JOIN pg_index i ON c.oid = i.indrelid
			   LEFT JOIN pg_class t ON c.reltoastrelid = t.oid
			   LEFT JOIN pg_class x ON t.reltoastidxid = x.oid
			   LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
			  WHERE c.relkind = ANY (ARRAY['r'::"char", 't'::"char"])
			  GROUP BY c.oid, n.nspname, c.relname, t.oid, x.oid;
		"""
		,
		"""
			 SELECT c.oid AS relid,
				n.nspname AS schemaname,
				c.relname
			   FROM pg_class c
			   LEFT JOIN pg_index i ON c.oid = i.indrelid
			   LEFT JOIN pg_class t ON c.reltoastrelid = t.oid
			   LEFT JOIN pg_class x ON t.reltoastidxid = x.oid
			   LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
			  WHERE c.relkind = ANY (ARRAY['r'::"char", 't'::"char", 'v'::"char"])
			  GROUP BY c.oid, n.nspname, c.relname, t.oid, x.oid;
		"""

		,conn, "number of ARRAY elements varies in ANY()  in where clause")

	# problematic query
	verify_statement_equivalency(
	"""
	SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
	  pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, c2.reltablespace
	FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
	  LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))
	WHERE c.oid = i.indrelid AND i.indexrelid = c2.oid
	ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname;
	""",
	"""
	SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
	  pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, c2.reltablespace
	FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
	  LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','d','x'))
	WHERE c.oid = i.indrelid AND i.indexrelid = c2.oid
	ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname;
	""",
	conn)

	verify_statement_differs(
	"""
	SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
	  pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, c2.reltablespace
	FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
	  LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x'))
	WHERE c.oid = i.indrelid AND i.indexrelid = c2.oid
	ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname;
	""",
	"""
	SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
	  pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, c2.reltablespace
	FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i
	  LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','d','x', 'd'))
	WHERE c.oid = i.indrelid AND i.indexrelid = c2.oid
	ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname;
	""",
	conn)

	# Declare cursor is a special sort of utility statement - one with
	# a query tree that looks pretty much like that of a select statement

	# Not really worth serializing the tree like a select statement though -
	# Just treat it as a utility statement
	verify_statement_equivalency(
	"declare test cursor for select * from orders;",
	"declare test cursor for select * from orders;",
	cleanup_sql = "close all;", conn = conn)

	# function-like dedicated ExprNodes

	# I'd speculate that if this was performed with every ExprNode that resembles
	# a function, it wouldn't actually fail, since there tends to be a good reason
	# for having a dedicated though function-like ExprNode rather than just an
	# SQL-callable function, and I'm naturally already serializing those differences
	# as they're rather obviously essential to the query

	# The concern here is a specific case of a more general one - that successive
	# queries with similar ExprNodes could incorrectly be considered equivalent.

	# NB:

	# I consider the question of whether or not it is necessary to escape query-tree
	# serializations (in a way that includes a magic number for the ExprNode, such as
	# its offset in the NodeTag enum) to be an open one.

	# Part of the difficulty is that OIDs cannot be expected to remain unique
	# over time and across pg_* tables
	verify_statement_differs(
	"select coalesce(orderid) from orders;",
	"select sum(orderid) from orders;",
	conn, "Don't confuse functions/function like nodes")

	# We have special handling for subselection Vars whose varnos reference
	# outer range tables - exercise that

	verify_statement_differs(
	"""
	SELECT a.attname,
	  pg_catalog.format_type(a.atttypid, a.atttypmod),
	  (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
	   FROM pg_catalog.pg_attrdef d
	   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
	  a.attnotnull, a.attnum,
	  (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
	   WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,
	  NULL AS indexdef,
	  NULL AS attfdwoptions,
	  a.attstorage,
	  CASE WHEN a.attstattarget=-1 THEN NULL ELSE a.attstattarget END AS attstattarget, pg_catalog.col_description(a.attrelid, a.attnum)
	FROM pg_catalog.pg_attribute a
	WHERE a.attrelid = '16438' AND a.attnum > 0 AND NOT a.attisdropped
	ORDER BY a.attnum;
	""",
	"""
	SELECT a.attname,
	  pg_catalog.format_type(a.atttypid, a.atttypmod),
	  (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
	   FROM pg_catalog.pg_attrdef d
	   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
	  a.attnotnull, a.attnum,
	  (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
	   WHERE c.oid = a.attcollation AND t.oid = a.attnum AND a.attcollation <> t.typcollation) AS attcollation,
	  NULL AS indexdef,
	  NULL AS attfdwoptions,
	  a.attstorage,
	  CASE WHEN a.attstattarget=-1 THEN NULL ELSE a.attstattarget END AS attstattarget, pg_catalog.col_description(a.attrelid, a.attnum)
	FROM pg_catalog.pg_attribute a
	WHERE a.attrelid = '16438' AND a.attnum > 0 AND NOT a.attisdropped
	ORDER BY a.attnum;
	""",
	conn)

	verify_statement_equivalency(
	"""
	SELECT a.attname,
	  pg_catalog.format_type(a.atttypid, a.atttypmod),
	  (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
	   FROM pg_catalog.pg_attrdef d
	   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
	  a.attnotnull, a.attnum,
	  (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
	   WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,
	  NULL AS indexdef,
	  NULL AS attfdwoptions,
	  a.attstorage,
	  CASE WHEN a.attstattarget=-1 THEN NULL ELSE a.attstattarget END AS attstattarget, pg_catalog.col_description(a.attrelid, a.attnum)
	FROM pg_catalog.pg_attribute a
	WHERE a.attrelid = '16438' AND a.attnum > 0 AND NOT a.attisdropped
	ORDER BY a.attnum;
	""",
	"""
	SELECT a.attname,
	  pg_catalog.format_type(a.atttypid, a.atttypmod),
	  (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
	   FROM pg_catalog.pg_attrdef d
	   WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
	  a.attnotnull, a.attnum,
	  (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
	   WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,
	  NULL AS indexdef,
	  NULL AS attfdwoptions,
	  a.attstorage,
	  CASE WHEN a.attstattarget=-1 THEN NULL ELSE a.attstattarget END AS attstattarget, pg_catalog.col_description(a.attrelid, a.attnum)
	FROM pg_catalog.pg_attribute a
	WHERE a.attrelid = '163438' AND a.attnum > 2 AND NOT a.attisdropped
	ORDER BY a.attnum;
	""",

	conn)


	verify_normalizes_correctly("select 1, 2, 3;", "select ?, ?, ?;", conn, "integer verification" )
	verify_normalizes_correctly("select 'foo';", "select ?;", conn, "unknown/string normalization verification" )
	verify_normalizes_correctly("select 'bar'::text;", "select ?::text;", conn, "text verification" )
	verify_normalizes_correctly("select $$bar$$;", "select ?;", conn, "unknown/string normalization verification" )
	verify_normalizes_correctly("select $$bar$$ from pg_database where datname = 'postgres';",
				    "select ? from pg_database where datname = ?;", conn, "Quals comparison" )

	# You can parameterize a limit constant, so our behavior is consistent with that
	verify_normalizes_correctly("select * from orders limit 1 offset 5;", "select * from orders limit ? offset ?;", conn, "integer verification" )
	# Note: Due to :: operator precedence, this behavior is correct:
	verify_normalizes_correctly("select -12345::integer;", "select -?::integer;", conn, "show operator precedence issue" )
	verify_normalizes_correctly("select -12345;", "select ?;", conn, "show no operator precedence issue" )

	verify_normalizes_correctly("select -12345.12345::float;", "select -?::float;", conn, "show operator precedence issue" )
	verify_normalizes_correctly("select -12345.12345;", "select ?;", conn, "show no operator precedence issue" )

	verify_normalizes_correctly("select array_agg(lower(upper(lower(initcap(lower('Baz')))))) from orders;",
				    "select array_agg(lower(upper(lower(initcap(lower(?)))))) from orders;", conn, "Function call")

	# Test this cast syntax works:
	verify_normalizes_correctly("select timestamp without time zone '2009-05-05 15:34:24';",
								"select ?;", conn, "exercise alternative cast syntax, timestamp")
	verify_normalizes_correctly("select timestamptz '2009-05-05 15:34:24.1234';",
								"select ?;", conn, "exercise alternative cast syntax, timestamptz")
	verify_normalizes_correctly("select date '2009-05-05';",
								"select ?;", conn, "exercise alternative cast syntax, date")
	verify_normalizes_correctly("select time '15:15:15';",
								"select ?;", conn, "exercise alternative cast syntax, time")
	verify_normalizes_correctly("select time with time zone '15:15:15';",
								"select ?;", conn, "exercise alternative cast syntax, time with time zone")
	verify_normalizes_correctly("select int4 '5';",
								"select ?;", conn, "exercise alternative cast syntax, int4")
	verify_normalizes_correctly("select integer '5';",
								"select ?;", conn, "exercise alternative cast syntax, integer")
	verify_normalizes_correctly("select numeric '5.5';",
								"select ?;", conn, "exercise alternative cast syntax, numeric")
	verify_normalizes_correctly("select decimal '5.5';",
								"select ?;", conn, "exercise alternative cast syntax, decimal")
	verify_normalizes_correctly("select name 'abc';",
								"select ?;", conn, "exercise alternative cast syntax, name")
	verify_normalizes_correctly("select text 'abc';",
								"select ?;", conn, "exercise alternative cast syntax, text")


	verify_normalizes_correctly("select interval '1 hour';", "select ?;", conn, "exercise alternative cast syntax, interval")
	# Binary/bit strings have special handling within parser
	verify_normalizes_correctly("select B'1001' | B'1111';", "select ? | ?;", conn, "bitstring parser handling")

	verify_normalizes_correctly(
	"insert into products(category, title, actor, price, special, common_prod_id) values (1,'abc','abc',4,5,6);",
	"insert into products(category, title, actor, price, special, common_prod_id) values (?,?,?,?,?,?);",
	conn)

	verify_normalizes_correctly(
	"insert into products(category, title, actor, price, special, common_prod_id) values (1,'abc','abc',4,5,6), (1,'abc','abc',4,5,6);",
	"insert into products(category, title, actor, price, special, common_prod_id) values (?,?,?,?,?,?), (?,?,?,?,?,?);",
	conn)

	# XXX: Sometimes, questionable implicit casts (beyond those deprecated in
	# 8.3) result in there being no Const nodes to get location in query string
	# from.  This case is judged to be too marginal to make additional changes
	# to the parser to fix, but it is demonstrated here for reviewer's
	# reference:
	verify_normalizes_correctly(
	"insert into products(category, title, actor, price, special, common_prod_id) values (1,2,3,4,5,6), (1,2,3,4,5,6);",
	"insert into products(category, title, actor, price, special, common_prod_id) values (?,2,3,?,?,?), (?,2,3,?,?,?);",
	conn)

	# This constant considerably exceeds track_activity_query_size, but it
	# doesn't matter - we still get a correctly canonicalized string representation.
	verify_normalizes_correctly("select " + ("1" * 2048)  + ";", "select ?;",
	conn, "constant exceeds track_activity_query_size")

	# Really try and stress the implementation here, with several ridiculously
	# long constants
	verify_normalizes_correctly("select " + ("1" * 553)  + ", " + ("2" * 532) + ", " + ("3" * 7343) + ", " + ("4" * 33) + ";", "select ?, ?, ?, ?;",
	conn, "constant exceeds track_activity_query_size, multiple constants (integer-like)")

	verify_normalizes_correctly("select '" + ("A" * 553)  + "', $$" + ("B" * 432) + "$$, $$" + ("c" * 343) + "$$, $foo$" + ("D" * 933) + "$foo$, $$"+ ("e"*31) +"$$;", "select ?, ?, ?, ?, ?;",
	conn, "constant exceeds track_activity_query_size, multiple constants (string)")

	demonstrate_buffer_limitation(conn)

if __name__=="__main__":
	main()

