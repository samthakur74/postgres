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

test_no = 0

def print_queries(conn):
	print "Queries that were found in pg_stat_statements: "
	cur = conn.cursor()
	cur.execute("select query from pg_stat_statements;")
	for i in cur:
		print i[0]


def verify_statement_equivalency(sql, equiv, conn, test_name = None):
	# Run both queries in isolation and verify that there
	# is only a single tuple
	global test_no
	cur = conn.cursor()
	cur.execute("select pg_stat_statements_reset();")
	cur.execute(sql)
	cur.execute(equiv)
	cur.execute("select count(*) from pg_stat_statements where query not ilike '%pg_stat_statements%';")
	for i in cur:
		tuple_n = i[0]

	if tuple_n != 1:
		#print_queries(conn)
		raise SystemExit("The SQL statements \n'{0}'\n and \n'{1}'\n do not appear to be equivalent! Test {2} failed.".format(sql, equiv, test_no if test_name is None else "'{0}' ({1})".format( test_name, test_no)) )

	print "The statements \n'{0}'\n and \n'{1}'\n are equivalent, as expected. Test {2} passed.\n\n".format(sql, equiv, test_no if test_name is None else "'{0}' ({1})".format( test_name, test_no))
	test_no +=1

def verify_statement_differs(sql, diff, conn, test_name = None):
	# Run both queries in isolation and verify that there are
	# two tuples
	global test_no
	cur = conn.cursor()
	cur.execute("select pg_stat_statements_reset();")
	cur.execute(sql)
	cur.execute(diff)
	cur.execute("select count(*) from pg_stat_statements where query not ilike '%pg_stat_statements%';")
	for i in cur:
		tuple_n = i[0]

	if tuple_n != 2:
		#print_queries(conn)
		raise SystemExit("The SQL statements \n'{0}'\n and \n'{1}'\n do not appear to be different! Test {2} failed.".format(sql, diff, test_no if test_name is None else "'{0}' ({1})".format( test_name, test_no)))

	print "The statements \n'{0}'\n and \n'{1}'\n are not equivalent, as expected. Test {2} passed.\n\n ".format(sql, diff, test_no if test_name is None else "'{0}' ({1})".format( test_name, test_no))
	test_no +=1

def main():
	conn = psycopg2.connect("")
	# Just let exceptions propagate

	verify_statement_equivalency("select '5'::integer;", "select  '17'::integer;", conn)
	verify_statement_equivalency("select 1;", "select      5   ;", conn)
	verify_statement_equivalency("select 'foo'::text;", "select  'bar'::text;", conn)
	# Date constant normalization
	verify_statement_equivalency("select * from orders where orderdate = '2001-01-01';" ,"select * from orders where orderdate = '1960-01-01'", conn)
	verify_statement_equivalency("select '5'::integer;","select  '17'::integer;", conn)
	# We don't care about whitespace or differences in constants:
	verify_statement_equivalency("select o.orderid from orders o join orderlines ol on o.orderid = ol.orderid     where    customerid        =  12345  ;", "select o.orderid from orders o join orderlines ol on o.orderid = ol.orderid where customerid = 6789;", conn)

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


	# Observe what we can do with noise words (no "outer" in later statement, plus we use AS in the second query):
	verify_statement_equivalency("select * from orders o left outer join orderlines ol on o.orderid = ol.orderid;",
					"select * from orders AS o left join orderlines AS ol on o.orderid = ol.orderid;", conn)

	# A different limit often means a different plan. In the case of limit and offset, constants matter:
	verify_statement_differs("select * from orders limit 1", "select * from orders limit 2", conn)
	verify_statement_differs("select * from orders limit 1 offset 1", "select * from orders limit 1 offset 2", conn)

	# There are lots of ways limits can be noise
	verify_statement_equivalency("select * from orders limit null", "select * from orders limit all", conn)
	verify_statement_equivalency("select * from orders", "select * from orders limit null", conn)

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

	# Use lots of different operators:
	verify_statement_differs(
	"select 1 < 2;",
	"select 1 <= 2;",
				conn)

	verify_statement_differs(
	"select ARRAY[1,2,3]::integer[] && ARRAY[1]::integer[];",
	"select ARRAY[1,2,3]::integer[] <@ ARRAY[1]::integer[];",
				conn)
	# Number of elements in ARRAY[] expression is not a differentiator:
	verify_statement_equivalency(
	"select ARRAY[1,2,3]::integer[] <@ ARRAY[1]::integer[];",
	"select ARRAY[999]::integer[]   <@ ARRAY[342, 543, 634 ,753]::integer[];",
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
	# equivalency recognized, but I highly doubt it's worth bothering, since recognising most kinds of semantic
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
				 "select 			 customerid, o.orderid from orderlines ol join orders o on o.orderid = ol.orderid;", conn)

	# select "all" is generally just a noise word:
	verify_statement_equivalency("select all customerid, o.orderid from orderlines ol join orders o on o.orderid = ol.orderid;",
				     "select                 customerid, o.orderid from orderlines ol join orders o on o.orderid = ol.orderid;", conn)



	# Updates are similarly normalised, and their internal representations shares much with select statements
	verify_statement_equivalency("update orders set orderdate='2011-01-06' where orderid=3;",
				     "update orders set orderdate='1992-01-06' where orderid     =     10;", conn)

	# updates with different columns are not equivalent:
	verify_statement_differs("update orders set orderdate='2011-01-06' where orderid = 3;",
				 "update orders set orderdate='1992-01-06' where customerid     =     10;", conn)



	verify_statement_differs(
	"with a as (select customerid from orders ), b as (select 'foo') select orderid from orders",
	"with a as (select customerid from orders ), b as (select 1)     select orderid from orders",
	conn)


	# Ditto recursive CTEs
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

	# set operation normalization occurs by waling the query tree recursively.
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



if __name__=="__main__":
	main()
