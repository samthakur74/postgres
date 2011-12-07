#!/usr/bin/env python
# Performance test for pg_stat_statements normalization
# Peter Geoghegan

# This test is intended to isolate the overhead of additional
# infrastructure in the grammar and plan tree, which is used to
# add a field to Const nodes so that the corresponding lexeme's
# length is known from the query tree.

# It is intended to be run without pg_stat_statements, because
# the really pertinent issue is if these changes impose any
# noticeable overhead on Postgres users that don't use
# pg_stat_statements, and the degree thereof.

# For best results, run with CPU frequency scaling disabled.

import psycopg2
import random
import time
import csv
from optparse import OptionParser

# store results of a given run in a dedicated csv file
def serialize_to_file(times, filename):
	wrt = csv.writer(open(filename, 'wb'), delimiter=',')

	# mark median run for runs of this
	# query (or if there is an even number of elements, near enough)
	median_i = (len(times) + 1) / 2 - 1

	for i, v in enumerate(times):
		wrt.writerow([ v[0], time.ctime(v[1]), str(v[2]) + " seconds", '*' if i == median_i else 'n'])

def run_test(conn, num_its, num_const_nodes):
	# A very unsympathetic query here is one with lots of
	# Const nodes that explain shows as a single "Result" node.

	# This is because parsing has a large overhead
	# relative to planning and execution, and there is an unusually
	# high number of Const nodes.

	# Use psuedo-random numbers with a consistent seed value - numbers
	# used are deterministic for absolute consistency, though I don't
	# believe that to be significant, at least for now.
	random.seed(55)

	cur = conn.cursor()

	times = []
	for i in range(0, num_its):
		# Generate new query with psuedo-random integer Const nodes
		qry = "select "
		for i in range(0, num_const_nodes):
			n = random.randint(0, 10000000)
			qry += str(n) + (", " if i != num_const_nodes - 1 else ";")
		begin = time.time()
		cur.execute(qry)
		end = time.time()

		elapsed = end - begin
		times.append((qry, begin, elapsed))


	# Sort values for reference, and to locate the median value
	sort_vals = sorted(times, key=lambda tot_time: tot_time[2])
	serialize_to_file(sort_vals, "test_results.csv")

def main():
	parser = OptionParser(description="")
	parser.add_option('-c', '--conninfo', type=str, help="libpq-style connection info string of database to connect to. "
						"Can be omitted, in which case we get details from our environment. "
						"You'll probably want to put this in double-quotes, like this: --conninfo \"hostaddr=127.0.0.1 port=5432 dbname=postgres user=postgres\". ", default="")
	parser.add_option('-n', '--num_its', type=int, help="Number of iterations (times a query is executed)", default=5000)
	parser.add_option('-s', '--num_const_nodes', type=int, help="Number of Const nodes that each query contains", default=300)

	args = parser.parse_args()[0]
	conn_str = args.conninfo
	num_its = args.num_its
	num_const_nodes = args.num_const_nodes

	conn = psycopg2.connect(conn_str)
	run_test(conn, num_its, num_const_nodes)

if __name__=="__main__":
	main()
