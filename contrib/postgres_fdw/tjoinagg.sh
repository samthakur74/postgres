#!/bin/sh
../../src/test/regress/pg_regress --inputdir=. --psqldir='/tmp/pg/bin'   --dbname=contrib_regression join_agg
