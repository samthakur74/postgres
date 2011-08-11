CREATE EXTENSION json;

\set ECHO none
SET client_min_messages = warning;

\i sql/test_strings.sql
CREATE TABLE valid_test_strings AS
	SELECT string FROM test_strings WHERE json_validate(string);

RESET client_min_messages;
\set ECHO all
