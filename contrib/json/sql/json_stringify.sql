-- Use unaligned output so results are consistent between PostgreSQL 8 and 9.
\a

SELECT json_stringify('false', '  ');
SELECT json_stringify('true', '  ');
SELECT json_stringify('null', '  ');
SELECT json_stringify('""', '  ');
SELECT json_stringify('[]', '  ');
SELECT json_stringify('[1]', '  ');
SELECT json_stringify('[1,2]', '  ');
SELECT json_stringify('{}', '  ');
SELECT json_stringify('{"k":"v"}', '  ');
SELECT json_stringify('{"null":null, "boolean":true,	"boolean" :false,"array":[1,2,3], "empty array":[], "empty object":{}}', '  ');

SELECT json_stringify(json(string)) FROM test_strings WHERE json_validate(string);

-- Turn aligned output back on.
\a
