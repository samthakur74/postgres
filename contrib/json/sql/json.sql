SELECT '[]'::JSON;
SELECT '['::JSON;
SELECT '[1,2,3]'::JSON;
SELECT '[1,2,3]'::JSON::TEXT;
SELECT '[1,2,3  ]'::JSON;
SELECT '[1,2,3  ,4]'::JSON;
SELECT '[1,2,3  ,4.0]'::JSON;
SELECT '[1,2,3  ,4]'::JSON;
SELECT 'true'::JSON;
SELECT 'true'::TEXT::JSON;
SELECT 'false'::JSON;
SELECT 'null'::JSON;
SELECT '1.1'::JSON;
SELECT '"string"'::JSON;
SELECT '{"key1":"value1", "key2":"value2"}'::JSON;
SELECT 15::JSON;

SELECT json_get_type('[]');
SELECT json_get_type('{}');
SELECT json_get_type('true');
SELECT json_get_type('false');
SELECT json_get_type('null');

CREATE TABLE testjson (j JSON);
INSERT INTO testjson VALUES ('[1,2,3,4]');
INSERT INTO testjson VALUES ('{"key":"value"}');
INSERT INTO testjson VALUES ('{"key":"value"');
INSERT INTO testjson VALUES ('');
INSERT INTO testjson VALUES ('""');
INSERT INTO testjson VALUES ('true');
INSERT INTO testjson VALUES ('false');
INSERT INTO testjson VALUES ('null');
INSERT INTO testjson VALUES ('[]');
INSERT INTO testjson VALUES ('{}');

SELECT * FROM testjson;

SELECT json_get_type(j) FROM testjson;
