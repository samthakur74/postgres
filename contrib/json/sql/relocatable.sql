-- This test needs to be run last.

DROP EXTENSION json CASCADE;

CREATE SCHEMA othernamespace;
CREATE EXTENSION json WITH SCHEMA othernamespace;

/*
 * json_get_type uses its own OID to figure out what schema the type
 * json_type is in so it can look up its enum label OIDs.
 */
SELECT othernamespace.json_get_type('[]');
SELECT othernamespace.json_get_type('{}');
SELECT othernamespace.json_get_type('"string"');
SELECT othernamespace.json_get_type('3.14');
SELECT othernamespace.json_get_type('true');
SELECT othernamespace.json_get_type('false');
SELECT othernamespace.json_get_type('null');

CREATE TABLE temp (json othernamespace.JSON);
INSERT INTO temp VALUES ('[]');
INSERT INTO temp VALUES ('{}');
INSERT INTO temp VALUES ('"string"');
INSERT INTO temp VALUES ('3.14');
INSERT INTO temp VALUES ('true');
INSERT INTO temp VALUES ('null');
SELECT othernamespace.json_get_type(json) FROM temp;

ALTER EXTENSION json SET SCHEMA public;

SELECT json_get_type('[]');
SELECT json_get_type('{}');
SELECT json_get_type('"string"');
SELECT json_get_type('3.14');
SELECT json_get_type('true');
SELECT json_get_type('false');
SELECT json_get_type('null');
SELECT json_get_type(json) FROM temp;

DROP TABLE temp;
DROP SCHEMA othernamespace CASCADE;
