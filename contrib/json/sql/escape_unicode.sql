SET client_encoding TO UTF8;

CREATE TABLE escape_unicode_test (json JSON);
INSERT INTO escape_unicode_test VALUES ($$"\u266b\uD835\uDD4E"$$);

-- Output should not be escaped.
SELECT json FROM escape_unicode_test;
SELECT json::TEXT FROM escape_unicode_test;
SELECT json_stringify(json) FROM escape_unicode_test;
SELECT json_stringify(json, '') FROM escape_unicode_test;

SET client_encoding TO SQL_ASCII;

-- Output should still not be escaped.
SELECT json FROM escape_unicode_test;
SELECT json::TEXT FROM escape_unicode_test;
SELECT json_stringify(json) FROM escape_unicode_test;
SELECT json_stringify(json, '') FROM escape_unicode_test;

SET client_encoding TO LATIN1;

-- Output should be escaped now.
SELECT json FROM escape_unicode_test;
SELECT json::TEXT FROM escape_unicode_test;
SELECT json_stringify(json) FROM escape_unicode_test;
SELECT json_stringify(json, '') FROM escape_unicode_test;
