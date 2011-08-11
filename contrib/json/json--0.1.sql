CREATE TYPE json;

CREATE OR REPLACE FUNCTION json_in(cstring)
RETURNS json
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION json_out(json)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE TYPE json (
	INPUT = json_in,
	OUTPUT = json_out,
	INTERNALLENGTH = VARIABLE,
	STORAGE = extended,

	-- make it a non-preferred member of string type category, as citext does
	CATEGORY       = 'S',
	PREFERRED      = false
);

-- Keep the labels of this enum in sync with enum_type_names[] in json_ops.c .
CREATE TYPE json_type AS ENUM ('null', 'string', 'number', 'bool', 'object', 'array');

CREATE OR REPLACE FUNCTION json_get_type(json)
RETURNS json_type
AS 'MODULE_PATHNAME','json_get_type_f'
LANGUAGE C STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION json_validate(text)
RETURNS boolean
AS 'MODULE_PATHNAME','json_validate_f'
LANGUAGE C STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION json_stringify(json)
RETURNS text
AS 'MODULE_PATHNAME','json_stringify_f'
LANGUAGE C STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION json_stringify(json, text)
RETURNS text
AS 'MODULE_PATHNAME','json_stringify_space'
LANGUAGE C STRICT IMMUTABLE;
