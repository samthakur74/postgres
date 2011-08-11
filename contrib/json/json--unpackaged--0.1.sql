ALTER EXTENSION json ADD type json;
ALTER EXTENSION json ADD function json_in(cstring);
ALTER EXTENSION json ADD function json_out(json);
ALTER EXTENSION json ADD type json_type;
ALTER EXTENSION json ADD function json_get_type(json);
ALTER EXTENSION json ADD function json_validate(text);
ALTER EXTENSION json ADD function json_stringify(json);
ALTER EXTENSION json ADD function json_stringify(json, text);
