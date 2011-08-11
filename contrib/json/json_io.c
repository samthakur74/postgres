/*-------------------------------------------------------------------------
 *
 * json_io.c
 *	  Primary input/output and conversion procedures
 *	  for JSON data type.
 *
 * Copyright (c) 2011, PostgreSQL Global Development Group
 * Written by Joey Adams <joeyadams3.14159@gmail.com>.
 *
 *-------------------------------------------------------------------------
 */

#include "json.h"

#include "funcapi.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

void
report_corrupt_json(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			 errmsg("corrupted JSON value")));
}

PG_FUNCTION_INFO_V1(json_in);
Datum		json_in(PG_FUNCTION_ARGS);
Datum
json_in(PG_FUNCTION_ARGS)
{
	char *string = PG_GETARG_CSTRING(0);
	size_t length = strlen(string);
	char *condensed;
	size_t condensed_length;

	if (!json_validate(string, length))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for JSON")));

	condensed = json_condense(string, length, &condensed_length);

	PG_RETURN_JSON_P(cstring_to_text_with_len(condensed, condensed_length));
}

PG_FUNCTION_INFO_V1(json_out);
Datum		json_out(PG_FUNCTION_ARGS);
Datum
json_out(PG_FUNCTION_ARGS)
{
	char *string = TextDatumGetCString(PG_GETARG_DATUM(0));

	Assert(json_validate(string, strlen(string)));

	if (json_need_to_escape_unicode())
		string = json_escape_unicode(string, strlen(string), NULL);

	PG_RETURN_CSTRING(string);
}

/* json_stringify(json).  Renamed to avoid clashing with C function. */
PG_FUNCTION_INFO_V1(json_stringify_f);
Datum		json_stringify_f(PG_FUNCTION_ARGS);
Datum
json_stringify_f(PG_FUNCTION_ARGS)
{
	if (json_need_to_escape_unicode())
	{
		json_varlena   *json = PG_GETARG_JSON_PP(0);
		char		   *escaped;
		size_t			escaped_length;

		escaped	= json_escape_unicode(VARDATA_ANY(json),
									  VARSIZE_ANY_EXHDR(json), &escaped_length);

		PG_RETURN_TEXT_P(cstring_to_text_with_len(escaped, escaped_length));
	}
	else
	{
		/* text and json_varlena are binary-compatible */
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}
}

/*
 * json_stringify(json, space) - Format a JSON value into text with indentation.
 */
PG_FUNCTION_INFO_V1(json_stringify_space);
Datum		json_stringify_space(PG_FUNCTION_ARGS);
Datum
json_stringify_space(PG_FUNCTION_ARGS)
{
	json_varlena   *json = PG_GETARG_JSON_PP(0);
	text		   *space = PG_GETARG_TEXT_PP(1);
	char		   *stringified;
	size_t			stringified_length;

	if (json_need_to_escape_unicode())
	{
		char	   *escaped;
		size_t		escaped_length;

		escaped	= json_escape_unicode(VARDATA_ANY(json),
									  VARSIZE_ANY_EXHDR(json), &escaped_length);
		stringified = json_stringify(escaped, escaped_length,
									 VARDATA_ANY(space), VARSIZE_ANY_EXHDR(space),
									 &stringified_length);
		pfree(escaped);
	}
	else
	{
		stringified = json_stringify(VARDATA_ANY(json),  VARSIZE_ANY_EXHDR(json),
									 VARDATA_ANY(space), VARSIZE_ANY_EXHDR(space),
									 &stringified_length);
	}

	if (stringified == NULL)
		report_corrupt_json();

	PG_RETURN_TEXT_P(cstring_to_text_with_len(stringified, stringified_length));
}
