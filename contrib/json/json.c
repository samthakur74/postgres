/*-------------------------------------------------------------------------
 *
 * json.c
 *	  Core JSON manipulation routines used by JSON data type support.
 *
 * Copyright (c) 2011, PostgreSQL Global Development Group
 * Written by Joey Adams <joeyadams3.14159@gmail.com>.
 *
 *-------------------------------------------------------------------------
 */

#include "json.h"

/*
 * We can't use <ctype.h> functions like isspace and isdigit because
 * they don't quite line up with the JSON spec [RFC4627]:
 *
 *  * isspace accepts \f and \v in the "C" and "POSIX" locales,
 *    but these aren't valid whitespace characters in JSON.
 *  * isspace may accept even more characters in other locales.
 *  * isdigit accepts superscript digits on some operating systems;
 *    see http://stackoverflow.com/questions/2898228/
 *
 * If we were to use the ctype functions, we would be accepting
 * a superset of JSON.  Although it wouldn't violate the spec per se,
 * it could lead to JSON being stored in the database that can't be
 * read elsewhere.
 */
#define is_space(c) ((c) == '\t' || (c) == '\n' || (c) == '\r' || (c) == ' ')
#define is_digit(c) ((c) >= '0' && (c) <= '9')
#define is_hex_digit(c) (((c) >= '0' && (c) <= '9') || \
                         ((c) >= 'A' && (c) <= 'F') || \
                         ((c) >= 'a' && (c) <= 'f'))

static unsigned int		read_hex16(const char *in);
static void				write_hex16(char *out, unsigned int val);
static pg_wchar			from_surrogate_pair(unsigned int uc, unsigned int lc);
static void				to_surrogate_pair(pg_wchar unicode, unsigned int *uc, unsigned int *lc);
static void				jsonAppendStringInfoUtf8(StringInfo str, pg_wchar unicode);
static void				jsonAppendStringInfoEscape(StringInfo str, unsigned int c);

static const char	   *stringify_value(StringInfo buf, const char *s, const char *e,
										const char *space, size_t space_length,
										int indent);
static void				append_indent(StringInfo buf, const char *space, size_t space_length,
									  int indent);

/*
 * Parsing functions and macros
 *
 * The functions and macros that follow are used to simplify the implementation
 * of the recursive descent parser used for JSON validation.  See the
 * implementation of expect_object() for a good example of them in action.
 *
 * These functions/macros use a few unifying concepts:
 *
 * * const char *s and const char *e form a "slice" within a string,
 *   where s points to the first character and e points after the last
 *   character.  Hence, the length of a slice is e - s.  Although it would be
 *   simpler to just get rid of const char *e and rely on strings being
 *   null-terminated, varlena texts are not guaranteed to be null-terminated,
 *   meaning we would have to copy them to parse them.
 *
 * * Every expect_* function sees if the beginning of a slice matches what it
 *   expects, and returns the end of the slice on success.  To illustrate:
 *
 *          s         expect_number(s, e)                  e
 *          |         |                                    |
 *   {"pi": 3.14159265, "e": 2.71828183, "phi": 1.61803399}
 *
 * * When a parse error occurs, s is set to NULL.  Moreover, the parser
 *   functions and macros check to see if s is NULL before using it.
 *   This means parser functions built entirely of parser functions can proceed
 *   with the illusion that the input will always be valid, rather than having
 *   to do a NULL check on every line (see expect_number, which has no explicit
 *   checks).  However, one must ensure that the parser will always halt,
 *   even in the NULL case.
 *
 * Bear in mind that while pop_*, optional_*, and skip_* update s,
 * the expect_* functions do not.  Example:
 *
 *     s = expect_char(s, e, '{');
 *     s = expect_space(s, e);
 *
 *     if (optional_char(s, e, '}'))
 *         return s;
 *
 * Also, note that functions traversing an already-validated JSON text
 * can take advantage of the assumption that the input is valid.
 * For example, stringify_value does not perform NULL checks, nor does it
 * check if s < e before dereferencing s.
 */

static const char *expect_value(const char *s, const char *e);
static const char *expect_object(const char *s, const char *e);
static const char *expect_array(const char *s, const char *e);
static const char *expect_string(const char *s, const char *e);
static const char *expect_number(const char *s, const char *e);

static const char *expect_literal(const char *s, const char *e, const char *literal);
static const char *expect_space(const char *s, const char *e);

/*
 * All of these macros evaluate s multiple times.
 *
 * Macros ending in _pred take a function or macro of the form:
 *
 *    bool pred(char c);
 *
 * Macros ending in _cond take an expression, where *s is the character in question.
 */

/*
 * expect_char:      Expect the next character to be @c, and consume it.
 * expect_char_pred: Expect pred(next character) to hold, and consume it.
 * expect_char_cond: Expect a character to be available and cond to hold, and consume.
 * expect_eof:       Expect there to be no more input left.
 *
 * These macros, like any expect_ macros/functions, return a new pointer
 * rather than updating @s.
 */
#define expect_char(s, e, c) expect_char_cond(s, e, *(s) == (c))
#define expect_char_pred(s, e, pred) expect_char_cond(s, e, pred(*(s)))
#define expect_char_cond(s, e, cond) \
	((s) != NULL && (s) < (e) && (cond) ? (s) + 1 : NULL)
#define expect_eof(s, e) ((s) != NULL && (s) == (e) ? (s) : NULL)

/*
 * next_char:      Get the next character, but do not consume it.
 * next_char_pred: Apply pred to the next character.
 * next_char_cond: Evaluate cond if a character is available.
 *
 * On EOF or error, next_char returns EOF, and
 * next_char_pred and next_char_cond return false.
 */
#define next_char(s, e) \
	((s) != NULL && (s) < (e) ? (int)(unsigned char) *(s) : (int) EOF)
#define next_char_pred(s, e, pred) next_char_cond(s, e, pred(*(s)))
#define next_char_cond(s, e, cond) ((s) != NULL && (s) < (e) ? (cond) : false)

/*
 * pop_char:      Consume the next character, and return it.
 * pop_char_pred: Consume the next character, and apply pred to it.
 * pop_char_cond is impossible to implement portably.
 *
 * On EOF or error, these macros do nothing,
 * pop_char returns EOF, and pop_char_cond returns false.
 */
#define pop_char(s, e) ((s) != NULL && (s) < (e) ? (int)(unsigned char) *(s)++ : (int) EOF)
#define pop_char_pred(s, e) \
	((s) != NULL && (s) < (e) ? (s)++, pred((s)[-1]) : false)

/*
 * optional_char:      If the next character is @c, consume it.
 * optional_char_pred: If pred(next character) holds, consume it.
 * optional_char_cond: If a character is available, and cond holds, consume.
 *
 * These macros, when they consume, update @s and return true.
 * Otherwise, they do nothing and return false.
 */
#define optional_char(s, e, c) optional_char_cond(s, e, *(s) == (c))
#define optional_char_pred(s, e, pred) optional_char_cond(s, e, pred(*(s)))
#define optional_char_cond(s, e, cond) \
	((s) != NULL && (s) < (e) && (cond) ? (s)++, true : false)

/*
 * skip_pred:  Skip zero or more characters matching pred.
 * skip1_pred: Skip one or more characters matching pred.
 * skip_cond:  Skip zero or more characters where cond holds.
 * skip1_cond: Skip one or more characters where cond holds.
 */
#define skip_pred(s, e, pred) skip_cond(s, e, pred(*(s)))
#define skip1_pred(s, e, pred) skip1_cond(s, e, pred(*(s)))
#define skip_cond(s, e, cond) do { \
		while (next_char_cond(s, e, cond)) \
			(s)++; \
	} while (0)
#define skip1_cond(s, e, cond) do { \
		if (next_char_cond(s, e, cond)) \
		{ \
			(s)++; \
			while (next_char_cond(s, e, cond)) \
				(s)++; \
		} \
		else \
		{ \
			(s) = NULL; \
		} \
	} while (0)

/*
 * json_validate - Test if text is valid JSON.
 *
 * Note: scalar values (strings, numbers, booleans, and nulls)
 *       are considered valid by this function, and by the JSON datatype.
 */
bool
json_validate(const char *str, size_t length)
{
	const char *s = str;
	const char *e = str + length;

	s = expect_space(s, e);
	s = expect_value(s, e);
	s = expect_space(s, e);
	s = expect_eof(s, e);

	return s != NULL;
}

/*
 * json_validate_nospace - Test if text is valid JSON and has no whitespace around tokens.
 *
 * JSON data is condensed on input, meaning spaces around tokens are removed.
 * The fact that these spaces are gone is exploited in functions that
 * traverse and manipulate JSON.
 */
bool json_validate_nospace(const char *str, size_t length)
{
	const char *s = str;
	const char *e = str + length;

	if (!json_validate(str, length))
		return false;

	while (s < e)
	{
		if (*s == '"')
		{
			s = expect_string(s, e);
			if (s == NULL) /* should never happen */
				return false;
		}
		else if (is_space(*s))
		{
			return false;
		}
		s++;
	}

	return true;
}

/*
 * json_condense - Make JSON content shorter by removing spaces
 *                 and unescaping characters.
 */
char *
json_condense(const char *json_str, size_t length, size_t *out_length)
{
	const char *s = json_str;
	const char *e = s + length;
	StringInfoData buf;
	bool inside_string = false;
	bool server_encoding_is_utf8 = GetDatabaseEncoding() == PG_UTF8;

	Assert(json_validate(json_str, length));

	initStringInfo(&buf);

	while (s < e)
	{
		/*
		 * To make sense of this structured mess, think of it as a flow chart
		 * that branches based on the characters that follow.
		 *
		 * When the algorithm wants to unescape a character,
		 * it will append the unescaped character, advance s,
		 * then continue.  Otherwise, it will perform the default
		 * behavior and emit the character as is.
		 */
		if (inside_string)
		{
			/* When we are inside a string literal, convert escapes
			 * to the characters they represent when possible. */
			if (*s == '\\' && e - s >= 2)
			{
				/* Change \/ to / */
				if (s[1] == '/')
				{
					appendStringInfoChar(&buf, '/');
					s += 2;
					continue;
				}

				/* Emit single-character escape as is now
				 * to avoid getting mixed up by \\ and \" */
				if (s[1] != 'u')
				{
					appendStringInfoChar(&buf, s[0]);
					appendStringInfoChar(&buf, s[1]);
					s += 2;
					continue;
				}

				/* Unescape \uXXXX if it is possible and feasible. */
				if (e - s >= 6 && s[1] == 'u')
				{
					unsigned int uc = read_hex16(s + 2);
					char c;

					/*
					 * Convert this to a single-character escape if possible.
					 * Also, keep " and \ from being unescaped by the
					 * next step, which would corrupt the data.
					 */
					switch (uc)
					{
						case '"':
							c = '"';
							break;
						case '\\':
							c = '\\';
							break;
						case '\b':
							c = 'b';
							break;
						case '\f':
							c = 'f';
							break;
						case '\n':
							c = 'n';
							break;
						case '\r':
							c = 'r';
							break;
						case '\t':
							c = 't';
							break;
						default:
							c = 0;
					}

					if (c != 0)
					{
						s += 6;
						appendStringInfoChar(&buf, '\\');
						appendStringInfoChar(&buf, c);
						continue;
					}

					/*
					 * If a \uXXXX escape stands for a non-control ASCII
					 * character, unescape it.
					 *
					 * Although U+007F..U+009F are also control characters,
					 * the JSON RFC doesn't mention them, and only considers
					 * U+0000..U+001F to be control characters.
					 */
					if (uc >= 0x20 && uc <= 0x7F)
					{
						s += 6;
						appendStringInfoChar(&buf, uc);
						continue;
					}

					/* Unescape Unicode characters only if
					 * the server encoding is UTF-8. */
					if (uc > 0x7F && server_encoding_is_utf8)
					{
						if (uc >= 0xD800 && uc <= 0xDFFF)
						{
							/* Unescape a UTF-16 surrogate pair,
							 * but only if it's present and valid. */
							if (e - s >= 12 && s[6] == '\\' && s[7] == 'u')
							{
								unsigned int lc = read_hex16(s + 8);

								if (uc >= 0xD800 && uc <= 0xDBFF &&
								    lc >= 0xDC00 && lc <= 0xDFFF)
								{
									s += 12;
									jsonAppendStringInfoUtf8(&buf, from_surrogate_pair(uc, lc));
									continue;
								}
							}
						}
						else
						{
							s += 6;
							jsonAppendStringInfoUtf8(&buf, uc);
							continue;
						}
					}
				}
			}
		}
		else
		{
			/* When we are not in a string literal, remove spaces. */
			if (is_space(*s))
			{
				do s++; while (s < e && is_space(*s));
				continue;
			}
		}

		/* If we get here, it means we want to emit this character as is. */
		appendStringInfoChar(&buf, *s);
		if (*s++ == '"')
			inside_string = !inside_string;
	}

	if (out_length != NULL)
		*out_length = buf.len;
	return buf.data;
}

/*
 * json_need_to_escape_unicode
 *    Determine whether we need to convert non-ASCII characters
 *    to \uXXXX escapes to prevent transcoding errors.
 *
 * If any of the following hold, no escaping needs to be done:
 *
 *  * The client encoding is UTF-8.  Escaping is not necessary because
 *    the client can encode all Unicode codepoints.
 *
 *  * The client encoding and the server encoding are the same.
 *    Escaping is not necessary because the client can encode all
 *    codepoints the server can encode.
 *
 *  * The server encoding is SQL_ASCII.  This encoding tells PostgreSQL
 *    to shirk transcoding in favor of speed.  It wasn't unescaped on input,
 *    so don't worry about escaping on output.
 *
 *  * The client encoding is SQL_ASCII.  This encoding tells PostgreSQL
 *    to not perform encoding conversion.
 *
 * Otherwise, (no matter how expensive it is) all non-ASCII characters are escaped.
 */
bool json_need_to_escape_unicode(void)
{
	int server_encoding = GetDatabaseEncoding();
	int client_encoding = pg_get_client_encoding();

	if (client_encoding == PG_UTF8 || client_encoding == server_encoding ||
		server_encoding == PG_SQL_ASCII || client_encoding == PG_SQL_ASCII)
		return false;

	return true;
}

/*
 * json_escape_unicode - Convert non-ASCII characters to \uXXXX escapes.
 */
char *
json_escape_unicode(const char *json, size_t length, size_t *out_length)
{
	const char *s;
	const char *e;
	StringInfoData buf;

	/* Convert to UTF-8 (if necessary). */
	{
		const char *orig = json;
		
		/* This is a no-op when the database encoding is already UTF-8. */
		json = (const char *)
			pg_do_encoding_conversion((unsigned char *) json, length,
									  GetDatabaseEncoding(), PG_UTF8);
		if (json != orig)
			length = strlen(json);
	}

	Assert(json_validate(json, length));
	s = json;
	e = json + length;
	initStringInfo(&buf);

	while (s < e)
	{
		if ((unsigned char) *s > 0x7F)
		{
			int len;
			pg_wchar u;

			len = pg_utf_mblen((const unsigned char *) s);
			if (len > e - s)
			{
				/* Clipped UTF-8 (shouldn't happen, as input should be valid) */
				Assert(false);
				appendStringInfoChar(&buf, *s);
				s++;
				continue;
			}

			u = utf8_to_unicode((const unsigned char *) s);
			s += len;

			if (u <= 0xFFFF)
			{
				jsonAppendStringInfoEscape(&buf, u);
			}
			else
			{
				unsigned int uc, lc;
				to_surrogate_pair(u, &uc, &lc);
				jsonAppendStringInfoEscape(&buf, uc);
				jsonAppendStringInfoEscape(&buf, lc);
			}
		}
		else
		{
			appendStringInfoChar(&buf, *s);
			s++;
		}
	}

	if (out_length != NULL)
		*out_length = buf.len;
	return buf.data;
}

/*
 * json_stringify - Format JSON into text with indentation.
 *
 * Input must be valid, condensed JSON.
 */
char *
json_stringify(const char *json, size_t length,
			   const char *space, size_t space_length,
			   size_t *out_length)
{
	const char *s = json;
	const char *e = json + length;
	StringInfoData buf;

	if (!json_validate_nospace(json, length))
		report_corrupt_json();

	initStringInfo(&buf);
	s = stringify_value(&buf, s, e, space, space_length, 0);
	Assert(s == e);

	if (out_length != NULL)
		*out_length = buf.len;
	return buf.data;
}

static const char *
stringify_value(StringInfo buf, const char *s, const char *e,
				const char *space, size_t space_length, int indent)
{
	const char *s2;

	Assert(s < e);

	switch (*s)
	{
		case '[':
			appendStringInfoString(buf, "[\n");
			s++;
			if (*s != ']')
			{
				for (;;)
				{
					append_indent(buf, space, space_length, indent + 1);
					s = stringify_value(buf, s, e, space, space_length, indent + 1);
					Assert(s < e && (*s == ',' || *s == ']'));
					if (*s == ']')
						break;
					appendStringInfoString(buf, ",\n");
					s++;
				}
				appendStringInfoChar(buf, '\n');
			}
			append_indent(buf, space, space_length, indent);
			appendStringInfoChar(buf, ']');
			return s + 1;

		case '{':
			appendStringInfoString(buf, "{\n");
			s++;
			if (*s != '}')
			{
				for (;;)
				{
					append_indent(buf, space, space_length, indent + 1);
					s2 = expect_string(s, e);
					appendBinaryStringInfo(buf, s, s2 - s);
					s = s2;
					Assert(s < e && *s == ':');
					appendStringInfoString(buf, ": ");
					s++;

					s = stringify_value(buf, s, e, space, space_length, indent + 1);
					Assert(s < e && (*s == ',' || *s == '}'));
					if (*s == '}')
						break;
					appendStringInfoString(buf, ",\n");
					s++;
				}
				appendStringInfoChar(buf, '\n');
			}
			append_indent(buf, space, space_length, indent);
			appendStringInfoChar(buf, '}');
			return s + 1;

		default:
			s2 = expect_value(s, e);
			appendBinaryStringInfo(buf, s, s2 - s);
			return s2;
	}
}

static void
append_indent(StringInfo buf, const char *space, size_t space_length, int indent)
{
	int i;

	for (i = 0; i < indent; i++)
		appendBinaryStringInfo(buf, space, space_length);
}

/*
 * json_get_type - Determine the type of JSON content
 *                 given the first non-space character.
 *
 * Return JSON_INVALID if the first character is not recognized.
 */
JsonType
json_get_type(int c)
{
	switch (c)
	{
		case 'n':
			return JSON_NULL;
		case '"':
			return JSON_STRING;
		case '-':
			return JSON_NUMBER;
		case 'f':
		case 't':
			return JSON_BOOL;
		case '{':
			return JSON_OBJECT;
		case '[':
			return JSON_ARRAY;
		default:
			if (is_digit(c))
				return JSON_NUMBER;

			return JSON_INVALID;
	}
}

/*
 * Reads exactly 4 hex characters (capital or lowercase).
 * Expects in[0..3] to be in bounds, and expects them to be hexadecimal characters.
 */
static unsigned int
read_hex16(const char *in)
{
	unsigned int i;
	unsigned int tmp;
	unsigned int ret = 0;

	for (i = 0; i < 4; i++)
	{
		char c = *in++;

		Assert(is_hex_digit(c));

		if (c >= '0' && c <= '9')
			tmp = c - '0';
		else if (c >= 'A' && c <= 'F')
			tmp = c - 'A' + 10;
		else /* c >= 'a' && c <= 'f' */
			tmp = c - 'a' + 10;

		ret <<= 4;
		ret += tmp;
	}

	return ret;
}

/*
 * Encodes a 16-bit number in hexadecimal, writing exactly 4 hex characters.
 */
static void
write_hex16(char *out, unsigned int val)
{
	const char *hex = "0123456789ABCDEF";

	*out++ = hex[(val >> 12) & 0xF];
	*out++ = hex[(val >> 8) & 0xF];
	*out++ = hex[(val >> 4) & 0xF];
	*out++ = hex[val & 0xF];
}

/* Compute the Unicode codepoint of a UTF-16 surrogate pair. */
static pg_wchar
from_surrogate_pair(unsigned int uc, unsigned int lc)
{
	Assert(uc >= 0xD800 && uc <= 0xDBFF && lc >= 0xDC00 && lc <= 0xDFFF);
	return 0x10000 + ((((pg_wchar)uc & 0x3FF) << 10) | (lc & 0x3FF));
}

/* Construct a UTF-16 surrogate pair given a Unicode codepoint. */
static void
to_surrogate_pair(pg_wchar unicode, unsigned int *uc, unsigned int *lc)
{
	pg_wchar n = unicode - 0x10000;
	*uc = ((n >> 10) & 0x3FF) | 0xD800;
	*lc = (n & 0x3FF) | 0xDC00;
}

/* Append a Unicode character by converting it to UTF-8. */
static void
jsonAppendStringInfoUtf8(StringInfo str, pg_wchar unicode)
{
	if (str->len + 4 >= str->maxlen)
		enlargeStringInfo(str, 4);

	unicode_to_utf8(unicode, (unsigned char *) &str->data[str->len]);
	str->len += pg_utf_mblen((const unsigned char *) &str->data[str->len]);
	str->data[str->len] = '\0';
}

/* Write a \uXXXX escape. */
static void
jsonAppendStringInfoEscape(StringInfo str, unsigned int c)
{
	Assert(c <= 0xFFFF);
	
	if (str->len + 6 >= str->maxlen)
		enlargeStringInfo(str, 6);

	str->data[str->len++] = '\\';
	str->data[str->len++] = 'u';
	write_hex16(str->data + str->len, c);
	str->len += 4;
	str->data[str->len] = '\0';
}

static const char *
expect_value(const char *s, const char *e)
{
	int c = next_char(s, e);

	switch (c)
	{
		case '{':
			return expect_object(s, e);
		case '[':
			return expect_array(s, e);
		case '"':
			return expect_string(s, e);
		case '-':
			return expect_number(s, e);
		case 'n':
			return expect_literal(s, e, "null");
		case 'f':
			return expect_literal(s, e, "false");
		case 't':
			return expect_literal(s, e, "true");
		default:
			if (is_digit(c))
				return expect_number(s, e);
			return NULL;
	}
}

static const char *
expect_object(const char *s, const char *e)
{
	s = expect_char(s, e, '{');
	s = expect_space(s, e);

	if (optional_char(s, e, '}'))
		return s;

	while (s != NULL)
	{
		s = expect_string(s, e);
		s = expect_space(s, e);
		s = expect_char(s, e, ':');
		s = expect_space(s, e);
		s = expect_value(s, e);
		s = expect_space(s, e);

		if (optional_char(s, e, '}'))
			return s;

		s = expect_char(s, e, ',');
		s = expect_space(s, e);
	}

	return NULL;
}

static const char *
expect_array(const char *s, const char *e)
{
	s = expect_char(s, e, '[');
	s = expect_space(s, e);

	if (optional_char(s, e, ']'))
		return s;

	while (s != NULL)
	{
		s = expect_value(s, e);
		s = expect_space(s, e);

		if (optional_char(s, e, ']'))
			return s;

		s = expect_char(s, e, ',');
		s = expect_space(s, e);
	}

	return NULL;
}

static const char *
expect_string(const char *s, const char *e)
{
	s = expect_char(s, e, '"');

	for (;;)
	{
		int c = pop_char(s, e);

		if (c <= 0x1F) /* Control character, EOF, or error */
			return NULL;

		if (c == '"')
			return s;

		if (c == '\\')
		{
			switch (pop_char(s, e))
			{
				case '"':
				case '\\':
				case '/':
				case 'b':
				case 'f':
				case 'n':
				case 'r':
				case 't':
					break;

				case 'u':
					{
						int i;

						for (i = 0; i < 4; i++)
						{
							c = pop_char(s, e);
							if (!is_hex_digit(c))
								return NULL;
						}
					}
					break;

				default:
					return NULL;
			}
		}
	}
}

static const char *
expect_number(const char *s, const char *e)
{
	optional_char(s, e, '-');

	if (!optional_char(s, e, '0'))
		skip1_pred(s, e, is_digit);

	if (optional_char(s, e, '.'))
		skip1_pred(s, e, is_digit);

	if (optional_char_cond(s, e, *s == 'E' || *s == 'e'))
	{
		optional_char_cond(s, e, *s == '+' || *s == '-');
		skip1_pred(s, e, is_digit);
	}

	return s;
}

static const char *
expect_literal(const char *s, const char *e, const char *literal)
{
	if (s == NULL)
		return NULL;

	while (*literal != '\0')
		if (s >= e || *s++ != *literal++)
			return NULL;

	return s;
}

/* Accepts *zero* or more spaces. */
static const char *
expect_space(const char *s, const char *e)
{
	if (s == NULL)
		return NULL;

	for (; s < e && is_space(*s); s++)
		{}

	return s;
}
