#ifndef JSON2_H
#define JSON2_H

#include "postgres.h"

#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"

typedef struct varlena json_varlena;

#define DatumGetJSONPP(X)	((json_varlena *) PG_DETOAST_DATUM_PACKED(X))
#define JSONPGetDatum(X)	PointerGetDatum(X)

#define PG_GETARG_JSON_PP(n) DatumGetJSONPP(PG_GETARG_DATUM(n))
#define PG_RETURN_JSON_P(x)  PG_RETURN_POINTER(x)

/* Keep the order of these enum entries in sync with
 * enum_type_names[] in json_op.c . */
typedef enum
{
	JSON_NULL,
	JSON_STRING,
	JSON_NUMBER,
	JSON_BOOL,
	JSON_OBJECT,
	JSON_ARRAY,
	JSON_TYPE_COUNT = JSON_ARRAY + 1,

	JSON_INVALID
} JsonType;

#define json_type_is_valid(type) ((type) >= 0 && (type) < JSON_TYPE_COUNT)

bool json_validate(const char *str, size_t length);
bool json_validate_nospace(const char *str, size_t length);
char *json_condense(const char *json_str, size_t length, size_t *out_length);

bool json_need_to_escape_unicode(void);
char *json_escape_unicode(const char *json, size_t length, size_t *out_length);

char *json_stringify(const char *json, size_t length,
					 const char *space, size_t space_length,
					 size_t *out_length);

JsonType json_get_type(int c);

void report_corrupt_json(void);

#endif
