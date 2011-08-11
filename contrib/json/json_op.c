/*-------------------------------------------------------------------------
 *
 * json_op.c
 *	  Manipulation procedures for JSON data type.
 *
 * Copyright (c) 2011, PostgreSQL Global Development Group
 * Written by Joey Adams <joeyadams3.14159@gmail.com>.
 *
 *-------------------------------------------------------------------------
 */

#include "json.h"

#include "catalog/namespace.h"
#include "catalog/pg_enum.h"
#include "funcapi.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#define PG_DETOAST_DATUM_FIRST_CHAR(datum) \
	pg_detoast_datum_first_char((struct varlena *) DatumGetPointer(datum))

typedef struct
{
	int			index;
	const char *label;
}	EnumLabel;

static int		pg_detoast_datum_first_char(struct varlena * datum);
static bool		getEnumLabelOids(const char *typname, Oid typnamespace,
								 EnumLabel labels[], Oid oid_out[], int count);
static int		enum_label_cmp(const void *left, const void *right);

/* Keep the order of these entries in sync with the enum in json.h . */
static EnumLabel enum_labels[JSON_TYPE_COUNT] =
{
	{JSON_NULL, "null"},
	{JSON_STRING, "string"},
	{JSON_NUMBER, "number"},
	{JSON_BOOL, "bool"},
	{JSON_OBJECT, "object"},
	{JSON_ARRAY, "array"}
};

/* json_validate(text).  Renamed to avoid clashing
 * with the C function json_validate. */
PG_FUNCTION_INFO_V1(json_validate_f);
Datum		json_validate_f(PG_FUNCTION_ARGS);
Datum
json_validate_f(PG_FUNCTION_ARGS)
{
	text *txt = PG_GETARG_TEXT_PP(0);

	PG_RETURN_BOOL(json_validate(VARDATA_ANY(txt), VARSIZE_ANY_EXHDR(txt)));
}

/* json_get_type(json).  Renamed to avoid clashing
 * with the C function json_get_type. */
PG_FUNCTION_INFO_V1(json_get_type_f);
Datum		json_get_type_f(PG_FUNCTION_ARGS);
Datum
json_get_type_f(PG_FUNCTION_ARGS)
{
	int			first_char;
	JsonType	type;
	Oid		   *label_oids;

	/* Because JSON is condensed on input, leading spaces are removed,
	 * meaning we can determine the type merely by looking at the
	 * first character. */
	first_char = PG_DETOAST_DATUM_FIRST_CHAR(PG_GETARG_DATUM(0));
	type = json_get_type(first_char);

	if (!json_type_is_valid(type))
		report_corrupt_json();

	label_oids = fcinfo->flinfo->fn_extra;
	if (label_oids == NULL)
	{
		label_oids = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
										JSON_TYPE_COUNT * sizeof(Oid));
		if (!getEnumLabelOids("json_type",
							  get_func_namespace(fcinfo->flinfo->fn_oid),
							  enum_labels, label_oids, JSON_TYPE_COUNT))
		{
			/* This should never happen, but if it does... */
			Oid namespace_oid = get_func_namespace(fcinfo->flinfo->fn_oid);
			const char *namespace = get_namespace_name(namespace_oid);

			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					errmsg("could not read enum %s.json_type",
						   namespace ? namespace : "(missing namespace)")));
		}

		fcinfo->flinfo->fn_extra = label_oids;
	}

	PG_RETURN_OID(label_oids[type]);
}

/*
 * pg_detoast_datum_first_char - Efficiently get the first character of a varlena.
 *
 * Return -1 if the varlena is empty.
 * Otherwise, return the first character casted to an unsigned char.
 */
static int
pg_detoast_datum_first_char(struct varlena * datum)
{
	struct varlena *slice;

	if (VARATT_IS_COMPRESSED(datum) || VARATT_IS_EXTERNAL(datum))
		slice = pg_detoast_datum_slice(datum, 0, 1);
	else
		slice = datum;

	if (VARSIZE_ANY_EXHDR(slice) < 1)
		return -1;

	return (unsigned char) VARDATA_ANY(slice)[0];
}

/*
 * getEnumLabelOids
 *	  Look up the OIDs of enum labels.	Enum label OIDs are needed to
 *	  return values of a custom enum type from a C function.
 *
 *	  Callers should typically cache the OIDs produced by this function
 *	  using fn_extra, as retrieving enum label OIDs is somewhat expensive.
 *
 *	  Every labels[i].index must be between 0 and count, and oid_out
 *	  must be allocated to hold count items.  Note that getEnumLabelOids
 *	  sorts the labels[] array passed to it.
 *
 *	  Any labels not found in the enum will have their corresponding
 *	  oid_out entries set to InvalidOid.
 *
 *    If the entire operation fails (most likely when the type does not exist),
 *    this function will return false.
 *
 *	  Sample usage:
 *
 *	  -- SQL --
 *	  CREATE TYPE colors AS ENUM ('red', 'green', 'blue');
 *
 *	  -- C --
 *	  enum Colors {RED, GREEN, BLUE, COLOR_COUNT};
 *
 *	  static EnumLabel enum_labels[COLOR_COUNT] =
 *	  {
 *		  {RED,   "red"},
 *		  {GREEN, "green"},
 *		  {BLUE,  "blue"}
 *	  };
 *
 *	  Oid *label_oids = palloc(COLOR_COUNT * sizeof(Oid));
 *	  if (!getEnumLabelOids("colors", PG_PUBLIC_NAMESPACE,
 *	                        enum_labels, label_oids, COLOR_COUNT))
 *	      elog(ERROR, "could not read enum colors");
 *
 *	  PG_RETURN_OID(label_oids[GREEN]);
 */
static bool
getEnumLabelOids(const char *typname, Oid typnamespace,
				 EnumLabel labels[], Oid oid_out[], int count)
{
	CatCList   *list;
	Oid			enumtypoid;
	int			total;
	int			i;
	EnumLabel	key;
	EnumLabel  *found;

	if (!OidIsValid(typnamespace))
		return false;
	enumtypoid = GetSysCacheOid2(TYPENAMENSP, PointerGetDatum(typname),
								 ObjectIdGetDatum(typnamespace));
	if (!OidIsValid(enumtypoid))
		return false;

	qsort(labels, count, sizeof(EnumLabel), enum_label_cmp);

	for (i = 0; i < count; i++)
	{
		/* Initialize oid_out items to InvalidOid. */
		oid_out[i] = InvalidOid;

		/* Make sure EnumLabel indices are in range. */
		Assert(labels[i].index >= 0 && labels[i].index < count);
	}

	list = SearchSysCacheList1(ENUMTYPOIDNAME,
							   ObjectIdGetDatum(enumtypoid));
	total = list->n_members;

	for (i = 0; i < total; i++)
	{
		HeapTuple	tup = &list->members[i]->tuple;
		Oid			oid = HeapTupleGetOid(tup);
		Form_pg_enum en = (Form_pg_enum) GETSTRUCT(tup);

		key.label = NameStr(en->enumlabel);
		found = bsearch(&key, labels, count, sizeof(EnumLabel), enum_label_cmp);
		if (found != NULL)
			oid_out[found->index] = oid;
	}

	ReleaseCatCacheList(list);
	return true;
}

static int
enum_label_cmp(const void *left, const void *right)
{
	const char *l = ((EnumLabel *) left)->label;
	const char *r = ((EnumLabel *) right)->label;

	return strcmp(l, r);
}
