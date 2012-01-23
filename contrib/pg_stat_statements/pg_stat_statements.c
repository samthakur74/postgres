/*-------------------------------------------------------------------------
 *
 * pg_stat_statements.c
 *		Track statement execution times across a whole database cluster.
 *
 * Note about locking issues: to create or delete an entry in the shared
 * hashtable, one must hold pgss->lock exclusively.  Modifying any field
 * in an entry except the counters requires the same.  To look up an entry,
 * one must hold the lock shared.  To read or update the counters within
 * an entry, one must hold the lock shared or exclusive (so the entry doesn't
 * disappear!) and also take the entry's mutex spinlock.
 *
 * Statements go through a normalization process before being stored.
 * Normalization is ignoring components of the query that don't normally
 * differentiate it for the purposes of isolating poorly performing queries.
 * For example, the statements 'SELECT * FROM t WHERE f=1' and
 * 'SELECT * FROM t WHERE f=2' would both be considered equivalent after
 * normalization.  This is implemented by generating a series of integers
 * from the query tree after the re-write stage, into a "query jumble". A
 * query jumble is distinct from a straight serialization of the query tree in
 * that Constants are canonicalized, and various extraneous information is
 * ignored, such as the collation of Vars.
 *
 * Copyright (c) 2008-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_stat_statements/pg_stat_statements.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/hash.h"
#include "executor/instrument.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/memutils.h"


PG_MODULE_MAGIC;


/* Location of stats file */
#define PGSS_DUMP_FILE	"global/pg_stat_statements.stat"

/* This constant defines the magic number in the stats file header */
static const uint32 PGSS_FILE_HEADER = 0x20100108;

/* XXX: Should USAGE_EXEC reflect execution time and/or buffer usage? */
#define USAGE_EXEC(duration)	(1.0)
#define USAGE_INIT				(1.0)	/* including initial planning */
#define USAGE_DECREASE_FACTOR	(0.99)	/* decreased every entry_dealloc */
#define USAGE_DEALLOC_PERCENT	5		/* free this % of entries at once */
#define JUMBLE_SIZE				1024    /* query serialization buffer size */
/* Magic values */
#define HASH_BUF				(0xFA)	/* buffer is a hash of query tree */
#define STR_BUF					(0xEB)	/* buffer is query string itself */

/*
 * Hashtable key that defines the identity of a hashtable entry.  The
 * hash comparators do not assume that the query string is null-terminated;
 * this lets us search for an mbcliplen'd string without copying it first.
 *
 * Presently, the query encoding is fully determined by the source database
 * and so we don't really need it to be in the key.  But that might not always
 * be true. Anyway it's notationally convenient to pass it as part of the key.
 */
typedef struct pgssHashKey
{
	Oid			userid;			/* user OID */
	Oid			dbid;			/* database OID */
	int			encoding;		/* query encoding */
	int64		query_id;		/* query identifier */
} pgssHashKey;

/*
 * The actual stats counters kept within pgssEntry.
 */
typedef struct Counters
{
	int64		calls;			/* # of times executed */
	double		total_time;		/* total execution time in seconds */
	int64		rows;			/* total # of retrieved or affected rows */
	int64		shared_blks_hit;	/* # of shared buffer hits */
	int64		shared_blks_read;		/* # of shared disk blocks read */
	int64		shared_blks_written;	/* # of shared disk blocks written */
	int64		local_blks_hit; /* # of local buffer hits */
	int64		local_blks_read;	/* # of local disk blocks read */
	int64		local_blks_written;		/* # of local disk blocks written */
	int64		temp_blks_read; /* # of temp blocks read */
	int64		temp_blks_written;		/* # of temp blocks written */
	double		usage;			/* usage factor */
} Counters;

/*
 * Statistics per statement
 *
 * NB: see the file read/write code before changing field order here.
 */
typedef struct pgssEntry
{
	pgssHashKey key;			/* hash key of entry - MUST BE FIRST */
	Counters	counters;		/* the statistics for this query */
	int			query_len;		/* # of valid bytes in query string */
	slock_t		mutex;			/* protects the counters only */
	char		query[1];		/* VARIABLE LENGTH ARRAY - MUST BE LAST */
	/* Note: the allocated length of query[] is actually pgss->query_size */
} pgssEntry;

/*
 * Global shared state
 */
typedef struct pgssSharedState
{
	LWLockId	lock;			/* protects hashtable search/modification */
	int			query_size;		/* max query length in bytes */
} pgssSharedState;

typedef struct pgssTokenOffset
{
	int offset;					/* token offset in query string in bytes */
	int len;					/* length of token in bytes */
} pgssTokenOffset;

typedef struct pgssQueryConEntry
{
	pgssHashKey		key;			/* hash key of entry - MUST BE FIRST */
	int				n_elems;		/* length of offsets array */
	pgssTokenOffset offsets[1];		/* VARIABLE LENGTH ARRAY - MUST BE LAST */

} pgssQueryConEntry;

/*---- Local variables ----*/
/* Jumble of last query tree seen pgss_Planner */
static unsigned char *last_jumble = NULL;
/* Ptr to buffer that represents position of normalized characters */
static pgssTokenOffset *last_offsets = NULL;
/* Current Length of last_offsets buffer */
static size_t last_offset_buf_size = 10;
/* Current number of actual offsets stored in last_offsets */
static size_t last_offset_num = 0;
/* Current nesting depth of ExecutorRun calls */
static int	nested_level = 0;
/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static parse_analyze_hook_type prev_parse_analyze_hook = NULL;
static parse_analyze_varparams_hook_type prev_parse_analyze_varparams_hook = NULL;

/* Links to shared memory state */
static pgssSharedState *pgss = NULL;
static HTAB *pgss_hash = NULL;

/* Backend-local hash table of info to canonicalize query strings */
static HTAB *pgss_const_pos = NULL;

/*---- GUC variables ----*/

typedef enum
{
	PGSS_TRACK_NONE,			/* track no statements */
	PGSS_TRACK_TOP,				/* only top level statements */
	PGSS_TRACK_ALL				/* all statements, including nested ones */
}	PGSSTrackLevel;

static const struct config_enum_entry track_options[] =
{
	{"none", PGSS_TRACK_NONE, false},
	{"top", PGSS_TRACK_TOP, false},
	{"all", PGSS_TRACK_ALL, false},
	{NULL, 0, false}
};

static int	pgss_max;			/* max # statements to track */
static int	pgss_track;			/* tracking level */
static bool pgss_track_utility; /* whether to track utility commands */
static bool pgss_save;			/* whether to save stats across shutdown */
static bool pgss_string_key;	/* whether to always only hash query str */

#define pgss_enabled() \
	(pgss_track == PGSS_TRACK_ALL || \
	(pgss_track == PGSS_TRACK_TOP && nested_level == 0))

/*---- Function declarations ----*/

void		_PG_init(void);
void		_PG_fini(void);

Datum		pg_stat_statements_reset(PG_FUNCTION_ARGS);
Datum		pg_stat_statements(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_stat_statements_reset);
PG_FUNCTION_INFO_V1(pg_stat_statements);

static void pgss_shmem_startup(void);
static void pgss_shmem_shutdown(int code, Datum arg);
static int comp_offset(const void *a, const void *b);
static Query *pgss_parse_analyze(Node *parseTree, const char *sourceText,
			  Oid *paramTypes, int numParams);
static Query *pgss_parse_analyze_varparams(Node *parseTree, const char *sourceText,
						Oid **paramTypes, int *numParams);
static void pgss_store_constants(uint64 query_id);
static uint32 get_constant_length(const char* query_str_const);
static uint64 JumbleQuery(Query *post_analysis_tree);
static void AppendJumb(unsigned char* item, unsigned char jumble[], size_t size, size_t *i);
static void PerformJumble(const Query *tree, size_t size, size_t *i);
static void QualsNode(const OpExpr *node, size_t size, size_t *i, List *rtable);
static void LeafNode(const Node *arg, size_t size, size_t *i, List *rtable);
static void LimitOffsetNode(const Node *node, size_t size, size_t *i, List *rtable);
static void JoinExprNode(JoinExpr *node, size_t size, size_t *i, List *rtable);
static void JoinExprNodeChild(const Node *node, size_t size, size_t *i, List *rtable);
static void pgss_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pgss_ExecutorRun(QueryDesc *queryDesc,
				 ScanDirection direction,
				 long count);
static void pgss_ExecutorFinish(QueryDesc *queryDesc);
static void pgss_ExecutorEnd(QueryDesc *queryDesc);
static void pgss_ProcessUtility(Node *treetree,
			  const char *queryString, ParamListInfo params, bool isTopLevel,
					DestReceiver *dest, char *completionTag);
static uint32 pgss_hash_fn(const void *key, Size keysize);
static int	pgss_match_fn(const void *key1, const void *key2, Size keysize);
static void pgss_store(const char *query, uint64 query_id,
				double total_time, uint64 rows,
				const BufferUsage *bufusage);
static Size pgss_memsize(void);
static pgssEntry *entry_alloc(pgssHashKey *key, const char* query, int new_query_len);
static void entry_dealloc(void);
static void entry_reset(void);


/*
 * Module load callback
 */
void
_PG_init(void)
{
	HASHCTL		info;
	MemoryContext cur_context;
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.  If not, fall out without hooking into any of
	 * the main system.  (We don't throw error here because it seems useful to
	 * allow the pg_stat_statements functions to be created even when the
	 * module isn't active.  The functions must protect themselves against
	 * being called then, however.)
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	/*
	 * Define (or redefine) custom GUC variables.
	 */
	DefineCustomIntVariable("pg_stat_statements.max",
	  "Sets the maximum number of statements tracked by pg_stat_statements.",
							NULL,
							&pgss_max,
							1000,
							100,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomEnumVariable("pg_stat_statements.track",
			   "Selects which statements are tracked by pg_stat_statements.",
							 NULL,
							 &pgss_track,
							 PGSS_TRACK_TOP,
							 track_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_stat_statements.track_utility",
	   "Selects whether utility commands are tracked by pg_stat_statements.",
							 NULL,
							 &pgss_track_utility,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_stat_statements.save",
			   "Save pg_stat_statements statistics across server shutdowns.",
							 NULL,
							 &pgss_save,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/*
	 * Support legacy pg_stat_statements behavior, for compatibility with
	 * versions shipped with Postgres 8.4, 9.0 and 9.1
	 */
	DefineCustomBoolVariable("pg_stat_statements.string_key",
			   "Differentiate queries based on query string alone.",
							 NULL,
							 &pgss_string_key,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	EmitWarningsOnPlaceholders("pg_stat_statements");

	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in pgss_shmem_startup().
	 */
	RequestAddinShmemSpace(pgss_memsize());
	RequestAddinLWLocks(1);

	/* Allocate a buffer to store selective serialization of the query tree
	 * for the purposes of query normalization.
	 */

	/*
	 * State that persists for the lifetime of the backend should be allocated
	 * in TopMemoryContext
	 */
	cur_context = MemoryContextSwitchTo(TopMemoryContext);

	last_jumble = palloc(JUMBLE_SIZE);
	/* Allocate space for bookkeeping information for query str normalization */
	last_offsets = palloc(last_offset_buf_size * sizeof(pgssTokenOffset));

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(pgssHashKey);
	/* Store offsets in hash table */
	info.entrysize = offsetof(pgssQueryConEntry, offsets) +
								1024 * sizeof(pgssQueryConEntry);
	info.hash = pgss_hash_fn;
	info.match = pgss_match_fn;

	pgss_const_pos = hash_create("pg_stat_statements constants hash",
							  10,
							  &info,
							  HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

	MemoryContextSwitchTo(cur_context);

	/*
	 * Install hooks.
	 */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgss_shmem_startup;
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pgss_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pgss_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pgss_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pgss_ExecutorEnd;
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = pgss_ProcessUtility;
	prev_parse_analyze_hook = parse_analyze_hook;
	parse_analyze_hook = pgss_parse_analyze;
	prev_parse_analyze_varparams_hook = parse_analyze_varparams_hook;
	parse_analyze_varparams_hook = pgss_parse_analyze_varparams;
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hooks. */
	shmem_startup_hook = prev_shmem_startup_hook;
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;
	ExecutorEnd_hook = prev_ExecutorEnd;
	ProcessUtility_hook = prev_ProcessUtility;
	parse_analyze_hook = prev_parse_analyze_hook;
	parse_analyze_varparams_hook = prev_parse_analyze_varparams_hook;

	pfree(last_jumble);
	pfree(last_offsets);

	hash_destroy(pgss_const_pos);
}

/*
 * shmem_startup hook: allocate or attach to shared memory,
 * then load any pre-existing statistics from file.
 */
static void
pgss_shmem_startup(void)
{
	bool		found;
	HASHCTL		info;
	FILE	   *file;
	uint32		header;
	int32		num;
	int32		i;
	int			query_size;
	int			buffer_size;
	char	   *buffer = NULL;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case this is a restart within the postmaster */
	pgss = NULL;
	pgss_hash = NULL;

	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	pgss = ShmemInitStruct("pg_stat_statements",
						   sizeof(pgssSharedState),
						   &found);

	if (!found)
	{
		/* First time through ... */
		pgss->lock = LWLockAssign();
		pgss->query_size = pgstat_track_activity_query_size;
	}

	/* Be sure everyone agrees on the hash table entry size */
	query_size = pgss->query_size;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(pgssHashKey);
	info.entrysize = offsetof(pgssEntry, query) +query_size;
	info.hash = pgss_hash_fn;
	info.match = pgss_match_fn;
	pgss_hash = ShmemInitHash("pg_stat_statements hash",
							  pgss_max, pgss_max,
							  &info,
							  HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

	LWLockRelease(AddinShmemInitLock);

	/*
	 * If we're in the postmaster (or a standalone backend...), set up a shmem
	 * exit hook to dump the statistics to disk.
	 */
	if (!IsUnderPostmaster)
		on_shmem_exit(pgss_shmem_shutdown, (Datum) 0);

	/*
	 * Attempt to load old statistics from the dump file, if this is the first
	 * time through and we weren't told not to.
	 */
	if (found || !pgss_save)
		return;

	/*
	 * Note: we don't bother with locks here, because there should be no other
	 * processes running when this code is reached.
	 */
	file = AllocateFile(PGSS_DUMP_FILE, PG_BINARY_R);
	if (file == NULL)
	{
		if (errno == ENOENT)
			return;				/* ignore not-found error */
		goto error;
	}

	buffer_size = query_size;
	buffer = (char *) palloc(buffer_size);

	if (fread(&header, sizeof(uint32), 1, file) != 1 ||
		header != PGSS_FILE_HEADER ||
		fread(&num, sizeof(int32), 1, file) != 1)
		goto error;

	for (i = 0; i < num; i++)
	{
		pgssEntry	temp;
		pgssEntry  *entry;

		if (fread(&temp, offsetof(pgssEntry, mutex), 1, file) != 1)
			goto error;

		/* Encoding is the only field we can easily sanity-check */
		if (!PG_VALID_BE_ENCODING(temp.key.encoding))
			goto error;


		/* Previous incarnation might have had a larger query_size */
		if (temp.query_len >= buffer_size)
		{
			buffer = (char *) repalloc(buffer, temp.query_len + 1);
			buffer_size = temp.query_len + 1;
		}

		if (fread(buffer, 1, temp.query_len, file) != temp.query_len)
			goto error;
		buffer[temp.query_len] = '\0';


		/* Clip to available length if needed */
		if (temp.query_len >= query_size)
			temp.query_len = pg_encoding_mbcliplen(temp.key.encoding,
													   buffer,
													   temp.query_len,
													   query_size - 1);

		/* make the hashtable entry (discards old entries if too many) */
		entry = entry_alloc(&temp.key, buffer, temp.query_len);

		/* copy in the actual stats */
		entry->counters = temp.counters;
	}

	pfree(buffer);
	FreeFile(file);
	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read pg_stat_statement file \"%s\": %m",
					PGSS_DUMP_FILE)));
	if (buffer)
		pfree(buffer);
	if (file)
		FreeFile(file);
	/* If possible, throw away the bogus file; ignore any error */
	unlink(PGSS_DUMP_FILE);
}

/*
 * shmem_shutdown hook: Dump statistics into file.
 *
 * Note: we don't bother with acquiring lock, because there should be no
 * other processes running when this is called.
 */
static void
pgss_shmem_shutdown(int code, Datum arg)
{
	FILE	   *file;
	HASH_SEQ_STATUS hash_seq;
	int32		num_entries;
	pgssEntry  *entry;

	/* Don't try to dump during a crash. */
	if (code)
		return;

	/* Safety check ... shouldn't get here unless shmem is set up. */
	if (!pgss || !pgss_hash)
		return;

	/* Don't dump if told not to. */
	if (!pgss_save)
		return;

	file = AllocateFile(PGSS_DUMP_FILE, PG_BINARY_W);
	if (file == NULL)
		goto error;

	if (fwrite(&PGSS_FILE_HEADER, sizeof(uint32), 1, file) != 1)
		goto error;
	num_entries = hash_get_num_entries(pgss_hash);
	if (fwrite(&num_entries, sizeof(int32), 1, file) != 1)
		goto error;

	hash_seq_init(&hash_seq, pgss_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		int			len = entry->query_len;

		if (fwrite(entry, offsetof(pgssEntry, mutex), 1, file) != 1 ||
			fwrite(entry->query, 1, len, file) != len)
			goto error;
	}

	if (FreeFile(file))
	{
		file = NULL;
		goto error;
	}

	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not write pg_stat_statement file \"%s\": %m",
					PGSS_DUMP_FILE)));
	if (file)
		FreeFile(file);
	unlink(PGSS_DUMP_FILE);

}

/*
 * comp_offset: Comparator for qsorting pgssTokenOffset values by their
 * position in the original query string. Often when walking the query tree,
 * Const tokens will be found in the same order as in the original query string,
 * but this is certainly not guaranteed.
 */
static int
comp_offset(const void *a, const void *b)
{
	int l = ((pgssTokenOffset*) a)->offset;
	int r = ((pgssTokenOffset*) b)->offset;
	if (l < r)
		return -1;
	else if (l > r)
		return +1;
	else
		return 0;
}

static Query *
pgss_parse_analyze(Node *parseTree, const char *sourceText,
			  Oid *paramTypes, int numParams)
{
	Query *post_analysis_tree;

	if (prev_parse_analyze_hook)
		post_analysis_tree = (*prev_parse_analyze_hook) (parseTree, sourceText,
			  paramTypes, numParams);
	else
		post_analysis_tree = standard_parse_analyze(parseTree, sourceText,
			  paramTypes, numParams);

	post_analysis_tree->query_id = JumbleQuery(post_analysis_tree);

	pgss_store_constants(post_analysis_tree->query_id);

	return post_analysis_tree;
}

static Query *
pgss_parse_analyze_varparams(Node *parseTree, const char *sourceText,
						Oid **paramTypes, int *numParams)
{
	Query *post_analysis_tree;

	 if (prev_parse_analyze_hook)
		post_analysis_tree = (*prev_parse_analyze_varparams_hook) (parseTree, sourceText,
			  paramTypes, numParams);
	else
		post_analysis_tree = standard_parse_analyze_varparams(parseTree, sourceText,
			  paramTypes, numParams);

	post_analysis_tree->query_id = JumbleQuery(post_analysis_tree);

	pgss_store_constants(post_analysis_tree->query_id);

	return post_analysis_tree;
}

static void
pgss_store_constants(uint64 query_id)
{
	bool found;
	pgssHashKey key;
	Assert(pgss_const_pos != NULL);
	/*
	 * Store constants for later canonicalization if this query doesn't already
	 * known about.
	 *
	 * Set up key for hashtable search.
	 */
	key.userid = GetUserId();
	key.dbid = MyDatabaseId;
	key.encoding = GetDatabaseEncoding();
	key.query_id = query_id;

	/* Lookup the hash table entry with shared lock. */
	LWLockAcquire(pgss->lock, LW_SHARED);

	(void) hash_search(pgss_hash, &key, HASH_FIND, &found);

	if (!found)
	{
		/*
		 * Store position of constants for later. Since the same query could be
		 * parsed many times before it is finally executed, we only avoid
		 * updated constant positions when there is already a shared hashtable
		 * entry that already has a canonicalized query string.
		 *
		 * Note that since the name of prepared queries influences the hash that
		 * is produced, there is no need to be concerned about the same query
		 * being separately prepared with a different amount of whitespace,
		 * where the earlier query prepared in executed with the constants of
		 * the later query.
		 */
		pgssQueryConEntry *entry;

		entry = (pgssQueryConEntry *) hash_search(pgss_const_pos, &key, HASH_ENTER, NULL);
		memcpy(entry->offsets, last_offsets, sizeof(pgssTokenOffset) * last_offset_num);
		entry->n_elems = last_offset_num;
	}

	LWLockRelease(pgss->lock);
}

/*
 * XXX: Given query_str_const, which points to the first character of a constant
 * within a null-terminated SQL query string, determine the total length of the
 * constant.
 *
 * The constant may use any available constant syntax, including but not limited
 * to numeric literals, single quoted strings and dollar-quoted strings. This is
 * done by independently parsing the constant from the query string, and
 * applying the known rules governing the representation of constants within
 * Postgres query strings, since there is not a better-principled mechanism for
 * calculating the length of the constant implemented in the core server, and
 * this situation is not expected to change.
 *
 * A number of external factors, such as the current value of
 * standard_conforming_strings, will potentially affect exactly how this is
 * performed.
 *
 * Here are some examples:
 *
 * 'abc'					-	returns 5
 * $$def$$					-	returns 7
 * $foo$def$foo$			-	returns 13
 * $foo$baz's $ money$foo$	-	returns 23
 *
 * If the string does not point to the first character of a valid constant, or
 * does not inclue the constant in its entirety, behavior is undefined. Since
 * the string and initial position the caller provides must originate within the
 * authoratative parser, this should not be a problem.
 */
static uint32
get_constant_length(const char* query_str_const)
{
	return 0;
}

/*
 * JumbleQuery: Selectively serialize query tree, and return a hash representing
 * that serialization - it's query id.
 *
 * Note that this doesn't necessarily uniquely identify the query across
 * different databases and encodings.
 */
static uint64
JumbleQuery(Query *post_analysis_tree)
{
	/* State for this run of PerformJumble */
	size_t i = 0;
	last_offset_num = 0;
	memset(last_jumble, 0, JUMBLE_SIZE);
	last_jumble[++i] = HASH_BUF;
	PerformJumble(post_analysis_tree, JUMBLE_SIZE, &i);
	/* Sort offsets for later query string canonicalization */
	qsort(last_offsets, last_offset_num, sizeof(pgssTokenOffset), comp_offset);
	return hash_any_var_width((const unsigned char* ) last_jumble, Min(i, JUMBLE_SIZE), false);
}

/*
 * AppendJumb: Append a value that is substantive to a given query to jumble,
 * while incrementing the iterator.
 */
static void
AppendJumb(unsigned char* item, unsigned char jumble[], size_t size, size_t *i)
{
	Assert(item != NULL);
	Assert(jumble != NULL);
	Assert(i != NULL);

	/*
	 * Copy the entire item to the buffer, or as much of it as possible to fill
	 * the buffer to capacity.
	 */
	memcpy(jumble + *i, item, Min(*i > JUMBLE_SIZE? 0:JUMBLE_SIZE - *i, size));

	/*
	 * Continually hash the query tree.
	 *
	 * Was JUMBLE_SIZE exceeded? If so, hash the jumble and append that to the
	 * start of the jumble buffer, and then continue to append the fraction of
	 * "item" that we might not have been able to fit at the end of the buffer
	 * in the last iteration. Since the value of i has been set to 0, there is
	 * no need to memset the buffer in advance of this new iteration, but
	 * effectively we are completely disguarding the prior iteration's jumble
	 * except for this hashed value.
	 */
	if (*i > JUMBLE_SIZE)
	{
		int64 start_hash = hash_any_var_width((const unsigned char* ) last_jumble, JUMBLE_SIZE, false);
		int hash_l = sizeof(start_hash);
		int part_left_l = size - ((*i - JUMBLE_SIZE) * -1);

		Assert(part_left_l >= 0);

		memcpy(jumble, &start_hash, hash_l);
		memcpy(jumble + hash_l, item + (size - part_left_l), part_left_l);
		*i = hash_l + part_left_l;
	}
	else
	{
		*i += size;
	}

}

/*
 * Wrapper around AppendJumb to encapsulate details of serialization
 * of individual local variable elements.
 */
#define APP_JUMB(item) \
AppendJumb((unsigned char*)&item, last_jumble, sizeof(item), i)

/*
 * Space in the jumble buffer is limited - we can compact enum representations
 * that will obviously never really need more than a single byte to store all
 * possible enumerations.
 *
 * It would be pretty questionable to attempt this with an enum that has
 * explicit integer values corresponding to constants, such as the huge enum
 * "Node" that we use to represent all possible nodes, and it would be
 * downright incorrect to do so with one with negative values explicitly
 * assigned to constants. This is intended to be used with enums with perhaps
 * less than a dozen possible values, that are never likely to far exceed that.
 */
#define COMPACT_ENUM(val) \
	(unsigned char) val;
/*
 * PerformJumble: Serialize the query tree "parse" and canonicalize
 * constants, while simply skipping over others that are not essential to the
 * query, such that it is usefully normalized, excluding things from the tree
 * that are not essential to the query itself.
 *
 * A guiding principal as to whether two queries should be considered
 * equivalent is whether whatever difference exists between the two queries
 * could be expected to result in two different plans, assuming that all
 * constants have the same selectivity estimate and excluding factors that are
 * not essential to the query and the relations on which it operates. A
 * non-obvious example of such a differentiator is a change in the definition
 * of a view referenced by a query. We pointedly serialize the query tree
 * *after* the rewriting stage, so entries in pg_stat_statements accurately
 * represent discrete operations, while not having artifacts from external
 * factors that are not essential to what the query actually does. Directly
 * hashing plans would have the undesirable side-effect of potentially having
 * totally external factors like planner cost constants differentiate a query,
 * so that particular implementation was not chosen. Directly hashing the raw
 * parse tree would prevent us from benefiting from the parser's own
 * normalization of queries in later stages, such as removing differences
 * between equivalent syntax, so that implementation was avoided too.
 *
 * The last_jumble buffer, which this function writes to, can be hashed to
 * uniquely identify a query that may use different constants in successive
 * calls.
 */
static void
PerformJumble(const Query *tree, size_t size, size_t *i)
{
	ListCell *l;
	/* table join tree (FROM and WHERE clauses) */
	FromExpr *jt = (FromExpr *) tree->jointree;
	/* # of result tuples to skip (int8 expr) */
	FuncExpr *off = (FuncExpr *) tree->limitOffset;
	/* # of result tuples to skip (int8 expr) */
	FuncExpr *limcount = (FuncExpr *) tree->limitCount;

	APP_JUMB(tree->resultRelation);

	if (tree->intoClause)
	{
		unsigned char OnCommit;
		unsigned char skipData;
		IntoClause *ic = tree->intoClause;
		RangeVar   *rel = ic->rel;

		OnCommit = COMPACT_ENUM(ic->onCommit);
		skipData = COMPACT_ENUM(ic->skipData);
		APP_JUMB(OnCommit);
		APP_JUMB(skipData);
		if (rel)
		{
			APP_JUMB(rel->relpersistence);
			/* Bypass macro abstraction to supply size directly.
			 *
			 * Serialize schemaname, relname themselves - this makes us
			 * somewhat consistent with the behavior of utility statements like "create
			 * table", which seems appropriate.
			 */
			if (rel->schemaname)
				AppendJumb((unsigned char *)rel->schemaname, last_jumble,
								strlen(rel->schemaname), i);
			if (rel->relname)
				AppendJumb((unsigned char *)rel->relname, last_jumble,
								strlen(rel->relname), i);
		}
	}

	/* WITH list (of CommonTableExpr's) */
	foreach(l, tree->cteList)
	{
		CommonTableExpr	*cte = (CommonTableExpr *) lfirst(l);
		Query			*cteq = (Query*) cte->ctequery;
		if (cteq)
			PerformJumble(cteq, size, i);
	}
	if (jt)
	{
		if (jt->quals)
		{
			if (IsA(jt->quals, OpExpr))
			{
				QualsNode((OpExpr*) jt->quals, size, i, tree->rtable);
			}
			else
			{
				LeafNode((Node*) jt->quals, size, i, tree->rtable);
			}
		}
		/* table join tree */
		foreach(l, jt->fromlist)
		{
			Node* fr = lfirst(l);
			if (IsA(fr, JoinExpr))
			{
				JoinExprNode((JoinExpr*) fr, size, i, tree->rtable);
			}
			else if (IsA(fr, RangeTblRef))
			{
				unsigned char rtekind;
				RangeTblRef   *rtf = (RangeTblRef *) fr;
				RangeTblEntry *rte = rt_fetch(rtf->rtindex, tree->rtable);
				APP_JUMB(rte->relid);
				rtekind = COMPACT_ENUM(rte->rtekind);
				APP_JUMB(rtekind);
				/* Subselection in where clause */
				if (rte->subquery)
					PerformJumble(rte->subquery, size, i);

				/* Function call in where clause */
				if (rte->funcexpr)
					LeafNode((Node*) rte->funcexpr, size, i, tree->rtable);
			}
			else
			{
				elog(ERROR, "unrecognized fromlist node type: %d",
					 (int) nodeTag(fr));
			}
		}
	}
	/*
	 * target list (of TargetEntry)
	 * columns returned by query
	 */
	foreach(l, tree->targetList)
	{
		TargetEntry *tg = (TargetEntry *) lfirst(l);
		Node        *e  = (Node*) tg->expr;
		if (tg->ressortgroupref)
			/* nonzero if referenced by a sort/group - for ORDER BY */
			APP_JUMB(tg->ressortgroupref);
		APP_JUMB(tg->resno); /* column number for select */
		/*
		 * Handle the various types of nodes in
		 * the select list of this query
		 */
		LeafNode(e, size, i, tree->rtable);
	}
	/* return-values list (of TargetEntry) */
	foreach(l, tree->returningList)
	{
		TargetEntry *rt = (TargetEntry *) lfirst(l);
		Expr        *e  = (Expr*) rt->expr;
		/*
		 * Handle the various types of nodes in
		 * the select list of this query
		 */
		if (IsA(e, Var)) /* table column */
		{
			Var *v = (Var*) e;
			RangeTblEntry *rte = rt_fetch(v->varno, tree->rtable);
			/* Identify column by XOR'ing relid + col number */
			Oid col_ident = rte->relid ^ v->varattno;
			APP_JUMB(col_ident);
		}
		else
		{
			elog(ERROR, "unrecognized node type for returnlist node: %d",
					(int) nodeTag(e));
		}
	}
	/* a list of SortGroupClause's */
	foreach(l, tree->groupClause)
	{
		SortGroupClause *gc = (SortGroupClause *) lfirst(l);
		APP_JUMB(gc->tleSortGroupRef);
		APP_JUMB(gc->nulls_first);
	}

	if (tree->havingQual)
	{
		if (IsA(tree->havingQual, OpExpr))
		{
			OpExpr *na = (OpExpr *) tree->havingQual;
			QualsNode(na, size, i, tree->rtable);
		}
		else
		{
			elog(ERROR, "unrecognized node type for havingclause node: %d",
					(int) nodeTag(tree->havingQual));
		}
	}
	foreach(l, tree->windowClause)
	{
		WindowClause *wc = (WindowClause *) lfirst(l);
		ListCell     *il;
		APP_JUMB(wc->frameOptions);
		foreach(il, wc->partitionClause) /* PARTITION BY list */
		{
			Node *n = (Node *) lfirst(il);
			LeafNode(n, size, i, tree->rtable);
		}
		foreach(il, wc->orderClause) /* ORDER BY list */
		{
			Node *n = (Node *) lfirst(il);
			LeafNode(n, size, i, tree->rtable);
		}
	}

	foreach(l, tree->distinctClause)
	{
		SortGroupClause *dc = (SortGroupClause *) lfirst(l);
		APP_JUMB(dc->tleSortGroupRef);
		APP_JUMB(dc->nulls_first);
	}

	/* Don't look at tree->sortClause,
	 * because the value ressortgroupref is already
	 * serialized when we iterate through targetList
	 */

	if (off)
		LimitOffsetNode((Node*) off, size, i, tree->rtable);

	if (limcount)
		LimitOffsetNode((Node*) limcount, size, i, tree->rtable);

	foreach(l, tree->rowMarks)
	{
		RowMarkClause *rc = (RowMarkClause *) lfirst(l);
		APP_JUMB(rc->rti);				/* range table index of target relation */
		APP_JUMB(rc->forUpdate);			/* true = FOR UPDATE, false = FOR SHARE */
		APP_JUMB(rc->noWait);				/* NOWAIT option */
		APP_JUMB(rc->pushedDown);			/* pushed down from higher query level? */
	}

	if (tree->setOperations)
	{
		/*
		 * set-operation tree if this is top
		 * level of a UNION/INTERSECT/EXCEPT query
		 */
		unsigned char op;
		SetOperationStmt *topop = (SetOperationStmt *) tree->setOperations;
		op = COMPACT_ENUM(topop->op);
		APP_JUMB(op);
		APP_JUMB(topop->all);

		/* leaf selects are RTE subselections */
		foreach(l, tree->rtable)
		{
			RangeTblEntry *r = (RangeTblEntry *) lfirst(l);
			if (r->subquery)
				PerformJumble(r->subquery, size, i);
		}
	}
}

/*
 * Perform selective serialization of "Quals" nodes when
 * they're IsA(*, OpExpr)
 */
static void
QualsNode(const OpExpr *node, size_t size, size_t *i, List *rtable)
{
	ListCell *l;
	APP_JUMB(node->opno);
	foreach(l, node->args)
	{
		Node *arg = (Node *) lfirst(l);
		LeafNode(arg, size, i, rtable);
	}
}

/*
 * LeafNode: Selectively serialize a selection of parser/prim nodes that are
 * frequently, though certainly not necesssarily leaf nodes, such as Vars
 * (columns), constants and function calls
 */
static void
LeafNode(const Node *arg, size_t size, size_t *i, List *rtable)
{
	ListCell *l;
	if (IsA(arg, Const))
	{
		/*
		 * Serialize generic magic value to
		 * normalize constants
		 */
		unsigned char magic = 0xC0;
		Const *c = (Const *) arg;
		APP_JUMB(magic);
		/* Datatype of the constant is a
		 * differentiator
		 */
		APP_JUMB(c->consttype);
		/*
		 * Some Const nodes naturally don't have a location.
		 *
		 * Views that have constants in their
		 * definitions will have a tok_len of 0
		 */
		if (c->location > 0 && c->tok_len > 0)
		{
			if (last_offset_num >= last_offset_buf_size)
			{
				last_offset_buf_size *= 2;
				last_offsets = repalloc(last_offsets,
								last_offset_buf_size *
								sizeof(pgssTokenOffset));

			}
			last_offsets[last_offset_num].offset = c->location;
			last_offsets[last_offset_num].len = c->tok_len;
			last_offset_num++;
		}
	}
	else if (IsA(arg, Var))
	{
		unsigned char		  magic = 0xFA;
		Var			  *v = (Var *) arg;
		RangeTblEntry *rte = rt_fetch(v->varno, rtable);
		/* Identify column by XOR'ing relid + col number */
		Oid col_ident = rte->relid ^ v->varattno;
		APP_JUMB(magic);
		APP_JUMB(col_ident);
	}
	else if (IsA(arg, Param))
	{
		Param *p = ((Param *) arg);
		APP_JUMB(p->paramkind);
		APP_JUMB(p->paramid);
	}
	else if (IsA(arg, RelabelType))
	{
		APP_JUMB(((RelabelType *) arg)->resulttype);
	}
	else if (IsA(arg, WindowFunc))
	{
		WindowFunc *wf = (WindowFunc *) arg;
		APP_JUMB(wf->winfnoid);
		foreach(l, wf->args)
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
	}
	else if (IsA(arg, FuncExpr))
	{
		FuncExpr *f = (FuncExpr *) arg;
		APP_JUMB(f->funcid);
		foreach(l, f->args)
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
	}
	else if (IsA(arg, OpExpr))
	{
		QualsNode((OpExpr*) arg, size, i, rtable);
	}
	else if (IsA(arg, CoerceViaIO))
	{
		APP_JUMB(((CoerceViaIO*) arg)->coerceformat);
		APP_JUMB(((CoerceViaIO*) arg)->resulttype);
	}
	else if (IsA(arg, Aggref))
	{
		Aggref *a =  (Aggref *) arg;
		APP_JUMB(a->aggfnoid);
		foreach(l, a->args)
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
	}
	else if (IsA(arg, SubLink))
	{
		SubLink *s = (SubLink*) arg;
		APP_JUMB(s->subLinkType);
		/* Serialize select-list subselect recursively */
		if (s->subselect)
			PerformJumble((Query*) s->subselect, size, i);
	}
	else if (IsA(arg, TargetEntry))
	{
		TargetEntry *rt = (TargetEntry *) arg;
		Node *e = (Node*) rt->expr;
		/* XOR OID of column's source table + index to sort/group clause */
		Oid target_repr = rt->resorigtbl ^ rt->ressortgroupref;
		APP_JUMB(target_repr);
		LeafNode(e, size, i, rtable);
	}
	else if (IsA(arg, BoolExpr))
	{
		BoolExpr *be = (BoolExpr *) arg;
		APP_JUMB(be->boolop);
		foreach(l, be->args)
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
	}
	else if (IsA(arg, NullTest))
	{
		NullTest *nt = (NullTest *) arg;
		Node     *arg = (Node *) nt->arg;
		unsigned char nulltesttype = COMPACT_ENUM(nt->nulltesttype);
		APP_JUMB(nulltesttype);	/* IS NULL, IS NOT NULL */
		APP_JUMB(nt->argisrow);		/* is input a composite type ? */
		LeafNode(arg, size, i, rtable);
	}
	else if (IsA(arg, ArrayExpr))
	{
		ArrayExpr *ae = (ArrayExpr *) arg;
		APP_JUMB(ae->array_typeid);	/* type of expression result */
		APP_JUMB(ae->element_typeid);	/* common type of array elements */
		foreach(l, ae->elements)
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
	}
	else if (IsA(arg, CaseExpr))
	{
		CaseExpr *ce = (CaseExpr*) arg;
		Assert(ce->casetype != InvalidOid);
		APP_JUMB(ce->casetype);
		foreach(l, ce->args)
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
		if (ce->arg)
			LeafNode((Node*) ce->arg, size, i, rtable);

		if (ce->defresult)
		{
			/* Default result (ELSE clause)
			 * The ptr may be NULL, because no else clause
			 * was actually specified, and thus the value is
			 * equivalent to SQL ELSE NULL
			 */
			LeafNode((Node*) ce->defresult, size, i, rtable); /* the default result (ELSE clause) */
		}
	}
	else if (IsA(arg, CaseTestExpr))
	{
		CaseTestExpr *ct = (CaseTestExpr*) arg;
		APP_JUMB(ct->typeId);
	}
	else if (IsA(arg, CaseWhen))
	{
		CaseWhen *cw = (CaseWhen*) arg;
		Node     *res = (Node*) cw->result;
		Node     *exp = (Node*) cw->expr;
		if (res)
			LeafNode(res, size, i, rtable);
		if (exp)
			LeafNode(exp, size, i, rtable);
	}
	else if (IsA(arg, MinMaxExpr))
	{
		MinMaxExpr *cw = (MinMaxExpr*) arg;
		APP_JUMB(cw->minmaxtype);
		APP_JUMB(cw->op);
		foreach(l, cw->args)
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
	}
	else if (IsA(arg, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *sa = (ScalarArrayOpExpr*) arg;
		APP_JUMB(sa->opfuncid);
		APP_JUMB(sa->useOr);
		foreach(l, sa->args)
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
	}
	else if (IsA(arg, CoalesceExpr))
	{
		CoalesceExpr *ca = (CoalesceExpr*) arg;
		foreach(l, ca->args)
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
	}
	else if (IsA(arg, ArrayCoerceExpr))
	{
		ArrayCoerceExpr *ac = (ArrayCoerceExpr *) arg;
		LeafNode((Node*) ac->arg, size, i, rtable);
	}
	else if (IsA(arg, WindowClause))
	{
		WindowClause *wc = (WindowClause*) arg;
		foreach(l, wc->partitionClause)
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
		foreach(l, wc->orderClause)
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
	}
	else if (IsA(arg, SortGroupClause))
	{
		SortGroupClause *sgc = (SortGroupClause*) arg;
		APP_JUMB(sgc->tleSortGroupRef);
		APP_JUMB(sgc->nulls_first);
	}
	else if (IsA(arg, Integer) ||
		  IsA(arg, Float) ||
		  IsA(arg, String) ||
		  IsA(arg, BitString) ||
		  IsA(arg, Null)
		)
	{
		/* It is not necessary to serialize Value nodes - they are seen when
		 * aliases are used, which is not something that is serialized.
		 */
		return;
	}
	else if (IsA(arg, BooleanTest))
	{
		BooleanTest *bt = (BooleanTest *) arg;
		APP_JUMB(bt->booltesttype);
		LeafNode((Node*) bt->arg, size, i, rtable);
	}
	else if (IsA(arg, ArrayRef))
	{
		ArrayRef *ar = (ArrayRef*) arg;
		APP_JUMB(ar->refarraytype);
		foreach(l, ar->refupperindexpr)
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
		foreach(l, ar->reflowerindexpr)
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
	}
	else if (IsA(arg, NullIfExpr))
	{
		/* NullIfExpr is just a typedef for OpExpr */
		QualsNode((OpExpr*) arg, size, i, rtable);
	}
	else if (IsA(arg, RowExpr))
	{
		RowExpr *re = (RowExpr*) arg;
		APP_JUMB(re->row_format);
		foreach(l, re->args)
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}

	}
	else if (IsA(arg, XmlExpr))
	{
		XmlExpr *xml = (XmlExpr*) arg;
		APP_JUMB(xml->op);
		foreach(l, xml->args)
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
		foreach(l, xml->named_args) /* non-XML expressions for xml_attributes */
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
		foreach(l, xml->arg_names) /* parallel list of Value strings */
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
	}
	else if (IsA(arg, RowCompareExpr))
	{
		RowCompareExpr *rc = (RowCompareExpr*) arg;
		foreach(l, rc->largs)
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
		foreach(l, rc->rargs)
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
	}
	else if (IsA(arg, SetToDefault))
	{
		SetToDefault *sd = (SetToDefault*) arg;
		APP_JUMB(sd->typeId);
		APP_JUMB(sd->typeMod);
	}
	else
	{
		elog(ERROR, "unrecognized node type for LeafNode node: %d",
				(int) nodeTag(arg));
	}
}

/*
 * Perform selective serialization of limit or offset nodes
 */
static void
LimitOffsetNode(const Node *node, size_t size, size_t *i, List *rtable)
{
	ListCell *l;
	if (IsA(node, FuncExpr))
	{
		foreach(l, ((FuncExpr*) node)->args)
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
	}
	else if (IsA(node, Const))
	{
		/* This should be a differentiator, as it results in the addition of a limit node */
		unsigned char magic = 0xEA;
		APP_JUMB(magic);
	}
	else
	{
		/* Fall back on leaf node representation */
		LeafNode(node, size, i, rtable);
	}
}

/*
 * JoinExprNode: Perform selective serialization of JoinExpr nodes
 */
static void
JoinExprNode(JoinExpr *node, size_t size, size_t *i, List *rtable)
{
	Node	 *larg = node->larg;	/* left subtree */
	Node	 *rarg = node->rarg;	/* right subtree */
	ListCell *l;

	Assert( IsA(node, JoinExpr));

	APP_JUMB(node->jointype);
	APP_JUMB(node->isNatural);

	if (node->quals)
	{
		if ( IsA(node, OpExpr))
		{
			QualsNode((OpExpr*) node->quals, size, i, rtable);
		}
		else
		{
			LeafNode((Node*) node->quals, size, i, rtable);
		}

	}
	foreach(l, node->usingClause) /* USING clause, if any (list of String) */
	{
		Node *arg = (Node *) lfirst(l);
		LeafNode(arg, size, i, rtable);
	}
	if (larg)
		JoinExprNodeChild(larg, size, i, rtable);
	if (rarg)
		JoinExprNodeChild(rarg, size, i, rtable);
}

/*
 * JoinExprNodeChild: Serialize children of the JoinExpr node
 */
static void
JoinExprNodeChild(const Node *node, size_t size, size_t *i, List *rtable)
{
	if (IsA(node, RangeTblRef))
	{
		RangeTblRef   *rt = (RangeTblRef*) node;
		RangeTblEntry *rte = rt_fetch(rt->rtindex, rtable);
		ListCell      *l;

		APP_JUMB(rte->relid);
		APP_JUMB(rte->jointype);

		if (rte->subquery)
			PerformJumble((Query*) rte->subquery, size, i);

		foreach(l, rte->joinaliasvars)
		{
			Node *arg = (Node *) lfirst(l);
			LeafNode(arg, size, i, rtable);
		}
	}
	else if (IsA(node, JoinExpr))
	{
		JoinExprNode((JoinExpr*) node, size, i, rtable);
	}
	else
	{
		LeafNode(node, size, i, rtable);
	}
}

/*
 * ExecutorStart hook: start up tracking if needed
 */
static void
pgss_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (pgss_enabled())
	{
		/*
		 * Set up to track total elapsed time in ExecutorRun.  Make sure the
		 * space is allocated in the per-query context so it will go away at
		 * ExecutorEnd.
		 */
		if (queryDesc->totaltime == NULL)
		{
			MemoryContext oldcxt;

			oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
			queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL);
			MemoryContextSwitchTo(oldcxt);
		}
	}
}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void
pgss_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count)
{
	nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count);
		else
			standard_ExecutorRun(queryDesc, direction, count);
		nested_level--;
	}
	PG_CATCH();
	{
		nested_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
pgss_ExecutorFinish(QueryDesc *queryDesc)
{
	nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
		nested_level--;
	}
	PG_CATCH();
	{
		nested_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * ExecutorEnd hook: store results if needed
 */
static void
pgss_ExecutorEnd(QueryDesc *queryDesc)
{
	if (queryDesc->totaltime && pgss_enabled())
	{
		int64 query_id = queryDesc->plannedstmt->queryId;

		/*
		 * Make sure stats accumulation is done.  (Note: it's okay if several
		 * levels of hook all do this.)
		 */
		InstrEndLoop(queryDesc->totaltime);

		elog(DEBUG1, "Query id: %lu", query_id);

		/*
		 * Once control gets here, there is an assumption that the last_*
		 * variables were set by a prior, corresponding call to
		 * pgss_Planner within this backend. That corresponding call should have
		 * jumbled the query tree.
		 *
		 * That call would only be recorded when the executor stack depth
		 * (which is monitored) was 0, so nested planner calls don't cause
		 * this assumption to fall down.
		 *
		 * A number of other things can prevent pgss_Planner from jumbling and
		 * therefore prevent control reaching here, such as utility
		 * statements and prepared queries.
		 */

		pgss_store(queryDesc->sourceText,
		   query_id,
		   queryDesc->totaltime->total,
		   queryDesc->estate->es_processed,
		   &queryDesc->totaltime->bufusage);

		if (last_offset_buf_size > 10)
		{
			last_offset_buf_size = 10;
			last_offsets = repalloc(last_offsets,
								last_offset_buf_size *
								sizeof(pgssTokenOffset));
		}
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

/*
 * ProcessUtility hook
 */
static void
pgss_ProcessUtility(Node *tree, const char *queryString,
					ParamListInfo params, bool isTopLevel,
					DestReceiver *dest, char *completionTag)
{
	if (pgss_track_utility && pgss_enabled())
	{
		instr_time	start;
		instr_time	duration;
		uint64		rows = 0;
		uint64		query_id = 0;
		BufferUsage bufusage;

		bufusage = pgBufferUsage;
		INSTR_TIME_SET_CURRENT(start);

		nested_level++;
		PG_TRY();
		{
			if (prev_ProcessUtility)
				prev_ProcessUtility(tree, queryString, params,
									isTopLevel, dest, completionTag);
			else
				standard_ProcessUtility(tree, queryString, params,
										isTopLevel, dest, completionTag);
			nested_level--;
		}
		PG_CATCH();
		{
			nested_level--;
			PG_RE_THROW();
		}
		PG_END_TRY();

		INSTR_TIME_SET_CURRENT(duration);
		INSTR_TIME_SUBTRACT(duration, start);

		/* parse command tag to retrieve the number of affected rows. */
		if (completionTag &&
			sscanf(completionTag, "COPY " UINT64_FORMAT, &rows) != 1)
			rows = 0;

		/* calc differences of buffer counters. */
		bufusage.shared_blks_hit =
			pgBufferUsage.shared_blks_hit - bufusage.shared_blks_hit;
		bufusage.shared_blks_read =
			pgBufferUsage.shared_blks_read - bufusage.shared_blks_read;
		bufusage.shared_blks_written =
			pgBufferUsage.shared_blks_written - bufusage.shared_blks_written;
		bufusage.local_blks_hit =
			pgBufferUsage.local_blks_hit - bufusage.local_blks_hit;
		bufusage.local_blks_read =
			pgBufferUsage.local_blks_read - bufusage.local_blks_read;
		bufusage.local_blks_written =
			pgBufferUsage.local_blks_written - bufusage.local_blks_written;
		bufusage.temp_blks_read =
			pgBufferUsage.temp_blks_read - bufusage.temp_blks_read;
		bufusage.temp_blks_written =
			pgBufferUsage.temp_blks_written - bufusage.temp_blks_written;

		/* In the case of utility statements, hash the query string directly */
		pgss_store(queryString, query_id, INSTR_TIME_GET_DOUBLE(duration), rows,
				   &bufusage);
	}
	else
	{
		if (prev_ProcessUtility)
			prev_ProcessUtility(tree, queryString, params,
								isTopLevel, dest, completionTag);
		else
			standard_ProcessUtility(tree, queryString, params,
									isTopLevel, dest, completionTag);
	}
}

/*
 * Calculate hash value for a key
 */
static uint32
pgss_hash_fn(const void *key, Size keysize)
{
	const pgssHashKey *k = (const pgssHashKey *) key;

	/* we don't bother to include encoding in the hash */
	return hash_uint32((uint32) k->userid) ^
		hash_uint32((uint32) k->dbid) ^
		DatumGetUInt32(hash_any((const unsigned char* ) &k->query_id, sizeof(k->query_id)) );
}

/*
 * Compare two keys - zero means match
 */
static int
pgss_match_fn(const void *key1, const void *key2, Size keysize)
{
	const pgssHashKey *k1 = (const pgssHashKey *) key1;
	const pgssHashKey *k2 = (const pgssHashKey *) key2;

	if (k1->userid == k2->userid &&
		k1->dbid == k2->dbid &&
		k1->encoding == k2->encoding &&
		k1->query_id == k2->query_id)
		return 0;
	else
		return 1;
}

/*
 * Store some statistics for a statement.
 */
static void
pgss_store(const char *query, uint64 query_id,
				double total_time, uint64 rows,
				const BufferUsage *bufusage)
{
	pgssHashKey key;
	double		usage;
	int		    new_query_len = strlen(query);
	char	   *norm_query = NULL;
	pgssEntry  *entry;

	Assert(query != NULL);

	/* Safety check... */
	if (!pgss || !pgss_hash)
		return;

	/* Set up key for hashtable search */
	key.userid = GetUserId();
	key.dbid = MyDatabaseId;
	key.encoding = GetDatabaseEncoding();
	key.query_id = query_id;

	if (new_query_len >= pgss->query_size)
		/* We don't have to worry about this later, because canonicalization
		 * cannot possibly result in a longer query string
		 */
		new_query_len = pg_encoding_mbcliplen(key.encoding,
											  query,
											  new_query_len,
											  pgss->query_size - 1);
	usage = USAGE_EXEC(duration);

	/* Lookup the hash table entry with shared lock. */
	LWLockAcquire(pgss->lock, LW_SHARED);

	entry = (pgssEntry *) hash_search(pgss_hash, &key, HASH_FIND, NULL);
	if (!entry)
	{
		pgssQueryConEntry *const_entry;
		pgssTokenOffset *offs;
		int off_n = 0;
		const_entry	= (pgssQueryConEntry *) hash_search(pgss_const_pos, &key, HASH_FIND, NULL);
		if (const_entry)
		{
			offs = const_entry->offsets;
			off_n = const_entry->n_elems;
		}

		/*
		 * It is necessary to generate a normalized version of the query
		 * string that will be used to represent it. It's important that
		 * the user be presented with a stable representation of the query.
		 *
		 * Note that the representation seen by the user will only have
		 * non-differentiating Const tokens swapped with '?' characters, and
		 * this does not for example take account of the fact that alias names
		 * could vary between successive calls of what is regarded as the same
		 * query.
		 */
		if (off_n > 0)
		{
			int i,
			  off = 0,			/* Offset from start for cur tok */
			  tok_len = 0,		/* length (in bytes) of that tok */
			  quer_it = 0,		/* Original query iterator */
			  n_quer_it = 0,	/* Normalized query iterator */
			  len_to_wrt = 0,	/* Length (in bytes) to write */
			  last_off = 0,		/* Offset from start for last iter tok */
			  last_tok_len = 0;	/* length (in bytes) of that tok */

			norm_query = palloc0(new_query_len + 1);
			for(i = 0; i < off_n; i++)
			{
				off = offs[i].offset;
				tok_len = offs[i].len;
				len_to_wrt = off - last_off;
				len_to_wrt -= last_tok_len;
				/*
				 * Each iteration copies everything prior to the current
				 * offset/token to be replaced, except bytes copied in
				 * previous iterations
				 */
				if (off + tok_len > new_query_len)
					break;
				memcpy(norm_query + n_quer_it, query + quer_it, len_to_wrt);

				n_quer_it += len_to_wrt;
				norm_query[n_quer_it++] = '?';
				quer_it += len_to_wrt + tok_len;
				last_off = off;
				last_tok_len = tok_len;
			}
			/* Copy end of query string (piece past last constant) */
			memcpy(norm_query + n_quer_it, query + (off + tok_len),
				Min( strlen(query) - (off + tok_len),
					new_query_len - n_quer_it ) );
			/*
			 * Must acquire exclusive lock to add a new entry.
			 * Leave that until as late as possible.
			 */
			LWLockRelease(pgss->lock);
			LWLockAcquire(pgss->lock, LW_EXCLUSIVE);

			entry = entry_alloc(&key, norm_query, strlen(norm_query));

		}
		else
		{
			/* Acquire exclusive lock as required by entry_alloc() */
			LWLockRelease(pgss->lock);
			LWLockAcquire(pgss->lock, LW_EXCLUSIVE);

			entry = entry_alloc(&key, query, new_query_len);
		}
		/*
		 * Free local memory that stores constant positions - we won't need
		 * it again until the entry is evicted from shared memory, after
		 * which we'll have to calculate new constant locations for a new,
		 * potentially non-substantively different query string
		 */
		hash_search(pgss_const_pos, &key, HASH_REMOVE, NULL);
	}

	/* Grab the spinlock while updating the counters. */
	{
		volatile pgssEntry *e = (volatile pgssEntry *) entry;

		SpinLockAcquire(&e->mutex);
		e->counters.calls += 1;
		e->counters.total_time += total_time;
		e->counters.rows += rows;
		e->counters.shared_blks_hit += bufusage->shared_blks_hit;
		e->counters.shared_blks_read += bufusage->shared_blks_read;
		e->counters.shared_blks_written += bufusage->shared_blks_written;
		e->counters.local_blks_hit += bufusage->local_blks_hit;
		e->counters.local_blks_read += bufusage->local_blks_read;
		e->counters.local_blks_written += bufusage->local_blks_written;
		e->counters.temp_blks_read += bufusage->temp_blks_read;
		e->counters.temp_blks_written += bufusage->temp_blks_written;
		e->counters.usage += usage;
		SpinLockRelease(&e->mutex);
	}
	LWLockRelease(pgss->lock);
	if (norm_query)
		pfree(norm_query);
}

/*
 * Reset all statement statistics.
 */
Datum
pg_stat_statements_reset(PG_FUNCTION_ARGS)
{
	if (!pgss || !pgss_hash)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_stat_statements must be loaded via shared_preload_libraries")));
	entry_reset();
	PG_RETURN_VOID();
}

#define PG_STAT_STATEMENTS_COLS		14

/*
 * Retrieve statement statistics.
 */
Datum
pg_stat_statements(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	Oid			userid = GetUserId();
	bool		is_superuser = superuser();
	HASH_SEQ_STATUS hash_seq;
	pgssEntry  *entry;

	if (!pgss || !pgss_hash)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_stat_statements must be loaded via shared_preload_libraries")));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	LWLockAcquire(pgss->lock, LW_SHARED);

	hash_seq_init(&hash_seq, pgss_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		Datum		values[PG_STAT_STATEMENTS_COLS];
		bool		nulls[PG_STAT_STATEMENTS_COLS];
		int			i = 0;
		Counters	tmp;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[i++] = ObjectIdGetDatum(entry->key.userid);
		values[i++] = ObjectIdGetDatum(entry->key.dbid);

		if (is_superuser || entry->key.userid == userid)
		{
			char	   *qstr;

			qstr = (char *)
				pg_do_encoding_conversion((unsigned char *) entry->query,
										  entry->query_len,
										  entry->key.encoding,
										  GetDatabaseEncoding());
			values[i++] = CStringGetTextDatum(qstr);
			if (qstr != entry->query)
				pfree(qstr);
		}
		else
			values[i++] = CStringGetTextDatum("<insufficient privilege>");

		/* copy counters to a local variable to keep locking time short */
		{
			volatile pgssEntry *e = (volatile pgssEntry *) entry;

			SpinLockAcquire(&e->mutex);
			tmp = e->counters;
			SpinLockRelease(&e->mutex);
		}

		values[i++] = Int64GetDatumFast(tmp.calls);
		values[i++] = Float8GetDatumFast(tmp.total_time);
		values[i++] = Int64GetDatumFast(tmp.rows);
		values[i++] = Int64GetDatumFast(tmp.shared_blks_hit);
		values[i++] = Int64GetDatumFast(tmp.shared_blks_read);
		values[i++] = Int64GetDatumFast(tmp.shared_blks_written);
		values[i++] = Int64GetDatumFast(tmp.local_blks_hit);
		values[i++] = Int64GetDatumFast(tmp.local_blks_read);
		values[i++] = Int64GetDatumFast(tmp.local_blks_written);
		values[i++] = Int64GetDatumFast(tmp.temp_blks_read);
		values[i++] = Int64GetDatumFast(tmp.temp_blks_written);

		Assert(i == PG_STAT_STATEMENTS_COLS);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	LWLockRelease(pgss->lock);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}

/*
 * Estimate shared memory space needed.
 */
static Size
pgss_memsize(void)
{
	Size		size;
	Size		entrysize;

	size = MAXALIGN(sizeof(pgssSharedState));
	entrysize = offsetof(pgssEntry, query) +pgstat_track_activity_query_size;
	size = add_size(size, hash_estimate_size(pgss_max, entrysize));

	return size;
}

/*
 * Allocate a new hashtable entry.
 * caller must hold an exclusive lock on pgss->lock
 *
 * Note: despite needing exclusive lock, it's not an error for the target
 * entry to already exist.	This is because pgss_store releases and
 * reacquires lock after failing to find a match; so someone else could
 * have made the entry while we waited to get exclusive lock.
 */
static pgssEntry *
entry_alloc(pgssHashKey *key, const char* query, int new_query_len)
{
	pgssEntry  *entry;
	bool		found;

	/* Make space if needed */
	while (hash_get_num_entries(pgss_hash) >= pgss_max)
		entry_dealloc();

	/* Find or create an entry with desired hash code */
	entry = (pgssEntry *) hash_search(pgss_hash, key, HASH_ENTER, &found);

	if (!found)
	{
		/* New entry, initialize it */

		entry->query_len = new_query_len;
		Assert(entry->query_len > 0);
		/* reset the statistics */
		memset(&entry->counters, 0, sizeof(Counters));
		entry->counters.usage = USAGE_INIT;
		/* re-initialize the mutex each time ... we assume no one using it */
		SpinLockInit(&entry->mutex);
		/* ... and don't forget the query text */
		memcpy(entry->query, query, entry->query_len);
		entry->query[entry->query_len] = '\0';
	}
	/* Caller must have clipped query properly */
	Assert(entry->query_len < pgss->query_size);

	return entry;
}

/*
 * qsort comparator for sorting into increasing usage order
 */
static int
entry_cmp_usage(const void *lhs, const void *rhs)
{
	double		l_usage = (*(pgssEntry * const *) lhs)->counters.usage;
	double		r_usage = (*(pgssEntry * const *) rhs)->counters.usage;

	if (l_usage < r_usage)
		return -1;
	else if (l_usage > r_usage)
		return +1;
	else
		return 0;
}

/*
 * Deallocate least used entries.
 * Caller must hold an exclusive lock on pgss->lock.
 */
static void
entry_dealloc(void)
{
	HASH_SEQ_STATUS hash_seq;
	pgssEntry **entries;
	pgssEntry  *entry;
	int			nvictims;
	int			i;

	/* Sort entries by usage and deallocate USAGE_DEALLOC_PERCENT of them. */

	entries = palloc(hash_get_num_entries(pgss_hash) * sizeof(pgssEntry *));

	i = 0;
	hash_seq_init(&hash_seq, pgss_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		entries[i++] = entry;
		entry->counters.usage *= USAGE_DECREASE_FACTOR;
	}

	qsort(entries, i, sizeof(pgssEntry *), entry_cmp_usage);
	nvictims = Max(10, i * USAGE_DEALLOC_PERCENT / 100);
	nvictims = Min(nvictims, i);

	for (i = 0; i < nvictims; i++)
	{
		hash_search(pgss_hash, &entries[i]->key, HASH_REMOVE, NULL);
	}

	pfree(entries);
}

/*
 * Release all entries.
 */
static void
entry_reset(void)
{
	HASH_SEQ_STATUS hash_seq;
	pgssEntry  *entry;

	LWLockAcquire(pgss->lock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, pgss_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		hash_search(pgss_hash, &entry->key, HASH_REMOVE, NULL);
	}

	LWLockRelease(pgss->lock);


}
