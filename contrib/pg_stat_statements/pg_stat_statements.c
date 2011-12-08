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
 * that Constants are canonicalized, and various exteneous information is
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
#include "optimizer/planner.h"
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
#define JUMBLE_SIZE				1024	    /* query serialization buffer size */
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
	char		parsed_jumble[JUMBLE_SIZE]; /* Integers from a query tree */
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
	int offset; /* Token offset in query string */
	int len;
} pgssTokenOffset;

/*---- Local variables ----*/
/* Some integers jumbled from last query tree seen */
static char *last_jumble = NULL;
/*
 * Array that represents where
 * normalized characters will be
 */
static pgssTokenOffset *last_offsets = NULL;
/* Length of offsets */
static int last_offset_buf_size = 10;
/* Number of actual offsets currently stored in offsets */
static int last_offset_num = 0;
/* Current nesting depth of ExecutorRun calls */
static int	nested_level = 0;

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static planner_hook_type prev_Planner = NULL;

/* Links to shared memory state */
static pgssSharedState *pgss = NULL;
static HTAB *pgss_hash = NULL;

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
static bool pgss_string;		/* whether to differentiate on query string */

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
static PlannedStmt *pgss_PlannerRun(Query *parse, int cursorOptions, ParamListInfo boundParams);
static int comp_offset(const void *a, const void *b);
static void JumbleCurQuery(Query *parse);
static bool AppendJumb(char* item, char jumble[], size_t size, int *i);
static bool PerformJumble(const Query *parse, char jumble[], size_t size, int *i);
static bool QualsNode(const OpExpr *node, char jumble[], size_t size, int *i, List *rtable);
static bool LeafNodes(const Node *arg, char jumble[], size_t size, int *i, List *rtable);
static bool LimitOffsetNode(const Node *node, char jumble[], size_t size, int *i, List *rtable);
static bool JoinExprNode(JoinExpr *node, char jumble[], size_t size, int *i, List *rtable);
static bool JoinExprNodeChild(const Node *node, char jumble[], size_t size, int *i, List *rtable);
static void pgss_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pgss_ExecutorRun(QueryDesc *queryDesc,
				 ScanDirection direction,
				 long count);
static void pgss_ExecutorFinish(QueryDesc *queryDesc);
static void pgss_ExecutorEnd(QueryDesc *queryDesc);
static void pgss_ProcessUtility(Node *parsetree,
			  const char *queryString, ParamListInfo params, bool isTopLevel,
					DestReceiver *dest, char *completionTag);
static uint32 pgss_hash_fn(const void *key, Size keysize);
static int	pgss_match_fn(const void *key1, const void *key2, Size keysize);
static void pgss_store(const char *query, char parsed_jumble[],
			pgssTokenOffset offs[], int off_n,
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
	DefineCustomBoolVariable("pg_stat_statements.string",
			   "Differentiate queries based on query string alone.",
							 NULL,
							 &pgss_string,
							 false,
							 PGC_SUSET,
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
	last_jumble = MemoryContextAlloc(TopMemoryContext, JUMBLE_SIZE);
	/* Allocate space for bookkeeping information for query str normalization */
	last_offsets = MemoryContextAlloc(TopMemoryContext, last_offset_buf_size * sizeof(pgssTokenOffset));

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
	/*
	 * Install hooks for query normalization
	 */
	prev_Planner = planner_hook;
	planner_hook = pgss_PlannerRun;
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
	prev_Planner = planner_hook;

	pfree(last_jumble);
	pfree(last_offsets);
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
 * pgss_PlannerRun: Selectively serialize the query tree for the purposes of
 * query normalization.
 */
PlannedStmt *
pgss_PlannerRun(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	if (!parse->utilityStmt && !pgss_string)
	{
		/*
		 * We deal with utility statements by hashing the query str
		 * directly
		 */
		JumbleCurQuery(parse);
	}
	/* Invoke the planner */
	if (prev_Planner)
		return prev_Planner(parse, cursorOptions, boundParams);
	else
		return standard_planner(parse, cursorOptions, boundParams);
}

static int comp_offset(const void *a, const void *b)
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

/*
 * JumbleCurQuery: Selectively serialize query tree, and store it until
 * needed to hash the current query
 */
static void JumbleCurQuery(Query *parse)
{
	int i = 0;
	last_offset_num = 0;
	memset(last_jumble, 0, JUMBLE_SIZE);
	last_jumble[++i] = HASH_BUF;
	PerformJumble(parse, last_jumble, JUMBLE_SIZE, &i);
	/* Sort offsets for query string normalization */
	qsort(last_offsets, last_offset_num, sizeof(pgssTokenOffset), comp_offset);
}

/*
 * AppendJumb: Append a given value that is substantive to a given
 * query to jumble, while incrementing the iterator.
 */
static bool AppendJumb(char* item, char jumble[], size_t size, int *i)
{
	if (size + *i >= JUMBLE_SIZE)
	{
		/*
		 * While byte-for-byte, the jumble representation will often be a lot more
		 * compact than the query string, it's possible JUMBLE_SIZE has been set to
		 * a very small value. Besides, it's always possible to contrive a query
		 * where it won't be as compact, such as
		 * "select * from table_with_many_columns"
		 */
		if (size + *i >= JUMBLE_SIZE * 2)
			/* Give up completely, lest we overflow the stack */
			return false;
		/*
		 * Don't append any more, but keep walking the tree to find Const nodes to
		 * canonicalize. Having done all we can with the jumble, we must still
		 * concern ourselves with the normalized query str.
		 */
		*i += size;
		return true;
	}
	memcpy(jumble + *i, item, size);
	*i += size;
	return true;
}

/*
 * Wrapper around AppendJumb to encapsulate details of serialization
 * of individual elements.
 */
#define APP_JUMB(item) \
if (!AppendJumb((char*)&item, jumble, sizeof(item), i))\
	return false;

/*
 * Simple wrappers around functions so that they return upon reaching
 * end of buffer
 */


#define PERFORM_JUMBLE(item, jumble, size, i) \
if (!PerformJumble(item, jumble, size, i)) \
	return false;\

#define QUALS_NODE(item, jumble, size, i, rtable) \
if (!QualsNode(item, jumble, size, i, rtable)) \
	return false;\

#define LEAF_NODES(item, jumble, size, i, rtable) \
if (!LeafNodes(item, jumble, size, i, rtable)) \
	return false;\

#define LIMIT_OFFSET_NODE(item, jumble, size, i, rtable) \
if (!LimitOffsetNode(item, jumble, size, i, rtable)) \
	return false;\

#define JOIN_EXPR_NODE(item, jumble, size, i, rtable) \
if (!JoinExprNode(item, jumble, size, i, rtable)) \
	return false;\

#define JOIN_EXPR_NODE_CHILD(item, jumble, size, i, rtable) \
if (!JoinExprNodeChild(item, jumble, size, i, rtable)) \
	return false;\

/*
 * PerformJumble: Serialize the query tree "parse" and canonicalize
 * constants, while simply skipping over others that are not essential to the
 * query, such that it is usefully normalized, excluding that which is not
 * essential to the query itself.
 *
 * A guiding principal as to whether two queries should be considered
 * equivalent is whether whatever difference exists between the two queries
 * could be expected to result in two different plans, assuming that all
 * constants have the same selectivity estimate. A Non-obvious example of
 * such a differentiator is a change in the definition of a view referenced
 * by a query. We pointedly serialize the query tree *after* the rewriting
 * stage, so entries in pg_stat_statements accurately represent discrete
 * operations, while not having artefacts from external factors that are not
 * essential to what the query does. Directly hashing plans would have the
 * undesirable side-effect of potentially having totally external factors like
 * planner cost constants differentiate a query, so that particular
 * implementation was not chosen.
 *
 * It is necessary to co-ordinate the hashing of a Query with the subsequent
 * use of the hash within executor hooks. Most queries result in a call to
 * the planner hook and a subsequent set of calls to executor hooks, so they
 * don't require special co-ordination, but prepared statements do.
 *
 * The resulting "jumble" can be hashed to uniquely identify a query that may
 * use different constants in successive calls.
 */
static bool PerformJumble(const Query *parse, char jumble[], size_t size, int *i)
{
	ListCell *l;
	FromExpr* jt = (FromExpr *) parse->jointree;		/* table join tree (FROM and WHERE clauses) */
	FuncExpr *off = (FuncExpr *) parse->limitOffset;	/* # of result tuples to skip (int8 expr) */
	FuncExpr *limcount = (FuncExpr *) parse->limitCount;	/* # of result tuples to skip (int8 expr) */

	APP_JUMB(parse->resultRelation);

	if (parse->intoClause)
	{
		IntoClause *ic = parse->intoClause;
		APP_JUMB(ic->onCommit)
		/* TODO: Serialize more from intoClause */
	}

	/* WITH list (of CommonTableExpr's) */
	foreach(l, parse->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(l);
		Query *cte_query = (Query*) cte->ctequery;
		if (cte_query)
			PERFORM_JUMBLE(cte_query, jumble, size, i);
	}
	if (jt)
	{
		if (jt->quals)
		{
			if (IsA(jt->quals, OpExpr))
			{
				QUALS_NODE((OpExpr*) jt->quals, jumble, size, i, parse->rtable);
			}
			else
			{
				LEAF_NODES((Node*) jt->quals, jumble, size, i, parse->rtable);
			}
		}
		/* table join tree */
		foreach(l, jt->fromlist)
		{
			Node* fr = lfirst(l);
			if (IsA(fr, JoinExpr))
			{
				JOIN_EXPR_NODE((JoinExpr*) fr, jumble, size, i, parse->rtable);
			}
			else if (IsA(fr, RangeTblRef))
			{
				RangeTblRef *rtf = (RangeTblRef *) fr;
				RangeTblEntry *rte = rt_fetch(rtf->rtindex, parse->rtable);
				APP_JUMB(rte->relid);
				APP_JUMB(rte->rtekind);
				/* Subselection in where clause */
				if (rte->subquery)
					PERFORM_JUMBLE(rte->subquery, jumble, size, i);

				/* Function call in where clause */
				if (rte->funcexpr)
					LEAF_NODES((Node*) rte->funcexpr, jumble, size, i, parse->rtable);
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
	foreach(l, parse->targetList)
	{
		TargetEntry *tg = (TargetEntry *) lfirst(l);
		Node *e = (Node*) tg->expr;
		if (tg->ressortgroupref)
			APP_JUMB(tg->ressortgroupref); /* nonzero if referenced by a sort/group - for ORDER BY */
		APP_JUMB(tg->resno); /* column number for select */
		/* Handle the various types of nodes in
		 * the select list of this query
		 */
		LEAF_NODES(e, jumble, size, i, parse->rtable);
	}
	/* return-values list (of TargetEntry) */
	foreach(l, parse->returningList)
	{
		TargetEntry *rt = (TargetEntry *) lfirst(l);
		Expr *e = (Expr*) rt->expr;
		/*
		 * Handle the various types of nodes in
		 * the select list of this query
		 */
		if (IsA(e, Var)) /* table column */
		{
			Var *v = (Var*) e;
			RangeTblEntry *rte = rt_fetch(v->varno, parse->rtable);
			APP_JUMB(rte->relid);
			APP_JUMB(v->varattno);
		}
		else
		{
			elog(ERROR, "unrecognized node type for returnlist node: %d",
					(int) nodeTag(e));
		}
	}
	/* a list of SortGroupClause's */
	foreach(l, parse->groupClause)
	{
		SortGroupClause *gc = (SortGroupClause *) lfirst(l);
		APP_JUMB(gc->tleSortGroupRef);
		APP_JUMB(gc->nulls_first);
	}

	if (parse->havingQual)
	{
		if (IsA(parse->havingQual, OpExpr))
		{
			OpExpr *na = (OpExpr *) parse->havingQual;
			QUALS_NODE(na,  jumble, size, i, parse->rtable);
		}
		else
		{
			elog(ERROR, "unrecognized node type for havingclause node: %d",
					(int) nodeTag(parse->havingQual));
		}
	}
	foreach(l, parse->windowClause)
	{
		WindowClause *wc = (WindowClause *) lfirst(l);
		ListCell *il;
		APP_JUMB(wc->frameOptions);
		foreach(il, wc->partitionClause) /* PARTITION BY list */
		{
			Node *n = (Node *) lfirst(il);
			LEAF_NODES(n, jumble, size, i, parse->rtable);
		}
		foreach(il, wc->orderClause) /* ORDER BY list */
		{
			Node *n = (Node *) lfirst(il);
			LEAF_NODES(n, jumble, size, i, parse->rtable);
		}
	}

	foreach(l, parse->distinctClause)
	{
		SortGroupClause *dc = (SortGroupClause *) lfirst(l);
		APP_JUMB(dc->tleSortGroupRef);
		APP_JUMB(dc->nulls_first);
	}

	/* Don't look at parse->sortClause,
	 * because the value ressortgroupref is already
	 * serialized when we iterate through targetList
	 */

	if (off)
		LIMIT_OFFSET_NODE((Node*) off, jumble, size, i, parse->rtable);

	if (limcount)
		LIMIT_OFFSET_NODE((Node*) limcount, jumble, size, i, parse->rtable);

	foreach(l, parse->rowMarks)
	{
		RowMarkClause *rc = (RowMarkClause *) lfirst(l);
		APP_JUMB(rc->rti);				/* range table index of target relation */
		APP_JUMB(rc->forUpdate);			/* true = FOR UPDATE, false = FOR SHARE */
		APP_JUMB(rc->noWait);				/* NOWAIT option */
		APP_JUMB(rc->pushedDown);			/* pushed down from higher query level? */
	}

	if (parse->setOperations)
	{
		/*
		 * set-operation tree if this is top
		 * level of a UNION/INTERSECT/EXCEPT query
		 */
		SetOperationStmt *topop = (SetOperationStmt *) parse->setOperations;
		APP_JUMB(topop->op);
		APP_JUMB(topop->all);

		/* leaf selects are RTE subselections */
		foreach(l, parse->rtable)
		{
			RangeTblEntry *rte = (RangeTblEntry *) lfirst(l);
			if (rte->subquery)
				PERFORM_JUMBLE(rte->subquery, jumble, size, i);
		}
	}
	return true;
}

/*
 * Perform selective serialization of "Quals" nodes when
 * they're IsA(*, OpExpr)
 */
static bool QualsNode(const OpExpr *node, char jumble[], size_t size, int *i, List *rtable)
{
	ListCell *l;
	APP_JUMB(node->opno);
	foreach(l, node->args)
	{
		Node *arg = (Node *) lfirst(l);
		LEAF_NODES(arg, jumble, size, i, rtable);
	}
	return true;
}

/*
 * SerLeafNodes: Serialize a selection of parser/prim nodes that are frequently,
 * though certainly not necesssarily leaf nodes, such as Variables (columns),
 * constants and function calls
 */
static bool LeafNodes(const Node *arg, char jumble[], size_t size, int *i, List *rtable)
{
	ListCell *l;
	if (IsA(arg, Const))
	{
		/*
		 * Serialize generic magic value to
		 * normalize constants
		 */
		char magic = 0xC0;
		Const *c = (Const *) arg;
		APP_JUMB(magic);
		/* Datatype of the constant is a
		 * differentiator
		 */
		APP_JUMB(c->consttype);
		/* Some Const nodes naturally don't have a location.
		 * Also, views that have constants in their
		 * definitions will have a tok_len of 0
		 */
		if (c->location > 0 && c->tok_len > 0)
		{
			if (last_offset_num >= last_offset_buf_size)
			{
				last_offset_buf_size *= 2;
				last_offsets = repalloc(last_offsets, last_offset_buf_size * sizeof(pgssTokenOffset));
			}
			last_offsets[last_offset_num].offset = c->location;
			last_offsets[last_offset_num].len = c->tok_len;
			/* should be added in the same order that as query string */
			last_offset_num++;
		}
	}
	else if (IsA(arg, Var))
	{
		char magic = 0xFA;
		Var *v = (Var *) arg;
		RangeTblEntry *rte = rt_fetch(v->varno, rtable);
		APP_JUMB(magic);
		APP_JUMB(rte->relid);
		APP_JUMB(v->varattno); /* column number of table */
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
		WindowFunc *wf =  (WindowFunc *) arg;
		APP_JUMB(wf->winfnoid);
		foreach(l, wf->args)
		{
			Node *arg = (Node *) lfirst(l);
			LEAF_NODES(arg, jumble, size, i, rtable);
		}
	}
	else if (IsA(arg, FuncExpr))
	{
		FuncExpr *f =  (FuncExpr *) arg;
		APP_JUMB(f->funcid);
		foreach(l, f->args)
		{
			Node *arg = (Node *) lfirst(l);
			LEAF_NODES(arg, jumble, size, i, rtable);
		}
	}
	else if (IsA(arg, OpExpr))
	{
		QUALS_NODE((OpExpr*) arg, jumble, size, i, rtable);
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
			LEAF_NODES(arg, jumble, size, i, rtable);
		}
	}
	else if (IsA(arg, SubLink))
	{
		SubLink *s = (SubLink*) arg;
		APP_JUMB(s->subLinkType);
		/* Serialize select-list subselect recursively */
		if (s->subselect)
			PERFORM_JUMBLE( (Query*) s->subselect, jumble, size, i);
	}
	else if (IsA(arg, TargetEntry))
	{
		TargetEntry *rt = (TargetEntry *) arg;
		Node *e = (Node*) rt->expr;
		APP_JUMB(rt->resorigtbl); /* OID of column's source table */
		APP_JUMB(rt->ressortgroupref); /*  nonzero if referenced by a sort/group - for ORDER BY */
		LEAF_NODES(e, jumble, size, i, rtable);
	}
	else if (IsA(arg, BoolExpr))
	{
		BoolExpr *be = (BoolExpr *) arg;
		APP_JUMB(be->boolop);
		foreach(l, be->args)
		{
			Node *arg = (Node *) lfirst(l);
			LEAF_NODES(arg, jumble, size, i, rtable);
		}
	}
	else if (IsA(arg, NullTest))
	{
		NullTest *nt = (NullTest *) arg;
		Node *arg = (Node *) nt->arg;
		APP_JUMB(nt->nulltesttype);	/* IS NULL, IS NOT NULL */
		APP_JUMB(nt->argisrow);		/* is input a composite type ? */
		LEAF_NODES(arg, jumble, size, i, rtable);
	}
	else if (IsA(arg, ArrayExpr))
	{
		ArrayExpr *ae = (ArrayExpr *) arg;
		APP_JUMB(ae->array_typeid);	/* type of expression result */
		APP_JUMB(ae->element_typeid);	/* common type of array elements */
		foreach(l, ae->elements)
		{
			Node *arg = (Node *) lfirst(l);
			LEAF_NODES(arg, jumble, size, i, rtable);
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
			LEAF_NODES(arg, jumble, size, i, rtable);
		}
		if (ce->arg)
			LEAF_NODES((Node*) ce->arg, jumble, size, i, rtable);

		if (ce->defresult)
		{
			/* Default result (ELSE clause)
			 * The ptr may be NULL, because no else clause
			 * was actually specified, and thus the value is
			 * equivalent to SQL ELSE NULL
			 */
			LEAF_NODES((Node*) ce->defresult, jumble, size, i, rtable); /* the default result (ELSE clause) */
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
		Node *res = (Node*) cw->result;
		Node *exp = (Node*) cw->expr;
		if (res)
			LEAF_NODES(res, jumble, size, i, rtable);
		if (exp)
			LEAF_NODES(exp, jumble, size, i, rtable);
	}
	else if (IsA(arg, MinMaxExpr))
	{
		MinMaxExpr *cw = (MinMaxExpr*) arg;
		APP_JUMB(cw->minmaxtype);
		APP_JUMB(cw->op);
		foreach(l, cw->args)
		{
			Node *arg = (Node *) lfirst(l);
			LEAF_NODES(arg, jumble, size, i, rtable);
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
			LEAF_NODES(arg, jumble, size, i, rtable);
		}
	}
	else if (IsA(arg, CoalesceExpr))
	{
		CoalesceExpr *ca = (CoalesceExpr*) arg;
		foreach(l, ca->args)
		{
			Node *arg = (Node *) lfirst(l);
			LEAF_NODES(arg, jumble, size, i, rtable);
		}
	}
	else if (IsA(arg, ArrayCoerceExpr))
	{
		ArrayCoerceExpr *ac = (ArrayCoerceExpr *) arg;
		LEAF_NODES((Node*) ac->arg, jumble, size, i, rtable);
	}
	else if (IsA(arg, WindowClause))
	{
		WindowClause *wc = (WindowClause*) arg;
		foreach(l, wc->partitionClause)
		{
			Node *arg = (Node *) lfirst(l);
			LEAF_NODES(arg, jumble, size, i, rtable);
		}
		foreach(l, wc->orderClause)
		{
			Node *arg = (Node *) lfirst(l);
			LEAF_NODES(arg, jumble, size, i, rtable);
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
		/* It is not necessary to
		 * serialize integral values
		 */
	}
	else if (IsA(arg, BooleanTest))
	{
		BooleanTest *bt = (BooleanTest *) arg;
		APP_JUMB(bt->booltesttype);
		LEAF_NODES((Node*) bt->arg, jumble, size, i, rtable);
	}
	else if (IsA(arg, ArrayRef))
	{
		ArrayRef *ar = (ArrayRef*) arg;
		APP_JUMB(ar->refarraytype);
		foreach(l, ar->refupperindexpr)
		{
			Node *arg = (Node *) lfirst(l);
			LEAF_NODES(arg, jumble, size, i, rtable);
		}
		foreach(l, ar->reflowerindexpr)
		{
			Node *arg = (Node *) lfirst(l);
			LEAF_NODES(arg, jumble, size, i, rtable);
		}
	}
	else if (IsA(arg, NullIfExpr))
	{
		/* NullIfExpr is just a typedef for OpExpr */
		QUALS_NODE((OpExpr*) arg, jumble, size, i, rtable);
	}
	else if (IsA(arg, RowExpr))
	{
		RowExpr *re = (RowExpr*) arg;
		APP_JUMB(re->row_format);
		foreach(l, re->args)
		{
			Node *arg = (Node *) lfirst(l);
			LEAF_NODES(arg, jumble, size, i, rtable);
		}

	}
	else if (IsA(arg, XmlExpr))
	{
		XmlExpr *xml = (XmlExpr*) arg;
		APP_JUMB(xml->op);
		foreach(l, xml->args)
		{
			Node *arg = (Node *) lfirst(l);
			LEAF_NODES(arg, jumble, size, i, rtable);
		}
		foreach(l, xml->named_args) /* non-XML expressions for xml_attributes */
		{
			Node *arg = (Node *) lfirst(l);
			LEAF_NODES(arg, jumble, size, i, rtable);
		}
		foreach(l, xml->arg_names) /* parallel list of Value strings */
		{
			Node *arg = (Node *) lfirst(l);
			LEAF_NODES(arg, jumble, size, i, rtable);
		}
	}
	else if (IsA(arg, RowCompareExpr))
	{
		RowCompareExpr *rc = (RowCompareExpr*) arg;
		foreach(l, rc->largs)
		{
			Node *arg = (Node *) lfirst(l);
			LEAF_NODES(arg, jumble, size, i, rtable);
		}
		foreach(l, rc->rargs)
		{
			Node *arg = (Node *) lfirst(l);
			LEAF_NODES(arg, jumble, size, i, rtable);
		}
	}
	else
	{
		elog(ERROR, "unrecognized node type for LeafNodes node: %d",
				(int) nodeTag(arg));
	}
	return true;
}

/*
 * Perform selective serialization of limit or offset nodes
 */
static bool LimitOffsetNode(const Node *node, char jumble[], size_t size, int *i, List *rtable)
{
	ListCell *l;
	if (IsA(node, FuncExpr))
	{
		foreach(l, ((FuncExpr*) node)->args)
		{
			Node *arg = (Node *) lfirst(l);
			LEAF_NODES(arg, jumble, size, i, rtable);
		}
	}
	else if (IsA(node, Const))
	{
		/* This should be a differentiator, as it results in the addition of a limit node */
		char magic = 0xEA;
		APP_JUMB(magic);
		return true;
	}
	else
	{
		/* Fall back on leaf node representation */
		LEAF_NODES(node, jumble, size, i, rtable);
	}
	return true;
}

/*
 * Perform selective serialization of JoinExpr nodes
 */
static bool JoinExprNode(JoinExpr *node, char jumble[], size_t size, int *i, List *rtable)
{
	Node	   *larg = node->larg;	/* left subtree */
	Node	   *rarg = node->rarg;	/* right subtree */
	ListCell *l;

	Assert( IsA(node, JoinExpr));

	APP_JUMB(node->jointype);
	APP_JUMB(node->isNatural);

	if (node->quals)
	{
		if ( IsA(node, OpExpr))
		{
			QUALS_NODE((OpExpr*) node->quals, jumble, size, i, rtable);
		}
		else
		{
			LEAF_NODES((Node*) node->quals, jumble, size, i, rtable);
		}

	}
	foreach(l, node->usingClause) /* USING clause, if any (list of String) */
	{
		Node *arg = (Node *) lfirst(l);
		LEAF_NODES(arg, jumble, size, i, rtable);
	}
	if (larg)
		JOIN_EXPR_NODE_CHILD(larg, jumble, size, i, rtable);
	if (rarg)
		JOIN_EXPR_NODE_CHILD(rarg, jumble, size, i, rtable);

	return true;
}

/*
 * Serialize children of the JoinExpr node
 */
static bool JoinExprNodeChild(const Node *node, char jumble[], size_t size, int *i, List *rtable)
{
	if (IsA(node, RangeTblRef))
	{
		RangeTblRef *rt = (RangeTblRef*) node;
		RangeTblEntry *rte = rt_fetch(rt->rtindex, rtable);
		ListCell *l;

		APP_JUMB(rte->relid);
		APP_JUMB(rte->jointype);

		if (rte->subquery)
			PERFORM_JUMBLE(rte->subquery, jumble, size, i);

		foreach(l, rte->joinaliasvars)
		{
			Node *arg = (Node *) lfirst(l);
			LEAF_NODES(arg, jumble, size, i, rtable);
		}
	}
	else if (IsA(node, JoinExpr))
	{
		JOIN_EXPR_NODE((JoinExpr*) node, jumble, size, i, rtable);
	}
	else
	{
		LEAF_NODES(node, jumble, size, i, rtable);
	}
	return true;
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
		/*
		 * Make sure stats accumulation is done.  (Note: it's okay if several
		 * levels of hook all do this.)
		 */
		InstrEndLoop(queryDesc->totaltime);

		if (queryDesc->params || queryDesc->utilitystmt || pgss_string)
		{
			/*
			 * This query was paramaterized or is a utility statement - hash
			 * the query string, as the last call to our planner plugin won't
			 * have hashed this particular query.
			 *
			 * If we're dealing with a parameterized query, the query string
			 * is the original query string anyway, so there is no need to
			 * worry about its stability as would be necessary if this was done
			 * with regular queries that undergo query tree hashing/normalization.
			 *
			 * The fact that constants in the prepared query won't be
			 * canonicalized is clearly a feature rather than a bug, as the
			 * user evidently considers the constant essential to the query, or
			 * they'd have paramaterized it.
			 */
			memset(last_jumble, 0, JUMBLE_SIZE);
			last_jumble[0] = STR_BUF;
			memcpy(last_jumble + 1, queryDesc->sourceText,
				Min(JUMBLE_SIZE - 1, strlen(queryDesc->sourceText) )
				);
		}

		pgss_store(queryDesc->sourceText,
				   last_jumble,
				   last_offsets,
				   last_offset_num,
				   queryDesc->totaltime->total,
				   queryDesc->estate->es_processed,
				   &queryDesc->totaltime->bufusage);

		if (last_offset_buf_size > 10)
		{
			last_offset_buf_size = 10;
			last_offsets = repalloc(last_offsets, last_offset_buf_size * sizeof(pgssTokenOffset));
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
pgss_ProcessUtility(Node *parsetree, const char *queryString,
					ParamListInfo params, bool isTopLevel,
					DestReceiver *dest, char *completionTag)
{
	if (pgss_track_utility && pgss_enabled())
	{
		instr_time	start;
		instr_time	duration;
		uint64		rows = 0;
		BufferUsage bufusage;

		bufusage = pgBufferUsage;
		INSTR_TIME_SET_CURRENT(start);

		nested_level++;
		PG_TRY();
		{
			if (prev_ProcessUtility)
				prev_ProcessUtility(parsetree, queryString, params,
									isTopLevel, dest, completionTag);
			else
				standard_ProcessUtility(parsetree, queryString, params,
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
		memset(last_jumble, 0, JUMBLE_SIZE);
		last_jumble[0] = STR_BUF;
		memcpy(last_jumble + 1, queryString,
			Min(JUMBLE_SIZE - 1, strlen(queryString) )
			);

		pgss_store(queryString, last_jumble, NULL, 0, INSTR_TIME_GET_DOUBLE(duration), rows,
				   &bufusage);
	}
	else
	{
		if (prev_ProcessUtility)
			prev_ProcessUtility(parsetree, queryString, params,
								isTopLevel, dest, completionTag);
		else
			standard_ProcessUtility(parsetree, queryString, params,
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
		DatumGetUInt32(hash_any((const unsigned char* ) k->parsed_jumble, JUMBLE_SIZE) );
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
		memcmp(k1->parsed_jumble, k2->parsed_jumble, JUMBLE_SIZE) == 0)
		return 0;
	else
		return 1;
}

/*
 * Store some statistics for a statement.
 */
static void
pgss_store(const char *query, char parsed_jumble[],
			pgssTokenOffset offs[], int off_n,
			double total_time, uint64 rows,
			const BufferUsage *bufusage)
{
	pgssHashKey key;
	double		usage;
	pgssEntry  *entry;
	int new_query_len = strlen(query);
	char *norm_query = NULL;

	Assert(query != NULL);

	/* Safety check... */
	if (!pgss || !pgss_hash)
		return;

	/* Set up key for hashtable search */
	key.userid = GetUserId();
	key.dbid = MyDatabaseId;
	key.encoding = GetDatabaseEncoding();
	memcpy(key.parsed_jumble, parsed_jumble, JUMBLE_SIZE);

	if (new_query_len >= pgss->query_size)
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
		/*
		 * It is necessary to generate a normalized version
		 * of the query string that will be used for this query.
		 *
		 * Note that the representation seen by the user will
		 * only have non-differentiating Const tokens swapped
		 * with '?' characters, and does not for example take
		 * account of the fact that alias names could vary
		 * between successive calls of what we regard as the
		 * same query.
		 */
		if (off_n > 0)
		{
			int i,
			  last_off = 0,
			  quer_it = 0,
			  n_quer_it = 0,
			  off = 0,
			  tok_len = 0,
			  len_to_wrt = 0,
			  last_tok_len = 0;
			norm_query = palloc0(new_query_len + 1);
			for(i = 0; i < off_n; i++)
			{
				off = offs[i].offset;
				tok_len = offs[i].len;
				len_to_wrt = off - last_off;
				len_to_wrt -= last_tok_len;
				/*
				 * Each iteration copies everything prior to the current
				 * offset/token to be replaced, except previously copied things
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
			/* Finish off last piece of query string */
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

		/* copy counters to a local variable to keep locking time short */
		{
			volatile pgssEntry *e = (volatile pgssEntry *) entry;

			SpinLockAcquire(&e->mutex);
			tmp = e->counters;
			SpinLockRelease(&e->mutex);
		}

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
		entry->query_len = new_query_len;
		Assert(entry->query_len > 0);
		/* New entry, initialize it */

		/* reset the statistics */
		memset(&entry->counters, 0, sizeof(Counters));
		entry->counters.usage = USAGE_INIT;
		/* re-initialize the mutex each time ... we assume no one using it */
		SpinLockInit(&entry->mutex);
		/* ... and don't forget the query text */
		/* memset previous entry in the slot */
		memset(entry->query, 0, pgss->query_size);
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
