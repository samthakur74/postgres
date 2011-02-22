/*-------------------------------------------------------------------------
 *
 * syncrep.h
 *	  Exports from replication/syncrep.c.
 *
 * Portions Copyright (c) 2010-2010, PostgreSQL Global Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef _SYNCREP_H
#define _SYNCREP_H

#include "access/xlog.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/spin.h"

#define SyncRepRequested()				(sync_rep_mode)

/*
 * Each synchronous rep queue lives in the WAL sender shmem area.
 */
typedef struct SyncRepQueue
{
	/*
	 * Current location of the head of the queue. All waiters should have
	 * a waitLSN that follows this value, or they are currently being woken
	 * to remove themselves from the queue.
	 */
	XLogRecPtr	lsn;

	PGPROC		*head;
	PGPROC		*tail;

	slock_t		qlock;			/* locks shared variables shown above */
} SyncRepQueue;

/* user-settable parameters for synchronous replication */
extern bool sync_rep_mode;
extern int 	sync_rep_timeout_client;
extern bool allow_standalone_primary;
extern char *SyncRepStandbyNames;

/* called by user backend */
extern void SyncRepWaitForLSN(XLogRecPtr XactCommitLSN);

/* called by wal sender */
extern void SyncRepInitConfig(void);
extern void SyncRepReleaseWaiters(void);

/* callback at exit */
extern void SyncRepCleanupAtProcExit(int code, Datum arg);

#endif   /* _SYNCREP_H */
