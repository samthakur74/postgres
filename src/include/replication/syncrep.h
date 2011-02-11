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
#define StandbyOffersSyncRepService()	(sync_rep_service)

/*
 * There is no reply from standby to primary for async mode, so the reply
 * message needs one less slot than the maximum number of modes
 */
#define NUM_SYNC_REP_WAIT_MODES		1

extern XLogRecPtr ReplyLSN[NUM_SYNC_REP_WAIT_MODES];

/*
 * Each synchronous rep wait mode has one SyncRepWaitQueue in shared memory.
 * These queues live in the WAL sender shmem area.
 */
typedef struct SyncRepQueue
{
	/*
	 * Current location of the head of the queue. Nobody should be waiting
	 * on the queue for an lsn equal to or earlier than this value. Procs
	 * on the queue will always be later than this value, though we don't
	 * record those values here.
	 */
	XLogRecPtr	lsn;

	PGPROC		*head;
	PGPROC		*tail;

	slock_t		qlock;			/* locks shared variables shown above */
} SyncRepQueue;

/* user-settable parameters for synchronous replication */
extern bool sync_rep_mode;
extern int sync_rep_timeout_client;
extern int sync_rep_timeout_server;
extern bool sync_rep_service;

extern bool hot_standby_feedback;

/* called by user backend */
extern void SyncRepWaitForLSN(XLogRecPtr XactCommitLSN);

/* called by wal sender */
extern void SyncRepReleaseWaiters(bool timeout);
extern void SyncRepTimeoutExceeded(void);

/* callback at exit */
extern void SyncRepCleanupAtProcExit(int code, Datum arg);

#endif   /* _SYNCREP_H */
