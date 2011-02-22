/*-------------------------------------------------------------------------
 *
 * syncrep.c
 *
 * Synchronous replication is new as of PostgreSQL 9.1.
 *
 * If requested, transaction commits wait until their commit LSN is
 * acknowledged by the standby, or the wait hits timeout.
 *
 * This module contains the code for waiting and release of backends.
 * All code in this module executes on the primary. The core streaming
 * replication transport remains within WALreceiver/WALsender modules.
 *
 * The essence of this design is that it isolates all logic about
 * waiting/releasing onto the primary. The primary defines which standbys
 * it wishes to wait for. The standby is completely unaware of the
 * durability requirements of transactions on the primary, reducing the
 * complexity of the code and streamlining both standby operations and
 * network bandwidth because there is no requirement to ship
 * per-transaction state information.
 *
 * The bookeeping approach we take is that a commit is either synchronous
 * or not synchronous (async). If it is async, we just fastpath out of
 * here. If it is sync, then in 9.1 we wait for the flush location on the
 * standby before releasing the waiting backend. Further complexity
 * in that interaction is expected in later releases.
 *
 * The best performing way to manage the waiting backends is to have a
 * single ordered queue of waiting backends, so that we can avoid
 * searching the through all waiters each time we receive a reply.
 *
 * Starting sync replication is a multi stage process. First, the standby
 * must be a potential synchronous standby. Next, we must have caught up
 * with the primary; that may take some time. If there is no current
 * synchronous standby then the WALsender will offer a sync rep service.
 *
 * Portions Copyright (c) 2010-2010, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/xact.h"
#include "access/xlog_internal.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "replication/syncrep.h"
#include "replication/walsender.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"

/* User-settable parameters for sync rep */
bool	sync_rep_mode = false;			/* Only set in user backends */
int		sync_rep_timeout_client = 120;	/* Only set in user backends */
bool	allow_standalone_primary;
char 	*SyncRepStandbyNames;


#define	IsOnSyncRepQueue()		(MyProc->lwWaiting)

static void SyncRepWaitOnQueue(XLogRecPtr XactCommitLSN);
static void SyncRepRemoveFromQueue(void);
static void SyncRepAddToQueue(void);
static bool SyncRepServiceAvailable(void);
static long SyncRepGetWaitTimeout(void);

static bool IsPotentialSyncRepStandby(void);
static int SyncRepWakeQueue(void);


/*
 * ===========================================================
 * Synchronous Replication functions for normal user backends
 * ===========================================================
 */

/*
 * Wait for synchronous replication, if requested by user.
 */
extern void
SyncRepWaitForLSN(XLogRecPtr XactCommitLSN)
{
	/*
	 * Fast exit if user has not requested sync replication, or
	 * streaming replication is inactive in this server.
	 */
	if (!SyncRepRequested() || max_wal_senders == 0)
		return;

	if (allow_standalone_primary)
	{
		/*
		 * Check that the service level we want is available.
		 * If not, downgrade the service level to async.
		 */
		if (SyncRepServiceAvailable())
			SyncRepWaitOnQueue(XactCommitLSN);
	}
	else
	{
		/*
		 * Wait, even if service is not yet available.
		 * Sounds weird, but this mode exists to provide
		 * higher levels of protection.
		 */
		SyncRepWaitOnQueue(XactCommitLSN);
	}
}

/*
 * Wait for specified LSN to be confirmed at the requested level
 * of durability. Each proc has its own wait latch, so we perform
 * a normal latch check/wait loop here.
 */
static void
SyncRepWaitOnQueue(XLogRecPtr XactCommitLSN)
{
	volatile WalSndCtlData *walsndctl = WalSndCtl;
	volatile SyncRepQueue *queue = &(walsndctl->sync_rep_queue);
	TimestampTz	now = GetCurrentTransactionStopTimestamp();
	long		timeout = SyncRepGetWaitTimeout();
	char 		*new_status = NULL;
	const char *old_status;
	int			len;

	ereport(DEBUG3,
			(errmsg("synchronous replication waiting for %X/%X starting at %s",
						XactCommitLSN.xlogid,
						XactCommitLSN.xrecoff,
						timestamptz_to_str(GetCurrentTransactionStopTimestamp()))));

	for (;;)
	{
		ResetLatch(&MyProc->waitLatch);

		/*
		 * First time through, add ourselves to the appropriate queue.
		 */
		if (!IsOnSyncRepQueue())
		{
			SpinLockAcquire(&queue->qlock);
			if (XLByteLE(XactCommitLSN, queue->lsn))
			{
				/* No need to wait */
				SpinLockRelease(&queue->qlock);
				return;
			}

			/*
			 * Set our waitLSN so WALSender will know when to wake us.
			 * We set this before we add ourselves to queue, so that
			 * any proc on the queue can be examined freely without
			 * taking a lock on each process in the queue.
			 */
			MyProc->waitLSN = XactCommitLSN;
			SyncRepAddToQueue();
			SpinLockRelease(&queue->qlock);

			/*
			 * Alter ps display to show waiting for sync rep.
			 */
			old_status = get_ps_display(&len);
			new_status = (char *) palloc(len + 21 + 1);
			memcpy(new_status, old_status, len);
			strcpy(new_status + len, " waiting for sync rep");
			set_ps_display(new_status, false);
			new_status[len] = '\0'; /* truncate off " waiting" */
		}
		else
		{
			bool release = false;
			bool timeout = false;

			SpinLockAcquire(&queue->qlock);

			/*
			 * Check the LSN on our queue and if it's moved far enough then
			 * remove us from the queue. First time through this is
			 * unlikely to be far enough, yet is possible. Next time we are
			 * woken we should be more lucky.
			 */
			if (XLByteLE(XactCommitLSN, queue->lsn))
				release = true;
			else if (timeout > 0 &&
				TimestampDifferenceExceeds(GetCurrentTransactionStopTimestamp(),
											now, timeout))
			{
				release = true;
				timeout = true;
			}

			if (release)
			{
				SyncRepRemoveFromQueue();
				SpinLockRelease(&queue->qlock);

				if (new_status)
				{
					/* Reset ps display */
					set_ps_display(new_status, false);
					pfree(new_status);
				}

				/*
				 * Our response to the timeout is to simply post a NOTICE and
				 * then return to the user. The commit has happened, we just
				 * haven't been able to verify it has been replicated in the
				 * way requested.
				 */
				if (timeout)
					ereport(NOTICE,
							(errmsg("synchronous replication timeout at %s",
										timestamptz_to_str(now))));
				else
					ereport(DEBUG3,
							(errmsg("synchronous replication wait complete at %s",
										timestamptz_to_str(now))));
				return;
			}

			SpinLockRelease(&queue->qlock);
		}

		WaitLatch(&MyProc->waitLatch, timeout);
		now = GetCurrentTimestamp();
	}
}

/*
 * Remove myself from sync rep wait queue.
 *
 * Assume on queue at start; will not be on queue at end.
 * Queue is already locked at start and remains locked on exit.
  */
void
SyncRepRemoveFromQueue(void)
{
	volatile WalSndCtlData *walsndctl = WalSndCtl;
	volatile SyncRepQueue *queue = &(walsndctl->sync_rep_queue);
	PGPROC	*proc = queue->head;

	Assert(IsOnSyncRepQueue());

	proc = queue->head;

	if (proc == MyProc)
	{
		if (MyProc->lwWaitLink == NULL)
		{
			/*
			 * We were the only waiter on the queue. Reset head and tail.
			 */
			Assert(queue->tail == MyProc);
			queue->head = NULL;
			queue->tail = NULL;
		}
		else
			/*
			 * Move head to next proc on the queue.
			 */
			queue->head = MyProc->lwWaitLink;
	}
	else
	{
		while (proc->lwWaitLink != NULL)
		{
			/* Are we the next proc in our traversal of the queue? */
			if (proc->lwWaitLink == MyProc)
			{
				/*
				 * Remove ourselves from middle of queue.
				 * No need to touch head or tail.
				 */
				proc->lwWaitLink = MyProc->lwWaitLink;
			}

			if (proc->lwWaitLink == NULL)
				elog(WARNING, "could not locate ourselves on wait queue");
			proc = proc->lwWaitLink;
		}

		if (proc->lwWaitLink == NULL)	/* At tail */
		{
			Assert(proc == MyProc);
			/* Remove ourselves from tail of queue */
			Assert(queue->tail == MyProc);
			queue->tail = proc;
			proc->lwWaitLink = NULL;
		}
	}
	MyProc->lwWaitLink = NULL;
	MyProc->lwWaiting = false;
}

/*
 * Add myself to sync rep wait queue.
 *
 * Assume not on queue at start; will be on queue at end.
 * Queue is already locked at start and remains locked on exit.
 */
static void
SyncRepAddToQueue(void)
{
	volatile WalSndCtlData *walsndctl = WalSndCtl;
	volatile SyncRepQueue *queue = &(walsndctl->sync_rep_queue);
	PGPROC	*tail = queue->tail;

	/*
	 * Add myself to tail of wait queue.
	 */
	if (tail == NULL)
	{
		queue->head = MyProc;
		queue->tail = MyProc;
	}
	else
	{
		/*
		 * XXX extra code needed here to maintain sorted invariant.
		 * Our approach should be same as racing car - slow in, fast out.
		 */
		Assert(tail->lwWaitLink == NULL);
		tail->lwWaitLink = MyProc;
	}
	queue->tail = MyProc;

	MyProc->lwWaiting = true;
	MyProc->lwWaitLink = NULL;
}

/*
 * Dynamically decide the sync rep wait mode. It may seem a trifle
 * wasteful to do this for every transaction but we need to do this
 * so we can cope sensibly with standby disconnections. It's OK to
 * spend a few cycles here anyway, since while we're doing this the
 * WALSender will be sending the data we want to wait for, so this
 * is dead time and the user has requested to wait anyway.
 */
static bool
SyncRepServiceAvailable(void)
{
	bool	 result = false;

	SpinLockAcquire(&WalSndCtl->ctlmutex);
	result = WalSndCtl->sync_rep_service_available;
	SpinLockRelease(&WalSndCtl->ctlmutex);

	return result;
}

/*
 * Return a value that we can use directly in WaitLatch(). We need to
 * handle special values, plus convert from seconds to microseconds.
 *
 */
static long
SyncRepGetWaitTimeout(void)
{
	if (sync_rep_timeout_client <= 0)
		return -1L;

	return 1000000L * sync_rep_timeout_client;
}

void
SyncRepCleanupAtProcExit(int code, Datum arg)
{
	volatile WalSndCtlData *walsndctl = WalSndCtl;
	volatile SyncRepQueue *queue = &(walsndctl->sync_rep_queue);

	if (IsOnSyncRepQueue())
	{
		SpinLockAcquire(&queue->qlock);
		SyncRepRemoveFromQueue();
		SpinLockRelease(&queue->qlock);
	}

	if (MyProc != NULL && MyProc->ownLatch)
	{
		DisownLatch(&MyProc->waitLatch);
		MyProc->ownLatch = false;
	}
}

/*
 * ===========================================================
 * Synchronous Replication functions for wal sender processes
 * ===========================================================
 */

/*
 * Check if we are in the list of sync standbys.
 *
 * Compare the parameter SyncRepStandbyNames against the application_name
 * for this WALSender.
 */
static bool
IsPotentialSyncRepStandby(void)
{
	char	   *rawstring;
	List	   *elemlist;
	ListCell   *l;

	/* Need a modifiable copy of string */
	rawstring = pstrdup(SyncRepStandbyNames);

	/* Parse string into list of identifiers */
	if (!SplitIdentifierString(rawstring, ',', &elemlist))
	{
		/* syntax error in list */
		pfree(rawstring);
		list_free(elemlist);
		ereport(FATAL,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		   errmsg("invalid list syntax for parameter \"synchronous_standby_names\"")));
		return false;
	}

	foreach(l, elemlist)
	{
		char	   *standby_name = (char *) lfirst(l);

		if (pg_strcasecmp(standby_name, application_name) == 0)
			return true;
	}

	return false;
}

/*
 * Take any action required to initialise sync rep state from config
 * data. Called at WALSender startup and after each SIGHUP.
 */
void
SyncRepInitConfig(void)
{
	bool sync_standby = IsPotentialSyncRepStandby();

	/*
	 * Determine if we are a potential sync standby and remember the result
	 * for handling replies from standby.
	 */
	if (!MyWalSnd->potential_sync_standby && sync_standby)
	{
		MyWalSnd->potential_sync_standby = true;
		ereport(DEBUG1,
				(errmsg("standby \"%s\" is a potential synchronous standby",
							application_name)));
	}
	else if (MyWalSnd->potential_sync_standby && !sync_standby)
	{
		/*
		 * We're no longer a potential sync standby.
		 */
		MyWalSnd->potential_sync_standby = false;

		/*
		 * Stop providing the sync rep service, to let another take over.
		 */
		if (MyWalSnd->sync_rep_service)
		{
			/*
			 * Update state for this WAL sender.
			 */
			{
				/* use volatile pointer to prevent code rearrangement */
				volatile WalSnd *walsnd = MyWalSnd;

				SpinLockAcquire(&walsnd->mutex);
				walsnd->sync_rep_service = false;
				SpinLockRelease(&walsnd->mutex);
			}

			/*
			 * Stop providing the sync rep service, even if there are
			 * waiting backends.
			 */
			{
				SpinLockAcquire(&WalSndCtl->ctlmutex);
				WalSndCtl->sync_rep_service_available = false;
				SpinLockRelease(&WalSndCtl->ctlmutex);
			}

			ereport(DEBUG1,
					(errmsg("standby \"%s\" is no longer the synchronous replication standby",
								application_name)));
		}
	}
}

/*
 * Update the LSNs on each queue based upon our latest state. This
 * implements a simple policy of first-valid-standby-releases-waiter.
 *
 * Other policies are possible, which would change what we do here and what
 * perhaps also which information we store as well.
 */
void
SyncRepReleaseWaiters(void)
{
	volatile WalSndCtlData *walsndctl = WalSndCtl;

	/*
	 * If this WALSender is serving a standby that is not on the list of
	 * potential standbys then we have nothing to do.
	 */
	if (!MyWalSnd->potential_sync_standby)
		return;

	/*
	 * We're a potential sync standby. If we aren't yet offering a sync
	 * rep service, check whether we need to begin offering that service.
	 * We check this dynamically to ensure that we can continue to offer
	 * a service if we have multiple potential standbys and the current
	 * sync standby fails.
	 *
	 * We don't attempt to enable sync rep service during a base backup since
	 * during that action we aren't sending WAL at all, so there cannot be
	 * any meaningful replies. We don't enable sync rep service while we
	 * are still in catchup mode either, since clients might experience an
	 * extended wait (perhaps hours) if they waited at that point.
	 */
	if (!MyWalSnd->sync_rep_service &&
		MyWalSnd->state == WALSNDSTATE_STREAMING)
	{
		if (SyncRepServiceAvailable())
		{
			/*
			 * Another WALSender is already providing the sync rep service.
			 */
			return;
		}
		else
		{
			bool	enable_service = false;

			/*
			 * We're a potential sync standby and there isn't currently
			 * a sync standby, so we're now going to become one. Watch for
			 * race conditions here.
			 */
			{
				SpinLockAcquire(&WalSndCtl->ctlmutex);
				if (!WalSndCtl->sync_rep_service_available)
				{
					WalSndCtl->sync_rep_service_available = true;
					enable_service = true;
				}
				SpinLockRelease(&WalSndCtl->ctlmutex);
			}

			/*
			 * Another WALSender just is already providing the sync rep service.
			 */
			if (!enable_service)
				return;

			ereport(DEBUG1,
					(errmsg("standby \"%s\" is now the synchronous replication standby",
								application_name)));

			/*
			 * Update state for this WAL sender.
			 */
			{
				/* use volatile pointer to prevent code rearrangement */
				volatile WalSnd *walsnd = MyWalSnd;

				SpinLockAcquire(&walsnd->mutex);
				walsnd->sync_rep_service = true;
				SpinLockRelease(&walsnd->mutex);
			}
		}
	}

	/*
	 * Maintain queue LSNs and release wakers.
	 */
	{
		volatile SyncRepQueue *queue = &(walsndctl->sync_rep_queue);
		int 	numprocs = 0;

		/*
		 * Lock the queue. Not really necessary with just one sync standby
		 * but it makes clear what needs to happen.
		 */
		SpinLockAcquire(&queue->qlock);
		if (XLByteLT(queue->lsn, MyWalSnd->flush))
		{
			/*
			 * Set the lsn first so that when we wake backends they will
			 * release up to this location.
			 */
			queue->lsn = MyWalSnd->flush;
			numprocs = SyncRepWakeQueue();
		}
		SpinLockRelease(&queue->qlock);

		elog(DEBUG3, "released %d procs up to %X/%X",
						numprocs,
						MyWalSnd->flush.xlogid,
						MyWalSnd->flush.xrecoff);
	}
}

/*
 * Walk queue from head setting the latches of any procs that need
 * to be woken. We don't modify the queue, we leave that for individual
 * procs to release themselves.
 *
 * Must hold spinlock on queue.
 */
static int
SyncRepWakeQueue(void)
{
	volatile WalSndCtlData *walsndctl = WalSndCtl;
	volatile SyncRepQueue *queue = &(walsndctl->sync_rep_queue);
	PGPROC	*proc = queue->head;
	int		numprocs = 0;

	/* fast exit for empty queue */
	if (proc == NULL)
		return 0;

	for (; proc != NULL; proc = proc->lwWaitLink)
	{
		/*
		 * Assume the queue is ordered by LSN
		 */
		if (XLByteLT(queue->lsn, proc->waitLSN))
			return numprocs;

		numprocs++;
		SetLatch(&proc->waitLatch);
	}

	return numprocs;
}
