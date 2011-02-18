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
 * waiting/releasing onto the primary. The primary is aware of which
 * standby servers offer a synchronisation service. The standby is
 * completely unaware of the durability requirements of transactions
 * on the primary, reducing the complexity of the code and streamlining
 * both standby operations and network bandwidth because there is no
 * requirement to ship per-transaction state information.
 *
 * The bookeeping approach we take is that a commit is either synchronous
 * or not synchronous (async). If it is async, we just fastpath out of
 * here. If it is sync, then it follows exactly one rigid definition of
 * synchronous replication as laid out by the various parameters. If we
 * change the definition of replication, we'll need to scan through all
 * waiting backends to see if we should now release them.
 *
 * The best performing way to manage the waiting backends is to have a
 * single ordered queue of waiting backends, so that we can avoid
 * searching the through all waiters each time we receive a reply.
 *
 * Starting sync replication is a two stage process. First, the standby
 * must have caught up with the primary; that may take some time. Next,
 * we must receive a reply from the standby before we change state so
 * that sync rep is fully active and commits can wait on us.
 *
 * XXX Changing state to a sync rep service while we are running allows
 * us to enable sync replication via SIGHUP on the standby at a later
 * time, without restart, if we need to do that. Though you can't turn
 * it off without disconnecting.
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
#include "utils/guc.h"
#include "utils/guc_tables.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"


/* User-settable parameters for sync rep */
bool		sync_rep_mode = false;				/* Only set in user backends */
int			sync_rep_timeout_client = 120;	/* Only set in user backends */
int			sync_rep_timeout_server = 30;	/* Only set in user backends */
bool		sync_rep_service = false;			/* Never set in user backends */
bool 		hot_standby_feedback = true;

/*
 * Queuing code is written to allow later extension to multiple
 * queues. Currently, we use just one queue (==FSYNC).
 *
 * XXX We later expect to have RECV, FSYNC and APPLY modes.
 */
#define SYNC_REP_NOT_ON_QUEUE 	-1
#define SYNC_REP_FSYNC			0
#define	IsOnSyncRepQueue()		(current_queue > SYNC_REP_NOT_ON_QUEUE)
/*
 * Queue identifier of the queue on which user backend currently waits.
 */
static int current_queue = SYNC_REP_NOT_ON_QUEUE;

static void SyncRepWaitOnQueue(XLogRecPtr XactCommitLSN, int qid);
static void SyncRepRemoveFromQueue(void);
static void SyncRepAddToQueue(int qid);
static bool SyncRepServiceAvailable(void);
static long SyncRepGetWaitTimeout(void);

static void SyncRepWakeFromQueue(int wait_queue, XLogRecPtr lsn);


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
	 * Fast exit if user has requested async replication, or
	 * streaming replication is inactive in this server.
	 */
	if (max_wal_senders == 0 || !sync_rep_mode)
		return;

	Assert(sync_rep_mode);

	if (allow_standalone_primary)
	{
		bool	avail_sync_mode;

		/*
		 * Check that the service level we want is available.
		 * If not, downgrade the service level to async.
		 */
		avail_sync_mode = SyncRepServiceAvailable();

		/*
		 * Perform the wait here, then drop through and exit.
		 */
		if (avail_sync_mode)
			SyncRepWaitOnQueue(XactCommitLSN, 0);
	}
	else
	{
		/*
		 * Wait only on the service level requested,
		 * whether or not it is currently available.
		 * Sounds weird, but this mode exists to protect
		 * against changes that will only occur on primary.
		 */
		SyncRepWaitOnQueue(XactCommitLSN, 0);
	}
}

/*
 * Wait for specified LSN to be confirmed at the requested level
 * of durability. Each proc has its own wait latch, so we perform
 * a normal latch check/wait loop here.
 */
static void
SyncRepWaitOnQueue(XLogRecPtr XactCommitLSN, int qid)
{
	volatile WalSndCtlData *walsndctl = WalSndCtl;
	volatile SyncRepQueue *queue = &(walsndctl->sync_rep_queue[0]);
	TimestampTz	now = GetCurrentTransactionStopTimestamp();
	long		timeout = SyncRepGetWaitTimeout();	/* seconds */
	char 		*new_status = NULL;
	const char *old_status;
	int			len;

	/*
	 * No need to wait for autovacuums. If the standby does go away and
	 * we wait for it to return we may as well do some usefulwork locally.
	 * This is critical since we may need to perform emergency vacuuming
	 * and cannot wait for standby to return.
	 */
	if (IsAutoVacuumWorkerProcess())
		return;

	ereport(DEBUG2,
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
			SyncRepAddToQueue(qid);
			SpinLockRelease(&queue->qlock);
			current_queue = qid; /* Remember which queue we're on */

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
			 * Check the LSN on our queue and if its moved far enough then
			 * remove us from the queue. First time through this is
			 * unlikely to be far enough, yet is possible. Next time we are
			 * woken we should be more lucky.
			 */
			if (XLByteLE(XactCommitLSN, queue->lsn))
				release = true;
			else if (timeout > 0 &&
				TimestampDifferenceExceeds(GetCurrentTransactionStopTimestamp(),
										now,
										timeout))
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
				 * haven't been able to verify it has been replicated to the
				 * level requested.
				 *
				 * XXX We could check here to see if our LSN has been sent to
				 * another standby that offers a lower level of service. That
				 * could be true if we had, for example, requested 'apply'
				 * with two standbys, one at 'apply' and one at 'recv' and the
				 * apply standby has just gone down. Something for the weekend.
				 */
				if (timeout)
					ereport(NOTICE,
							(errmsg("synchronous replication timeout at %s",
										timestamptz_to_str(now))));
				else
					ereport(DEBUG2,
							(errmsg("synchronous replication wait complete at %s",
										timestamptz_to_str(now))));

				/* XXX Do we need to unset the latch? */
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
 *
 * XXX Implements design pattern "Reinvent Wheel", think about changing
 */
void
SyncRepRemoveFromQueue(void)
{
	volatile WalSndCtlData *walsndctl = WalSndCtl;
	volatile SyncRepQueue *queue = &(walsndctl->sync_rep_queue[current_queue]);
	PGPROC	*proc = queue->head;
	int 	numprocs = 0;

	Assert(IsOnSyncRepQueue());

#ifdef SYNCREP_DEBUG
	elog(DEBUG3, "removing myself from queue %d", current_queue);
#endif

	for (; proc != NULL; proc = proc->lwWaitLink)
	{
		if (proc == MyProc)
		{
			elog(LOG, "proc %d lsn %X/%X is MyProc",
						numprocs,
						proc->waitLSN.xlogid,
						proc->waitLSN.xrecoff);
		}
		else
		{
			elog(LOG, "proc %d lsn %X/%X",
						numprocs,
						proc->waitLSN.xlogid,
						proc->waitLSN.xrecoff);
		}
		numprocs++;
	}

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
	current_queue = SYNC_REP_NOT_ON_QUEUE;
}

/*
 * Add myself to sync rep wait queue.
 *
 * Assume not on queue at start; will be on queue at end.
 * Queue is already locked at start and remains locked on exit.
 */
static void
SyncRepAddToQueue(int qid)
{
	volatile WalSndCtlData *walsndctl = WalSndCtl;
	volatile SyncRepQueue *queue = &(walsndctl->sync_rep_queue[qid]);
	PGPROC	*tail = queue->tail;

#ifdef SYNCREP_DEBUG
	elog(DEBUG3, "adding myself to queue %d", qid);
#endif

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

	/*
	 * This used to be an Assert, but it keeps failing... why?
	 */
	MyProc->lwWaitLink = NULL;	/* to be sure */
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
 * Allows more complex decision making about what the wait time should be.
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
/*
	volatile WalSndCtlData *walsndctl = WalSndCtl;
	volatile SyncRepQueue *queue = &(walsndctl->sync_rep_queue[qid]);

	if (IsOnSyncRepQueue())
	{
		SpinLockAcquire(&queue->qlock);
		SyncRepRemoveFromQueue();
		SpinLockRelease(&queue->qlock);
	}
*/

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
 * Update the LSNs on each queue based upon our latest state. This
 * implements a simple policy of first-valid-standby-releases-waiter.
 *
 * Other policies are possible, which would change what we do here and what
 * perhaps also which information we store as well.
 */
void
SyncRepReleaseWaiters(bool timeout)
{
	volatile WalSndCtlData *walsndctl = WalSndCtl;
	int mode;

	/*
	 * If we are now streaming, and haven't yet enabled the sync rep service
	 * do so now. We don't enable sync rep service during a base backup since
	 * during that action we aren't sending WAL at all, so there cannot be
	 * any meaningful replies. We don't enable sync rep service while we
	 * are still in catchup mode either, since clients might experience an
	 * extended wait (perhaps hours) if they waited at that point.
	 *
	 * Note that we do release waiters, even if they aren't enabled yet.
	 * That sounds strange, but we may have dropped the connection and
	 * reconnected, so there may still be clients waiting for a response
	 * from when we were connected previously.
	 *
	 * If we already have a sync rep server connected, don't enable
	 * this server as well.
	 *
	 * XXX expect to be able to support multiple sync standbys in future.
	 */
	if (!MyWalSnd->sync_rep_service &&
		MyWalSnd->state == WALSNDSTATE_STREAMING &&
		!SyncRepServiceAvailable())
	{
		ereport(LOG,
				(errmsg("enabling synchronous replication service for standby")));

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

		/*
		 * We have at least one standby, so we're open for business.
		 */
		{
			SpinLockAcquire(&WalSndCtl->ctlmutex);
			WalSndCtl->sync_rep_service_available = true;
			SpinLockRelease(&WalSndCtl->ctlmutex);
		}

		/*
		 * Let postmaster know we can allow connections, if the user
		 * requested waiting until sync rep was active before starting.
		 * We send this unconditionally to avoid more complexity in
		 * postmaster code.
		 */
		if (IsUnderPostmaster)
			SendPostmasterSignal(PMSIGNAL_SYNC_REPLICATION_ACTIVE);
	}

	/*
	 * No point trying to release waiters while doing a base backup
	 */
	if (MyWalSnd->state == WALSNDSTATE_BACKUP)
		return;

#ifdef SYNCREP_DEBUG
	elog(LOG, "releasing waiters up to flush = %X/%X",
					MyWalSnd->flush.xlogid, MyWalSnd->flush.xrecoff);
#endif


	/*
	 * Only maintain LSNs of queues for which we advertise a service.
	 * This is important to ensure that we only wakeup users when a
	 * preferred standby has reached the required LSN.
	 *
	 * Since sycnhronous_replication_mode is currently a boolean, we either
	 * offer all modes, or none.
	 */
	for (mode = 0; mode < NUM_SYNC_REP_WAIT_MODES; mode++)
	{
		volatile SyncRepQueue *queue = &(walsndctl->sync_rep_queue[mode]);

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
			SyncRepWakeFromQueue(mode, MyWalSnd->flush);
		}
		SpinLockRelease(&queue->qlock);

#ifdef SYNCREP_DEBUG
		elog(DEBUG2, "q%d queue = %X/%X flush = %X/%X", mode,
					queue->lsn.xlogid, queue->lsn.xrecoff,
					MyWalSnd->flush.xlogid, MyWalSnd->flush.xrecoff);
#endif
	}
}

/*
 * Walk queue from head setting the latches of any procs that need
 * to be woken. We don't modify the queue, we leave that for individual
 * procs to release themselves.
 *
 * Must hold spinlock on queue.
 */
static void
SyncRepWakeFromQueue(int wait_queue, XLogRecPtr lsn)
{
	volatile WalSndCtlData *walsndctl = WalSndCtl;
	volatile SyncRepQueue *queue = &(walsndctl->sync_rep_queue[wait_queue]);
	PGPROC	*proc = queue->head;
	int		numprocs = 0;
	int		totalprocs = 0;

	if (proc == NULL)
		return;

	for (; proc != NULL; proc = proc->lwWaitLink)
	{
		elog(LOG, "proc %d lsn %X/%X",
					numprocs,
					proc->waitLSN.xlogid,
					proc->waitLSN.xrecoff);

		if (XLByteLE(proc->waitLSN, lsn))
		{
			numprocs++;
			SetLatch(&proc->waitLatch);
		}
		totalprocs++;
	}
	elog(DEBUG2, "released %d procs out of %d waiting procs", numprocs, totalprocs);
#ifdef SYNCREP_DEBUG
	elog(DEBUG2, "released %d procs up to %X/%X", numprocs, lsn.xlogid, lsn.xrecoff);
#endif
}

void
SyncRepTimeoutExceeded(void)
{
	SyncRepReleaseWaiters(true);
}
