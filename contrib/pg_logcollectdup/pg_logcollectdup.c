/*
 * pg_logcollectdup.c
 *
 * Implements a module to be loaded via shared_preload_libraries that, should
 * "logcollectdup.destination" be set in postgresql.conf and the log collector
 * ("syslogger") be enabled will allow a copy of the log collection protocol
 * traffic to be forwarded to file system path of once's choice.  It is
 * suggested that this path is most useful if it is a mkfifo named pipe, so
 * that a completely seperate program can handle the protocol traffic.
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "funcapi.h"
#include "postmaster/syslogger.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

/* GUC-configured destination of the log pages */
static char *destination;

static ProcessLogCollect_hook_type prev_ProcessLogCollect = NULL;

static void openDestFd(char *dest, int *currentFd);
static void closeDestFd(int *currentFd);
static void logcollectdup_ProcessLogCollect(char *buf, int len);
static void call_ProcessLogCollect(char *buf, int len);

/*
 * File descriptor that log pages are written to.  Is re-set if a
 * write fails.
 */
static int currentFd = -1;

void _PG_init(void);
void _PG_fini(void);

/*
 * _PG_init()			- library load-time initialization
 *
 * DO NOT make this static nor change its name!
 *
 * Init the module, all we have to do here is getting our GUC
 */
void
_PG_init(void) {
	PG_TRY();
	{
		destination = GetConfigOptionByName(
			"logcollectdup.destination", NULL);
	}
	PG_CATCH();
	{
		DefineCustomStringVariable("logcollectdup.destination",
								   "Path send log collector bytes to",
								   "",
								   &destination,
								   "",
								   PGC_SUSET,
								   GUC_NOT_IN_SAMPLE,
								   NULL,
								   NULL,
								   NULL);
		EmitWarningsOnPlaceholders("logcollectdup.destination");
	}
	PG_END_TRY();

	prev_ProcessLogCollect = ProcessLogCollect_hook;
	ProcessLogCollect_hook = logcollectdup_ProcessLogCollect;
}

/*
 * Checks if the file descriptor pointed to by *fd is a fifo, logging
 * a problem, closing it, and invalidating if is not.
 */
static bool
checkFifoOrInvalidate(int *fd)
{
	const int save_errno = errno;
	int statRes;
	struct stat st;

	/*
	 * If the file descriptor is not valid, it's definitely not a
	 * fifo, and there's no need to invalidate it, either.
	 */
	if (*fd < 0)
		return false;

	errno = 0;
	statRes = fstat(*fd, &st);

	if (statRes < 0)
	{
		/* 
		 * Couldn't fstat.  Without confirmation that the file
		 * descriptor points to a pipe, assume it is not one.
		 */
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("pg_logcollectdup could not get information "
						"about its output pipe"),
				 errdetail("The fstat() request failed with the message: %s.",
						   strerror(errno))));
		goto closeFail;
	}
	else if (statRes == 0 && !S_ISFIFO(st.st_mode))
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("pg_logcollectdup only supports writing into "
						"named pipes")));
		goto closeFail;
	}

	/* Success: it's a valid fifo. */
	Assert(statRes == 0);
	Assert(S_ISFIFO(stat.st_mode));
	errno = save_errno;
	return true;

closeFail:
	closeDestFd(fd);
	errno = save_errno;
	return false;
}

/*
 * Open the destination file descriptor for writing, refusing to return until
 * there is success, writing the fd value into *fd.
 *
 * That may sound extreme, but considering that the log collector would also
 * cause logging processes to block were it to halt or close and the whole
 * point of this module is to allow some other process to obtain a copy of the
 * protocol traffic, it seems reasonable.
 */
static void
openDestFd(char *dest, int *fd)
{
	const int save_errno = errno;

	/* Spin until the pipe can be opened */
	while (dest != NULL && *fd < 0)
	{
		errno = 0;
		*fd = open(dest, O_WRONLY);

		if (errno != 0)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("pg_logcollectdup cannot open destination"),
					 errdetail("The open() request failed with "
							   "the message: %s.", strerror(errno))));

		/*
		 * Even if the file descriptor was opened successfully, it
		 * might not be a pipe.  If it isn't a pipe, whine, close, and
		 * invalidate the file descriptor.
		 */
		checkFifoOrInvalidate(fd);

		sleep(1);
	}

	/* Must have a valid file descriptor here */
	Assert(*fd >= 0);

	errno = save_errno;
}

/*
 * Close the passed file descriptor and invalidate it.
 */
static void
closeDestFd(int *fd)
{
	const int save_errno = errno;

	do
	{
		errno = 0;

		/*
		 * Ignore errors except EINTR: other than EINTR, there is no
		 * obvious handling one can do from a failed close() that matters
		 * in this case.
		 */
		close(*fd);

		if (errno == EINTR)
			continue;

		if (errno == EBADF)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("pg_logcollectdup attempted to close an "
							"invalid file descriptor"),
					 errdetail("The file descriptor that failed a close "
							   "attempt was %d, and it failed with "
							   "the reason: %s.",
							   *fd, strerror(errno))));

		/* Exit the EINTR retry loop */
		*fd = -1;
	} while (*fd >= 0);

	errno = save_errno;
}

static void
logcollectdup_ProcessLogCollect(char *buf, int len)
{
	int save_errno = errno;
	int bytesWritten;

	do
	{
		if (destination == NULL && currentFd < 0)
		{
			/*
			 * No destination defined, and no file descriptor open; in this
			 * case this extension was loaded but not configured, so just exit.
			 */
			goto exit;
		}
		else if (destination != NULL && currentFd < 0)
		{
			/*
			 * Destination defined, but no file descriptor open yet.  Open the
			 * file descriptor very insistently; when this returns it must be
			 * open, which also means backends that need to log *will block*
			 * until this succeeds.
			 */
			openDestFd(destination, &currentFd);
		}
		else if (destination == NULL && currentFd >= 0)
		{
			/*
			 * Destination undefined, but a file descriptor is still open.
			 * This can be the result of a SIGHUP/configuration change, so
			 * close and invalidate the file descriptor.
			 *
			 * Invalidates currentFd, continuing the retry loop.
			 */
			closeDestFd(&currentFd);
		}
	} while (currentFd < 0);

writeAgain:
	errno = 0;
	bytesWritten = write(currentFd, buf, len);

	/*
	 * Given PIPE_BUF atomicity, only expect failed writes or complete
	 * writes.
	 */

	Assert(bytesWritten < 0 || bytesWritten == len);
	if (bytesWritten < 0)
	{
		if (errno == EINTR)
			goto writeAgain;
	
		Assert(errno != 0);
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("pg_logcollectdup cannot write to pipe"),
				 errdetail("The write failed with the message: %s",
						   strerror(errno))));

		/*
		 * Try very hard to toss out the old file descriptor and get a new one
		 * when things go awry.
		 */
		closeDestFd(&currentFd);
		openDestFd(destination, &currentFd);
		goto writeAgain;
	}

exit:
	errno = save_errno;
	call_ProcessLogCollect(buf, len);
}

static void
call_ProcessLogCollect(char *buf, int len)
{
	if (prev_ProcessLogCollect != NULL)
		prev_ProcessLogCollect(buf, len);
	else
		standard_ProcessLogCollect(buf, len);
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hook */
	ProcessLogCollect_hook = prev_ProcessLogCollect;
}
