/*
 * pg_logcollect_sample.c
 *
 * Implements a stand-alone program that can read log collector aka syslogger
 * pipe traffic and print it out in a readable character sequences (NUL bytes
 * and binary numbers converted), without making any effort to defragment it.
 * It's mostly intended to be a demonstration or sample, although it may be
 * useful in its own right.
 *
 * Notably, this program does not link against Postgres at all.
 */
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

/* Taken from postgres/src/include/c.h */
typedef unsigned short uint16;
typedef signed int int32;

/* PIPE_CHUNK_SIZE definition taken from syslogger.h */

/*
 * Primitive protocol structure for writing to syslogger pipe(s).  The
 * idea here is to divide long messages into chunks that are not more
 * than PIPE_BUF bytes long, which according to POSIX spec must be
 * written into the pipe atomically.  The pipe reader then uses the
 * protocol headers to reassemble the parts of a message into a single
 * string.  The reader can also cope with non-protocol data coming
 * down the pipe, though we cannot guarantee long strings won't get
 * split apart.
 *
 * We use non-nul bytes in is_last to make the protocol a tiny bit
 * more robust against finding a false double nul byte prologue. But
 * we still might find it in the len and/or pid bytes unless we're
 * careful.
 */

#ifdef PIPE_BUF
/* Are there any systems with PIPE_BUF > 64K?  Unlikely, but ... */
#if PIPE_BUF > 65536
#define PIPE_CHUNK_SIZE  65536
#else
#define PIPE_CHUNK_SIZE  ((int) PIPE_BUF)
#endif
#else							/* not defined */
/* POSIX says the value of PIPE_BUF must be at least 512, so use that */
#define PIPE_CHUNK_SIZE  512
#endif


/* End PIPE_CHUNK_SIZE define */

/* Constants defined by both the protocol and the system's PIPE_BUF */
#define PIPE_HEADER_SIZE (9)
#define PIPE_MAX_PAYLOAD ((PIPE_CHUNK_SIZE - PIPE_HEADER_SIZE))

static ssize_t safe_read(int fd, void *buf, size_t count);
static void printInput(char *logbuffer, int *bytes_in_logbuffer);

/*
 * read, but retry as long as one receives EINTR.
 */
ssize_t
safe_read(int fd, void *buf, size_t count)
{
	const int save_errno = 0;
	ssize_t numRead;

readAgain:
	errno = 0;
	numRead = read(fd, buf, count);

	if (numRead < 0 && errno == EINTR)
		goto readAgain;

	/*
	 * If read() succeeds, then restore the old errno to avoid clearing errors
	 * on behalf of the caller.  If it fails, then leave errno alone, since
	 * that's what read normally does anyway.
	 */
	if (errno == 0)
		errno = save_errno;

	return numRead;
}

/*
 * Print input data, using code taken from syslogger.c but stripped of most of
 * its more interesting functionality except stepping through the input buffer
 * and printing it in a more palatable human-readable format.
 */
static void
printInput(char *logBuf, int *logBufLen)
{
	char		*cursor = logBuf;

	/* While there is enough data for a header, process it */
	while (*logBufLen >= PIPE_HEADER_SIZE)
	{
		char	*nuls	 = cursor;
		uint16	*len	 = (void *) (logBuf + 2);
		int32	*pid	 = (void *) (logBuf + 4);
		char	*fmt	 = logBuf + 8;
		char	*payload = logBuf + 9;

		/*
		 * Sometimes, non-protocol traffic (e.g. libraries that write directly
		 * stderr) end up piped out of a Postgres process.  Here, detect the
		 * Postgres format as obeyed by ereport/elog and handle if possible.
		 * The header looks like this:
		 *
		 * [NUL] [NUL] [DATALEN]*2 [PID Integer Fragment]*4 [t|T|f|F]
		 *
		 * The last byte deserves more explanation:
		 *
		 *   * If capitalized, this is a CSV formatted record.  If lower case,
		 *     the log record respects the user-specified or default
		 *     formatting.
		 *
		 *   * If 't' or 'T', then this is the last fragment (termination) for
		 *     a log message.
		 *
		 *   * If 'f' or 'F', then this is a fragment that has a continuation
		 *     yet to come.
		 *
		 * This also means the minimum protocol-abiding traffic may be nine
		 * bytes long on read(), and at maximum can be PIPE_CHUNK_SIZE, since
		 * protocol traffic relies on the atomic nature of fragmenting into
		 * PIPE_CHUNK_SIZE pieces.
		 */
		if (nuls[0] == '\0' && nuls[1] == '\0' &&
			*len > 0 && *len <= PIPE_MAX_PAYLOAD &&
			*pid != 0 &&
			(*fmt == 't' || *fmt == 'f' ||
			 *fmt == 'T' || *fmt == 'F'))
		{
			const int chunkLen = PIPE_HEADER_SIZE + *len;

			printf("LEN=%d PID=%d FMT=%c %*s\n", *len, *pid, *fmt,
				   *len, payload);

			/* Finished processing this chunk */
			cursor	   += chunkLen;
			*logBufLen -= chunkLen;
		}
		else
		{
			int protoCur;

			/*
			 * Process non-protocol data, but just in case, look for the
			 * beginning of some protocol traffic and re-start the formatting
			 * routine if that happens.
			 *
			 * It is expected that in many scenarios, a non-protocol message
			 * will arrive all in one read(), and we want to respect the read()
			 * boundary if possible.
			 *
			 * NB: Skip looking at the first byte, because the previous branch
			 * would have already spotted a valid chunk that is aligned
			 * properly, and not found inside some arbitrary data.
			 */
			for (protoCur = 1; protoCur < *logBufLen; protoCur += 1)
			{
				if (cursor[protoCur] == '\0')
					break;
			}

			printf("LEN= PID= FMT= %*s\n", protoCur, cursor);

			cursor += protoCur;
			*logBufLen  -= protoCur;
		}
	}

	/* Don't have a full chunk, so left-align what remains in the buffer */
	if (*logBufLen > 0 && cursor != logBuf)
		memmove(logBuf, cursor, *logBufLen);
}

int
main(void)
{
	char		 buf[2 * PIPE_CHUNK_SIZE];
	int			 bufLen = 0;

	while (1)
	{
		int numRead;

		numRead = safe_read(0, buf, sizeof buf);

		/* Handle EOF */
		if (numRead == 0)
			return 0;

		/* Exit if read doesn't work for whatever reason */
		if (numRead < 0)
			return 1;

		bufLen += numRead;

		printInput(buf, &bufLen);
		fflush(stdout);

	}
}
