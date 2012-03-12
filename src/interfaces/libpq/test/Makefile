subdir = src/interfaces/libpq/test
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

all: installcheck

installcheck:
	BINDIR='$(bindir)' $(SHELL) ./regress.sh

clean distclean maintainer-clean:
	rm -f regress.out regress.diff
