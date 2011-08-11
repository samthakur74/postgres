# contrib/json/Makefile

MODULE_big = json
OBJS = json.o json_io.o json_op.o

EXTENSION = json
DATA = json--0.1.sql json--unpackaged--0.1.sql

REGRESS = init json validate condense json_stringify escape_unicode relocatable

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/json
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
