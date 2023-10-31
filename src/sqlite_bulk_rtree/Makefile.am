
AM_CPPFLAGS = -I$(top_srcdir)/src/headers
AM_CPPFLAGS += @CFLAGS@ @CPPFLAGS@

noinst_HEADERS = sqlite_rtree_bulk_load.h
noinst_LTLIBRARIES = libbulkrtreeload.la bulkrtreeload.la

libbulkrtreeload_la_SOURCES = sqlite_rtree_bulk_load.c RTreeBulkLoad.c

bulkrtreeload_la_SOURCES = sqlite_rtree_bulk_load.c RTreeBulkLoad.c

bulkrtreeload_la_CPPFLAGS = -I$(top_srcdir)/src/headers -I.
bulkrtreeload_la_CPPFLAGS += @CFLAGS@ @CPPFLAGS@
bulkrtreeload_la_CPPFLAGS += -DLOADABLE_EXTENSION
bulkrtreeload_la_LDFLAGS = -module
bulkrtreeload_la_LIBTOOLFLAGS = --tag=disable-static

MOSTLYCLEANFILES = *.gcna *.gcno *.gcda
