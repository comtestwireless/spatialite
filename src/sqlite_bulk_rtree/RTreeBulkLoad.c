/*

 RTreeBulkLoad.c -- interface for SpatiaLite supporting
                    the ultra-fast SqliteRtreeBulkLoad
                    module released by 
 Even Rouault <even dot rouault at spatialys dot com>

 version 5.1.0, 2023 October 27

 Author: Sandro Furieri a.furieri@lqt.it

 -----------------------------------------------------------------------------
 
 Version: MPL 1.1/GPL 2.0/LGPL 2.1
 
 The contents of this file are subject to the Mozilla Public License Version
 1.1 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at
 http://www.mozilla.org/MPL/
 
Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the
License.

The Original Code is the SpatiaLite library

The Initial Developer of the Original Code is Alessandro Furieri
 
Portions created by the Initial Developer are Copyright (C) 2008-2023
the Initial Developer. All Rights Reserved.

Contributor(s): 

Alternatively, the contents of this file may be used under the terms of
either the GNU General Public License Version 2 or later (the "GPL"), or
the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
in which case the provisions of the GPL or the LGPL are applicable instead
of those above. If you wish to allow use of your version of this file only
under the terms of either the GPL or the LGPL, and not to allow others to
use your version of this file under the terms of the MPL, indicate your
decision by deleting the provisions above and replace them with the notice
and other provisions required by the GPL or the LGPL. If you do not delete
the provisions above, a recipient may use your version of this file under
the terms of any one of the MPL, the GPL or the LGPL.
 
*/

#include "sqlite_rtree_bulk_load.h"

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <spatialite.h>
#include <spatialite/spatialite_ext.h>
#include <spatialite_private.h>
#include <spatialite/gaiaaux.h>
#include <spatialite/debug.h>

#ifdef __cplusplus
#define STATIC_CAST(type, value) static_cast<type>(value)
#else
#define STATIC_CAST(type, value) (type)(value)
#endif

static int
get_page_size (sqlite3 * sqlite)
{
/* retrieving the current SQLite page-size */
    int ret;
    int i;
    char **results;
    int rows;
    int columns;
    int page_size = 4096;

    ret =
	sqlite3_get_table (sqlite, "PRAGMA page_size", &results, &rows,
			   &columns, NULL);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		page_size = atoi (results[(i * columns) + 0]);
	    }
      }
    sqlite3_free_table (results);
    return page_size;
}

SPATIALITE_PRIVATE int
RTreeBulkLoad (sqlite3 * sqlite, const char *table, const char *column)
{
/* using the super-fast BulkLoad to create an populate the R*Tree */
    sqlite3_stmt *stmt = NULL;
    int ret;
    char *sql;
    char *quoted_table = gaiaDoubleQuotedSql (table);
    char *quoted_column = gaiaDoubleQuotedSql (column);
    struct sqlite_rtree_bl *rtree_str = NULL;
    int page_size = get_page_size (sqlite);
    char *error_msg = NULL;
    char *rtree_name;
    char *xrtree_name;
    int is_savepoint = 0;

/* creating the fast RTree Bulk Loader object */
    rtree_str = SQLITE_RTREE_BL_SYMBOL (sqlite_rtree_bl_new) (page_size);
    if (rtree_str == NULL)
      {
	  spatialite_e
	      ("RTreeBulkLoad: unable to create the BulkLoader object\n");
	  goto stop;
      }

/* inserting all BBOXes into the fast RTree Bulk Loader object */
    sql =
	sqlite3_mprintf
	("SELECT ROWID, MbrMinX(\"%s\"), MbrMaxX(\"%s\"), MbrMinY(\"%s\"), MbrMaxY(\"%s\") "
	 "FROM \"%s\" WHERE MbrMinX(\"%s\") IS NOT NULL", quoted_column,
	 quoted_column, quoted_column, quoted_column, quoted_table,
	 quoted_column);
    free (quoted_table);
    free (quoted_column);
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("RTreeBulkLoad: error %d \"%s\"\n",
			sqlite3_errcode (sqlite), sqlite3_errmsg (sqlite));
	  goto stop;
      }
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* inserting a BBOX into the fast RTree Bulk Loader object */
		int64_t id = sqlite3_column_int64 (stmt, 0);
		const double minx = sqlite3_column_double (stmt, 1);
		const double maxx = sqlite3_column_double (stmt, 2);
		const double miny = sqlite3_column_double (stmt, 3);
		const double maxy = sqlite3_column_double (stmt, 4);
		if (!SQLITE_RTREE_BL_SYMBOL (sqlite_rtree_bl_insert)
		    (rtree_str, id, minx, miny, maxx, maxy))
		  {
		      spatialite_e ("sqlite_rtree_bl_insert failed\n");
		      goto stop;
		  }
	    }
	  else
	    {
		spatialite_e ("RTreeBulkLoad read: error %d \"%s\"\n",
			      sqlite3_errcode (sqlite),
			      sqlite3_errmsg (sqlite));
		goto stop;
	    }
      }
    sqlite3_finalize (stmt);
    stmt = NULL;

/* setting a Savepoint */
    sql = "SAVEPOINT rtree_bulk_load";
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &error_msg);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("RTreeBulkLoad SAVEPOINT error: \"%s\"\n", error_msg);
	  sqlite3_free (error_msg);
	  goto stop;
      }
    is_savepoint = 1;

/* dropping an already existing R*Tree table (just in case) */
    rtree_name = sqlite3_mprintf ("idx_%s_%s", table, column);
    xrtree_name = gaiaDoubleQuotedSql (rtree_name);
    sql = sqlite3_mprintf ("DROP TABLE IF EXISTS \"%s\"", xrtree_name);
    free (xrtree_name);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &error_msg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("RTreeBulkLoad DROP OLD-IDX error: \"%s\"\n",
			error_msg);
	  sqlite3_free (error_msg);
	  goto stop;
      }

/* creating and populating the R*Tree */
    if (!SQLITE_RTREE_BL_SYMBOL (sqlite_rtree_bl_serialize)
	(rtree_str, sqlite, rtree_name, "pkid", "xmin", "ymin", "xmax", "ymax",
	 &error_msg))
      {
	  spatialite_e ("RTreeBulkLoad error: %s\n", error_msg);
	  sqlite3_free (error_msg);
	  sqlite3_free (rtree_name);
	  goto stop;
      }
    sqlite3_free (rtree_name);

/* releasing the Savepoint */
    sql = "RELEASE SAVEPOINT rtree_bulk_load";
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &error_msg);
    if (ret != SQLITE_OK)
      {
	  spatialite_e ("RTreeBulkLoad RELEASE SAVEPOINT error: \"%s\"\n",
			error_msg);
	  sqlite3_free (error_msg);
	  goto stop;
      }

/* memory cleanup - freeing the fast RTree Bulk Loader object */
    SQLITE_RTREE_BL_SYMBOL (sqlite_rtree_bl_free) (rtree_str);
    return 1;

  stop:
    if (stmt != NULL)
	sqlite3_finalize (stmt);
    if (rtree_str != NULL)
	SQLITE_RTREE_BL_SYMBOL (sqlite_rtree_bl_free) (rtree_str);

    if (is_savepoint)
      {
	  /* rolling back the Savepoint */
	  sql = "ROLLBACK TO rtreee_bulk_load";
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &error_msg);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e
		    ("RTreeBulkLoad ROLLBACK TO SAVEPOINT error: \"%s\"\n",
		     error_msg);
		sqlite3_free (error_msg);
	    }
	  /* releasing the Savepoint */
	  sql = "RELEASE SAVEPOINT rtree_bulk_load";
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &error_msg);
	  if (ret != SQLITE_OK)
	    {
		spatialite_e ("RTreeBulkLoad RELEASE SAVEPOINT error: \"%s\"\n",
			      error_msg);
		sqlite3_free (error_msg);
	    }
      }
    return 0;
}
