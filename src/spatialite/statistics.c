/*

 statistics.c -- helper functions updating internal statistics

 version 4.3, 2015 June 29

 Author: Sandro Furieri a.furieri@lqt.it

 ------------------------------------------------------------------------------
 
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
 
Portions created by the Initial Developer are Copyright (C) 2008-2015
the Initial Developer. All Rights Reserved.

Contributor(s):
Pepijn Van Eeckhoudt <pepijnvaneeckhoudt@luciad.com>
(implementing Android support)

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

#include <sys/types.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <float.h>
#include <locale.h>
#include <ctype.h>

#if defined(_WIN32) && !defined(__MINGW32__)
#include "config-msvc.h"
#else
#include "config.h"
#endif

#if defined(_WIN32) || defined(WIN32)
#include <io.h>
#define isatty	_isatty
#else
#include <unistd.h>
#endif

#include <spatialite/sqlite.h>
#include <spatialite/debug.h>

#include <spatialite/gaiageo.h>
#include <spatialite.h>
#include <spatialite_private.h>
#include <spatialite/gaiaaux.h>

#ifdef _WIN32
#define strcasecmp	_stricmp
#endif /* not WIN32 */

struct field_item_infos
{
    int ordinal;
    char *col_name;
    int null_values;
    int integer_values;
    int double_values;
    int text_values;
    int blob_values;
    int max_size;
    int int_minmax_set;
    int int_min;
    int int_max;
    int dbl_minmax_set;
    double dbl_min;
    double dbl_max;
    struct field_item_infos *next;
};

struct field_container_infos
{
    struct field_item_infos *first;
    struct field_item_infos *last;
};

static int
do_update_layer_statistics_v4 (sqlite3 * sqlite, const char *table,
			       const char *column, int count, int has_coords,
			       double min_x, double min_y, double max_x,
			       double max_y)
{
/* update GEOMETRY_COLUMNS_STATISTICS Version >= 4.0.0 */
    char sql[8192];
    int ret;
    int error = 0;
    sqlite3_stmt *stmt;

    strcpy (sql, "INSERT OR REPLACE INTO geometry_columns_statistics ");
    strcat (sql, "(f_table_name, f_geometry_column, last_verified, ");
    strcat (sql, "row_count, extent_min_x, extent_min_y, ");
    strcat (sql, "extent_max_x, extent_max_y) VALUES (?, ?, ");
    strcat (sql, "strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), ?, ?, ?, ?, ?)");

/* compiling SQL prepared statement */
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
	return 0;

/* binding INSERT params */
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, table, strlen (table), SQLITE_STATIC);
    sqlite3_bind_text (stmt, 2, column, strlen (column), SQLITE_STATIC);
    sqlite3_bind_int (stmt, 3, count);
    if (has_coords)
      {
	  sqlite3_bind_double (stmt, 4, min_x);
	  sqlite3_bind_double (stmt, 5, min_y);
	  sqlite3_bind_double (stmt, 6, max_x);
	  sqlite3_bind_double (stmt, 7, max_y);
      }
    else
      {
	  sqlite3_bind_null (stmt, 4);
	  sqlite3_bind_null (stmt, 5);
	  sqlite3_bind_null (stmt, 6);
	  sqlite3_bind_null (stmt, 7);
      }
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	;
    else
	error = 1;
    ret = sqlite3_finalize (stmt);
    if (ret != SQLITE_OK)
	return 0;
    if (error)
	return 0;
    return 1;
}

static int
do_update_layer_statistics (sqlite3 * sqlite, const char *table,
			    const char *column, int count, int has_coords,
			    double min_x, double min_y, double max_x,
			    double max_y)
{
/* update LAYER_STATISTICS [single table/geometry] */
    char sql[8192];
    int ret;
    int error = 0;
    sqlite3_stmt *stmt;
    int metadata_version = checkSpatialMetaData (sqlite);

    if (metadata_version == 3)
      {
	  /* current metadata style >= v.4.0.0 */
	  return do_update_layer_statistics_v4 (sqlite, table, column, count,
						has_coords, min_x, min_y, max_x,
						max_y);
      }

    if (!check_layer_statistics (sqlite))
	return 0;
    strcpy (sql, "INSERT OR REPLACE INTO layer_statistics ");
    strcat (sql, "(raster_layer, table_name, geometry_column, ");
    strcat (sql, "row_count, extent_min_x, extent_min_y, ");
    strcat (sql, "extent_max_x, extent_max_y) ");
    strcat (sql, "VALUES (0, ?, ?, ?, ?, ?, ?, ?)");

/* compiling SQL prepared statement */
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
	return 0;
/* binding INSERT params */
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, table, strlen (table), SQLITE_STATIC);
    sqlite3_bind_text (stmt, 2, column, strlen (column), SQLITE_STATIC);
    sqlite3_bind_int (stmt, 3, count);
    if (has_coords)
      {
	  sqlite3_bind_double (stmt, 4, min_x);
	  sqlite3_bind_double (stmt, 5, min_y);
	  sqlite3_bind_double (stmt, 6, max_x);
	  sqlite3_bind_double (stmt, 7, max_y);
      }
    else
      {
	  sqlite3_bind_null (stmt, 4);
	  sqlite3_bind_null (stmt, 5);
	  sqlite3_bind_null (stmt, 6);
	  sqlite3_bind_null (stmt, 7);
      }
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	;
    else
	error = 1;
    ret = sqlite3_finalize (stmt);
    if (ret != SQLITE_OK)
	return 0;
    if (error)
	return 0;
    return 1;
}

static int
do_update_views_layer_statistics_v4 (sqlite3 * sqlite, const char *table,
				     const char *column, int count,
				     int has_coords, double min_x,
				     double min_y, double max_x, double max_y)
{
/* update VIEWS_GEOMETRY_COLUMNS_STATISTICS Version >= 4.0.0 */
    char sql[8192];
    int ret;
    int error = 0;
    sqlite3_stmt *stmt;

    strcpy (sql, "INSERT OR REPLACE INTO views_geometry_columns_statistics ");
    strcat (sql, "(view_name, view_geometry, last_verified, ");
    strcat (sql, "row_count, extent_min_x, extent_min_y, ");
    strcat (sql, "extent_max_x, extent_max_y) VALUES (?, ?, ");
    strcat (sql, "strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), ?, ?, ?, ?, ?)");

/* compiling SQL prepared statement */
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
	return 0;

/* binding INSERT params */
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, table, strlen (table), SQLITE_STATIC);
    sqlite3_bind_text (stmt, 2, column, strlen (column), SQLITE_STATIC);
    sqlite3_bind_int (stmt, 3, count);
    if (has_coords)
      {
	  sqlite3_bind_double (stmt, 4, min_x);
	  sqlite3_bind_double (stmt, 5, min_y);
	  sqlite3_bind_double (stmt, 6, max_x);
	  sqlite3_bind_double (stmt, 7, max_y);
      }
    else
      {
	  sqlite3_bind_null (stmt, 4);
	  sqlite3_bind_null (stmt, 5);
	  sqlite3_bind_null (stmt, 6);
	  sqlite3_bind_null (stmt, 7);
      }
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	;
    else
	error = 1;
    ret = sqlite3_finalize (stmt);
    if (ret != SQLITE_OK)
	return 0;
    if (error)
	return 0;
    return 1;
}

static int
do_update_views_layer_statistics (sqlite3 * sqlite, const char *table,
				  const char *column, int count,
				  int has_coords, double min_x, double min_y,
				  double max_x, double max_y)
{
/* update VIEWS_LAYER_STATISTICS [single table/geometry] */
    char sql[8192];
    int ret;
    int error = 0;
    sqlite3_stmt *stmt;
    int metadata_version = checkSpatialMetaData (sqlite);

    if (metadata_version == 3)
      {
	  /* current metadata style >= v.4.0.0 */
	  return do_update_views_layer_statistics_v4 (sqlite, table, column,
						      count, has_coords, min_x,
						      min_y, max_x, max_y);
      }

    if (!check_views_layer_statistics (sqlite))
	return 0;
    strcpy (sql, "INSERT OR REPLACE INTO views_layer_statistics ");
    strcat (sql, "(view_name, view_geometry, ");
    strcat (sql, "row_count, extent_min_x, extent_min_y, ");
    strcat (sql, "extent_max_x, extent_max_y) ");
    strcat (sql, "VALUES (?, ?, ?, ?, ?, ?, ?)");

/* compiling SQL prepared statement */
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
	return 0;

/* binding INSERT params */
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, table, strlen (table), SQLITE_STATIC);
    sqlite3_bind_text (stmt, 2, column, strlen (column), SQLITE_STATIC);
    sqlite3_bind_int (stmt, 3, count);
    if (has_coords)
      {
	  sqlite3_bind_double (stmt, 4, min_x);
	  sqlite3_bind_double (stmt, 5, min_y);
	  sqlite3_bind_double (stmt, 6, max_x);
	  sqlite3_bind_double (stmt, 7, max_y);
      }
    else
      {
	  sqlite3_bind_null (stmt, 4);
	  sqlite3_bind_null (stmt, 5);
	  sqlite3_bind_null (stmt, 6);
	  sqlite3_bind_null (stmt, 7);
      }
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	;
    else
	error = 1;
    ret = sqlite3_finalize (stmt);
    if (ret != SQLITE_OK)
	return 0;
    if (error)
	return 0;
    return 1;
}

static int
do_update_virts_layer_statistics_v4 (sqlite3 * sqlite, const char *table,
				     const char *column, int count,
				     int has_coords, double min_x,
				     double min_y, double max_x, double max_y)
{
/* update VIRTS_GEOMETRY_COLUMNS_STATISTICS Version >= 4.0.0 */
    char sql[8192];
    int ret;
    int error = 0;
    sqlite3_stmt *stmt;

    strcpy (sql, "INSERT OR REPLACE INTO virts_geometry_columns_statistics ");
    strcat (sql, "(virt_name, virt_geometry, last_verified, ");
    strcat (sql, "row_count, extent_min_x, extent_min_y, ");
    strcat (sql, "extent_max_x, extent_max_y) VALUES (?, ?, ");
    strcat (sql, "strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), ?, ?, ?, ?, ?)");

/* compiling SQL prepared statement */
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
	return 0;

/* binding INSERT params */
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, table, strlen (table), SQLITE_STATIC);
    sqlite3_bind_text (stmt, 2, column, strlen (column), SQLITE_STATIC);
    sqlite3_bind_int (stmt, 3, count);
    if (has_coords)
      {
	  sqlite3_bind_double (stmt, 4, min_x);
	  sqlite3_bind_double (stmt, 5, min_y);
	  sqlite3_bind_double (stmt, 6, max_x);
	  sqlite3_bind_double (stmt, 7, max_y);
      }
    else
      {
	  sqlite3_bind_null (stmt, 4);
	  sqlite3_bind_null (stmt, 5);
	  sqlite3_bind_null (stmt, 6);
	  sqlite3_bind_null (stmt, 7);
      }
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	;
    else
	error = 1;
    ret = sqlite3_finalize (stmt);
    if (ret != SQLITE_OK)
	return 0;
    if (error)
	return 0;
    return 1;
}

static int
do_update_virts_layer_statistics (sqlite3 * sqlite, const char *table,
				  const char *column, int count,
				  int has_coords, double min_x, double min_y,
				  double max_x, double max_y)
{
/* update VIRTS_LAYER_STATISTICS [single table/geometry] */
    char sql[8192];
    int ret;
    int error = 0;
    sqlite3_stmt *stmt;
    int metadata_version = checkSpatialMetaData (sqlite);

    if (metadata_version == 3)
      {
	  /* current metadata style >= v.4.0.0 */
	  return do_update_virts_layer_statistics_v4 (sqlite, table, column,
						      count, has_coords, min_x,
						      min_y, max_x, max_y);
      }

    if (!check_virts_layer_statistics (sqlite))
	return 0;
    strcpy (sql, "INSERT OR REPLACE INTO virts_layer_statistics ");
    strcat (sql, "(virt_name, virt_geometry, ");
    strcat (sql, "row_count, extent_min_x, extent_min_y, ");
    strcat (sql, "extent_max_x, extent_max_y) ");
    strcat (sql, "VALUES (?, ?, ?, ?, ?, ?, ?)");

/* compiling SQL prepared statement */
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
	return 0;

/* binding INSERT params */
    sqlite3_reset (stmt);
    sqlite3_clear_bindings (stmt);
    sqlite3_bind_text (stmt, 1, table, strlen (table), SQLITE_STATIC);
    sqlite3_bind_text (stmt, 2, column, strlen (column), SQLITE_STATIC);
    sqlite3_bind_int (stmt, 3, count);
    if (has_coords)
      {
	  sqlite3_bind_double (stmt, 4, min_x);
	  sqlite3_bind_double (stmt, 5, min_y);
	  sqlite3_bind_double (stmt, 6, max_x);
	  sqlite3_bind_double (stmt, 7, max_y);
      }
    else
      {
	  sqlite3_bind_null (stmt, 4);
	  sqlite3_bind_null (stmt, 5);
	  sqlite3_bind_null (stmt, 6);
	  sqlite3_bind_null (stmt, 7);
      }
    ret = sqlite3_step (stmt);
    if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	;
    else
	error = 1;
    ret = sqlite3_finalize (stmt);
    if (ret != SQLITE_OK)
	return 0;
    if (error)
	return 0;
    return 1;
}

static void
update_field_infos (struct field_container_infos *infos, int ordinal,
		    const char *col_name, const char *type, int size, int count)
{
/* updating the field container infos */
    int len;
    struct field_item_infos *p = infos->first;
    while (p)
      {
	  if (strcasecmp (col_name, p->col_name) == 0)
	    {
		/* updating an already defined field */
		if (strcasecmp (type, "null") == 0)
		    p->null_values += count;
		if (strcasecmp (type, "integer") == 0)
		    p->integer_values += count;
		if (strcasecmp (type, "real") == 0)
		    p->double_values += count;
		if (strcasecmp (type, "text") == 0)
		  {
		      p->text_values += count;
		      if (size > p->max_size)
			  p->max_size = size;
		  }
		if (strcasecmp (type, "blob") == 0)
		  {
		      p->blob_values += count;
		      if (size > p->max_size)
			  p->max_size = size;
		  }
		return;
	    }
	  p = p->next;
      }
/* inserting a new field */
    p = malloc (sizeof (struct field_item_infos));
    p->ordinal = ordinal;
    len = strlen (col_name);
    p->col_name = malloc (len + 1);
    strcpy (p->col_name, col_name);
    p->null_values = 0;
    p->integer_values = 0;
    p->double_values = 0;
    p->text_values = 0;
    p->blob_values = 0;
    p->max_size = -1;
    p->int_minmax_set = 0;
    p->int_min = 0;
    p->int_max = 0;
    p->dbl_minmax_set = 0;
    p->dbl_min = 0.0;
    p->dbl_max = 0.0;
    p->next = NULL;
    if (strcasecmp (type, "null") == 0)
	p->null_values += count;
    if (strcasecmp (type, "integer") == 0)
	p->integer_values += count;
    if (strcasecmp (type, "real") == 0)
	p->double_values += count;
    if (strcasecmp (type, "text") == 0)
      {
	  p->text_values += count;
	  if (size > p->max_size)
	      p->max_size = size;
      }
    if (strcasecmp (type, "blob") == 0)
      {
	  p->blob_values += count;
	  if (size > p->max_size)
	      p->max_size = size;
      }
    if (infos->first == NULL)
	infos->first = p;
    if (infos->last != NULL)
	infos->last->next = p;
    infos->last = p;
}

static void
update_field_infos_int_minmax (struct field_container_infos *infos,
			       const char *col_name, int int_min, int int_max)
{
/* updating the field container infos - Int MinMax */
    struct field_item_infos *p = infos->first;
    while (p)
      {
	  if (strcasecmp (col_name, p->col_name) == 0)
	    {
		p->int_minmax_set = 1;
		p->int_min = int_min;
		p->int_max = int_max;
		return;
	    }
	  p = p->next;
      }
}

static void
update_field_infos_double_minmax (struct field_container_infos *infos,
				  const char *col_name, double dbl_min,
				  double dbl_max)
{
/* updating the field container infos - Double MinMax */
    struct field_item_infos *p = infos->first;
    while (p)
      {
	  if (strcasecmp (col_name, p->col_name) == 0)
	    {
		p->dbl_minmax_set = 1;
		p->dbl_min = dbl_min;
		p->dbl_max = dbl_max;
		return;
	    }
	  p = p->next;
      }
}

static void
free_field_infos (struct field_container_infos *infos)
{
/* memory cleanup - freeing a field infos container */
    struct field_item_infos *p = infos->first;
    struct field_item_infos *pn;
    while (p)
      {
	  /* destroying field items */
	  pn = p->next;
	  if (p->col_name)
	      free (p->col_name);
	  free (p);
	  p = pn;
      }
}

static int
do_update_field_infos (sqlite3 * sqlite, const char *table,
		       const char *column, struct field_container_infos *infos)
{
/* update GEOMETRY_COLUMNS_FIELD_INFOS Version >= 4.0.0 */
    char sql[8192];
    char *sql_statement;
    int ret;
    int error = 0;
    sqlite3_stmt *stmt;
    struct field_item_infos *p = infos->first;

/* deleting any previous row */
    sql_statement = sqlite3_mprintf ("DELETE FROM geometry_columns_field_infos "
				     "WHERE Lower(f_table_name) = Lower(%Q) AND "
				     "Lower(f_geometry_column) = Lower(%Q)",
				     table, column);
    ret = sqlite3_exec (sqlite, sql_statement, NULL, NULL, NULL);
    sqlite3_free (sql_statement);
    if (ret != SQLITE_OK)
	return 0;

/* reinserting yet again */
    strcpy (sql, "INSERT INTO geometry_columns_field_infos ");
    strcat (sql, "(f_table_name, f_geometry_column, ordinal, ");
    strcat (sql, "column_name, null_values, integer_values, ");
    strcat (sql, "double_values, text_values, blob_values, max_size, ");
    strcat (sql, "integer_min, integer_max, double_min, double_max) ");
    strcat (sql, "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

/* compiling SQL prepared statement */
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
	return 0;

    while (p)
      {
/* binding INSERT params */
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_text (stmt, 1, table, strlen (table), SQLITE_STATIC);
	  sqlite3_bind_text (stmt, 2, column, strlen (column), SQLITE_STATIC);
	  sqlite3_bind_int (stmt, 3, p->ordinal);
	  sqlite3_bind_text (stmt, 4, p->col_name, strlen (p->col_name),
			     SQLITE_STATIC);
	  sqlite3_bind_int (stmt, 5, p->null_values);
	  sqlite3_bind_int (stmt, 6, p->integer_values);
	  sqlite3_bind_int (stmt, 7, p->double_values);
	  sqlite3_bind_int (stmt, 8, p->text_values);
	  sqlite3_bind_int (stmt, 9, p->blob_values);
	  if (p->max_size < 0)
	      sqlite3_bind_null (stmt, 10);
	  else
	      sqlite3_bind_int (stmt, 10, p->max_size);
	  if (p->int_minmax_set)
	    {
		sqlite3_bind_int (stmt, 11, p->int_min);
		sqlite3_bind_int (stmt, 12, p->int_max);
	    }
	  else
	    {
		sqlite3_bind_null (stmt, 11);
		sqlite3_bind_null (stmt, 12);
	    }
	  if (p->dbl_minmax_set)
	    {
		sqlite3_bind_double (stmt, 13, p->dbl_min);
		sqlite3_bind_double (stmt, 14, p->dbl_max);
	    }
	  else
	    {
		sqlite3_bind_null (stmt, 13);
		sqlite3_bind_null (stmt, 14);
	    }
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      ;
	  else
	      error = 1;
	  p = p->next;
      }
    ret = sqlite3_finalize (stmt);
    if (ret != SQLITE_OK)
	return 0;
    if (error)
	return 0;
    return 1;
}

static int
do_update_views_field_infos (sqlite3 * sqlite, const char *table,
			     const char *column,
			     struct field_container_infos *infos)
{
/* update VIEW_GEOMETRY_COLUMNS_FIELD_INFOS Version >= 4.0.0 */
    char sql[8192];
    char *sql_statement;
    int ret;
    int error = 0;
    sqlite3_stmt *stmt;
    struct field_item_infos *p = infos->first;

/* deleting any previous row */
    sql_statement =
	sqlite3_mprintf ("DELETE FROM views_geometry_columns_field_infos "
			 "WHERE Lower(view_name) = Lower(%Q) AND "
			 "Lower(view_geometry) = Lower(%Q)", table, column);
    ret = sqlite3_exec (sqlite, sql_statement, NULL, NULL, NULL);
    sqlite3_free (sql_statement);
    if (ret != SQLITE_OK)
	return 0;

/* reinserting yet again */
    strcpy (sql, "INSERT INTO views_geometry_columns_field_infos ");
    strcat (sql, "(view_name, view_geometry, ordinal, ");
    strcat (sql, "column_name, null_values, integer_values, ");
    strcat (sql, "double_values, text_values, blob_values, max_size, ");
    strcat (sql, "integer_min, integer_max, double_min, double_max) ");
    strcat (sql, "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

/* compiling SQL prepared statement */
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
	return 0;

    while (p)
      {
/* binding INSERT params */
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_text (stmt, 1, table, strlen (table), SQLITE_STATIC);
	  sqlite3_bind_text (stmt, 2, column, strlen (column), SQLITE_STATIC);
	  sqlite3_bind_int (stmt, 3, p->ordinal);
	  sqlite3_bind_text (stmt, 4, p->col_name, strlen (p->col_name),
			     SQLITE_STATIC);
	  sqlite3_bind_int (stmt, 5, p->null_values);
	  sqlite3_bind_int (stmt, 6, p->integer_values);
	  sqlite3_bind_int (stmt, 7, p->double_values);
	  sqlite3_bind_int (stmt, 8, p->text_values);
	  sqlite3_bind_int (stmt, 9, p->blob_values);
	  if (p->max_size < 0)
	      sqlite3_bind_null (stmt, 10);
	  else
	      sqlite3_bind_int (stmt, 10, p->max_size);
	  if (p->int_minmax_set)
	    {
		sqlite3_bind_int (stmt, 11, p->int_min);
		sqlite3_bind_int (stmt, 12, p->int_max);
	    }
	  else
	    {
		sqlite3_bind_null (stmt, 11);
		sqlite3_bind_null (stmt, 12);
	    }
	  if (p->dbl_minmax_set)
	    {
		sqlite3_bind_double (stmt, 13, p->dbl_min);
		sqlite3_bind_double (stmt, 14, p->dbl_max);
	    }
	  else
	    {
		sqlite3_bind_null (stmt, 13);
		sqlite3_bind_null (stmt, 14);
	    }
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      ;
	  else
	      error = 1;
	  p = p->next;
      }
    ret = sqlite3_finalize (stmt);
    if (ret != SQLITE_OK)
	return 0;
    if (error)
	return 0;
    return 1;
}

static int
do_update_virts_field_infos (sqlite3 * sqlite, const char *table,
			     const char *column,
			     struct field_container_infos *infos)
{
/* update VIRTS_GEOMETRY_COLUMNS_FIELD_INFOS Version >= 4.0.0 */
    char sql[8192];
    char *sql_statement;
    int ret;
    int error = 0;
    sqlite3_stmt *stmt;
    struct field_item_infos *p = infos->first;

/* deleting any previous row */
    sql_statement =
	sqlite3_mprintf ("DELETE FROM virts_geometry_columns_field_infos "
			 "WHERE Lower(virt_name) = Lower(%Q) AND "
			 "Lower(virt_geometry) = Lower(%Q)", table, column);
    ret = sqlite3_exec (sqlite, sql_statement, NULL, NULL, NULL);
    sqlite3_free (sql_statement);
    if (ret != SQLITE_OK)
	return 0;

/* reinserting yet again */
    strcpy (sql, "INSERT INTO virts_geometry_columns_field_infos ");
    strcat (sql, "(virt_name, virt_geometry, ordinal, ");
    strcat (sql, "column_name, null_values, integer_values, ");
    strcat (sql, "double_values, text_values, blob_values, max_size, ");
    strcat (sql, "integer_min, integer_max, double_min, double_max) ");
    strcat (sql, "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

/* compiling SQL prepared statement */
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
	return 0;

    while (p)
      {
/* binding INSERT params */
	  sqlite3_reset (stmt);
	  sqlite3_clear_bindings (stmt);
	  sqlite3_bind_text (stmt, 1, table, strlen (table), SQLITE_STATIC);
	  sqlite3_bind_text (stmt, 2, column, strlen (column), SQLITE_STATIC);
	  sqlite3_bind_int (stmt, 3, p->ordinal);
	  sqlite3_bind_text (stmt, 4, p->col_name, strlen (p->col_name),
			     SQLITE_STATIC);
	  sqlite3_bind_int (stmt, 5, p->null_values);
	  sqlite3_bind_int (stmt, 6, p->integer_values);
	  sqlite3_bind_int (stmt, 7, p->double_values);
	  sqlite3_bind_int (stmt, 8, p->text_values);
	  sqlite3_bind_int (stmt, 9, p->blob_values);
	  if (p->max_size < 0)
	      sqlite3_bind_null (stmt, 10);
	  else
	      sqlite3_bind_int (stmt, 10, p->max_size);
	  if (p->int_minmax_set)
	    {
		sqlite3_bind_int (stmt, 11, p->int_min);
		sqlite3_bind_int (stmt, 12, p->int_max);
	    }
	  else
	    {
		sqlite3_bind_null (stmt, 11);
		sqlite3_bind_null (stmt, 12);
	    }
	  if (p->dbl_minmax_set)
	    {
		sqlite3_bind_double (stmt, 13, p->dbl_min);
		sqlite3_bind_double (stmt, 14, p->dbl_max);
	    }
	  else
	    {
		sqlite3_bind_null (stmt, 13);
		sqlite3_bind_null (stmt, 14);
	    }
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE || ret == SQLITE_ROW)
	      ;
	  else
	      error = 1;
	  p = p->next;
      }
    ret = sqlite3_finalize (stmt);
    if (ret != SQLITE_OK)
	return 0;
    if (error)
	return 0;
    return 1;
}

static int
do_compute_minmax (sqlite3 * sqlite, const char *table,
		   struct field_container_infos *infos)
{
/* Pass2 - computing Integer / Double min/max ranges */
    char *quoted;
    char *sql_statement;
    int int_min;
    int int_max;
    double dbl_min;
    double dbl_max;
    int ret;
    int i;
    int c;
    char **results;
    int rows;
    int columns;
    const char *col_name;
    int is_double;
    int comma = 0;
    int empty = 1;
    gaiaOutBuffer out_buf;
    struct field_item_infos *ptr;

    gaiaOutBufferInitialize (&out_buf);
    gaiaAppendToOutBuffer (&out_buf, "SELECT DISTINCT ");
    ptr = infos->first;
    while (ptr)
      {
	  quoted = gaiaDoubleQuotedSql (ptr->col_name);
	  if (ptr->integer_values >= 0 && ptr->double_values == 0
	      && ptr->blob_values == 0 && ptr->text_values == 0)
	    {
		if (comma)
		    sql_statement =
			sqlite3_mprintf (", 0, %Q, min(\"%s\"), max(\"%s\")",
					 ptr->col_name, quoted, quoted);
		else
		  {
		      comma = 1;
		      sql_statement =
			  sqlite3_mprintf (" 0, %Q, min(\"%s\"), max(\"%s\")",
					   ptr->col_name, quoted, quoted);
		  }
		gaiaAppendToOutBuffer (&out_buf, sql_statement);
		sqlite3_free (sql_statement);
		empty = 0;
	    }
	  if (ptr->double_values >= 0 && ptr->integer_values == 0
	      && ptr->blob_values == 0 && ptr->text_values == 0)
	    {
		if (comma)
		    sql_statement =
			sqlite3_mprintf (", 1, %Q, min(\"%s\"), max(\"%s\")",
					 ptr->col_name, quoted, quoted);
		else
		  {
		      comma = 1;
		      sql_statement =
			  sqlite3_mprintf (" 1, %Q, min(\"%s\"), max(\"%s\")",
					   ptr->col_name, quoted, quoted);
		  }
		gaiaAppendToOutBuffer (&out_buf, sql_statement);
		sqlite3_free (sql_statement);
		empty = 0;
	    }
	  free (quoted);
	  ptr = ptr->next;
      }
    if (out_buf.Buffer == NULL)
	return 0;
    if (empty)
      {
	  /* no columns to check */
	  gaiaOutBufferReset (&out_buf);
	  return 1;

      }
    quoted = gaiaDoubleQuotedSql (table);
    sql_statement = sqlite3_mprintf (" FROM \"%s\"", quoted);
    free (quoted);
    gaiaAppendToOutBuffer (&out_buf, sql_statement);
    sqlite3_free (sql_statement);
/* executing the SQL query */
    ret = sqlite3_get_table (sqlite, out_buf.Buffer, &results, &rows, &columns,
			     NULL);
    gaiaOutBufferReset (&out_buf);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		for (c = 0; c < columns; c += 4)
		  {
		      /* retrieving field infos */
		      is_double = atoi (results[(i * columns) + c + 0]);
		      col_name = results[(i * columns) + c + 1];
		      if (results[(i * columns) + c + 2] != NULL
			  && results[(i * columns) + c + 3] != NULL)
			{
			    if (!is_double)
			      {
				  int_min =
				      atoi (results[(i * columns) + c + 2]);
				  int_max =
				      atoi (results[(i * columns) + c + 3]);
				  update_field_infos_int_minmax (infos,
								 col_name,
								 int_min,
								 int_max);
			      }
			    else
			      {
				  dbl_min =
				      atof (results[(i * columns) + c + 2]);
				  dbl_max =
				      atof (results[(i * columns) + c + 3]);
				  update_field_infos_double_minmax (infos,
								    col_name,
								    dbl_min,
								    dbl_max);
			      }
			}
		  }
	    }
      }
    sqlite3_free_table (results);

    return 1;
}

static void
copy_attributes_into_layer (struct field_container_infos *infos,
			    gaiaVectorLayerPtr lyr)
{
/* copying the AttributeField definitions into the VectorLayer */
    gaiaLayerAttributeFieldPtr fld;
    int len;
    struct field_item_infos *p = infos->first;
    while (p)
      {
	  /* adding an AttributeField definition */
	  fld = malloc (sizeof (gaiaLayerAttributeField));
	  fld->Ordinal = p->ordinal;
	  len = strlen (p->col_name);
	  fld->AttributeFieldName = malloc (len + 1);
	  strcpy (fld->AttributeFieldName, p->col_name);
	  fld->NullValuesCount = p->null_values;
	  fld->IntegerValuesCount = p->integer_values;
	  fld->DoubleValuesCount = p->double_values;
	  fld->TextValuesCount = p->text_values;
	  fld->BlobValuesCount = p->blob_values;
	  fld->MaxSize = NULL;
	  fld->IntRange = NULL;
	  fld->DoubleRange = NULL;
	  if (p->max_size)
	    {
		fld->MaxSize = malloc (sizeof (gaiaAttributeFieldMaxSize));
		fld->MaxSize->MaxSize = p->max_size;
	    }
	  if (p->int_minmax_set)
	    {
		fld->IntRange = malloc (sizeof (gaiaAttributeFieldIntRange));
		fld->IntRange->MinValue = p->int_min;
		fld->IntRange->MaxValue = p->int_max;
	    }
	  if (p->dbl_minmax_set)
	    {
		fld->DoubleRange =
		    malloc (sizeof (gaiaAttributeFieldDoubleRange));
		fld->DoubleRange->MinValue = p->dbl_min;
		fld->DoubleRange->MaxValue = p->dbl_max;
	    }
	  fld->Next = NULL;
	  if (lyr->First == NULL)
	      lyr->First = fld;
	  if (lyr->Last != NULL)
	      lyr->Last->Next = fld;
	  lyr->Last = fld;
	  p = p->next;
      }
}

SPATIALITE_PRIVATE int
doComputeFieldInfos (void *p_sqlite, const char *table,
		     const char *column, int stat_type, void *p_lyr)
{
/* computes FIELD_INFOS [single table/geometry] */
    sqlite3 *sqlite = (sqlite3 *) p_sqlite;
    gaiaVectorLayerPtr lyr = (gaiaVectorLayerPtr) p_lyr;
    char *sql_statement;
    char *quoted;
    int ret;
    int i;
    int c;
    char **results;
    int rows;
    int columns;
    int ordinal;
    const char *col_name;
    const char *type;
    const char *sz;
    int size;
    int count;
    int error = 0;
    int comma = 0;
    gaiaOutBuffer out_buf;
    gaiaOutBuffer group_by;
    struct field_container_infos infos;

    gaiaOutBufferInitialize (&out_buf);
    gaiaOutBufferInitialize (&group_by);
    infos.first = NULL;
    infos.last = NULL;

/* retrieving the column names for the current table */
/* then building the SQL query statement */
    quoted = gaiaDoubleQuotedSql (table);
    sql_statement = sqlite3_mprintf ("PRAGMA table_info(\"%s\")", quoted);
    free (quoted);
    ret =
	sqlite3_get_table (sqlite, sql_statement, &results, &rows, &columns,
			   NULL);
    sqlite3_free (sql_statement);
    if (ret != SQLITE_OK)
	return 0;

    if (rows < 1)
	;
    else
      {
	  gaiaAppendToOutBuffer (&out_buf, "SELECT DISTINCT Count(*)");
	  gaiaAppendToOutBuffer (&group_by, "GROUP BY");
	  for (i = 1; i <= rows; i++)
	    {
		ordinal = atoi (results[(i * columns) + 0]);
		col_name = results[(i * columns) + 1];
		quoted = gaiaDoubleQuotedSql (col_name);
		sql_statement =
		    sqlite3_mprintf
		    (", %d, %Q AS col_%d, typeof(\"%s\") AS typ_%d, max(length(\"%s\"))",
		     ordinal, col_name, ordinal, quoted, ordinal, quoted);
		free (quoted);
		gaiaAppendToOutBuffer (&out_buf, sql_statement);
		sqlite3_free (sql_statement);
		if (!comma)
		  {
		      comma = 1;
		      sql_statement =
			  sqlite3_mprintf (" col_%d, typ_%d", ordinal, ordinal);
		  }
		else
		    sql_statement =
			sqlite3_mprintf (", col_%d, typ_%d", ordinal, ordinal);
		gaiaAppendToOutBuffer (&group_by, sql_statement);
		sqlite3_free (sql_statement);
	    }
      }
    sqlite3_free_table (results);

    if (out_buf.Buffer == NULL)
	return 0;
    quoted = gaiaDoubleQuotedSql (table);
    sql_statement = sqlite3_mprintf (" FROM \"%s\" ", quoted);
    free (quoted);
    gaiaAppendToOutBuffer (&out_buf, sql_statement);
    sqlite3_free (sql_statement);
    gaiaAppendToOutBuffer (&out_buf, group_by.Buffer);
    gaiaOutBufferReset (&group_by);

/* executing the SQL query */
    ret = sqlite3_get_table (sqlite, out_buf.Buffer, &results, &rows, &columns,
			     NULL);
    gaiaOutBufferReset (&out_buf);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		count = atoi (results[(i * columns) + 0]);
		for (c = 1; c < columns; c += 4)
		  {
		      /* retrieving field infos */
		      ordinal = atoi (results[(i * columns) + c + 0]);
		      col_name = results[(i * columns) + c + 1];
		      type = results[(i * columns) + c + 2];
		      sz = results[(i * columns) + c + 3];
		      if (sz == NULL)
			  size = -1;
		      else
			  size = atoi (sz);
		      update_field_infos (&infos, ordinal, col_name, type, size,
					  count);
		  }
	    }
      }
    sqlite3_free_table (results);

/* Pass-2: computing INTEGER and DOUBLE min/max ranges */
    if (!error)
      {
	  if (!do_compute_minmax (sqlite, table, &infos))
	      error = 1;
      }

    switch (stat_type)
      {
      case SPATIALITE_STATISTICS_LEGACY:
	  if (!error)
	      copy_attributes_into_layer (&infos, lyr);
	  free_field_infos (&infos);
	  if (error)
	      return 0;
	  return 1;
	  break;
      case SPATIALITE_STATISTICS_GENUINE:
	  if (!do_update_field_infos (sqlite, table, column, &infos))
	      error = 1;
	  break;
      case SPATIALITE_STATISTICS_VIEWS:
	  if (!do_update_views_field_infos (sqlite, table, column, &infos))
	      error = 1;
	  break;
      case SPATIALITE_STATISTICS_VIRTS:
	  if (!do_update_virts_field_infos (sqlite, table, column, &infos))
	      error = 1;
	  break;
      };
    free_field_infos (&infos);
    if (error)
	return 0;
    return 1;
}

static int
do_compute_layer_statistics (sqlite3 * sqlite, const char *table,
			     const char *column, int stat_type)
{
/* computes LAYER_STATISTICS [single table/geometry] */
    int ret;
    int error = 0;
    int count;
    double min_x = DBL_MAX;
    double min_y = DBL_MAX;
    double max_x = 0.0 - DBL_MAX;
    double max_y = 0.0 - DBL_MAX;
    int has_coords = 1;
    char *quoted;
    char *col_quoted;
    char *sql_statement;
    sqlite3_stmt *stmt;
    int metadata_version = checkSpatialMetaData (sqlite);

    quoted = gaiaDoubleQuotedSql ((const char *) table);
    col_quoted = gaiaDoubleQuotedSql ((const char *) column);
    sql_statement = sqlite3_mprintf ("SELECT Count(*), "
				     "Min(MbrMinX(\"%s\")), Min(MbrMinY(\"%s\")), Max(MbrMaxX(\"%s\")), Max(MbrMaxY(\"%s\")) "
				     "FROM \"%s\"", col_quoted, col_quoted,
				     col_quoted, col_quoted, quoted);
    free (quoted);
    free (col_quoted);

/* compiling SQL prepared statement */
    ret =
	sqlite3_prepare_v2 (sqlite, sql_statement, strlen (sql_statement),
			    &stmt, NULL);
    sqlite3_free (sql_statement);
    if (ret != SQLITE_OK)
	return 0;
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		count = sqlite3_column_int (stmt, 0);
		if (sqlite3_column_type (stmt, 1) == SQLITE_NULL)
		    has_coords = 0;
		else
		    min_x = sqlite3_column_double (stmt, 1);
		if (sqlite3_column_type (stmt, 2) == SQLITE_NULL)
		    has_coords = 0;
		else
		    min_y = sqlite3_column_double (stmt, 2);
		if (sqlite3_column_type (stmt, 3) == SQLITE_NULL)
		    has_coords = 0;
		else
		    max_x = sqlite3_column_double (stmt, 3);
		if (sqlite3_column_type (stmt, 4) == SQLITE_NULL)
		    has_coords = 0;
		else
		    max_y = sqlite3_column_double (stmt, 4);
		switch (stat_type)
		  {
		  case SPATIALITE_STATISTICS_GENUINE:
		      if (!do_update_layer_statistics
			  (sqlite, table, column, count, has_coords, min_x,
			   min_y, max_x, max_y))
			  error = 1;
		      break;
		  case SPATIALITE_STATISTICS_VIEWS:
		      if (!do_update_views_layer_statistics
			  (sqlite, table, column, count, has_coords, min_x,
			   min_y, max_x, max_y))
			  error = 1;
		      break;
		  case SPATIALITE_STATISTICS_VIRTS:
		      if (!do_update_virts_layer_statistics
			  (sqlite, table, column, count, has_coords, min_x,
			   min_y, max_x, max_y))
			  error = 1;
		      break;
		  };
	    }
	  else
	      error = 1;
      }
    ret = sqlite3_finalize (stmt);
    if (ret != SQLITE_OK)
	return 0;
    if (error)
	return 0;
    if (metadata_version == 3)
      {
	  /* current metadata style >= v.4.0.0 */
	  if (!doComputeFieldInfos (sqlite, table, column, stat_type, NULL))
	      return 0;
      }
    return 1;
}

static int
genuine_layer_statistics_v4 (sqlite3 * sqlite, const char *table,
			     const char *column)
{
/* updating GEOMETRY_COLUMNS_STATISTICS Version >= 4.0.0 */
    char *sql_statement;
    int ret;
    const char *f_table_name;
    const char *f_geometry_column;
    int i;
    char **results;
    int rows;
    int columns;
    int error = 0;

    if (table == NULL && column == NULL)
      {
	  /* processing any table/geometry found in GEOMETRY_COLUMNS */
	  sql_statement =
	      sqlite3_mprintf ("SELECT t.f_table_name, t.f_geometry_column "
			       "FROM geometry_columns_time AS t, "
			       "geometry_columns_statistics AS s "
			       "WHERE Lower(s.f_table_name) = Lower(t.f_table_name) AND "
			       "Lower(s.f_geometry_column) = Lower(t.f_geometry_column) AND "
			       "(s.last_verified < t.last_insert OR "
			       "s.last_verified < t.last_update OR "
			       "s.last_verified < t.last_delete OR "
			       "s.last_verified IS NULL)");
      }
    else if (column == NULL)
      {
	  /* processing any geometry belonging to this table */
	  sql_statement =
	      sqlite3_mprintf ("SELECT t.f_table_name, t.f_geometry_column "
			       "FROM geometry_columns_time AS t, "
			       "geometry_columns_statistics AS s "
			       "WHERE Lower(t.f_table_name) = Lower(%Q) AND "
			       "Lower(s.f_table_name) = Lower(t.f_table_name) AND "
			       "Lower(s.f_geometry_column) = Lower(t.f_geometry_column) AND "
			       "(s.last_verified < t.last_insert OR "
			       "s.last_verified < t.last_update OR "
			       "s.last_verified < t.last_delete OR "
			       "s.last_verified IS NULL)", table);
      }
    else
      {
	  /* processing a single table/geometry entry */
	  sql_statement =
	      sqlite3_mprintf ("SELECT t.f_table_name, t.f_geometry_column "
			       "FROM geometry_columns_time AS t, "
			       "geometry_columns_statistics AS s "
			       "WHERE Lower(t.f_table_name) = Lower(%Q) AND "
			       "Lower(t.f_geometry_column) = Lower(%Q) AND "
			       "Lower(s.f_table_name) = Lower(t.f_table_name) AND "
			       "Lower(s.f_geometry_column) = Lower(t.f_geometry_column) AND "
			       "(s.last_verified < t.last_insert OR "
			       "s.last_verified < t.last_update OR "
			       "s.last_verified < t.last_delete OR "
			       "s.last_verified IS NULL)", table, column);
      }
    ret =
	sqlite3_get_table (sqlite, sql_statement, &results, &rows, &columns,
			   NULL);
    sqlite3_free (sql_statement);
    if (ret != SQLITE_OK)
	return 0;

    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		f_table_name = results[(i * columns) + 0];
		f_geometry_column = results[(i * columns) + 1];
		if (!do_compute_layer_statistics
		    (sqlite, f_table_name, f_geometry_column,
		     SPATIALITE_STATISTICS_GENUINE))
		  {
		      error = 1;
		      break;
		  }
	    }
      }
    sqlite3_free_table (results);
    if (error)
	return 0;
    return 1;
}

static int
genuine_layer_statistics (sqlite3 * sqlite, const char *table,
			  const char *column)
{
/* updating genuine LAYER_STATISTICS metadata */
    char *sql_statement;
    int ret;
    const char *f_table_name;
    const char *f_geometry_column;
    int i;
    char **results;
    int rows;
    int columns;
    int error = 0;
    int metadata_version = checkSpatialMetaData (sqlite);

    if (metadata_version == 3)
      {
	  /* current metadata style >= v.4.0.0 */
	  return genuine_layer_statistics_v4 (sqlite, table, column);
      }

    if (table == NULL && column == NULL)
      {
	  /* processing any table/geometry found in GEOMETRY_COLUMNS */
	  sql_statement =
	      sqlite3_mprintf ("SELECT f_table_name, f_geometry_column "
			       "FROM geometry_columns");
      }
    else if (column == NULL)
      {
	  /* processing any geometry belonging to this table */
	  sql_statement =
	      sqlite3_mprintf ("SELECT f_table_name, f_geometry_column "
			       "FROM geometry_columns "
			       "WHERE Lower(f_table_name) = Lower(%Q)", table);
      }
    else
      {
	  /* processing a single table/geometry entry */
	  sql_statement =
	      sqlite3_mprintf ("SELECT f_table_name, f_geometry_column "
			       "FROM geometry_columns "
			       "WHERE Lower(f_table_name) = Lower(%Q) "
			       "AND Lower(f_geometry_column) = Lower(%Q)",
			       table, column);
      }
    ret =
	sqlite3_get_table (sqlite, sql_statement, &results, &rows, &columns,
			   NULL);
    sqlite3_free (sql_statement);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		f_table_name = results[(i * columns) + 0];
		f_geometry_column = results[(i * columns) + 1];
		if (!do_compute_layer_statistics
		    (sqlite, f_table_name, f_geometry_column,
		     SPATIALITE_STATISTICS_GENUINE))
		  {
		      error = 1;
		      break;
		  }
	    }
      }
    sqlite3_free_table (results);
    if (error)
	return 0;
    return 1;
}

static int
views_layer_statistics (sqlite3 * sqlite, const char *table, const char *column)
{
/* updating VIEWS_LAYER_STATISTICS metadata */
    char *sql_statement;
    int ret;
    const char *view_name;
    const char *view_geometry;
    int i;
    char **results;
    int rows;
    int columns;
    int error = 0;

    if (table == NULL && column == NULL)
      {
	  /* processing any table/geometry found in VIEWS_GEOMETRY_COLUMNS */
	  sql_statement = sqlite3_mprintf ("SELECT view_name, view_geometry "
					   "FROM views_geometry_columns");
      }
    else if (column == NULL)
      {
	  /* processing any geometry belonging to this table */
	  sql_statement = sqlite3_mprintf ("SELECT view_name, view_geometry "
					   "FROM views_geometry_columns "
					   "WHERE Lower(view_name) = Lower(%Q)",
					   table);
      }
    else
      {
	  /* processing a single table/geometry entry */
	  sql_statement = sqlite3_mprintf ("SELECT view_name, view_geometry "
					   "FROM views_geometry_columns "
					   "WHERE Lower(view_name) = Lower(%Q) "
					   "AND Lower(view_geometry) = Lower(%Q)",
					   table, column);
      }
    ret =
	sqlite3_get_table (sqlite, sql_statement, &results, &rows, &columns,
			   NULL);
    sqlite3_free (sql_statement);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		view_name = results[(i * columns) + 0];
		view_geometry = results[(i * columns) + 1];
		if (!do_compute_layer_statistics
		    (sqlite, view_name, view_geometry,
		     SPATIALITE_STATISTICS_VIEWS))
		  {
		      error = 1;
		      break;
		  }
	    }
      }
    sqlite3_free_table (results);
    if (error)
	return 0;
    return 1;
}

static int
virts_layer_statistics (sqlite3 * sqlite, const char *table, const char *column)
{
/* updating VIRTS_LAYER_STATISTICS metadata */
    char *sql_statement;
    int ret;
    const char *f_table_name;
    const char *f_geometry_column;
    int i;
    char **results;
    int rows;
    int columns;
    int error = 0;

    if (table == NULL && column == NULL)
      {
	  /* processing any table/geometry found in VIRTS_GEOMETRY_COLUMNS */
	  sql_statement = sqlite3_mprintf ("SELECT virt_name, virt_geometry "
					   "FROM virts_geometry_columns");
      }
    else if (column == NULL)
      {
	  /* processing any geometry belonging to this table */
	  sql_statement = sqlite3_mprintf ("SELECT virt_name, virt_geometry "
					   "FROM virts_geometry_columns "
					   "WHERE Lower(virt_name) = Lower(%Q)",
					   table);
      }
    else
      {
	  /* processing a single table/geometry entry */
	  sql_statement = sqlite3_mprintf ("SELECT virt_name, virt_geometry "
					   "FROM virts_geometry_columns "
					   "WHERE Lower(virt_name) = Lower(%Q) "
					   "AND Lower(virt_geometry) = Lower(%Q)",
					   table, column);
      }
    ret =
	sqlite3_get_table (sqlite, sql_statement, &results, &rows, &columns,
			   NULL);
    sqlite3_free (sql_statement);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		f_table_name = results[(i * columns) + 0];
		f_geometry_column = results[(i * columns) + 1];
		if (!do_compute_layer_statistics
		    (sqlite, f_table_name, f_geometry_column,
		     SPATIALITE_STATISTICS_VIRTS))
		  {
		      error = 1;
		      break;
		  }
	    }
      }
    sqlite3_free_table (results);
    if (error)
	return 0;
    return 1;
}

static int
has_views_metadata (sqlite3 * sqlite)
{
/* testing if the VIEWS_GEOMETRY_COLUMNS table exists */
    char **results;
    int rows;
    int columns;
    int ret;
    int i;
    int defined = 0;
    ret =
	sqlite3_get_table (sqlite, "PRAGMA table_info(views_geometry_columns)",
			   &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	      defined = 1;
      }
    sqlite3_free_table (results);
    return defined;
}

static int
has_virts_metadata (sqlite3 * sqlite)
{
/* testing if the VIRTS_GEOMETRY_COLUMNS table exists */
    char **results;
    int rows;
    int columns;
    int ret;
    int i;
    int defined = 0;
    ret =
	sqlite3_get_table (sqlite, "PRAGMA table_info(virts_geometry_columns)",
			   &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	      defined = 1;
      }
    sqlite3_free_table (results);
    return defined;
}

SPATIALITE_DECLARE int
update_layer_statistics (sqlite3 * sqlite, const char *table,
			 const char *column)
{
/* updating LAYER_STATISTICS metadata [main] */
    if (!genuine_layer_statistics (sqlite, table, column))
	return 0;
    if (has_views_metadata (sqlite))
      {
	  if (!views_layer_statistics (sqlite, table, column))
	      return 0;
      }
    if (has_virts_metadata (sqlite))
      {
	  if (!virts_layer_statistics (sqlite, table, column))
	      return 0;
      }
    return 1;
}

struct table_params
{
/* a struct supporting Drop Table / Rename Table / Rename Column */
    char **rtrees;
    int n_rtrees;
    int is_view;
    int ok_geometry_columns;
    int ok_geometry_columns_time;
    int ok_views_geometry_columns;
    int ok_virts_geometry_columns;
    int ok_geometry_columns_auth;
    int ok_geometry_columns_field_infos;
    int ok_geometry_columns_statistics;
    int ok_views_geometry_columns_auth;
    int ok_views_geometry_columns_field_infos;
    int ok_views_geometry_columns_statistics;
    int ok_virts_geometry_columns_auth;
    int ok_virts_geometry_columns_field_infos;
    int ok_virts_geometry_columns_statistics;
    int ok_layer_statistics;
    int ok_views_layer_statistics;
    int ok_virts_layer_statistics;
    int ok_layer_params;
    int ok_layer_sub_classes;
    int ok_layer_table_layout;
    int ok_vector_coverages;
    int ok_vector_coverages_keyword;
    int ok_vector_coverages_srid;
    int ok_se_vector_styled_layers;
    char *error_message;
};

static int
do_check_existing (sqlite3 * sqlite, const char *prefix, const char *table,
		   int table_only)
{
/* checking for an existing Table or View */
    char *sql;
    char *q_prefix;
    int count = 0;
    int ret;
    int i;
    char **results;
    int rows;
    int columns;

    q_prefix = gaiaDoubleQuotedSql (prefix);
    if (table_only)
      {
	  sql =
	      sqlite3_mprintf ("SELECT Count(*) FROM \"%s\".sqlite_master "
			       "WHERE Upper(name) = Upper(%Q) AND type = 'table'",
			       q_prefix, table);
      }
    else
      {
	  sql =
	      sqlite3_mprintf ("SELECT Count(*) FROM \"%s\".sqlite_master "
			       "WHERE Upper(name) = Upper(%Q) AND type IN ('table', 'view')",
			       q_prefix, table);
      }
    free (q_prefix);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	goto unknown;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	      count = atoi (results[(i * columns) + 0]);
      }
    sqlite3_free_table (results);
  unknown:
    return count;
}

static int
do_check_view (sqlite3 * sqlite, const char *prefix, const char *view)
{
/* checking for an existing View */
    char *sql;
    char *q_prefix;
    int count = 0;
    int ret;
    int i;
    char **results;
    int rows;
    int columns;

    q_prefix = gaiaDoubleQuotedSql (prefix);
    sql =
	sqlite3_mprintf ("SELECT Count(*) FROM \"%s\".sqlite_master "
			 "WHERE Upper(name) = Upper(%Q) AND type = 'view'",
			 q_prefix, view);
    free (q_prefix);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	goto unknown;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	      count = atoi (results[(i * columns) + 0]);
      }
    sqlite3_free_table (results);
  unknown:
    return count;
}

static int
do_check_existing_column (sqlite3 * sqlite, const char *prefix,
			  const char *table, const char *column)
{
/* checking for an existing Table Column */
    char *sql;
    char *q_prefix;
    int ret;
    int i;
    char **results;
    int rows;
    int columns;
    int exists = 0;

    q_prefix = gaiaDoubleQuotedSql (prefix);
    sql = sqlite3_mprintf ("PRAGMA \"%s\".table_info(%Q)", q_prefix, table);
    free (q_prefix);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	goto unknown;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		const char *col = results[(i * columns) + 1];
		if (strcasecmp (column, col) == 0)
		    exists = 1;
	    }
      }
    sqlite3_free_table (results);
  unknown:
    return exists;
}

static char *
do_retrieve_coverage_name (sqlite3 * sqlite, const char *prefix,
			   const char *table, int table_only)
{
/* checking for a registered Vector Coverage */
    char *sql;
    char *q_prefix;
    char *coverage_name = NULL;
    int ret;
    int i;
    char **results;
    int rows;
    int columns;

    q_prefix = gaiaDoubleQuotedSql (prefix);
    if (table_only)
      {
	  sql =
	      sqlite3_mprintf
	      ("SELECT coverage_name FROM \"%s\".vector_coverages "
	       "WHERE f_table_name = %Q", q_prefix, table);
      }
    else
      {
	  sql =
	      sqlite3_mprintf
	      ("SELECT coverage_name FROM \"%s\".vector_coverages "
	       "WHERE f_table_name = %Q OR view_name = %Q OR virt_name = %Q",
	       q_prefix, table, table, table);
      }
    free (q_prefix);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	goto unknown;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		const char *cvg = results[(i * columns) + 0];
		if (cvg != NULL)
		  {
		      int len = strlen (cvg);
		      if (coverage_name != NULL)
			  free (coverage_name);
		      coverage_name = malloc (len + 1);
		      strcpy (coverage_name, cvg);
		  }
	    }
      }
    sqlite3_free_table (results);
  unknown:
    return coverage_name;
}

static int
do_drop_table (sqlite3 * sqlite, const char *prefix, const char *table,
	       struct table_params *aux)
{
/* performing the actual work */
    char *sql;
    char *q_prefix;
    char *q_name;
    int i;
    int ret;
    char *errMsg = NULL;

    if (!do_check_existing (sqlite, prefix, table, 0))
	return 0;

    if (aux->ok_vector_coverages)
      {
	  /* deleting from VECTOR_COVERAGES */
	  char *coverage = do_retrieve_coverage_name (sqlite, prefix, table, 0);
	  if (coverage != NULL)
	    {
		q_prefix = gaiaDoubleQuotedSql (prefix);
		if (aux->ok_vector_coverages_srid)
		  {
		      /* deleting from VECTOR_COVERAGES_SRID  */
		      sql =
			  sqlite3_mprintf
			  ("DELETE FROM \"%s\".vector_coverages_srid "
			   "WHERE lower(coverage_name) = lower(%Q)", q_prefix,
			   coverage);
		      ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
		      sqlite3_free (sql);
		      if (ret != SQLITE_OK)
			{
			    aux->error_message = errMsg;
			    return 0;
			}
		  }
		if (aux->ok_vector_coverages_keyword)
		  {
		      /* deleting from VECTOR_COVERAGES_KEYWORD  */
		      sql =
			  sqlite3_mprintf
			  ("DELETE FROM \"%s\".vector_coverages_keyword "
			   "WHERE lower(coverage_name) = lower(%Q)", q_prefix,
			   coverage);
		      ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
		      sqlite3_free (sql);
		      if (ret != SQLITE_OK)
			{
			    aux->error_message = errMsg;
			    return 0;
			}
		  }
		if (aux->ok_se_vector_styled_layers)
		  {
		      /* deleting from SE_VECTOR_STYLED_LAYERS  */
		      sql =
			  sqlite3_mprintf
			  ("DELETE FROM \"%s\".SE_vector_styled_layers "
			   "WHERE lower(coverage_name) = lower(%Q)", q_prefix,
			   coverage);
		      ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
		      sqlite3_free (sql);
		      if (ret != SQLITE_OK)
			{
			    aux->error_message = errMsg;
			    return 0;
			}
		  }
		/* and finally deleting from VECTOR COVERAGES */
		sql = sqlite3_mprintf ("DELETE FROM \"%s\".vector_coverages "
				       "WHERE lower(coverage_name) = lower(%Q)",
				       q_prefix, coverage);
		ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
		sqlite3_free (sql);
		if (ret != SQLITE_OK)
		  {
		      aux->error_message = errMsg;
		      return 0;
		  }
		free (q_prefix);
		free (coverage);
	    }
      }

    if (aux->is_view)
      {
	  /* dropping a View */
	  q_name = gaiaDoubleQuotedSql (table);
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf ("DROP VIEW IF EXISTS \"%s\".\"%s\"", q_prefix,
			       q_name);
	  free (q_prefix);
	  free (q_name);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    else
      {
	  /* dropping a Table */
	  q_name = gaiaDoubleQuotedSql (table);
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf ("DROP TABLE IF EXISTS \"%s\".\"%s\"", q_prefix,
			       q_name);
	  free (q_prefix);
	  free (q_name);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }

    for (i = 0; i < aux->n_rtrees; i++)
      {
	  /* dropping any R*Tree */
	  q_name = gaiaDoubleQuotedSql (*(aux->rtrees + i));
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf ("DROP TABLE IF EXISTS \"%s\".\"%s\"", q_prefix,
			       q_name);
	  free (q_prefix);
	  free (q_name);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }

    if (aux->ok_layer_params)
      {
	  /* deleting from LAYER_PARAMS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql = sqlite3_mprintf ("DELETE FROM \"%s\".layer_params "
				 "WHERE lower(table_name) = lower(%Q)",
				 q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_layer_sub_classes)
      {
	  /* deleting from LAYER_SUB_CLASSES */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql = sqlite3_mprintf ("DELETE FROM \"%s\".layer_sub_classes "
				 "WHERE lower(table_name) = lower(%Q)",
				 q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_layer_table_layout)
      {
	  /* deleting from LAYER_TABLE_LAYOUT */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql = sqlite3_mprintf ("DELETE FROM \"%s\".layer_table_layout "
				 "WHERE lower(table_name) = lower(%Q)",
				 q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_geometry_columns_auth)
      {
	  /* deleting from GEOMETRY_COLUMNS_AUTH */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql = sqlite3_mprintf ("DELETE FROM \"%s\".geometry_columns_auth "
				 "WHERE lower(f_table_name) = lower(%Q)",
				 q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_geometry_columns_time)
      {
	  /* deleting from GEOMETRY_COLUMNS_TIME */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql = sqlite3_mprintf ("DELETE FROM \"%s\".geometry_columns_time "
				 "WHERE lower(f_table_name) = lower(%Q)",
				 q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_geometry_columns_field_infos)
      {
	  /* deleting from GEOMETRY_COLUMNS_FIELD_INFOS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".geometry_columns_field_infos "
	       "WHERE lower(f_table_name) = lower(%Q)", q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_geometry_columns_statistics)
      {
	  /* deleting from GEOMETRY_COLUMNS_STATISTICS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf ("DELETE FROM \"%s\".geometry_columns_statistics "
			       "WHERE lower(f_table_name) = lower(%Q)",
			       q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_views_geometry_columns_auth)
      {
	  /* deleting from VIEWS_GEOMETRY_COLUMNS_AUTH */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf ("DELETE FROM \"%s\".views_geometry_columns_auth "
			       "WHERE lower(view_name) = lower(%Q)", q_prefix,
			       table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_views_geometry_columns_field_infos)
      {
	  /* deleting from VIEWS_GEOMETRY_COLUMNS_FIELD_INFOS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".views_geometry_columns_field_infos "
	       "WHERE view_name = %Q", q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_views_geometry_columns_statistics)
      {
	  /* deleting from VIEWS_GEOMETRY_COLUMNS_STATISTICS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".views_geometry_columns_statistics "
	       "WHERE lower(view_name) = lower(%Q)", q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_virts_geometry_columns_auth)
      {
	  /* deleting from VIRTS_GEOMETRY_COLUMNS_AUTH */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf ("DELETE FROM \"%s\".virts_geometry_columns_auth "
			       "WHERE lower(virt_name) = lower(%Q)", q_prefix,
			       table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_virts_geometry_columns_field_infos)
      {
	  /* deleting from VIRTS_GEOMETRY_COLUMNS_FIELD_INFOS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".virts_geometry_columns_field_infos "
	       "WHERE lower(virt_name) = lower(%Q)", q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_virts_geometry_columns_statistics)
      {
	  /* deleting from VIRTS_GEOMETRY_COLUMNS_STATISTICS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".virts_geometry_columns_statistics "
	       "WHERE lower(virt_name) = lower(%Q)", q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_layer_statistics)
      {
	  /* deleting from LAYER_STATISTICS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql = sqlite3_mprintf ("DELETE FROM \"%s\".layer_statistics "
				 "WHERE lower(table_name) = lower(%Q)",
				 q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_views_layer_statistics)
      {
	  /* deleting from VIEWS_LAYER_STATISTICS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql = sqlite3_mprintf ("DELETE FROM \"%s\".views_layer_statistics "
				 "WHERE lower(view_name) = lower(%Q)", q_prefix,
				 table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_virts_layer_statistics)
      {
	  /* deleting from VIRTS_LAYER_STATISTICS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql = sqlite3_mprintf ("DELETE FROM \"%s\".virts_layer_statistics "
				 "WHERE lower(virt_name) = lower(%Q)", q_prefix,
				 table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_geometry_columns)
      {
	  /* deleting from GEOMETRY_COLUMNS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql = sqlite3_mprintf ("DELETE FROM \"%s\".geometry_columns "
				 "WHERE lower(f_table_name) = lower(%Q)",
				 q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_views_geometry_columns)
      {
	  /* deleting from VIEWS_GEOMETRY_COLUMNS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql = sqlite3_mprintf ("DELETE FROM \"%s\".views_geometry_columns "
				 "WHERE lower(view_name) = lower(%Q)", q_prefix,
				 table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_virts_geometry_columns)
      {
	  /* deleting from VIEWS_GEOMETRY_COLUMNS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql = sqlite3_mprintf ("DELETE FROM \"%s\".virts_geometry_columns "
				 "WHERE lower(virt_name) = lower(%Q)", q_prefix,
				 table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }

    return 1;
}

static int
doDropGeometryTriggers (sqlite3 * sqlite, const char *table, const char *geom,
			char **error_message)
{
/* dropping Geometry Triggers */
    int ret;
    int i;
    char **results;
    int rows;
    int columns;
    char *sql;
    char *string;
    char *errMsg = NULL;

/* searching all Geometry Triggers to be dropped */
    string = sqlite3_mprintf ("%%_%s_%s", table, geom);
    sql =
	sqlite3_mprintf
	("SELECT name FROM MAIN.sqlite_master WHERE name LIKE %Q "
	 "AND type = 'trigger'", string);
    sqlite3_free (string);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		/* found a Geometry Trigger */
		const char *trigger = results[(i * columns) + 0];
		char *q_trigger = gaiaDoubleQuotedSql (trigger);
		sql = sqlite3_mprintf ("DROP TRIGGER main.\"%s\"", q_trigger);
		free (q_trigger);
		ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
		sqlite3_free (sql);
		if (ret != SQLITE_OK)
		  {
		      if (error_message != NULL)
			  *error_message = errMsg;
		      return 0;
		  }
	    }
      }
    sqlite3_free_table (results);
    return 1;
}

static int
do_drop_geotriggers (sqlite3 * sqlite, const char *table, char **error_message)
{
/* dropping any previous Geometry Trigger */
    int ret;
    int i;
    char **results;
    int rows;
    int columns;
    char *sql;

/* searching all Geometry Triggers defined by the old Table */
    sql =
	sqlite3_mprintf
	("SELECT f_geometry_column FROM MAIN.geometry_columns "
	 "WHERE Lower(f_table_name) = Lower(%Q)", table);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		/* dropping Triggers for some Geometry Column */
		const char *geom = results[(i * columns) + 0];
		if (!doDropGeometryTriggers
		    (sqlite, table, geom, error_message))
		  {
		      sqlite3_free_table (results);
		      return 0;
		  }
	    }
      }
    sqlite3_free_table (results);
    return 1;
}

static int
do_rebuild_geotriggers (sqlite3 * sqlite, const char *table)
{
/* re-installing all Geometry Triggers */
    int ret;
    int i;
    char **results;
    int rows;
    int columns;
    char *sql;

/* searching all Geometry Triggers defined by the Table */
    sql =
	sqlite3_mprintf
	("SELECT f_geometry_column FROM MAIN.geometry_columns "
	 "WHERE Lower(f_table_name) = Lower(%Q)", table);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		/* found a Geometry Column */
		const char *geom = results[(i * columns) + 0];
		updateGeometryTriggers (sqlite, table, geom);
	    }
      }
    sqlite3_free_table (results);
    return 1;
}

static int
do_rename_column_pre (sqlite3 * sqlite, const char *prefix, const char *table,
		      const char *old_name, const char *new_name,
		      struct table_params *aux, char **error_message)
{
/* renaming a Column - preparatory steps */
    char *sql;
    char *q_prefix;
    int ret;
    char *errMsg = NULL;

/* dropping any previous Geometry Trigger */
    if (!do_drop_geotriggers (sqlite, table, error_message))
	return 0;

/* updating fist all metadata tables */
    if (aux->ok_geometry_columns)
      {
	  /* updating GEOMETRY_COLUMNS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".geometry_columns SET f_geometry_column = lower(%Q) "
	       "WHERE lower(f_table_name) = lower(%Q) AND lower(f_geometry_column) = lower(%Q)",
	       q_prefix, new_name, table, old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }

    if (aux->ok_layer_params)
      {
	  /* updating LAYER_PARAMS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".layer_params SET geometry_column = lower(%Q) "
	       "WHERE lower(table_name) = lower(%Q) AND lower(geometry_column) = lower(%Q)",
	       q_prefix, new_name, table, old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_layer_sub_classes)
      {
	  /* updating LAYER_SUB_CLASSES */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".layer_sub_classes SET geometry_column = lower(%Q) "
	       "WHERE lower(table_name) = lower(%Q) AND lower(geometry_column) = lower(%Q)",
	       q_prefix, new_name, table, old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_layer_table_layout)
      {
	  /* updating LAYER_TABLE_LAYOUT */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".layer_table_layout SET geometry_column = lower(%Q) "
	       "WHERE lower(table_name) = lower(%Q) AND lower(geometry_column) = lower(%Q)",
	       q_prefix, new_name, table, old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_geometry_columns_auth)
      {
	  /* updating GEOMETRY_COLUMNS_AUTH */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".geometry_columns_auth SET f_geometry_column = lower(%Q) "
	       "WHERE lower(f_table_name) = lower(%Q) AND lower(f_geometry_column) = lower(%Q)",
	       q_prefix, new_name, table, old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_geometry_columns_time)
      {
	  /* updating GEOMETRY_COLUMNS_TIME */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".geometry_columns_time SET f_geometry_column = lower(%Q) "
	       "WHERE lower(f_table_name) = lower(%Q) AND lower(f_geometry_column) = lower(%Q)",
	       q_prefix, new_name, table, old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_geometry_columns_field_infos)
      {
	  /* updating GEOMETRY_COLUMNS_FIELD_INFOS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".geometry_columns_field_infos SET f_geometry_column = lower(%Q) "
	       "WHERE lower(f_table_name) = lower(%Q) AND lower(f_geometry_column) = lower(%Q)",
	       q_prefix, new_name, table, old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_geometry_columns_statistics)
      {
	  /* updating GEOMETRY_COLUMNS_STATISTICS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".geometry_columns_statistics SET f_geometry_column = lower(%Q) "
	       "WHERE lower(f_table_name) = lower(%Q) AND lower(f_geometry_column) = lower(%Q)",
	       q_prefix, new_name, table, old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_layer_statistics)
      {
	  /* updating LAYER_STATISTICS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".layer_statistics SET geometry_column = lower(%Q) "
	       "WHERE lower(table_name) = lower(%Q) AND lower(geometry_column) = lower(%Q)",
	       q_prefix, new_name, old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_vector_coverages)
      {
	  /* updating VECTOR_COVERAGES */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".vector_coverages SET f_geometry_column = %Q "
	       "WHERE lower(f_table_name) = lower(%Q) AND lower(f_geometry_column) = lower(%Q)",
	       q_prefix, new_name, table, old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    return 1;
}

static int
do_rename_column_post (sqlite3 * sqlite, const char *prefix, const char *table,
		       const char *old_name, const char *new_name,
		       char **error_message)
{
/* renaming a Column - final steps */
    char *sql;
    char *q_prefix;
    char *q_table;
    char *q_old;
    char *q_new;
    int ret;
    char *errMsg = NULL;

/* renaming the Column itself */
    q_prefix = gaiaDoubleQuotedSql (prefix);
    q_table = gaiaDoubleQuotedSql (table);
    q_old = gaiaDoubleQuotedSql (old_name);
    q_new = gaiaDoubleQuotedSql (new_name);
    sql =
	sqlite3_mprintf
	("ALTER TABLE \"%s\".\"%s\" RENAME COLUMN \"%s\" TO \"%s\"", q_prefix,
	 q_table, q_old, q_new);
    free (q_prefix);
    free (q_table);
    free (q_old);
    free (q_new);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  if (error_message != NULL)
	      *error_message = errMsg;
	  return 0;
      }

/* re-installing all Geometry Triggers */
    if (!do_rebuild_geotriggers (sqlite, table))
      {
	  if (error_message != NULL)
	      *error_message =
		  sqlite3_mprintf ("unable to rebuild Geometry Triggers");
	  return 0;
      }
    return 1;
}

static int
do_drop_table5 (sqlite3 * sqlite, const char *prefix, const char *table,
		struct table_params *aux, char **error_message)
{
/* dropping a Table */
    char *sql;
    char *q_prefix;
    char *q_table;
    int ret;
    char *errMsg = NULL;

/* updating fist all metadata tables */
    if (aux->ok_geometry_columns)
      {
	  /* deleting from GEOMETRY_COLUMNS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".geometry_columns WHERE lower(f_table_name) = lower(%Q)",
	       q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_views_geometry_columns)
      {
	  /* deleting from VIEWS_GEOMETRY_COLUMNS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".views_geometry_columns WHERE lower(view_name) = lower(%Q)",
	       q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }

    if (aux->ok_layer_params)
      {
	  /* deleting from LAYER_PARAMS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".layer_params WHERE lower(table_name) = lower(%Q)",
	       q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_layer_sub_classes)
      {
	  /* deleting from LAYER_SUB_CLASSES */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".layer_sub_classes WHERE lower(table_name) = lower(%Q)",
	       q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_layer_table_layout)
      {
	  /* deleting from LAYER_TABLE_LAYOUT */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".layer_table_layout WHERE lower(table_name) = lower(%Q)",
	       q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_geometry_columns_auth)
      {
	  /* deleting from GEOMETRY_COLUMNS_AUTH */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".geometry_columns_auth WHERE lower(f_table_name) = lower(%Q)",
	       q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_geometry_columns_time)
      {
	  /* deleting from GEOMETRY_COLUMNS_TIME */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".geometry_columns_time WHERE lower(f_table_name) = lower(%Q)",
	       q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_geometry_columns_field_infos)
      {
	  /* deleting from GEOMETRY_COLUMNS_FIELD_INFOS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".geometry_columns_field_infos WHERE lower(f_table_name) = lower(%Q)",
	       q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_geometry_columns_statistics)
      {
	  /* deleting from GEOMETRY_COLUMNS_STATISTICS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".geometry_columns_statistics WHERE lower(f_table_name) = lower(%Q)",
	       q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_layer_statistics)
      {
	  /* deleting from LAYER_STATISTICS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".layer_statistics WHERE lower(table_name) = lower(%Q)",
	       q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_vector_coverages)
      {
	  /* deleting from VECTOR_COVERAGES */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".vector_coverages WHERE lower(f_table_name) = lower(%Q)",
	       q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_views_geometry_columns_auth)
      {
	  /* deleting from VIEWS_GEOMETRY_COLUMNS_AUTH */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf ("DELETE FROM \"%s\".views_geometry_columns_auth "
			       "WHERE lower(view_name) = lower(%Q)", q_prefix,
			       table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_views_geometry_columns_field_infos)
      {
	  /* deleting from VIEWS_GEOMETRY_COLUMNS_FIELD_INFOS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".views_geometry_columns_field_infos "
	       "WHERE view_name = %Q", q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_views_geometry_columns_statistics)
      {
	  /* deleting from VIEWS_GEOMETRY_COLUMNS_STATISTICS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".views_geometry_columns_statistics "
	       "WHERE lower(view_name) = lower(%Q)", q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }

/* dropping the Table itself */
    q_prefix = gaiaDoubleQuotedSql (prefix);
    q_table = gaiaDoubleQuotedSql (table);
    if (aux->is_view)
	sql = sqlite3_mprintf ("DROP VIEW \"%s\".\"%s\"", q_prefix, table);
    else
	sql = sqlite3_mprintf ("DROP TABLE \"%s\".\"%s\"", q_prefix, table);
    free (q_prefix);
    free (q_table);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  if (error_message != NULL)
	      *error_message = errMsg;
	  return 0;
      }

    if (aux->ok_virts_geometry_columns)
      {
	  /* deleting from VIRTS_GEOMETRY_COLUMNS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".virts_geometry_columns WHERE lower(virt_name) = lower(%Q)",
	       q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_virts_geometry_columns_auth)
      {
	  /* deleting from VIRTS_GEOMETRY_COLUMNS_AUTH */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf ("DELETE FROM \"%s\".virts_geometry_columns_auth "
			       "WHERE lower(virt_name) = lower(%Q)", q_prefix,
			       table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_virts_geometry_columns_field_infos)
      {
	  /* deleting from VIRTS_GEOMETRY_COLUMNS_FIELD_INFOS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".virts_geometry_columns_field_infos "
	       "WHERE lower(virt_name) = lower(%Q)", q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_virts_geometry_columns_statistics)
      {
	  /* deleting from VIRTS_GEOMETRY_COLUMNS_STATISTICS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("DELETE FROM \"%s\".virts_geometry_columns_statistics "
	       "WHERE lower(virt_name) = lower(%Q)", q_prefix, table);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		aux->error_message = errMsg;
		return 0;
	    }
      }
    return 1;
}

static int
do_rename_table_pre (sqlite3 * sqlite, const char *prefix, const char *old_name,
		     const char *new_name, struct table_params *aux,
		     char **error_message)
{
/* renaming a Table - preparatory steps */
    char *sql;
    char *q_prefix;
    int ret;
    char *errMsg = NULL;

/* dropping any previous Geometry Trigger */
    if (!do_drop_geotriggers (sqlite, old_name, error_message))
	return 0;

/* updating fist all metadata tables */
    if (aux->ok_geometry_columns)
      {
	  /* updating GEOMETRY_COLUMNS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".geometry_columns SET f_table_name = lower(%Q) "
	       "WHERE lower(f_table_name) = lower(%Q)", q_prefix, new_name,
	       old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }

    if (aux->ok_layer_params)
      {
	  /* updating LAYER_PARAMS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".layer_params SET table_name = lower(%Q) "
	       "WHERE lower(table_name) = lower(%Q)", q_prefix, new_name,
	       old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_layer_sub_classes)
      {
	  /* updating LAYER_SUB_CLASSES */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".layer_sub_classes SET table_name = lower(%Q) "
	       "WHERE lower(table_name) = lower(%Q)", q_prefix, new_name,
	       old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_layer_table_layout)
      {
	  /* updating LAYER_TABLE_LAYOUT */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".layer_table_layout SET table_name = lower(%Q) "
	       "WHERE lower(table_name) = lower(%Q)", q_prefix, new_name,
	       old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_geometry_columns_auth)
      {
	  /* updating GEOMETRY_COLUMNS_AUTH */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".geometry_columns_auth SET f_table_name = lower(%Q) "
	       "WHERE lower(f_table_name) = lower(%Q)", q_prefix, new_name,
	       old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_geometry_columns_time)
      {
	  /* updating GEOMETRY_COLUMNS_TIME */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".geometry_columns_time SET f_table_name = lower(%Q) "
	       "WHERE lower(f_table_name) = lower(%Q)", q_prefix, new_name,
	       old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_geometry_columns_field_infos)
      {
	  /* updating GEOMETRY_COLUMNS_FIELD_INFOS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".geometry_columns_field_infos SET f_table_name = lower(%Q) "
	       "WHERE lower(f_table_name) = lower(%Q)", q_prefix, new_name,
	       old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_geometry_columns_statistics)
      {
	  /* updating GEOMETRY_COLUMNS_STATISTICS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".geometry_columns_statistics SET f_table_name = lower(%Q) "
	       "WHERE lower(f_table_name) = lower(%Q)", q_prefix, new_name,
	       old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_layer_statistics)
      {
	  /* updating LAYER_STATISTICS */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".layer_statistics SET table_name = lower(%Q) "
	       "WHERE lower(table_name) = lower(%Q)", q_prefix, new_name,
	       old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    if (aux->ok_vector_coverages)
      {
	  /* updating VECTOR_COVERAGES */
	  q_prefix = gaiaDoubleQuotedSql (prefix);
	  sql =
	      sqlite3_mprintf
	      ("UPDATE \"%s\".vector_coverages SET f_table_name = %Q "
	       "WHERE lower(f_table_name) = lower(%Q)", q_prefix, new_name,
	       old_name);
	  free (q_prefix);
	  ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
	  sqlite3_free (sql);
	  if (ret != SQLITE_OK)
	    {
		if (error_message != NULL)
		    *error_message = errMsg;
		return 0;
	    }
      }
    return 1;
}

static int
do_rename_table_post (sqlite3 * sqlite, const char *prefix,
		      const char *old_name, const char *new_name,
		      char **error_message)
{
/* renaming a Table - final steps */
    char *sql;
    char *q_prefix;
    char *q_old;
    char *q_new;
    int ret;
    char *errMsg = NULL;

/* renaming the Table itself */
    q_prefix = gaiaDoubleQuotedSql (prefix);
    q_old = gaiaDoubleQuotedSql (old_name);
    q_new = gaiaDoubleQuotedSql (new_name);
    sql =
	sqlite3_mprintf ("ALTER TABLE \"%s\".\"%s\" RENAME TO \"%s\"", q_prefix,
			 q_old, q_new);
    free (q_prefix);
    free (q_old);
    free (q_new);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  if (error_message != NULL)
	      *error_message = errMsg;
	  return 0;
      }

/* re-installing all Geometry Triggers */
    if (!do_rebuild_geotriggers (sqlite, new_name))
      {
	  if (error_message != NULL)
	      *error_message =
		  sqlite3_mprintf ("unable to rebuild Geometry Triggers");
	  return 0;
      }
    return 1;
}

static int
do_drop_rtree (sqlite3 * sqlite, const char *prefix, const char *rtree,
	       char **error_message)
{
/* dropping some R*Tree */
    char *sql;
    char *q_prefix;
    char *q_rtree;
    int ret;
    char *errMsg = NULL;

    q_prefix = gaiaDoubleQuotedSql (prefix);
    q_rtree = gaiaDoubleQuotedSql (rtree);
    sql = sqlite3_mprintf ("DROP TABLE \"%s\".\"%s\"", q_prefix, q_rtree);
    free (q_prefix);
    free (q_rtree);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
      {
	  if (error_message != NULL)
	      *error_message = errMsg;
	  return 0;
      }
    return 1;
}

static int
do_drop_sub_view (sqlite3 * sqlite, const char *prefix, const char *table,
		  struct table_params *aux)
{
/* dropping any depending View */
    int ret;
    int i;
    char **results;
    int rows;
    int columns;
    char *sql;
    char *q_prefix;
    struct table_params aux2;

/* initializing the aux params */
    aux2.rtrees = NULL;
    aux2.n_rtrees = 0;
    aux2.is_view = 1;
    aux2.ok_geometry_columns = 0;
    aux2.ok_geometry_columns_time = aux->ok_geometry_columns_time;
    aux2.ok_views_geometry_columns = aux->ok_views_geometry_columns;
    aux2.ok_virts_geometry_columns = aux->ok_virts_geometry_columns;
    aux2.ok_geometry_columns_auth = aux->ok_geometry_columns_auth;
    aux2.ok_geometry_columns_field_infos = aux->ok_geometry_columns_field_infos;
    aux2.ok_geometry_columns_statistics = aux->ok_geometry_columns_statistics;
    aux2.ok_views_geometry_columns_auth = aux->ok_views_geometry_columns_auth;
    aux2.ok_views_geometry_columns_field_infos =
	aux->ok_views_geometry_columns_field_infos;
    aux2.ok_views_geometry_columns_statistics =
	aux->ok_views_geometry_columns_statistics;
    aux2.ok_virts_geometry_columns_auth = aux->ok_virts_geometry_columns_auth;
    aux2.ok_virts_geometry_columns_field_infos =
	aux->ok_virts_geometry_columns_field_infos;
    aux2.ok_virts_geometry_columns_statistics =
	aux->ok_virts_geometry_columns_statistics;
    aux2.ok_layer_statistics = aux->ok_layer_statistics;
    aux2.ok_views_layer_statistics = aux->ok_views_layer_statistics;
    aux2.ok_virts_layer_statistics = aux->ok_virts_layer_statistics;
    aux2.ok_layer_params = aux->ok_layer_params;
    aux2.ok_layer_sub_classes = aux->ok_layer_sub_classes;
    aux2.ok_layer_table_layout = aux->ok_layer_table_layout;
    aux2.ok_vector_coverages = aux->ok_vector_coverages;
    aux2.ok_vector_coverages_keyword = aux->ok_vector_coverages_keyword;
    aux2.ok_vector_coverages_srid = aux->ok_vector_coverages_srid;
    aux2.ok_se_vector_styled_layers = aux->ok_se_vector_styled_layers;

    if (aux->ok_views_geometry_columns == 0)
	return 1;

/* identifying any View depending on the target */
    q_prefix = gaiaDoubleQuotedSql (prefix);
    sql =
	sqlite3_mprintf ("SELECT view_name FROM \"%s\".views_geometry_columns "
			 "WHERE Lower(f_table_name) = Lower(%Q)", q_prefix,
			 table);
    free (q_prefix);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		const char *name = results[(i * columns) + 0];
		/* dropping the view itself */
		if (!do_drop_table (sqlite, prefix, name, &aux2))
		    return 0;
	    }
      }
    sqlite3_free_table (results);
    return 1;
}

static int
check_table_layout (sqlite3 * sqlite, const char *prefix, const char *table,
		    struct table_params *aux)
{
/* checking the actual DB configuration */
    int i;
    char **results;
    int rows;
    int columns;
    char jolly = '%';
    int ret;
    char *sql;
    char *q_prefix = gaiaDoubleQuotedSql (prefix);

    if (strcasecmp (prefix, "TEMP") == 0)
      {
	  /* TEMPORARY object; unconditioanally returning TRUE */
	  return 1;
      }
    sql =
	sqlite3_mprintf
	("SELECT type, name FROM \"%s\".sqlite_master WHERE type = 'table' or type = 'view'",
	 q_prefix);
    free (q_prefix);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	ret = 0;
    else
      {
	  ret = 1;
	  for (i = 1; i <= rows; i++)
	    {
		const char *type = results[(i * columns) + 0];
		const char *name = results[(i * columns) + 1];
		if (name)
		  {
		      /* checking which tables are actually defined */
		      if (strcasecmp (name, "geometry_columns") == 0)
			  aux->ok_geometry_columns = 1;
		      if (strcasecmp (name, "geometry_columns_time") == 0)
			  aux->ok_geometry_columns_time = 1;
		      if (strcasecmp (name, "views_geometry_columns") == 0)
			  aux->ok_views_geometry_columns = 1;
		      if (strcasecmp (name, "virts_geometry_columns") == 0)
			  aux->ok_virts_geometry_columns = 1;
		      if (strcasecmp (name, "geometry_columns_auth") == 0)
			  aux->ok_geometry_columns_auth = 1;
		      if (strcasecmp (name, "views_geometry_columns_auth") == 0)
			  aux->ok_views_geometry_columns_auth = 1;
		      if (strcasecmp (name, "virts_geometry_columns_auth") == 0)
			  aux->ok_virts_geometry_columns_auth = 1;
		      if (strcasecmp (name, "geometry_columns_statistics") == 0)
			  aux->ok_geometry_columns_statistics = 1;
		      if (strcasecmp (name, "views_geometry_columns_statistics")
			  == 0)
			  aux->ok_views_geometry_columns_statistics = 1;
		      if (strcasecmp (name, "virts_geometry_columns_statistics")
			  == 0)
			  aux->ok_virts_geometry_columns_statistics = 1;
		      if (strcasecmp (name, "geometry_columns_field_infos") ==
			  0)
			  aux->ok_geometry_columns_field_infos = 1;
		      if (strcasecmp
			  (name, "views_geometry_columns_field_infos") == 0)
			  aux->ok_views_geometry_columns_field_infos = 1;
		      if (strcasecmp
			  (name, "virts_geometry_columns_field_infos") == 0)
			  aux->ok_virts_geometry_columns_field_infos = 1;
		      if (strcasecmp (name, "layer_params") == 0)
			  aux->ok_layer_params = 1;
		      if (strcasecmp (name, "layer_statistics") == 0)
			  aux->ok_layer_statistics = 1;
		      if (strcasecmp (name, "layer_sub_classes") == 0)
			  aux->ok_layer_sub_classes = 1;
		      if (strcasecmp (name, "layer_table_layout") == 0)
			  aux->ok_layer_table_layout = 1;
		      if (strcasecmp (name, "views_geometry_columns") == 0)
			  aux->ok_views_geometry_columns = 1;
		      if (strcasecmp (name, "virts_geometry_columns") == 0)
			  aux->ok_virts_geometry_columns = 1;
		      if (strcasecmp (name, "vector_coverages") == 0)
			  aux->ok_vector_coverages = 1;
		      if (strcasecmp (name, "vector_coverages_keyword") == 0)
			  aux->ok_vector_coverages_keyword = 1;
		      if (strcasecmp (name, "vector_coverages_srid") == 0)
			  aux->ok_vector_coverages_srid = 1;
		      if (strcasecmp (name, "se_vector_styled_layers") == 0)
			  aux->ok_se_vector_styled_layers = 1;
		      if (strcasecmp (name, table) == 0)
			{
			    /* checking if the target is a view */
			    if (strcasecmp (type, "view") == 0)
				aux->is_view = 1;
			}
		  }
	    }
      }
    sqlite3_free_table (results);
    if (!ret)
	return 0;

/* identifying any possible R*Tree supporting the main target */
    q_prefix = gaiaDoubleQuotedSql (prefix);
    sql =
	sqlite3_mprintf
	("SELECT name FROM \"%s\".sqlite_master WHERE type = 'table' AND "
	 "Lower(name) IN (SELECT "
	 "Lower('idx_' || f_table_name || '_' || f_geometry_column) "
	 "FROM \"%s\".geometry_columns WHERE Lower(f_table_name) = Lower(%Q)) "
	 "AND sql LIKE('%cvirtual%c') AND sql LIKE('%crtree%c')",
	 q_prefix, q_prefix, table, jolly, jolly, jolly, jolly);
    free (q_prefix);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	return 0;
    if (rows < 1)
	;
    else
      {
/* allocating the rtrees array */
	  aux->n_rtrees = rows;
	  aux->rtrees = malloc (sizeof (char **) * aux->n_rtrees);
	  for (i = 1; i <= rows; i++)
	    {
		const char *name = results[(i * columns) + 0];
		int len = strlen (name);
		*(aux->rtrees + (i - 1)) = malloc (len + 1);
		strcpy (*(aux->rtrees + (i - 1)), name);
	    }
      }
    sqlite3_free_table (results);
    return 1;
}

static int
check_topology_table (sqlite3 * sqlite, const char *prefix, const char *table)
{
/* checking for GeoTables belonging to some TopoGeo or TopoNet */
    char *xprefix;
    char *sql;
    char *table_name;
    int ret;
    int i;
    char **results;
    int rows;
    int columns;
    int found = 0;

    if (prefix == NULL)
	prefix = "main";

/* testing within Topologies */
    xprefix = gaiaDoubleQuotedSql (prefix);
    sql =
	sqlite3_mprintf ("SELECT topology_name FROM \"%s\".topologies",
			 xprefix);
    free (xprefix);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	goto networks;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		const char *name = results[(i * columns) + 0];
		table_name = sqlite3_mprintf ("%s_node", name);
		if (strcasecmp (table, table_name) == 0)
		    found = 1;
		sqlite3_free (table_name);
		table_name = sqlite3_mprintf ("%s_edge", name);
		if (strcasecmp (table, table_name) == 0)
		    found = 1;
		sqlite3_free (table_name);
		table_name = sqlite3_mprintf ("%s_face", name);
		if (strcasecmp (table, table_name) == 0)
		    found = 1;
		sqlite3_free (table_name);
		table_name = sqlite3_mprintf ("%s_seeds", name);
		if (strcasecmp (table, table_name) == 0)
		    found = 1;
		sqlite3_free (table_name);
		table_name = sqlite3_mprintf ("%s_topofeatures", name);
		if (strcasecmp (table, table_name) == 0)
		    found = 1;
		sqlite3_free (table_name);
		table_name = sqlite3_mprintf ("%s_topolayers", name);
		if (strcasecmp (table, table_name) == 0)
		    found = 1;
		sqlite3_free (table_name);
	    }
      }
    sqlite3_free_table (results);
    if (found)
      {
	  spatialite_e ("DropTable: can't drop TopoGeo table \"%s\".\"%s\"",
			prefix, table);
	  return 1;
      }

  networks:
/* testing within Networks */
    xprefix = gaiaDoubleQuotedSql (prefix);
    sql = sqlite3_mprintf ("SELECT network_name FROM \"%s\".netowrks", xprefix);
    free (xprefix);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	goto end;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		const char *name = results[(i * columns) + 0];
		table_name = sqlite3_mprintf ("%s_node", name);
		if (strcasecmp (table, table_name) == 0)
		    found = 1;
		sqlite3_free (table_name);
		table_name = sqlite3_mprintf ("%s_link", name);
		if (strcasecmp (table, table_name) == 0)
		    found = 1;
		sqlite3_free (table_name);
		table_name = sqlite3_mprintf ("%s_seeds", name);
		if (strcasecmp (table, table_name) == 0)
		    found = 1;
		sqlite3_free (table_name);
	    }
      }
    sqlite3_free_table (results);
    if (found)
	return 1;

  end:
    return 0;
}

static int
check_raster_table (sqlite3 * sqlite, const char *prefix, const char *table)
{
/* checking for Raster Coverage tables */
    char *xprefix;
    char *sql;
    char *table_name;
    int ret;
    int i;
    char **results;
    int rows;
    int columns;
    int found = 0;

    if (prefix == NULL)
	prefix = "main";

/* testing within Raster Coverages */
    xprefix = gaiaDoubleQuotedSql (prefix);
    sql =
	sqlite3_mprintf ("SELECT coverage_name FROM \"%s\".raster_coverages",
			 xprefix);
    free (xprefix);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	goto end;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		const char *name = results[(i * columns) + 0];
		table_name = sqlite3_mprintf ("%s_node", name);
		if (strcasecmp (table, table_name) == 0)
		    found = 1;
		sqlite3_free (table_name);
		table_name = sqlite3_mprintf ("%s_levels", name);
		if (strcasecmp (table, table_name) == 0)
		    found = 1;
		sqlite3_free (table_name);
		table_name = sqlite3_mprintf ("%s_sections", name);
		if (strcasecmp (table, table_name) == 0)
		    found = 1;
		sqlite3_free (table_name);
		table_name = sqlite3_mprintf ("%s_tiles", name);
		if (strcasecmp (table, table_name) == 0)
		    found = 1;
		sqlite3_free (table_name);
		table_name = sqlite3_mprintf ("%s_tile_data", name);
		if (strcasecmp (table, table_name) == 0)
		    found = 1;
		sqlite3_free (table_name);
	    }
      }
    sqlite3_free_table (results);
    if (found)
	return 1;
  end:
    return 0;
}

static int
check_rtree_internal_table (sqlite3 * sqlite, const char *prefix,
			    const char *table)
{
/* checking for R*Tree internal tables */
    char *xprefix;
    char *sql;
    char *table_name;
    int ret;
    int i;
    char **results;
    int rows;
    int columns;
    int found = 0;

    if (prefix == NULL)
	prefix = "main";

/* testing within Raster Coverages */
    xprefix = gaiaDoubleQuotedSql (prefix);
    sql =
	sqlite3_mprintf
	("SELECT f_table_name, f_geometry_column FROM \"%s\".geometry_columns "
	 "WHERE spatial_index_enabled = 1", xprefix);
    free (xprefix);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	goto end;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		const char *tbl = results[(i * columns) + 0];
		const char *geom = results[(i * columns) + 1];
		table_name = sqlite3_mprintf ("idx_%s_%s_node", tbl, geom);
		if (strcasecmp (table, table_name) == 0)
		    found = 1;
		sqlite3_free (table_name);
		table_name = sqlite3_mprintf ("idx_%s_%s_parent", tbl, geom);
		if (strcasecmp (table, table_name) == 0)
		    found = 1;
		sqlite3_free (table_name);
		table_name = sqlite3_mprintf ("idx_%s_%s_rowid", tbl, geom);
		if (strcasecmp (table, table_name) == 0)
		    found = 1;
		sqlite3_free (table_name);
	    }
      }
    sqlite3_free_table (results);
    if (found)
	return 1;
  end:
    return 0;
}

static int
check_spatialite_table (const char *table)
{
/* checking for SpatiaLite / RasterLite2 internal tables */
    if (strcasecmp (table, "data_licences") == 0)
	return 1;
    if (strcasecmp (table, "geometry_columns") == 0)
	return 1;
    if (strcasecmp (table, "geometry_columns_time") == 0)
	return 1;
    if (strcasecmp (table, "networks") == 0)
	return 1;
    if (strcasecmp (table, "postgres_geometry_columns") == 0)
	return 1;
    if (strcasecmp (table, "raster_coverages") == 0)
	return 1;
    if (strcasecmp (table, "raster_coverages_keyword") == 0)
	return 1;
    if (strcasecmp (table, "raster_coverages_srid") == 0)
	return 1;
    if (strcasecmp (table, "spatial_ref_sys") == 0)
	return 1;
    if (strcasecmp (table, "spatial_ref_sys_aux") == 0)
	return 1;
    if (strcasecmp (table, "spatialite_history") == 0)
	return 1;
    if (strcasecmp (table, "stored_procedures") == 0)
	return 1;
    if (strcasecmp (table, "stored_variables") == 0)
	return 1;
    if (strcasecmp (table, "tmp_vector_coverages") == 0)
	return 1;
    if (strcasecmp (table, "topologies") == 0)
	return 1;
    if (strcasecmp (table, "vector_coverages") == 0)
	return 1;
    if (strcasecmp (table, "vector_coverages_keyword") == 0)
	return 1;
    if (strcasecmp (table, "vector_coverages_ref_sys") == 0)
	return 1;
    if (strcasecmp (table, "vector_coverages_srid") == 0)
	return 1;
    if (strcasecmp (table, "views_geometry_columns") == 0)
	return 1;
    if (strcasecmp (table, "virts_geometry_columns") == 0)
	return 1;
    if (strcasecmp (table, "geometry_columns_auth") == 0)
	return 1;
    if (strcasecmp (table, "geometry_columns_field_infos") == 0)
	return 1;
    if (strcasecmp (table, "geometry_columns_statistics") == 0)
	return 1;
    if (strcasecmp (table, "sql_statement_log") == 0)
	return 1;
    if (strcasecmp (table, "vector_layers_auth") == 0)
	return 1;
    if (strcasecmp (table, "vector_layers_field_infos") == 0)
	return 1;
    if (strcasecmp (table, "vector_layers_statistics") == 0)
	return 1;
    if (strcasecmp (table, "views_geometry_columns_auth") == 0)
	return 1;
    if (strcasecmp (table, "views_geometry_columns_field_infos") == 0)
	return 1;
    if (strcasecmp (table, "views_geometry_columns_statistics") == 0)
	return 1;
    if (strcasecmp (table, "virts_geometry_columns_auth") == 0)
	return 1;
    if (strcasecmp (table, "virts_geometry_columns_field_infos") == 0)
	return 1;
    if (strcasecmp (table, "virts_geometry_columns_statistics") == 0)
	return 1;
    if (strcasecmp (table, "SE_external_graphics") == 0)
	return 1;
    if (strcasecmp (table, "SE_fonts") == 0)
	return 1;
    if (strcasecmp (table, "SE_group_styles") == 0)
	return 1;
    if (strcasecmp (table, "SE_raster_styled_layers") == 0)
	return 1;
    if (strcasecmp (table, "SE_styled_group_refs") == 0)
	return 1;
    if (strcasecmp (table, "SE_vector_styled_layers") == 0)
	return 1;
    if (strcasecmp (table, "SE_vector_styles") == 0)
	return 1;
    if (strcasecmp (table, "iso_metadata") == 0)
	return 1;
    if (strcasecmp (table, "iso_metadata_reference") == 0)
	return 1;
    if (strcasecmp (table, "KNN") == 0)
	return 1;
    if (strcasecmp (table, "SpatialIndex") == 0)
	return 1;
    return 0;
}

static int
do_check_geotable (sqlite3 * sqlite, const char *prefix, const char *table)
{
/* checking for a registered GeoTable */
    char *xprefix;
    char *sql;
    int ret;
    int i;
    char **results;
    int rows;
    int columns;
    int is_geotable = 0;

    if (prefix == NULL)
	prefix = "main";

/* parsing the SQL CREATE statement */
    xprefix = gaiaDoubleQuotedSql (prefix);
    sql =
	sqlite3_mprintf
	("SELECT Count(*) FROM \"%s\".geometry_columns WHERE Upper(f_table_name) = Upper(%Q)",
	 xprefix, table);
    free (xprefix);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	goto end;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		const char *count = results[(i * columns) + 0];
		if (atoi (count) > 0)
		    is_geotable = 1;
	    }
      }
    sqlite3_free_table (results);
    if (is_geotable)
	return 1;
  end:
    return 0;
}

static int
check_virtual_table (sqlite3 * sqlite, const char *prefix, const char *table)
{
/* checking for Virtual Tables */
    char *xprefix;
    char *sql;
    int ret;
    int i;
    char **results;
    int rows;
    int columns;
    int is_virtual = 0;

    if (prefix == NULL)
	prefix = "main";

/* parsing the SQL CREATE statement */
    xprefix = gaiaDoubleQuotedSql (prefix);
    sql =
	sqlite3_mprintf ("SELECT sql FROM \"%s\".sqlite_master "
			 "WHERE type = 'table' AND Upper(name) = Upper(%Q)",
			 xprefix, table);
    free (xprefix);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
    sqlite3_free (sql);
    if (ret != SQLITE_OK)
	goto end;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		int step = 0;
		char *token1 = NULL;
		char *token2 = NULL;
		char *token3 = NULL;
		char *token4 = NULL;
		char *token5 = NULL;
		const char *sql = results[(i * columns) + 0];
		const char *ps = sql;
		const char *pe;

		while (*ps != '\0')
		  {
		      while (1)
			{
			    /* consuming leading blanks */
			    if (*ps == '\0')
				goto mismatch;
			    if (*ps == ' ' || *ps == '\t' || *ps == '\n'
				|| *ps == '\r')
			      {
				  ps++;
				  continue;
			      }
			    break;
			}
		      pe = ps;
		      while (1)
			{
			    /* searching the next name delimiter */
			    if (*pe == ' ' || *pe == '\t' || *pe == '\n'
				|| *pe == '\r' || *pe == '\0')
				break;
			    pe++;
			}
		      switch (step)
			{
			case 0:
			    token1 = malloc ((pe - ps) + 1);
			    memcpy (token1, ps, (pe - ps));
			    *(token1 + (pe - ps)) = '\0';
			    step++;
			    ps = pe + 1;
			    break;
			case 1:
			    token2 = malloc ((pe - ps) + 1);
			    memcpy (token2, ps, (pe - ps));
			    *(token2 + (pe - ps)) = '\0';
			    step++;
			    ps = pe + 1;
			    break;
			case 2:
			    token3 = malloc ((pe - ps) + 1);
			    memcpy (token3, ps, (pe - ps));
			    *(token3 + (pe - ps)) = '\0';
			    step++;
			    ps = pe + 1;
			    break;
			case 3:
			    token4 = malloc ((pe - ps) + 1);
			    memcpy (token4, ps, (pe - ps));
			    *(token4 + (pe - ps)) = '\0';
			    step++;
			    ps = pe + 1;
			    break;
			case 4:
			    token5 = malloc ((pe - ps) + 1);
			    memcpy (token5, ps, (pe - ps));
			    *(token5 + (pe - ps)) = '\0';
			    step++;
			    ps = pe + 1;
			    break;
			};
		      if (step >= 5)
			  break;
		  }
		if (token1 == NULL || token2 == NULL || token3 == NULL
		    || token4 == NULL || token5 == NULL)
		    ;
		else
		  {
		      char *xtable = gaiaDoubleQuotedSql (table);
		      char *qtable = sqlite3_mprintf ("\"%s\"", xtable);
		      free (xtable);
		      if (strcasecmp (token1, "CREATE") == 0 &&
			  strcasecmp (token2, "VIRTUAL") == 0 &&
			  strcasecmp (token3, "TABLE") == 0 &&
			  (strcasecmp (token4, table) == 0
			   || strcasecmp (token4, qtable) == 0)
			  && strcasecmp (token5, "USING") == 0)
			  is_virtual = 1;
		      sqlite3_free (qtable);
		      free (token1);
		      free (token2);
		      free (token3);
		      free (token4);
		      free (token5);
		  }
	    }
      }
    sqlite3_free_table (results);
    if (is_virtual)
	return 1;
    goto end;
  mismatch:
    sqlite3_free_table (results);
  end:
    return 0;
}

SPATIALITE_DECLARE int
gaiaDropTable (sqlite3 * sqlite, const char *table)
{
    return gaiaDropTableEx (sqlite, "main", table);
}

SPATIALITE_DECLARE int
gaiaDropTableEx (sqlite3 * sqlite, const char *prefix, const char *table)
{
    return gaiaDropTableEx2 (sqlite, prefix, table, 1);
}

SPATIALITE_DECLARE int
gaiaDropTableEx2 (sqlite3 * sqlite, const char *prefix, const char *table,
		  int transaction)
{
    return gaiaDropTableEx3 (sqlite, prefix, table, transaction, NULL);
}

SPATIALITE_DECLARE int
gaiaDropTableEx3 (sqlite3 * sqlite, const char *prefix, const char *table,
		  int transaction, char **error_message)
{
/*
/ DEPRECATED !!!!
/ use gaiaDropTable5() as a full replacement
/
/ dropping a Spatial Table and any other related stuff 
*/
    int ret;
    struct table_params aux;

/* initializing the aux params */
    aux.rtrees = NULL;
    aux.n_rtrees = 0;
    aux.is_view = 0;
    aux.ok_geometry_columns = 0;
    aux.ok_geometry_columns_time = 0;
    aux.ok_views_geometry_columns = 0;
    aux.ok_virts_geometry_columns = 0;
    aux.ok_geometry_columns_auth = 0;
    aux.ok_geometry_columns_field_infos = 0;
    aux.ok_geometry_columns_statistics = 0;
    aux.ok_views_geometry_columns_auth = 0;
    aux.ok_views_geometry_columns_field_infos = 0;
    aux.ok_views_geometry_columns_statistics = 0;
    aux.ok_virts_geometry_columns_auth = 0;
    aux.ok_virts_geometry_columns_field_infos = 0;
    aux.ok_virts_geometry_columns_statistics = 0;
    aux.ok_layer_statistics = 0;
    aux.ok_views_layer_statistics = 0;
    aux.ok_virts_layer_statistics = 0;
    aux.ok_layer_params = 0;
    aux.ok_layer_sub_classes = 0;
    aux.ok_layer_table_layout = 0;
    aux.ok_vector_coverages = 0;
    aux.ok_vector_coverages_keyword = 0;
    aux.ok_vector_coverages_srid = 0;
    aux.ok_se_vector_styled_layers = 0;
    aux.error_message = NULL;

    if (error_message != NULL)
	*error_message = NULL;
    if (prefix == NULL)
	return 0;
    if (table == NULL)
	return 0;

    if (transaction)
      {
	  /* the whole operation is a single transaction */
	  ret = sqlite3_exec (sqlite, "BEGIN", NULL, NULL, NULL);
	  if (ret != SQLITE_OK)
	      return 0;
      }

/* checking the actual DB configuration */
    if (!check_table_layout (sqlite, prefix, table, &aux))
	goto rollback;
/* avoiding to drop TopoGeo or TopoNet tables */
    if (check_topology_table (sqlite, prefix, table))
	goto rollback;
/* avoiding to drop Raster Coverage tables */
    if (check_raster_table (sqlite, prefix, table))
	goto rollback;
/* avoiding to drop R*Tree internal tables */
    if (check_rtree_internal_table (sqlite, prefix, table))
	goto rollback;
/* avoiding to drop SpatiaLite own tables */
    if (check_spatialite_table (table))
	goto rollback;
/* recursively dropping any depending View */
    if (!do_drop_sub_view (sqlite, prefix, table, &aux))
	goto rollback;
    if (!do_drop_table (sqlite, prefix, table, &aux))
	goto rollback;

    if (transaction)
      {
	  /* committing the still pending transaction */
	  ret = sqlite3_exec (sqlite, "COMMIT", NULL, NULL, NULL);
	  if (ret != SQLITE_OK)
	      goto rollback;
      }

    if (aux.rtrees)
      {
	  /* memory cleanup - rtrees */
	  int i;
	  for (i = 0; i < aux.n_rtrees; i++)
	    {
		if (*(aux.rtrees + i) != NULL)
		    free (*(aux.rtrees + i));
	    }
	  free (aux.rtrees);
      }
    return 1;

  rollback:
    if (transaction)
      {
	  /* invalidating the still pending transaction */
	  sqlite3_exec (sqlite, "ROLLBACK", NULL, NULL, NULL);
      }

    if (aux.rtrees)
      {
	  /* memory cleanup - rtrees */
	  int i;
	  for (i = 0; i < aux.n_rtrees; i++)
	    {
		if (*(aux.rtrees + i) != NULL)
		    free (*(aux.rtrees + i));
	    }
	  free (aux.rtrees);
      }
    if (aux.error_message != NULL)
      {
	  if (error_message != NULL)
	      *error_message = aux.error_message;
	  else
	    {
		spatialite_e ("DropGeoTable error: %s\r", aux.error_message);
		sqlite3_free (aux.error_message);
	    }
      }
    return 0;
}

SPATIALITE_DECLARE int
gaiaDropTable5 (sqlite3 * sqlite,
		const char *prefix, const char *table, char **error_message)
{
/* dropping a Spatial Table and any other related stuff */
    int ret;
    struct table_params aux;

/* initializing the aux params */
    aux.rtrees = NULL;
    aux.n_rtrees = 0;
    aux.is_view = 0;
    aux.ok_geometry_columns = 0;
    aux.ok_geometry_columns_time = 0;
    aux.ok_views_geometry_columns = 0;
    aux.ok_virts_geometry_columns = 0;
    aux.ok_geometry_columns_auth = 0;
    aux.ok_geometry_columns_field_infos = 0;
    aux.ok_geometry_columns_statistics = 0;
    aux.ok_views_geometry_columns_auth = 0;
    aux.ok_views_geometry_columns_field_infos = 0;
    aux.ok_views_geometry_columns_statistics = 0;
    aux.ok_virts_geometry_columns_auth = 0;
    aux.ok_virts_geometry_columns_field_infos = 0;
    aux.ok_virts_geometry_columns_statistics = 0;
    aux.ok_layer_statistics = 0;
    aux.ok_views_layer_statistics = 0;
    aux.ok_virts_layer_statistics = 0;
    aux.ok_layer_params = 0;
    aux.ok_layer_sub_classes = 0;
    aux.ok_layer_table_layout = 0;
    aux.ok_vector_coverages = 0;
    aux.ok_vector_coverages_keyword = 0;
    aux.ok_vector_coverages_srid = 0;
    aux.ok_se_vector_styled_layers = 0;
    aux.error_message = NULL;

    if (error_message != NULL)
	*error_message = NULL;

    if (prefix == NULL)
	prefix = "main";
    if (table == NULL)
	goto invalid_arg;

/* checking for an existing table */
    if (!do_check_existing (sqlite, prefix, table, 0))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf ("not existing table [%s.%s]", prefix, table);
	  return 0;
      }
/* avoiding to drop TopoGeo or TopoNet tables */
    if (check_topology_table (sqlite, prefix, table))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf ("forbidden: Topology internal Table [%s.%s]",
				   prefix, table);
	  return 0;
      }
/* avoiding to drop Raster Coverage tables */
    if (check_raster_table (sqlite, prefix, table))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf
		  ("forbidden: Raster Coverage internal Table [%s.%s]", prefix,
		   table);
	  return 0;
      }
/* avoiding to drop R*Tree internal tables */
    if (check_rtree_internal_table (sqlite, prefix, table))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf
		  ("forbidden: R*Tree (Spatial Index) internal Table [%s.%s]",
		   prefix, table);
	  return 0;
      }
/* avoiding to drop SpatiaLite own tables */
    if (check_spatialite_table (table))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf
		  ("forbidden: SpatiaLite internal Table [%s.%s]", prefix,
		   table);
	  return 0;
      }

/* checking the actual DB configuration */
    if (!check_table_layout (sqlite, prefix, table, &aux))
      {
	  if (error_message)
	      *error_message = sqlite3_mprintf ("unable to get the DB layout");
	  return 0;
      }

/* the whole operation is a single transaction */
    ret = sqlite3_exec (sqlite, "SAVEPOINT drop_table", NULL, NULL, NULL);
    if (ret != SQLITE_OK)
      {
	  if (error_message)
	      *error_message = sqlite3_mprintf ("unable to set a SAVEPOINT");
	  return 0;
      }
    if (!do_drop_table5 (sqlite, prefix, table, &aux, error_message))
	goto rollback;

    if (aux.rtrees)
      {
	  /* dropping old R*Trees */
	  int i;
	  for (i = 0; i < aux.n_rtrees; i++)
	    {
		if (*(aux.rtrees + i) != NULL)
		  {
		      if (!do_drop_rtree
			  (sqlite, prefix, *(aux.rtrees + i), error_message))
			  goto rollback;
		  }
	    }
      }
    if (aux.rtrees)
      {
	  /* memory cleanup - rtrees */
	  int i;
	  for (i = 0; i < aux.n_rtrees; i++)
	    {
		if (*(aux.rtrees + i) != NULL)
		    free (*(aux.rtrees + i));
	    }
	  free (aux.rtrees);
      }

/* confirming the transaction */
    ret =
	sqlite3_exec (sqlite, "RELEASE SAVEPOINT drop_table", NULL, NULL, NULL);
    if (ret != SQLITE_OK)
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf ("unable to RELEASE the SAVEPOINT");
	  return 0;
      }
    return 1;

  invalid_arg:
    if (error_message)
	*error_message = sqlite3_mprintf ("invalid argument.");
    return 0;

  rollback:
    if (aux.rtrees)
      {
	  /* memory cleanup - rtrees */
	  int i;
	  for (i = 0; i < aux.n_rtrees; i++)
	    {
		if (*(aux.rtrees + i) != NULL)
		    free (*(aux.rtrees + i));
	    }
	  free (aux.rtrees);
      }
    sqlite3_exec (sqlite, "ROLLBACK TO SAVEPOINT drop_table", NULL, NULL, NULL);
    sqlite3_exec (sqlite, "RELEASE SAVEPOINT drop_table", NULL, NULL, NULL);
    return 0;
}

SPATIALITE_DECLARE int
gaiaRenameTable (sqlite3 * sqlite,
		 const char *prefix,
		 const char *old_name,
		 const char *new_name, char **error_message)
{
/* renaming a Spatial Table and any other related stuff */
    int ret;
    struct table_params aux;
    int fk_on = 1;
    char **results;
    int rows;
    int columns;
    int i;

/* initializing the aux params */
    aux.rtrees = NULL;
    aux.n_rtrees = 0;
    aux.is_view = 0;
    aux.ok_geometry_columns = 0;
    aux.ok_geometry_columns_time = 0;
    aux.ok_views_geometry_columns = 0;
    aux.ok_virts_geometry_columns = 0;
    aux.ok_geometry_columns_auth = 0;
    aux.ok_geometry_columns_field_infos = 0;
    aux.ok_geometry_columns_statistics = 0;
    aux.ok_views_geometry_columns_auth = 0;
    aux.ok_views_geometry_columns_field_infos = 0;
    aux.ok_views_geometry_columns_statistics = 0;
    aux.ok_virts_geometry_columns_auth = 0;
    aux.ok_virts_geometry_columns_field_infos = 0;
    aux.ok_virts_geometry_columns_statistics = 0;
    aux.ok_layer_statistics = 0;
    aux.ok_views_layer_statistics = 0;
    aux.ok_virts_layer_statistics = 0;
    aux.ok_layer_params = 0;
    aux.ok_layer_sub_classes = 0;
    aux.ok_layer_table_layout = 0;
    aux.ok_vector_coverages = 0;
    aux.ok_vector_coverages_keyword = 0;
    aux.ok_vector_coverages_srid = 0;
    aux.ok_se_vector_styled_layers = 0;
    aux.error_message = NULL;

    if (error_message != NULL)
	*error_message = NULL;

/* checking the version of SQLite */
    if (sqlite3_libversion_number () < 3025000)
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf
		  ("libsqlite 3.25 or later is strictly required");
	  return 0;
      }
    if (prefix == NULL)
	prefix = "main";
    if (old_name == NULL)
	goto invalid_arg;
    if (new_name == NULL)
	goto invalid_arg;

/* checking for Views */
    if (do_check_view (sqlite, prefix, old_name))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf
		  ("forbidden: can't rename a View, only Tables are supported [%s.%s]",
		   prefix, old_name);
	  return 0;
      }
/* checking for an existing table */
    if (!do_check_existing (sqlite, prefix, old_name, 1))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf ("not existing table [%s.%s]", prefix,
				   old_name);
	  return 0;
      }
    if (strcasecmp (prefix, "main") != 0)
      {
	  /* checking for a GeoTable not in the MAIN DB */
	  if (do_check_geotable (sqlite, prefix, old_name))
	    {
		if (error_message)
		    *error_message =
			sqlite3_mprintf
			("forbidden: Spatial Table not in the MAIN DB [%s.%s]",
			 prefix, old_name);
		return 0;
	    }
      }
/* checking if a table with the new-name already exists */
    if (do_check_existing (sqlite, prefix, new_name, 0))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf ("already existing table [%s.%s]", prefix,
				   new_name);
	  return 0;
      }
/* avoiding to rename TopoGeo or TopoNet tables */
    if (check_topology_table (sqlite, prefix, old_name))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf ("forbidden: Topology internal Table [%s.%s]",
				   prefix, old_name);
	  return 0;
      }
/* avoiding to rename Raster Coverage tables */
    if (check_raster_table (sqlite, prefix, old_name))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf
		  ("forbidden: Raster Coverage internal Table [%s.%s]", prefix,
		   old_name);
	  return 0;
      }
/* avoiding to rename R*Tree internal tables */
    if (check_rtree_internal_table (sqlite, prefix, old_name))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf
		  ("forbidden: R*Tree (Spatial Index) internal Table [%s.%s]",
		   prefix, old_name);
	  return 0;
      }
/* avoiding to rename Virtual Tables */
    if (check_virtual_table (sqlite, prefix, old_name))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf
		  ("forbidden: can't rename a Virtual Table [%s.%s]", prefix,
		   old_name);
	  return 0;
      }
/* avoiding to rename SpatiaLite own tables */
    if (check_spatialite_table (old_name))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf
		  ("forbidden: SpatiaLite internal Table [%s.%s]", prefix,
		   old_name);
	  return 0;
      }

/* checking the actual DB configuration */
    if (!check_table_layout (sqlite, prefix, old_name, &aux))
      {
	  if (error_message)
	      *error_message = sqlite3_mprintf ("unable to get the DB layout");
	  return 0;
      }

/* saving the current FKs mode */
    ret =
	sqlite3_get_table (sqlite, "PRAGMA foreign_keys", &results, &rows,
			   &columns, NULL);
    if (ret == SQLITE_OK)
      {
	  if (rows < 1)
	      ;
	  else
	    {
		for (i = 1; i <= rows; i++)
		    fk_on = atoi (results[(i * columns) + 0]);
	    }
	  sqlite3_free_table (results);
      }
    if (fk_on)
      {
	  /* disabling FKs constraints */
	  ret =
	      sqlite3_exec (sqlite, "PRAGMA foreign_keys = 0", NULL, NULL,
			    NULL);
	  if (ret != SQLITE_OK)
	    {
		if (error_message)
		    *error_message =
			sqlite3_mprintf ("unable to disable FKs constraints");
		return 0;
	    }
      }

/* starting a transaction */
    ret = sqlite3_exec (sqlite, "SAVEPOINT rename_table_pre", NULL, NULL, NULL);
    if (ret != SQLITE_OK)
      {
	  if (error_message)
	      *error_message = sqlite3_mprintf ("unable to set a SAVEPOINT");
	  return 0;
      }
    if (!do_rename_table_pre
	(sqlite, prefix, old_name, new_name, &aux, error_message))
	goto rollback;

    if (aux.rtrees)
      {
	  /* dropping old R*Trees */
	  int i;
	  for (i = 0; i < aux.n_rtrees; i++)
	    {
		if (*(aux.rtrees + i) != NULL)
		  {
		      if (!do_drop_rtree
			  (sqlite, prefix, *(aux.rtrees + i), error_message))
			  goto rollback;
		  }
	    }
      }
    if (aux.rtrees)
      {
	  /* memory cleanup - rtrees */
	  int i;
	  for (i = 0; i < aux.n_rtrees; i++)
	    {
		if (*(aux.rtrees + i) != NULL)
		    free (*(aux.rtrees + i));
	    }
	  free (aux.rtrees);
      }

/* confirming the transaction */
    ret =
	sqlite3_exec (sqlite, "RELEASE SAVEPOINT rename_table_pre", NULL, NULL,
		      NULL);
    if (ret != SQLITE_OK)
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf ("unable to RELEASE the SAVEPOINT");
	  return 0;
      }

    if (fk_on)
      {
	  /* re-enabling FKs constraints */
	  ret =
	      sqlite3_exec (sqlite, "PRAGMA foreign_keys = 1", NULL, NULL,
			    NULL);
	  if (ret != SQLITE_OK)
	    {
		if (error_message)
		    *error_message =
			sqlite3_mprintf ("unable to re-enable FKs constraints");
		return 0;
	    }
      }

/* and finally renaming the table itself */
    ret =
	sqlite3_exec (sqlite, "SAVEPOINT rename_table_post", NULL, NULL, NULL);
    if (ret != SQLITE_OK)
      {
	  if (error_message)
	      *error_message = sqlite3_mprintf ("unable to set a SAVEPOINT");
	  return 0;
      }
    if (!do_rename_table_post
	(sqlite, prefix, old_name, new_name, error_message))
	goto rollback_post;
    ret =
	sqlite3_exec (sqlite, "RELEASE SAVEPOINT rename_table_post", NULL, NULL,
		      NULL);
    if (ret != SQLITE_OK)
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf ("unable to RELEASE the SAVEPOINT");
	  return 0;
      }
    return 1;

  invalid_arg:
    if (error_message)
	*error_message = sqlite3_mprintf ("invalid argument.");
    return 0;

  rollback:
    if (aux.rtrees)
      {
	  /* memory cleanup - rtrees */
	  int i;
	  for (i = 0; i < aux.n_rtrees; i++)
	    {
		if (*(aux.rtrees + i) != NULL)
		    free (*(aux.rtrees + i));
	    }
	  free (aux.rtrees);
      }
    sqlite3_exec (sqlite, "ROLLBACK TO SAVEPOINT rename_table_pre", NULL,
		  NULL, NULL);
    sqlite3_exec (sqlite, "RELEASE SAVEPOINT rename_table_pre", NULL, NULL,
		  NULL);
    return 0;

  rollback_post:
    sqlite3_exec (sqlite, "ROLLBACK TO SAVEPOINT rename_table_post", NULL,
		  NULL, NULL);
    sqlite3_exec (sqlite, "RELEASE SAVEPOINT rename_table_post", NULL, NULL,
		  NULL);
    return 0;
}

SPATIALITE_DECLARE int
gaiaRenameColumn (sqlite3 * sqlite,
		  const char *prefix,
		  const char *table,
		  const char *old_name,
		  const char *new_name, char **error_message)
{
/* renaming a Spatial Table's Column and any other related stuff */
    int ret;
    struct table_params aux;
    int fk_on = 1;
    char **results;
    int rows;
    int columns;
    int i;

/* initializing the aux params */
    aux.rtrees = NULL;
    aux.n_rtrees = 0;
    aux.is_view = 0;
    aux.ok_geometry_columns = 0;
    aux.ok_geometry_columns_time = 0;
    aux.ok_views_geometry_columns = 0;
    aux.ok_virts_geometry_columns = 0;
    aux.ok_geometry_columns_auth = 0;
    aux.ok_geometry_columns_field_infos = 0;
    aux.ok_geometry_columns_statistics = 0;
    aux.ok_views_geometry_columns_auth = 0;
    aux.ok_views_geometry_columns_field_infos = 0;
    aux.ok_views_geometry_columns_statistics = 0;
    aux.ok_virts_geometry_columns_auth = 0;
    aux.ok_virts_geometry_columns_field_infos = 0;
    aux.ok_virts_geometry_columns_statistics = 0;
    aux.ok_layer_statistics = 0;
    aux.ok_views_layer_statistics = 0;
    aux.ok_virts_layer_statistics = 0;
    aux.ok_layer_params = 0;
    aux.ok_layer_sub_classes = 0;
    aux.ok_layer_table_layout = 0;
    aux.ok_vector_coverages = 0;
    aux.ok_vector_coverages_keyword = 0;
    aux.ok_vector_coverages_srid = 0;
    aux.ok_se_vector_styled_layers = 0;
    aux.error_message = NULL;

    if (error_message != NULL)
	*error_message = NULL;

/* checking the version of SQLite */
    if (sqlite3_libversion_number () < 3025000)
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf
		  ("libsqlite 3.25 or later is strictly required");
	  return 0;
      }
    if (prefix == NULL)
	prefix = "main";
    if (old_name == NULL)
	goto invalid_arg;
    if (new_name == NULL)
	goto invalid_arg;

/* checking for Views */
    if (do_check_view (sqlite, prefix, table))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf
		  ("forbidden: can't rename a View Column, only Table Columns are supported [%s.%s]",
		   prefix, table);
	  return 0;
      }
/* checking for an existing table */
    if (!do_check_existing (sqlite, prefix, table, 1))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf ("not existing table [%s.%s]", prefix, table);
	  return 0;
      }
/* checking for an existing column */
    if (!do_check_existing_column (sqlite, prefix, table, old_name))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf ("not existing column [%s.%s] %s", prefix,
				   table, old_name);
	  return 0;
      }
    if (strcasecmp (prefix, "main") != 0)
      {
	  /* checking for a GeoTable not in the MAIN DB */
	  if (do_check_geotable (sqlite, prefix, table))
	    {
		if (error_message)
		    *error_message =
			sqlite3_mprintf
			("forbidden: Spatial Table not in the MAIN DB [%s.%s]",
			 prefix, table);
		return 0;
	    }
      }
/* checking if a column with the new-name is already defined */
    if (do_check_existing_column (sqlite, prefix, table, new_name))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf ("column already defined [%s.%s] %s", prefix,
				   table, new_name);
	  return 0;
      }
/* avoiding to rename columns in TopoGeo or TopoNet tables */
    if (check_topology_table (sqlite, prefix, table))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf ("forbidden: Topology internal Table [%s.%s]",
				   prefix, old_name);
	  return 0;
      }
/* avoiding to rename columns in Raster Coverage tables */
    if (check_raster_table (sqlite, prefix, table))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf
		  ("forbidden: Raster Coverage internal Table [%s.%s]", prefix,
		   old_name);
	  return 0;
      }
/* avoiding to rename columns in R*Tree internal tables */
    if (check_rtree_internal_table (sqlite, prefix, table))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf
		  ("forbidden: R*Tree (Spatial Index) internal Table - %s.%s",
		   prefix, old_name);
	  return 0;
      }
/* avoiding to rename Virtual Table Columns */
    if (check_virtual_table (sqlite, prefix, table))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf
		  ("forbidden: can't rename a Virtual Table Column [%s.%s]",
		   prefix, old_name);
	  return 0;
      }

/* checking the actual DB configuration */
    if (!check_table_layout (sqlite, prefix, table, &aux))
      {
	  if (error_message)
	      *error_message = sqlite3_mprintf ("unable to get the DB layout");
	  return 0;
      }
/* avoiding to rename columns in SpatiaLite own tables */
    if (check_spatialite_table (table))
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf
		  ("forbidden: SpatiaLite internal Table [%s.%s]", prefix,
		   old_name);
	  return 0;
      }

/* saving the current FKs mode */
    ret =
	sqlite3_get_table (sqlite, "PRAGMA foreign_keys", &results, &rows,
			   &columns, NULL);
    if (ret == SQLITE_OK)
      {
	  if (rows < 1)
	      ;
	  else
	    {
		for (i = 1; i <= rows; i++)
		    fk_on = atoi (results[(i * columns) + 0]);
	    }
	  sqlite3_free_table (results);
      }
    if (fk_on)
      {
	  /* disabling FKs constraints */
	  ret =
	      sqlite3_exec (sqlite, "PRAGMA foreign_keys = 0", NULL, NULL,
			    NULL);
	  if (ret != SQLITE_OK)
	    {
		if (error_message)
		    *error_message =
			sqlite3_mprintf ("unable to disable FKs constraints");
		return 0;
	    }
      }

/* the whole operation is a single transaction */
    ret =
	sqlite3_exec (sqlite, "SAVEPOINT rename_column_pre", NULL, NULL, NULL);
    if (ret != SQLITE_OK)
      {
	  if (error_message)
	      *error_message = sqlite3_mprintf ("unable to set a SAVEPOINT");
	  return 0;
      }
    if (!do_rename_column_pre
	(sqlite, prefix, table, old_name, new_name, &aux, error_message))
	goto rollback;

    if (aux.rtrees)
      {
	  /* dropping old R*Trees */
	  int i;
	  for (i = 0; i < aux.n_rtrees; i++)
	    {
		if (*(aux.rtrees + i) != NULL)
		  {
		      if (!do_drop_rtree
			  (sqlite, prefix, *(aux.rtrees + i), error_message))
			  goto rollback;
		  }
	    }
      }
    if (aux.rtrees)
      {
	  /* memory cleanup - rtrees */
	  int i;
	  for (i = 0; i < aux.n_rtrees; i++)
	    {
		if (*(aux.rtrees + i) != NULL)
		    free (*(aux.rtrees + i));
	    }
	  free (aux.rtrees);
      }

/* confirming the transaction */
    ret =
	sqlite3_exec (sqlite, "RELEASE SAVEPOINT rename_column_pre", NULL, NULL,
		      NULL);
    if (ret != SQLITE_OK)
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf ("unable to RELEASE the SAVEPOINT");
	  return 0;
      }

    if (fk_on)
      {
	  /* re-enabling FKs constraints */
	  ret =
	      sqlite3_exec (sqlite, "PRAGMA foreign_keys = 1", NULL, NULL,
			    NULL);
	  if (ret != SQLITE_OK)
	    {
		if (error_message)
		    *error_message =
			sqlite3_mprintf ("unable to re-enable FKs constraints");
		return 0;
	    }
      }

/* renaming the column itself */
    ret =
	sqlite3_exec (sqlite, "SAVEPOINT rename_column_post", NULL, NULL, NULL);
    if (ret != SQLITE_OK)
      {
	  if (error_message)
	      *error_message = sqlite3_mprintf ("unable to set a SAVEPOINT");
	  return 0;
      }
    if (!do_rename_column_post
	(sqlite, prefix, table, old_name, new_name, error_message))
	goto rollback_post;
    ret =
	sqlite3_exec (sqlite, "RELEASE SAVEPOINT rename_column_post", NULL,
		      NULL, NULL);
    if (ret != SQLITE_OK)
      {
	  if (error_message)
	      *error_message =
		  sqlite3_mprintf ("unable to RELEASE the SAVEPOINT");
	  return 0;
      }
    return 1;

  invalid_arg:
    if (error_message)
	*error_message = sqlite3_mprintf ("invalid argument.");
    return 0;

  rollback:
    if (aux.rtrees)
      {
	  /* memory cleanup - rtrees */
	  int i;
	  for (i = 0; i < aux.n_rtrees; i++)
	    {
		if (*(aux.rtrees + i) != NULL)
		    free (*(aux.rtrees + i));
	    }
	  free (aux.rtrees);
      }
    sqlite3_exec (sqlite, "ROLLBACK TO SAVEPOINT rename_column_pre", NULL,
		  NULL, NULL);
    sqlite3_exec (sqlite, "RELEASE SAVEPOINT rename_column_pre", NULL, NULL,
		  NULL);
    return 0;

  rollback_post:
    sqlite3_exec (sqlite, "ROLLBACK TO SAVEPOINT rename_column_post", NULL,
		  NULL, NULL);
    sqlite3_exec (sqlite, "RELEASE SAVEPOINT rename_column_post", NULL, NULL,
		  NULL);
    return 0;
}
