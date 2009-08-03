/*

 spatialite.c -- SQLite3 spatial extension

 version 2.3, 2008 October 13

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
 
Portions created by the Initial Developer are Copyright (C) 2008
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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <float.h>
#include <locale.h>
#include <errno.h>

#include <spatialite/sqlite3ext.h>
#include <spatialite/gaiageo.h>
#include <spatialite/gaiaexif.h>
#include <spatialite/spatialite.h>
#include <spatialite.h>

#define GAIA_UNUSED() if (argc || argv) argc = argc;

static SQLITE_EXTENSION_INIT1 struct spatial_index_str
{
/* a struct to implement a linked list of spatial-indexes */
    char ValidRtree;
    char ValidCache;
    char *TableName;
    char *ColumnName;
    struct spatial_index_str *Next;
};

struct stddev_str
{
/* a struct to implement StandardVariation and Variance aggregate functions */
    int cleaned;
    double mean;
    double quot;
    double count;
};

struct fdo_table
{
/* a struct to implement a linked-list for FDO-ORG table names */
    char *table;
    struct fdo_table *next;
};

static void
fnct_spatialite_version (sqlite3_context * context, int argc,
			 sqlite3_value ** argv)
{
/* SQL function:
/ spatialite_version()
/
/ return a text string representing the current SpatiaLite version
*/
    int len;
    const char *p_result = spatialite_version ();
    GAIA_UNUSED ();
    len = strlen (p_result);
    sqlite3_result_text (context, p_result, len, SQLITE_TRANSIENT);
}

static void
fnct_geos_version (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ geos_version()
/
/ return a text string representing the current GEOS version
/ or NULL if GEOS is currently unsupported
*/

#ifndef OMIT_GEOS		/* GEOS version */
    int len;
    const char *p_result = GEOSversion ();
    GAIA_UNUSED ();
    len = strlen (p_result);
    sqlite3_result_text (context, p_result, len, SQLITE_TRANSIENT);
#else
    sqlite3_result_null (context);
#endif
}


static void
fnct_proj4_version (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ proj4_version()
/
/ return a text string representing the current PROJ.4 version
/ or NULL if PROJ.4 is currently unsupported
*/

#ifndef OMIT_PROJ		/* PROJ.4 version */
    int len;
    const char *p_result = pj_get_release ();
    GAIA_UNUSED ();
    len = strlen (p_result);
    sqlite3_result_text (context, p_result, len, SQLITE_TRANSIENT);
#else
    sqlite3_result_null (context);
#endif
}

static void
fnct_GeometryConstraints (sqlite3_context * context, int argc,
			  sqlite3_value ** argv)
{
/* SQL function:
/ GeometryConstraints(BLOBencoded geometry, geometry-type, srid)
/
/ checks geometry constraints, returning:
/
/ -1 - if some error occurred
/ 1 - if geometry constraints validation passes
/ 0 - if geometry constraints validation fails
/
*/
    int little_endian;
    int endian_arch = gaiaEndianArch ();
    unsigned char *p_blob = NULL;
    int n_bytes = 0;
    int srid;
    int geom_srid = -1;
    const unsigned char *type;
    int xtype;
    int geom_type = -1;
    int ret;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_BLOB
	|| sqlite3_value_type (argv[0]) == SQLITE_NULL)
	;
    else
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    if (sqlite3_value_type (argv[1]) == SQLITE_TEXT)
	type = sqlite3_value_text (argv[1]);
    else
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    if (sqlite3_value_type (argv[2]) == SQLITE_INTEGER)
	srid = sqlite3_value_int (argv[2]);
    else
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    if (sqlite3_value_type (argv[0]) == SQLITE_BLOB)
      {
	  p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
	  n_bytes = sqlite3_value_bytes (argv[0]);
      }
    if (p_blob)
      {
	  /* quick Geometry validation */
	  if (n_bytes < 45)
	      goto illegal_geometry;	/* cannot be an internal BLOB WKB geometry */
	  if (*(p_blob + 0) != GAIA_MARK_START)
	      goto illegal_geometry;	/* failed to recognize START signature */
	  if (*(p_blob + (n_bytes - 1)) != GAIA_MARK_END)
	      goto illegal_geometry;	/* failed to recognize END signature */
	  if (*(p_blob + 38) != GAIA_MARK_MBR)
	      goto illegal_geometry;	/* failed to recognize MBR signature */
	  if (*(p_blob + 1) == GAIA_LITTLE_ENDIAN)
	      little_endian = 1;
	  else if (*(p_blob + 1) == GAIA_BIG_ENDIAN)
	      little_endian = 0;
	  else
	      goto illegal_geometry;	/* unknown encoding; nor litte-endian neither big-endian */
	  geom_type = gaiaImport32 (p_blob + 39, little_endian, endian_arch);
	  geom_srid = gaiaImport32 (p_blob + 2, little_endian, endian_arch);
	  goto valid_geometry;
	illegal_geometry:
	  sqlite3_result_int (context, -1);
	  return;
      }
  valid_geometry:
    xtype = GAIA_UNKNOWN;
    if (strcasecmp ((char *) type, "POINT") == 0)
	xtype = GAIA_POINT;
    if (strcasecmp ((char *) type, "LINESTRING") == 0)
	xtype = GAIA_LINESTRING;
    if (strcasecmp ((char *) type, "POLYGON") == 0)
	xtype = GAIA_POLYGON;
    if (strcasecmp ((char *) type, "MULTIPOINT") == 0)
	xtype = GAIA_MULTIPOINT;
    if (strcasecmp ((char *) type, "MULTILINESTRING") == 0)
	xtype = GAIA_MULTILINESTRING;
    if (strcasecmp ((char *) type, "MULTIPOLYGON") == 0)
	xtype = GAIA_MULTIPOLYGON;
    if (strcasecmp ((char *) type, "GEOMETRYCOLLECTION") == 0)
	xtype = GAIA_GEOMETRYCOLLECTION;
    if (strcasecmp ((char *) type, "GEOMETRY") == 0)
	xtype = -1;
    if (xtype == GAIA_UNKNOWN)
	sqlite3_result_int (context, -1);
    else
      {
	  ret = 1;
	  if (p_blob)
	    {
		/* skipping NULL Geometry; this is assumed to be always good */
		if (geom_srid != srid)
		    ret = 0;
		if (xtype == -1)
		    ;
		else if (xtype != geom_type)
		    ret = 0;
	    }
	  sqlite3_result_int (context, ret);
      }
}

static int
checkSpatialMetaData (sqlite3 * sqlite)
{
/* internal utility function:
/
/ for FDO-OGR interoperability:
/ tests the SpatialMetadata type, returning:
/
/ 0 - if no valid SpatialMetaData where found
/ 1 - if SpatiaLite-like SpatialMetadata where found
/ 2- if FDO-OGR-like SpatialMetadata where found
/
*/
    int spatialite_rs = 0;
    int fdo_rs = 0;
    int spatialite_gc = 0;
    int fdo_gc = 0;
    int rs_srid = 0;
    int auth_name = 0;
    int auth_srid = 0;
    int srtext = 0;
    int ref_sys_name = 0;
    int proj4text = 0;
    int f_table_name = 0;
    int f_geometry_column = 0;
    int geometry_type = 0;
    int coord_dimension = 0;
    int gc_srid = 0;
    int geometry_format = 0;
    int type = 0;
    int spatial_index_enabled = 0;
    char sql[1024];
    int ret;
    const char *name;
    int i;
    char **results;
    int rows;
    int columns;
/* checking the GEOMETRY_COLUMNS table */
    strcpy (sql, "PRAGMA table_info(\"geometry_columns\")");
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	goto unknown;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		name = results[(i * columns) + 1];
		if (strcasecmp (name, "f_table_name") == 0)
		    f_table_name = 1;
		if (strcasecmp (name, "f_geometry_column") == 0)
		    f_geometry_column = 1;
		if (strcasecmp (name, "geometry_type") == 0)
		    geometry_type = 1;
		if (strcasecmp (name, "coord_dimension") == 0)
		    coord_dimension = 1;
		if (strcasecmp (name, "srid") == 0)
		    gc_srid = 1;
		if (strcasecmp (name, "geometry_format") == 0)
		    geometry_format = 1;
		if (strcasecmp (name, "type") == 0)
		    type = 1;
		if (strcasecmp (name, "spatial_index_enabled") == 0)
		    spatial_index_enabled = 1;
	    }
      }
    sqlite3_free_table (results);
    if (f_table_name
	&&
	f_geometry_column
	&& type && coord_dimension && gc_srid && spatial_index_enabled)
	spatialite_gc = 1;
    if (f_table_name
	&&
	f_geometry_column
	&& geometry_type && coord_dimension && gc_srid && geometry_format)
	fdo_gc = 1;
/* checking the SPATIAL_REF_SYS table */
    strcpy (sql, "PRAGMA table_info(\"spatial_ref_sys\")");
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	goto unknown;
    if (rows < 1)
	;
    else
      {
	  for (i = 1; i <= rows; i++)
	    {
		name = results[(i * columns) + 1];
		if (strcasecmp (name, "srid") == 0)
		    rs_srid = 1;
		if (strcasecmp (name, "auth_name") == 0)
		    auth_name = 1;
		if (strcasecmp (name, "auth_srid") == 0)
		    auth_srid = 1;
		if (strcasecmp (name, "srtext") == 0)
		    srtext = 1;
		if (strcasecmp (name, "ref_sys_name") == 0)
		    ref_sys_name = 1;
		if (strcasecmp (name, "proj4text") == 0)
		    proj4text = 1;
	    }
      }
    sqlite3_free_table (results);
    if (rs_srid && auth_name && auth_srid && ref_sys_name && proj4text)
	spatialite_rs = 1;
    if (rs_srid && auth_name && auth_srid && srtext)
	fdo_rs = 1;
/* verifying the MetaData format */
    if (spatialite_gc && spatialite_rs)
	return 1;
    if (fdo_gc && fdo_rs)
	return 2;
  unknown:
    return 0;
}

static void
add_fdo_table (struct fdo_table **first, struct fdo_table **last,
	       const char *table, int len)
{
/* adds an FDO-OGR styled Geometry Table to corresponding linked list */
    struct fdo_table *p = malloc (sizeof (struct fdo_table));
    p->table = malloc (len + 1);
    strcpy (p->table, table);
    p->next = NULL;
    if (!(*first))
	(*first) = p;
    if ((*last))
	(*last)->next = p;
    (*last) = p;
}

static void
free_fdo_tables (struct fdo_table *first)
{
/* memory cleanup; destroying the FDO-OGR tables linked list */
    struct fdo_table *p;
    struct fdo_table *pn;
    p = first;
    while (p)
      {
	  pn = p->next;
	  if (p->table)
	      free (p->table);
	  free (p);
	  p = pn;
      }
}

static void
fnct_AutoFDOStart (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ AutoFDOStart(void)
/
/ for FDO-OGR interoperability:
/ tests the SpatialMetadata type, then automatically
/ creating a VirtualFDO table for each FDO-OGR main table 
/ declared within FDO-styled SpatialMetadata
/
*/
    int ret;
    const char *name;
    int i;
    char **results;
    int rows;
    int columns;
    char sql[1024];
    int count = 0;
    struct fdo_table *first = NULL;
    struct fdo_table *last = NULL;
    struct fdo_table *p;
    int len;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    GAIA_UNUSED ();
    if (checkSpatialMetaData (sqlite) == 2)
      {
	  /* ok, creating VirtualFDO tables */
	  strcpy (sql, "SELECT DISTINCT f_table_name FROM geometry_columns");
	  ret =
	      sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
	  if (ret != SQLITE_OK)
	      goto error;
	  if (rows < 1)
	      ;
	  else
	    {
		for (i = 1; i <= rows; i++)
		  {
		      name = results[(i * columns) + 0];
		      if (name)
			{
			    len = strlen (name);
			    add_fdo_table (&first, &last, name, len);
			}
		  }
	    }
	  sqlite3_free_table (results);
	  p = first;
	  while (p)
	    {
		/* destroying the VirtualFDO table [if existing] */
		sprintf (sql, "DROP TABLE IF EXISTS \"fdo_%s\"", p->table);
		ret = sqlite3_exec (sqlite, sql, NULL, 0, NULL);
		if (ret != SQLITE_OK)
		    goto error;
		/* creating the VirtualFDO table */
		sprintf (sql,
			 "CREATE VIRTUAL TABLE \"fdo_%s\" USING VirtualFDO(%s)",
			 p->table, p->table);
		ret = sqlite3_exec (sqlite, sql, NULL, 0, NULL);
		if (ret != SQLITE_OK)
		    goto error;
		count++;
		p = p->next;
	    }
	error:
	  free_fdo_tables (first);
	  sqlite3_result_int (context, count);
	  return;
      }
    sqlite3_result_int (context, 0);
    return;
}

static void
fnct_AutoFDOStop (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ AutoFDOStop(void)
/
/ for FDO-OGR interoperability:
/ tests the SpatialMetadata type, then automatically
/ removes any VirtualFDO table 
/
*/
    int ret;
    const char *name;
    int i;
    char **results;
    int rows;
    int columns;
    char sql[1024];
    int count = 0;
    struct fdo_table *first = NULL;
    struct fdo_table *last = NULL;
    struct fdo_table *p;
    int len;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    GAIA_UNUSED ();
    if (checkSpatialMetaData (sqlite) == 2)
      {
	  /* ok, creating VirtualFDO tables */
	  strcpy (sql, "SELECT DISTINCT f_table_name FROM geometry_columns");
	  ret =
	      sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
	  if (ret != SQLITE_OK)
	      goto error;
	  if (rows < 1)
	      ;
	  else
	    {
		for (i = 1; i <= rows; i++)
		  {
		      name = results[(i * columns) + 0];
		      if (name)
			{
			    len = strlen (name);
			    add_fdo_table (&first, &last, name, len);
			}
		  }
	    }
	  sqlite3_free_table (results);
	  p = first;
	  while (p)
	    {
		/* destroying the VirtualFDO table [if existing] */
		sprintf (sql, "DROP TABLE IF EXISTS \"fdo_%s\"", p->table);
		ret = sqlite3_exec (sqlite, sql, NULL, 0, NULL);
		if (ret != SQLITE_OK)
		    goto error;
		count++;
		p = p->next;
	    }
	error:
	  free_fdo_tables (first);
	  sqlite3_result_int (context, count);
	  return;
      }
    sqlite3_result_int (context, 0);
    return;
}

static void
fnct_CheckSpatialMetaData (sqlite3_context * context, int argc,
			   sqlite3_value ** argv)
{
/* SQL function:
/ CheckSpatialMetaData(void)
/
/ for FDO-OGR interoperability:
/ tests the SpatialMetadata type, returning:
/
/ 0 - if no valid SpatialMetaData where found
/ 1 - if SpatiaLite-like SpatialMetadata where found
/ 2- if FDO-OGR-like SpatialMetadata where found
/
*/
    sqlite3 *sqlite;
    int ret;
    GAIA_UNUSED ();
    sqlite = sqlite3_context_db_handle (context);
    ret = checkSpatialMetaData (sqlite);
    sqlite3_result_int (context, ret);
    return;
}

static void
fnct_InitSpatialMetaData (sqlite3_context * context, int argc,
			  sqlite3_value ** argv)
{
/* SQL function:
/ InitSpatialMetaData(void)
/
/ creates the SPATIAL_REF_SYS and GEOMETRY_COLUMNS tables
/ returns 1 on success
/ 0 on failure
*/
    char sql[1024];
    char *errMsg = NULL;
    int ret;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    GAIA_UNUSED ();
/* creating the SPATIAL_REF_SYS tables */
    strcpy (sql, "CREATE TABLE spatial_ref_sys (\n");
    strcat (sql, "srid INTEGER NOT NULL PRIMARY KEY,\n");
    strcat (sql, "auth_name VARCHAR(256) NOT NULL,\n");
    strcat (sql, "auth_srid INTEGER NOT NULL,\n");
    strcat (sql, "ref_sys_name VARCHAR(256),\n");
    strcat (sql, "proj4text VARCHAR(2048) NOT NULL)");
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
/* setting a trigger to ensure referential integrity on DELETE */
    strcpy (sql,
	    "CREATE TRIGGER fkd_refsys_geocols BEFORE DELETE ON spatial_ref_sys\n");
    strcat (sql, "FOR EACH ROW BEGIN\n");
    strcat (sql,
	    "SELECT RAISE(ROLLBACK, 'delete on table ''spatial_ref_sys'' violates constraint: ''geometry_columns.srid''')\n");
    strcat (sql,
	    "WHERE (SELECT srid FROM geometry_columns WHERE srid = OLD.srid) IS NOT NULL;\n");
    strcat (sql, "END;");
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
/* creating the GEOMETRY_COLUMN tables */
    strcpy (sql, "CREATE TABLE geometry_columns (\n");
    strcat (sql, "f_table_name VARCHAR(256) NOT NULL,\n");
    strcat (sql, "f_geometry_column VARCHAR(256) NOT NULL,\n");
    strcat (sql, "type VARCHAR(30) NOT NULL,\n");
    strcat (sql, "coord_dimension INTEGER NOT NULL,\n");
    strcat (sql, "srid INTEGER,\n");
    strcat (sql, "spatial_index_enabled INTEGER NOT NULL)");
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
/* setting a trigger to ensure referential integrity on INSERT */
    strcpy (sql,
	    "CREATE TRIGGER fki_geocols_refsys BEFORE INSERT ON geometry_columns\n");
    strcat (sql, "FOR EACH ROW BEGIN\n");
    strcat (sql,
	    "SELECT RAISE(ROLLBACK, 'insert on table ''geometry_columns'' violates constraint: ''spatial_ref_sys.srid''')\n");
    strcat (sql, "WHERE  NEW.\"srid\" IS NOT NULL\n");
    strcat (sql,
	    "AND (SELECT srid FROM spatial_ref_sys WHERE srid = NEW.srid) IS NULL;\n");
    strcat (sql, "END;");
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
/* setting a trigger to ensure referential integrity on UPDATE */
    strcpy (sql,
	    "CREATE TRIGGER fku_geocols_refsys BEFORE UPDATE ON geometry_columns\n");
    strcat (sql, "FOR EACH ROW BEGIN\n");
    strcat (sql,
	    "SELECT RAISE(ROLLBACK, 'update on table ''geometry_columns'' violates constraint: ''spatial_ref_sys.srid''')\n");
    strcat (sql, "WHERE  NEW.srid IS NOT NULL\n");
    strcat (sql,
	    "AND (SELECT srid FROM spatial_ref_sys WHERE srid = NEW.srid) IS NULL;\n");
    strcat (sql, "END;");
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
/* creating an UNIQUE INDEX */
    strcpy (sql, "CREATE UNIQUE INDEX idx_geocols ON geometry_columns\n");
    strcat (sql, "(f_table_name, f_geometry_column)");
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
/* creating the GEOM_COLS_REF_SYS view */
    strcpy (sql, "CREATE VIEW geom_cols_ref_sys AS\n");
    strcat (sql, "SELECT  f_table_name, f_geometry_column, type,\n");
    strcat (sql, "coord_dimension, spatial_ref_sys.srid AS srid,\n");
    strcat (sql, "auth_name, auth_srid, ref_sys_name, proj4text\n");
    strcat (sql, "FROM geometry_columns, spatial_ref_sys\n");
    strcat (sql, "WHERE geometry_columns.srid = spatial_ref_sys.srid");
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    sqlite3_result_int (context, 1);
    return;
  error:
    fprintf (stderr, "InitSpatiaMetaData() error: \"%s\"\n", errMsg);
    sqlite3_free (errMsg);
    sqlite3_result_int (context, 0);
    return;
}

static int
recoverGeomColumn (sqlite3 * sqlite, const unsigned char *table,
		   const unsigned char *column, int xtype, int srid)
{
/* checks if TABLE.COLUMN exists and has the required features */
    int ok = 1;
    char sql[1024];
    int type;
    sqlite3_stmt *stmt;
    gaiaGeomCollPtr geom;
    const void *blob_value;
    int len;
    int ret;
    int i_col;
    sprintf (sql, "SELECT %s FROM \"%s\"", column, table);
/* compiling SQL prepared statement */
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "recoverGeomColumn: error %d \"%s\"\n",
		   sqlite3_errcode (sqlite), sqlite3_errmsg (sqlite));
	  return 0;
      }
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* cecking Geometry features */
		geom = NULL;
		for (i_col = 0; i_col < sqlite3_column_count (stmt); i_col++)
		  {
		      if (sqlite3_column_type (stmt, i_col) != SQLITE_BLOB)
			  ok = 0;
		      else
			{
			    blob_value = sqlite3_column_blob (stmt, i_col);
			    len = sqlite3_column_bytes (stmt, i_col);
			    geom = gaiaFromSpatiaLiteBlobWkb (blob_value, len);
			    if (!geom)
				ok = 0;
			    else
			      {
				  if (geom->Srid != srid)
				      ok = 0;
				  type = gaiaGeometryType (geom);
				  if (xtype == type)
				      ;
				  else
				      ok = 0;
				  gaiaFreeGeomColl (geom);
			      }
			}
		  }
	    }
	  if (!ok)
	      break;
      }
    ret = sqlite3_finalize (stmt);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "recoverGeomColumn: error %d \"%s\"\n",
		   sqlite3_errcode (sqlite), sqlite3_errmsg (sqlite));
	  return 0;
      }
    return ok;
}

static void
buildSpatialIndex (sqlite3 * sqlite, const unsigned char *table, char *col_name)
{
/* loading a SpatialIndex [RTree] */
    char sql[2048];
    char sql2[1024];
    char *errMsg = NULL;
    int ret;
    sprintf (sql,
	     "INSERT INTO \"idx_%s_%s\" (\"pkid\", \"xmin\", \"xmax\", \"ymin\", \"ymax\") ",
	     table, col_name);
    sprintf (sql2,
	     "SELECT ROWID, MbrMinX(\"%s\"), MbrMaxX(\"%s\"), MbrMinY(\"%s\"), MbrMaxY(\"%s\") FROM \"%s\"",
	     col_name, col_name, col_name, col_name, table);
    strcat (sql, sql2);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "buildSpatialIndex error: \"%s\"\n", errMsg);
	  sqlite3_free (errMsg);
      }
}

static void
updateGeometryTriggers (sqlite3 * sqlite, const unsigned char *table,
			const unsigned char *column)
{
/* updates triggers for some Spatial Column */
    char sql[256];
    char trigger[4096];
    char **results;
    int ret;
    int rows;
    int columns;
    int i;
    char tblname[256];
    char colname[256];
    char col_type[32];
    char col_srid[32];
    char col_index[32];
    int srid;
    int index;
    int cached;
    int len;
    char *errMsg = NULL;
    char dummy[512];
    struct spatial_index_str *first_idx = NULL;
    struct spatial_index_str *last_idx = NULL;
    struct spatial_index_str *curr_idx;
    struct spatial_index_str *next_idx;
    sprintf (sql,
	     "SELECT f_table_name, f_geometry_column, type, srid, spatial_index_enabled FROM geometry_columns WHERE f_table_name LIKE '%s' AND f_geometry_column LIKE '%s'",
	     table, column);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, &errMsg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "updateTableTriggers: \"%s\"\n", errMsg);
	  sqlite3_free (errMsg);
	  return;
      }
    for (i = 1; i <= rows; i++)
      {
	  /* preparing the triggers */
	  strcpy (tblname, results[(i * columns)]);
	  strcpy (colname, results[(i * columns) + 1]);
	  strcpy (col_type, results[(i * columns) + 2]);
	  strcpy (col_srid, results[(i * columns) + 3]);
	  strcpy (col_index, results[(i * columns) + 4]);
	  srid = atoi (col_srid);
	  if (atoi (col_index) == 1)
	      index = 1;
	  else
	      index = 0;
	  if (atoi (col_index) == 2)
	      cached = 1;
	  else
	      cached = 0;

	  /* trying to delete old versions [v2.0, v2.2] triggers[if any] */
	  sprintf (trigger, "DROP TRIGGER IF EXISTS \"gti_%s_%s\"", tblname,
		   colname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  sprintf (trigger, "DROP TRIGGER IF EXISTS \"gtu_%s_%s\"", tblname,
		   colname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  sprintf (trigger, "DROP TRIGGER IF EXISTS \"gsi_%s_%s\"", tblname,
		   colname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  sprintf (trigger, "DROP TRIGGER IF EXISTS \"gsu_%s_%s\"", tblname,
		   colname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  /* end deletion old versions [v2.0, v2.2] triggers[if any] */

	  /* deleting the old INSERT trigger TYPE [if any] */
	  sprintf (trigger, "DROP TRIGGER IF EXISTS \"ggi_%s_%s\"", tblname,
		   colname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  /* inserting the new INSERT trigger TYPE */
	  sprintf (trigger,
		   "CREATE TRIGGER \"ggi_%s_%s\" BEFORE INSERT ON \"%s\"\n",
		   tblname, colname, tblname);
	  strcat (trigger, "FOR EACH ROW BEGIN\n");
	  sprintf (dummy,
		   "SELECT RAISE(ROLLBACK, '\"%s\".\"%s\" violates Geometry constraint [geom-type or SRID not allowed]')\n",
		   tblname, colname);
	  strcat (trigger, dummy);
	  strcat (trigger, "WHERE (SELECT type FROM geometry_columns\n");
	  sprintf (dummy,
		   "WHERE f_table_name = '%s' AND f_geometry_column = '%s'\n",
		   tblname, colname);
	  strcat (trigger, dummy);
	  sprintf (dummy,
		   "AND GeometryConstraints(NEW.\"%s\", type, srid) = 1) IS NULL;\n",
		   colname);
	  strcat (trigger, dummy);
	  strcat (trigger, "END;");
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  /* deleting the old UPDATE trigger TYPE [if any] */
	  sprintf (trigger, "DROP TRIGGER IF EXISTS \"ggu_%s_%s\"", tblname,
		   colname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  /* inserting the new UPDATE trigger TYPE */
	  sprintf (trigger,
		   "CREATE TRIGGER \"ggu_%s_%s\" BEFORE UPDATE ON \"%s\"\n",
		   tblname, colname, tblname);
	  strcat (trigger, "FOR EACH ROW BEGIN\n");
	  sprintf (dummy,
		   "SELECT RAISE(ROLLBACK, '\"%s\".\"%s\" violates Geometry constraint [geom-type or SRID not allowed]')\n",
		   tblname, colname);
	  strcat (trigger, dummy);
	  strcat (trigger,
		  "WHERE (SELECT \"type\" FROM \"geometry_columns\"\n");
	  sprintf (dummy,
		   "WHERE f_table_name = '%s' AND f_geometry_column = '%s'\n",
		   tblname, colname);
	  strcat (trigger, dummy);
	  sprintf (dummy,
		   "AND GeometryConstraints(NEW.\"%s\", type, srid) = 1) IS NULL;\n",
		   colname);
	  strcat (trigger, dummy);
	  strcat (trigger, "END;");
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  /* inserting SpatialIndex infos into the linked list */
	  curr_idx = malloc (sizeof (struct spatial_index_str));
	  len = strlen (tblname);
	  curr_idx->TableName = malloc (len + 1);
	  strcpy (curr_idx->TableName, tblname);
	  len = strlen ((char *) colname);
	  curr_idx->ColumnName = malloc (len + 1);
	  strcpy (curr_idx->ColumnName, (char *) colname);
	  curr_idx->ValidRtree = (char) index;
	  curr_idx->ValidCache = (char) cached;
	  curr_idx->Next = NULL;
	  if (!first_idx)
	      first_idx = curr_idx;
	  if (last_idx)
	      last_idx->Next = curr_idx;
	  last_idx = curr_idx;
	  /* deleting the old INSERT trigger SPATIAL_INDEX [if any] */
	  sprintf (trigger, "DROP TRIGGER IF EXISTS \"gii_%s_%s\"", tblname,
		   colname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  if (index)
	    {
		/* inserting the new INSERT trigger SRID */
		sprintf (trigger,
			 "CREATE TRIGGER \"gii_%s_%s\" AFTER INSERT ON \"%s\"\n",
			 tblname, colname, tblname);
		strcat (trigger, "FOR EACH ROW BEGIN\n");
		sprintf (dummy,
			 "INSERT INTO \"idx_%s_%s\" (pkid, xmin, xmax, ymin, ymax) VALUES (NEW.ROWID,\n",
			 tblname, colname);
		strcat (trigger, dummy);
		sprintf (dummy, "MbrMinX(NEW.\"%s\"), ", colname);
		strcat (trigger, dummy);
		sprintf (dummy, "MbrMaxX(NEW.\"%s\"), ", colname);
		strcat (trigger, dummy);
		sprintf (dummy, "MbrMinY(NEW.\"%s\"), ", colname);
		strcat (trigger, dummy);
		sprintf (dummy, "MbrMaxY(NEW.\"%s\"));\n", colname);
		strcat (trigger, dummy);
		strcat (trigger, "END;");
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
	    }
	  /* deleting the old UPDATE trigger SPATIAL_INDEX [if any] */
	  sprintf (trigger, "DROP TRIGGER IF EXISTS \"giu_%s_%s\"", tblname,
		   colname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  if (index)
	    {
		/* inserting the new UPDATE trigger SRID */
		sprintf (trigger,
			 "CREATE TRIGGER \"giu_%s_%s\" AFTER UPDATE ON \"%s\"\n",
			 tblname, colname, tblname);
		strcat (trigger, "FOR EACH ROW BEGIN\n");
		sprintf (dummy, "UPDATE \"idx_%s_%s\" SET ", tblname, colname);
		strcat (trigger, dummy);
		sprintf (dummy, "\"xmin\" = MbrMinX(NEW.\"%s\"), ", colname);
		strcat (trigger, dummy);
		sprintf (dummy, "\"xmax\" = MbrMaxX(NEW.\"%s\"), ", colname);
		strcat (trigger, dummy);
		sprintf (dummy, "\"ymin\" = MbrMinY(NEW.\"%s\"), ", colname);
		strcat (trigger, dummy);
		sprintf (dummy, "\"ymax\" = MbrMaxY(NEW.\"%s\")\n", colname);
		strcat (trigger, dummy);
		strcat (trigger, "WHERE \"pkid\" = NEW.ROWID;\n");
		strcat (trigger, "END;");
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
	    }
	  /* deleting the old UPDATE trigger SPATIAL_INDEX [if any] */
	  sprintf (trigger, "DROP TRIGGER IF EXISTS \"gid_%s_%s\"", tblname,
		   colname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  if (index)
	    {
		/* inserting the new DELETE trigger SRID */
		sprintf (trigger,
			 "CREATE TRIGGER \"gid_%s_%s\" AFTER DELETE ON \"%s\"\n",
			 tblname, colname, tblname);
		strcat (trigger, "FOR EACH ROW BEGIN\n");
		sprintf (dummy,
			 "DELETE FROM \"idx_%s_%s\" WHERE pkid = OLD.ROWID;\n",
			 tblname, colname);
		strcat (trigger, dummy);
		strcat (trigger, "END;");
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
	    }
	  /* deleting the old INSERT trigger MBR_CACHE [if any] */
	  sprintf (trigger, "DROP TRIGGER IF EXISTS \"gci_%s_%s\"", tblname,
		   colname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  if (cached)
	    {
		/* inserting the new INSERT trigger SRID */
		sprintf (trigger,
			 "CREATE TRIGGER \"gci_%s_%s\" AFTER INSERT ON \"%s\"\n",
			 tblname, colname, tblname);
		strcat (trigger, "FOR EACH ROW BEGIN\n");
		sprintf (dummy,
			 "INSERT INTO \"cache_%s_%s\" (rowid, mbr) VALUES (NEW.ROWID,\nBuildMbrFilter(",
			 tblname, colname);
		strcat (trigger, dummy);
		sprintf (dummy, "MbrMinX(NEW.\"%s\"), ", colname);
		strcat (trigger, dummy);
		sprintf (dummy, "MbrMinY(NEW.\"%s\"), ", colname);
		strcat (trigger, dummy);
		sprintf (dummy, "MbrMaxX(NEW.\"%s\"), ", colname);
		strcat (trigger, dummy);
		sprintf (dummy, "MbrMaxY(NEW.\"%s\")));\n", colname);
		strcat (trigger, dummy);
		strcat (trigger, "END;");
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
	    }
	  /* deleting the old UPDATE trigger MBR_CACHE [if any] */
	  sprintf (trigger, "DROP TRIGGER IF EXISTS \"gcu_%s_%s\"", tblname,
		   colname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  if (cached)
	    {
		/* inserting the new UPDATE trigger SRID */
		sprintf (trigger,
			 "CREATE TRIGGER \"gcu_%s_%s\" AFTER UPDATE ON \"%s\"\n",
			 tblname, colname, tblname);
		strcat (trigger, "FOR EACH ROW BEGIN\n");
		sprintf (dummy, "UPDATE \"cache_%s_%s\" SET ", tblname,
			 colname);
		strcat (trigger, dummy);
		sprintf (dummy,
			 "\"mbr\" = BuildMbrFilter(MbrMinX(NEW.\"%s\"), ",
			 colname);
		strcat (trigger, dummy);
		sprintf (dummy, "MbrMinY(NEW.\"%s\"), ", colname);
		strcat (trigger, dummy);
		sprintf (dummy, "MbrMaxX(NEW.\"%s\"), ", colname);
		strcat (trigger, dummy);
		sprintf (dummy, "MbrMaxY(NEW.\"%s\"))\n", colname);
		strcat (trigger, dummy);
		strcat (trigger, "WHERE \"rowid\" = NEW.ROWID;\n");
		strcat (trigger, "END;");
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
	    }
	  /* deleting the old UPDATE trigger MBR_CACHE [if any] */
	  sprintf (trigger, "DROP TRIGGER IF EXISTS \"gcd_%s_%s\"", tblname,
		   colname);
	  ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
	  if (ret != SQLITE_OK)
	      goto error;
	  if (cached)
	    {
		/* inserting the new DELETE trigger SRID */
		sprintf (trigger,
			 "CREATE TRIGGER \"gcd_%s_%s\" AFTER DELETE ON \"%s\"\n",
			 tblname, colname, tblname);
		strcat (trigger, "FOR EACH ROW BEGIN\n");
		sprintf (dummy,
			 "DELETE FROM \"cache_%s_%s\" WHERE \"rowid\" = OLD.ROWID;\n",
			 tblname, colname);
		strcat (trigger, dummy);
		strcat (trigger, "END;");
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
	    }
      }
    sqlite3_free_table (results);
/* now we'll adjust any related SpatialIndex as required */
    curr_idx = first_idx;
    while (curr_idx)
      {
	  if (curr_idx->ValidRtree)
	    {
		/* building RTree SpatialIndex */
		sprintf (trigger,
			 "CREATE VIRTUAL TABLE \"idx_%s_%s\" USING rtree(\n",
			 curr_idx->TableName, curr_idx->ColumnName);
		strcat (trigger, "pkid, xmin, xmax, ymin, ymax)");
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
		buildSpatialIndex (sqlite,
				   (unsigned char *) (curr_idx->TableName),
				   curr_idx->ColumnName);
	    }
	  if (curr_idx->ValidCache)
	    {
		/* building MbrCache SpatialIndex */
		sprintf (trigger,
			 "CREATE VIRTUAL TABLE \"cache_%s_%s\" USING MbrCache(%s, %s)\n",
			 curr_idx->TableName, curr_idx->ColumnName,
			 curr_idx->TableName, curr_idx->ColumnName);
		ret = sqlite3_exec (sqlite, trigger, NULL, NULL, &errMsg);
		if (ret != SQLITE_OK)
		    goto error;
	    }
	  curr_idx = curr_idx->Next;
      }
    goto index_cleanup;
  error:
    fprintf (stderr, "updateTableTriggers: \"%s\"\n", errMsg);
    sqlite3_free (errMsg);
  index_cleanup:
    curr_idx = first_idx;
    while (curr_idx)
      {
	  next_idx = curr_idx->Next;
	  if (curr_idx->TableName)
	      free (curr_idx->TableName);
	  if (curr_idx->ColumnName)
	      free (curr_idx->ColumnName);
	  free (curr_idx);
	  curr_idx = next_idx;
      }
}

static void
fnct_AddGeometryColumn (sqlite3_context * context, int argc,
			sqlite3_value ** argv)
{
/* SQL function:
/ AddGeometryColumn(table, column, srid, type , dimension  [  , not-null ]  )
/
/ creates a new COLUMN of given TYPE into TABLE
/ returns 1 on success
/ 0 on failure
*/
    const unsigned char *table;
    const unsigned char *column;
    const unsigned char *type;
    int xtype;
    int srid = -1;
    int dimension = 2;
    char dummy[32];
    char sql[1024];
    char *errMsg = NULL;
    int ret;
    char **results;
    int rows;
    int columns;
    int i;
    char tblname[256];
    int notNull = 0;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    if (sqlite3_value_type (argv[0]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "AddGeometryColumn() error: argument 1 [table_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    table = sqlite3_value_text (argv[0]);
    if (sqlite3_value_type (argv[1]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "AddGeometryColumn() error: argument 2 [column_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    column = sqlite3_value_text (argv[1]);
    if (sqlite3_value_type (argv[2]) != SQLITE_INTEGER)
      {
	  fprintf (stderr,
		   "AddGeometryColumn() error: argument 3 [SRID] is not of the Integer type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    srid = sqlite3_value_int (argv[2]);
    if (sqlite3_value_type (argv[3]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "AddGeometryColumn() error: argument 4 [geometry_type] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    type = sqlite3_value_text (argv[3]);
    if (sqlite3_value_type (argv[4]) != SQLITE_INTEGER)
      {
	  fprintf (stderr,
		   "AddGeometryColumn() error: argument 5 [dimension] is not of the Integer type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    dimension = sqlite3_value_int (argv[4]);
    if (argc > 5)
      {
	  /* optional NOT NULL arg */
	  if (sqlite3_value_type (argv[5]) != SQLITE_INTEGER)
	    {
		fprintf (stderr,
			 "AddGeometryColumn() error: argument 6 [not null] is not of the Integer type\n");
		sqlite3_result_int (context, 0);
		return;
	    }
	  notNull = sqlite3_value_int (argv[5]);
      }
    xtype = GAIA_UNKNOWN;
    if (strcasecmp ((char *) type, "POINT") == 0)
	xtype = GAIA_POINT;
    if (strcasecmp ((char *) type, "LINESTRING") == 0)
	xtype = GAIA_LINESTRING;
    if (strcasecmp ((char *) type, "POLYGON") == 0)
	xtype = GAIA_POLYGON;
    if (strcasecmp ((char *) type, "MULTIPOINT") == 0)
	xtype = GAIA_MULTIPOINT;
    if (strcasecmp ((char *) type, "MULTILINESTRING") == 0)
	xtype = GAIA_MULTILINESTRING;
    if (strcasecmp ((char *) type, "MULTIPOLYGON") == 0)
	xtype = GAIA_MULTIPOLYGON;
    if (strcasecmp ((char *) type, "GEOMETRYCOLLECTION") == 0)
	xtype = GAIA_GEOMETRYCOLLECTION;
    if (strcasecmp ((char *) type, "GEOMETRY") == 0)
	xtype = -1;
    if (xtype == GAIA_UNKNOWN)
      {
	  fprintf (stderr,
		   "AddGeometryColumn() error: argument 3 [geometry_type] has an illegal value\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    if (dimension != 2)
      {
	  fprintf (stderr,
		   "AddGeometryColumn() error: argument 5 [dimension] current version only accepts dimension=2\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
/* checking if the table exists */
    sprintf (sql,
	     "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '%s'",
	     table);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, &errMsg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "AddGeometryColumn: \"%s\"\n", errMsg);
	  sqlite3_free (errMsg);
	  return;
      }
    *tblname = '\0';
    for (i = 1; i <= rows; i++)
      {
	  /* preparing the triggers */
	  strcpy (tblname, results[(i * columns)]);
      }
    sqlite3_free_table (results);
    if (*tblname == '\0')
      {
	  fprintf (stderr,
		   "AddGeometryColumn() error: table '%s' does not exists\n",
		   table);
	  sqlite3_result_int (context, 0);
	  return;
      }
/* trying to add the column */
    strcpy (sql, "ALTER TABLE \"");
    strcat (sql, (char *) table);
    strcat (sql, "\" ADD COLUMN \"");
    strcat (sql, (char *) column);
    strcat (sql, "\" ");
    switch (xtype)
      {
      case GAIA_POINT:
	  strcat (sql, "POINT");
	  break;
      case GAIA_LINESTRING:
	  strcat (sql, "LINESTRING");
	  break;
      case GAIA_POLYGON:
	  strcat (sql, "POLYGON");
	  break;
      case GAIA_MULTIPOINT:
	  strcat (sql, "MULTIPOINT");
	  break;
      case GAIA_MULTILINESTRING:
	  strcat (sql, "MULTILINESTRING");
	  break;
      case GAIA_MULTIPOLYGON:
	  strcat (sql, "MULTIPOLYGON");
	  break;
      case GAIA_GEOMETRYCOLLECTION:
	  strcat (sql, "GEOMETRYCOLLECTION");
	  break;
      case -1:
	  strcat (sql, "GEOMETRY");
	  break;
      };
    if (notNull)
      {
	  /* adding a NOT NULL clause */
	  strcat (sql, " NOT NULL DEFAULT ''");
      }
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
/*ok, inserting into geometry_columns [Spatial Metadata] */
    strcpy (sql,
	    "INSERT INTO geometry_columns (f_table_name, f_geometry_column, type, ");
    strcat (sql, "coord_dimension, srid, spatial_index_enabled) VALUES (");
    strcat (sql, "'");
    strcat (sql, (char *) tblname);
    strcat (sql, "', '");
    strcat (sql, (char *) column);
    strcat (sql, "', '");
    switch (xtype)
      {
      case GAIA_POINT:
	  strcat (sql, "POINT");
	  break;
      case GAIA_LINESTRING:
	  strcat (sql, "LINESTRING");
	  break;
      case GAIA_POLYGON:
	  strcat (sql, "POLYGON");
	  break;
      case GAIA_MULTIPOINT:
	  strcat (sql, "MULTIPOINT");
	  break;
      case GAIA_MULTILINESTRING:
	  strcat (sql, "MULTILINESTRING");
	  break;
      case GAIA_MULTIPOLYGON:
	  strcat (sql, "MULTIPOLYGON");
	  break;
      case GAIA_GEOMETRYCOLLECTION:
	  strcat (sql, "GEOMETRYCOLLECTION");
	  break;
      case -1:
	  strcat (sql, "GEOMETRY");
	  break;
      };
    strcat (sql, "', 2, ");
    if (srid <= 0)
	strcat (sql, "-1");
    else
      {
	  sprintf (dummy, "%d", srid);
	  strcat (sql, dummy);
      }
    strcat (sql, ", 0)");
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    updateGeometryTriggers (sqlite, table, column);
    sqlite3_result_int (context, 1);
    return;
  error:
    fprintf (stderr, "AddGeometryColumn() error: \"%s\"\n", errMsg);
    sqlite3_free (errMsg);
    sqlite3_result_int (context, 0);
    return;
}

static void
fnct_RecoverGeometryColumn (sqlite3_context * context, int argc,
			    sqlite3_value ** argv)
{
/* SQL function:
/ RecoverGeometryColumn(table, column, srid, type , dimension )
/
/ checks if an existing TABLE.COLUMN satisfies the required geometric features
/ if yes adds it to SpatialMetaData and enabling triggers
/ returns 1 on success
/ 0 on failure
*/
    const unsigned char *table;
    const unsigned char *column;
    const unsigned char *type;
    int xtype;
    int srid = -1;
    int dimension = 2;
    char dummy[32];
    char sql[1024];
    char *errMsg = NULL;
    int ret;
    char **results;
    int rows;
    int columns;
    int i;
    char tblname[256];
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "RecoverGeometryColumn() error: argument 1 [table_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    table = sqlite3_value_text (argv[0]);
    if (sqlite3_value_type (argv[1]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "RecoverGeometryColumn() error: argument 2 [column_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    column = sqlite3_value_text (argv[1]);
    if (sqlite3_value_type (argv[2]) != SQLITE_INTEGER)
      {
	  fprintf (stderr,
		   "RecoverGeometryColumn() error: argument 3 [SRID] is not of the Integer type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    srid = sqlite3_value_int (argv[2]);
    if (sqlite3_value_type (argv[3]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "RecoverGeometryColumn() error: argument 4 [geometry_type] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    type = sqlite3_value_text (argv[3]);
    if (sqlite3_value_type (argv[4]) != SQLITE_INTEGER)
      {
	  fprintf (stderr,
		   "RecoverGeometryColumn() error: argument 5 [dimension] is not of the Integer type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    dimension = sqlite3_value_int (argv[4]);
    xtype = GAIA_UNKNOWN;
    if (strcasecmp ((char *) type, "POINT") == 0)
	xtype = GAIA_POINT;
    if (strcasecmp ((char *) type, "LINESTRING") == 0)
	xtype = GAIA_LINESTRING;
    if (strcasecmp ((char *) type, "POLYGON") == 0)
	xtype = GAIA_POLYGON;
    if (strcasecmp ((char *) type, "MULTIPOINT") == 0)
	xtype = GAIA_MULTIPOINT;
    if (strcasecmp ((char *) type, "MULTILINESTRING") == 0)
	xtype = GAIA_MULTILINESTRING;
    if (strcasecmp ((char *) type, "MULTIPOLYGON") == 0)
	xtype = GAIA_MULTIPOLYGON;
    if (strcasecmp ((char *) type, "GEOMETRYCOLLECTION") == 0)
	xtype = GAIA_GEOMETRYCOLLECTION;
    if (strcasecmp ((char *) type, "GEOMETRY") == 0)
	xtype = -1;
    if (xtype == GAIA_UNKNOWN)
      {
	  fprintf (stderr,
		   "RecoverGeometryColumn() error: argument 3 [geometry_type] has an illegal value\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    if (dimension != 2)
      {
	  fprintf (stderr,
		   "RecoverGeometryColumn() error: argument 5 [dimension] current version only accepts dimension=2\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
/* checking if the table exists */
    sprintf (sql,
	     "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '%s'",
	     table);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, &errMsg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "RecoverGeometryColumn: \"%s\"\n", errMsg);
	  sqlite3_free (errMsg);
	  return;
      }
    *tblname = '\0';
    for (i = 1; i <= rows; i++)
      {
	  /* preparing the triggers */
	  strcpy (tblname, results[(i * columns)]);
      }
    sqlite3_free_table (results);
    if (*tblname == '\0')
      {
	  fprintf (stderr,
		   "RecoverGeometryColumn() error: table '%s' does not exists\n",
		   table);
	  sqlite3_result_int (context, 0);
	  return;
      }
    if (!recoverGeomColumn (sqlite, table, column, xtype, srid))
      {
	  fprintf (stderr, "RecoverGeometryColumn(): validation failed\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    strcpy (sql,
	    "INSERT INTO geometry_columns (f_table_name, f_geometry_column, type, ");
    strcat (sql, "coord_dimension, srid, spatial_index_enabled) VALUES (");
    strcat (sql, "'");
    strcat (sql, (char *) tblname);
    strcat (sql, "', '");
    strcat (sql, (char *) column);
    strcat (sql, "', '");
    switch (xtype)
      {
      case GAIA_POINT:
	  strcat (sql, "POINT");
	  break;
      case GAIA_LINESTRING:
	  strcat (sql, "LINESTRING");
	  break;
      case GAIA_POLYGON:
	  strcat (sql, "POLYGON");
	  break;
      case GAIA_MULTIPOINT:
	  strcat (sql, "MULTIPOINT");
	  break;
      case GAIA_MULTILINESTRING:
	  strcat (sql, "MULTILINESTRING");
	  break;
      case GAIA_MULTIPOLYGON:
	  strcat (sql, "MULTIPOLYGON");
	  break;
      case GAIA_GEOMETRYCOLLECTION:
	  strcat (sql, "GEOMETRYCOLLECTION");
	  break;
      case -1:
	  strcat (sql, "GEOMETRY");
	  break;
      };
    strcat (sql, "', 2, ");
    if (srid <= 0)
	strcat (sql, "-1");
    else
      {
	  sprintf (dummy, "%d", srid);
	  strcat (sql, dummy);
      }
    strcat (sql, ", 0)");
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    updateGeometryTriggers (sqlite, table, column);
    sqlite3_result_int (context, 1);
    return;
  error:
    fprintf (stderr, "RecoverGeometryColumn() error: \"%s\"\n", errMsg);
    sqlite3_free (errMsg);
    sqlite3_result_int (context, 0);
    return;
}

static void
fnct_DiscardGeometryColumn (sqlite3_context * context, int argc,
			    sqlite3_value ** argv)
{
/* SQL function:
/ DiscardGeometryColumn(table, column)
/
/ removes TABLE.COLUMN from the Spatial MetaData [thus disablig triggers too]
/ returns 1 on success
/ 0 on failure
*/
    const unsigned char *table;
    const unsigned char *column;
    char sql[1024];
    char *errMsg = NULL;
    int ret;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "DiscardGeometryColumn() error: argument 1 [table_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    table = sqlite3_value_text (argv[0]);
    if (sqlite3_value_type (argv[1]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "DiscardGeometryColumn() error: argument 2 [column_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    column = sqlite3_value_text (argv[1]);
    sprintf (sql,
	     "DELETE FROM geometry_columns WHERE f_table_name LIKE '%s' AND f_geometry_column LIKE '%s'",
	     (char *) table, (char *) column);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
/* removing triggers too */
    sprintf (sql,
	     "DROP TRIGGER IF EXISTS \"ggi_%s_%s\"",
	     (char *) table, (char *) column);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    sprintf (sql,
	     "DROP TRIGGER IF EXISTS \"ggu_%s_%s\"",
	     (char *) table, (char *) column);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    sprintf (sql,
	     "DROP TRIGGER IF EXISTS \"gii_%s_%s\"",
	     (char *) table, (char *) column);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    sprintf (sql,
	     "DROP TRIGGER IF EXISTS \"giu_%s_%s\"",
	     (char *) table, (char *) column);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    sprintf (sql,
	     "DROP TRIGGER IF EXISTS \"gid_%s_%s\"",
	     (char *) table, (char *) column);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    sprintf (sql,
	     "DROP TRIGGER IF EXISTS \"gci_%s_%s\"",
	     (char *) table, (char *) column);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    sprintf (sql,
	     "DROP TRIGGER IF EXISTS \"gcu_%s_%s\"",
	     (char *) table, (char *) column);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    sprintf (sql,
	     "DROP TRIGGER IF EXISTS \"gcd_%s_%s\"",
	     (char *) table, (char *) column);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;

    /* trying to delete old versions [v2.0, v2.2] triggers[if any] */
    sprintf (sql, "DROP TRIGGER IF EXISTS \"gti_%s_%s\"", table, column);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    sprintf (sql, "DROP TRIGGER IF EXISTS \"gtu_%s_%s\"", table, column);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    sprintf (sql, "DROP TRIGGER IF EXISTS \"gsi_%s_%s\"", table, column);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    sprintf (sql, "DROP TRIGGER IF EXISTS \"gsu_%s_%s\"", table, column);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    /* end deletion old versions [v2.0, v2.2] triggers[if any] */

    sqlite3_result_int (context, 1);
    return;
  error:
    fprintf (stderr, "DiscardGeometryColumn() error: \"%s\"\n", errMsg);
    sqlite3_free (errMsg);
    sqlite3_result_int (context, 0);
    return;
}

static void
fnct_InitFDOSpatialMetaData (sqlite3_context * context, int argc,
			     sqlite3_value ** argv)
{
/* SQL function:
/ InitFDOSpatialMetaData(void)
/
/ creates the FDO-styled SPATIAL_REF_SYS and GEOMETRY_COLUMNS tables
/ returns 1 on success
/ 0 on failure
*/
    char sql[1024];
    char *errMsg = NULL;
    int ret;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    GAIA_UNUSED ();
/* creating the SPATIAL_REF_SYS tables */
    strcpy (sql, "CREATE TABLE spatial_ref_sys (\n");
    strcat (sql, "srid INTEGER PRIMARY KEY,\n");
    strcat (sql, "auth_name TEXT,\n");
    strcat (sql, "auth_srid INTEGER,\n");
    strcat (sql, "srtext TEXT)");
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
/* creating the GEOMETRY_COLUMN tables */
    strcpy (sql, "CREATE TABLE geometry_columns (\n");
    strcat (sql, "f_table_name TEXT,\n");
    strcat (sql, "f_geometry_column TEXT,\n");
    strcat (sql, "geometry_type INTEGER,\n");
    strcat (sql, "coord_dimension INTEGER,\n");
    strcat (sql, "srid INTEGER,\n");
    strcat (sql, "geometry_format TEXT)");
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    sqlite3_result_int (context, 1);
    return;
  error:
    fprintf (stderr, "InitFDOSpatiaMetaData() error: \"%s\"\n", errMsg);
    sqlite3_free (errMsg);
    sqlite3_result_int (context, 0);
    return;
}

static int
recoverFDOGeomColumn (sqlite3 * sqlite, const unsigned char *table,
		      const unsigned char *column, int xtype, int srid)
{
/* checks if TABLE.COLUMN exists and has the required features */
    int ok = 1;
    char sql[1024];
    int type;
    sqlite3_stmt *stmt;
    gaiaGeomCollPtr geom;
    const void *blob_value;
    int len;
    int ret;
    int i_col;
    sprintf (sql, "SELECT \"%s\" FROM \"%s\"", column, table);
/* compiling SQL prepared statement */
    ret = sqlite3_prepare_v2 (sqlite, sql, strlen (sql), &stmt, NULL);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "recoverFDOGeomColumn: error %d \"%s\"\n",
		   sqlite3_errcode (sqlite), sqlite3_errmsg (sqlite));
	  return 0;
      }
    while (1)
      {
	  /* scrolling the result set rows */
	  ret = sqlite3_step (stmt);
	  if (ret == SQLITE_DONE)
	      break;		/* end of result set */
	  if (ret == SQLITE_ROW)
	    {
		/* cecking Geometry features */
		geom = NULL;
		for (i_col = 0; i_col < sqlite3_column_count (stmt); i_col++)
		  {
		      if (sqlite3_column_type (stmt, i_col) != SQLITE_BLOB)
			  ok = 0;
		      else
			{
			    blob_value = sqlite3_column_blob (stmt, i_col);
			    len = sqlite3_column_bytes (stmt, i_col);
			    geom = gaiaFromSpatiaLiteBlobWkb (blob_value, len);
			    if (!geom)
				ok = 0;
			    else
			      {
				  if (geom->Srid != srid)
				      ok = 0;
				  type = gaiaGeometryType (geom);
				  if (xtype == type)
				      ;
				  else
				      ok = 0;
				  gaiaFreeGeomColl (geom);
			      }
			}
		  }
	    }
	  if (!ok)
	      break;
      }
    ret = sqlite3_finalize (stmt);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "recoverFDOGeomColumn: error %d \"%s\"\n",
		   sqlite3_errcode (sqlite), sqlite3_errmsg (sqlite));
	  return 0;
      }
    return ok;
}

static void
fnct_AddFDOGeometryColumn (sqlite3_context * context, int argc,
			   sqlite3_value ** argv)
{
/* SQL function:
/ AddFDOGeometryColumn(table, column, srid, geometry_type , dimension, geometry_format )
/
/ creates a new COLUMN of given TYPE into TABLE
/ returns 1 on success
/ 0 on failure
*/
    const char *table;
    const char *column;
    const char *format;
    char xformat[64];
    int type;
    int srid = -1;
    int dimension = 2;
    char dummy[32];
    char sql[1024];
    char *errMsg = NULL;
    int ret;
    char **results;
    int rows;
    int columns;
    int i;
    char tblname[256];
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "AddFDOGeometryColumn() error: argument 1 [table_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    table = (const char *) sqlite3_value_text (argv[0]);
    if (sqlite3_value_type (argv[1]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "AddFDOGeometryColumn() error: argument 2 [column_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    column = (const char *) sqlite3_value_text (argv[1]);
    if (sqlite3_value_type (argv[2]) != SQLITE_INTEGER)
      {
	  fprintf (stderr,
		   "AddFDOGeometryColumn() error: argument 3 [SRID] is not of the Integer type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    srid = sqlite3_value_int (argv[2]);
    if (sqlite3_value_type (argv[3]) != SQLITE_INTEGER)
      {
	  fprintf (stderr,
		   "AddFDOGeometryColumn() error: argument 4 [geometry_type] is not of the Integer type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    type = sqlite3_value_int (argv[3]);
    if (sqlite3_value_type (argv[4]) != SQLITE_INTEGER)
      {
	  fprintf (stderr,
		   "AddFDOGeometryColumn() error: argument 5 [dimension] is not of the Integer type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    dimension = sqlite3_value_int (argv[4]);
    if (sqlite3_value_type (argv[5]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "AddFDOGeometryColumn() error: argument 6 [geometry_format] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    format = (const char *) sqlite3_value_text (argv[5]);
    if (type ==
	GAIA_POINT
	|| type ==
	GAIA_LINESTRING
	|| type ==
	GAIA_POLYGON
	|| type ==
	GAIA_MULTIPOINT
	|| type ==
	GAIA_MULTILINESTRING
	|| type == GAIA_MULTIPOLYGON || type == GAIA_GEOMETRYCOLLECTION)
	;
    else
      {
	  fprintf (stderr,
		   "AddFDOGeometryColumn() error: argument 4 [geometry_type] has an illegal value\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    if (dimension < 2 || dimension > 4)
      {
	  fprintf (stderr,
		   "AddFDOGeometryColumn() error: argument 5 [dimension] current version only accepts dimension=2,3,4\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    if (strcasecmp (format, "WKT") == 0)
	strcpy (xformat, "WKT");
    else if (strcasecmp (format, "WKB") == 0)
	strcpy (xformat, "WKB");
    else if (strcasecmp (format, "FGF") == 0)
	strcpy (xformat, "FGF");
    else
      {
	  fprintf (stderr,
		   "AddFDOGeometryColumn() error: argument 6 [geometry_format] has to be one of: WKT,WKB,FGF\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
/* checking if the table exists */
    sprintf (sql,
	     "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '%s'",
	     table);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, &errMsg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "AddFDOGeometryColumn: \"%s\"\n", errMsg);
	  sqlite3_free (errMsg);
	  return;
      }
    *tblname = '\0';
    for (i = 1; i <= rows; i++)
      {
	  strcpy (tblname, results[(i * columns)]);
      }
    sqlite3_free_table (results);
    if (*tblname == '\0')
      {
	  fprintf (stderr,
		   "AddFDOGeometryColumn() error: table '%s' does not exists\n",
		   table);
	  sqlite3_result_int (context, 0);
	  return;
      }
/* trying to add the column */
    strcpy (sql, "ALTER TABLE ");
    strcat (sql, (char *) table);
    strcat (sql, " ADD COLUMN ");
    strcat (sql, (char *) column);
    strcat (sql, " BLOB");
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
/*ok, inserting into geometry_columns [FDO Spatial Metadata] */
    strcpy (sql,
	    "INSERT INTO geometry_columns (f_table_name, f_geometry_column, geometry_type, ");
    strcat (sql, "coord_dimension, srid, geometry_format) VALUES (");
    strcat (sql, "'");
    strcat (sql, (char *) tblname);
    strcat (sql, "', '");
    strcat (sql, (char *) column);
    strcat (sql, "', ");
    sprintf (dummy, "%d, %d, ", type, dimension);
    strcat (sql, dummy);
    if (srid <= 0)
	strcat (sql, "-1");
    else
      {
	  sprintf (dummy, "%d", srid);
	  strcat (sql, dummy);
      }
    strcat (sql, ", '");
    strcat (sql, xformat);
    strcat (sql, "')");
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    sqlite3_result_int (context, 1);
    return;
  error:
    fprintf (stderr, "AddFDOGeometryColumn() error: \"%s\"\n", errMsg);
    sqlite3_free (errMsg);
    sqlite3_result_int (context, 0);
    return;
}

static void
fnct_RecoverFDOGeometryColumn (sqlite3_context * context, int argc,
			       sqlite3_value ** argv)
{
/* SQL function:
/ RecoverFDOGeometryColumn(table, column, srid, geometry_type , dimension, geometry_format )
/
/ checks if an existing TABLE.COLUMN satisfies the required geometric features
/ if yes adds it to FDO-styled SpatialMetaData 
/ returns 1 on success
/ 0 on failure
*/
    const char *table;
    const char *column;
    const char *format;
    char xformat[64];
    int type;
    int srid = -1;
    int dimension = 2;
    char dummy[32];
    char sql[1024];
    char *errMsg = NULL;
    int ret;
    char **results;
    int rows;
    int columns;
    int i;
    char tblname[256];
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "RecoverFDOGeometryColumn() error: argument 1 [table_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    table = (const char *) sqlite3_value_text (argv[0]);
    if (sqlite3_value_type (argv[1]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "RecoverFDOGeometryColumn() error: argument 2 [column_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    column = (const char *) sqlite3_value_text (argv[1]);
    if (sqlite3_value_type (argv[2]) != SQLITE_INTEGER)
      {
	  fprintf (stderr,
		   "RecoverFDOGeometryColumn() error: argument 3 [SRID] is not of the Integer type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    srid = sqlite3_value_int (argv[2]);
    if (sqlite3_value_type (argv[3]) != SQLITE_INTEGER)
      {
	  fprintf (stderr,
		   "RecoverFDOGeometryColumn() error: argument 4 [geometry_type] is not of the Integer type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    type = sqlite3_value_int (argv[3]);
    if (sqlite3_value_type (argv[4]) != SQLITE_INTEGER)
      {
	  fprintf (stderr,
		   "RecoverFDOGeometryColumn() error: argument 5 [dimension] is not of the Integer type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    dimension = sqlite3_value_int (argv[4]);
    if (sqlite3_value_type (argv[5]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "RecoverFDOGeometryColumn() error: argument 6 [geometry_format] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    format = (const char *) sqlite3_value_text (argv[5]);
    if (type ==
	GAIA_POINT
	|| type ==
	GAIA_LINESTRING
	|| type ==
	GAIA_POLYGON
	|| type ==
	GAIA_MULTIPOINT
	|| type ==
	GAIA_MULTILINESTRING
	|| type == GAIA_MULTIPOLYGON || type == GAIA_GEOMETRYCOLLECTION)
	;
    else
      {
	  fprintf (stderr,
		   "RecoverFDOGeometryColumn() error: argument 4 [geometry_type] has an illegal value\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    if (dimension < 2 || dimension > 4)
      {
	  fprintf (stderr,
		   "RecoverFDOGeometryColumn() error: argument 5 [dimension] current version only accepts dimension=2,3,4\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    if (strcasecmp (format, "WKT") == 0)
	strcpy (xformat, "WKT");
    else if (strcasecmp (format, "WKB") == 0)
	strcpy (xformat, "WKB");
    else if (strcasecmp (format, "FGF") == 0)
	strcpy (xformat, "FGF");
    else
      {
	  fprintf (stderr,
		   "RecoverFDOGeometryColumn() error: argument 6 [geometry_format] has to be one of: WKT,WKB,FGF\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
/* checking if the table exists */
    sprintf (sql,
	     "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '%s'",
	     table);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, &errMsg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "RecoverFDOGeometryColumn: \"%s\"\n", errMsg);
	  sqlite3_free (errMsg);
	  return;
      }
    *tblname = '\0';
    for (i = 1; i <= rows; i++)
      {
	  strcpy (tblname, results[(i * columns)]);
      }
    sqlite3_free_table (results);
    if (*tblname == '\0')
      {
	  fprintf (stderr,
		   "RecoverFDOGeometryColumn() error: table '%s' does not exists\n",
		   table);
	  sqlite3_result_int (context, 0);
	  return;
      }
    if (!recoverFDOGeomColumn
	(sqlite, (const unsigned char *) table, (const unsigned char *) column,
	 type, srid))
      {
	  fprintf (stderr, "RecoverFDOGeometryColumn(): validation failed\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    strcpy (sql,
	    "INSERT INTO geometry_columns (f_table_name, f_geometry_column, geometry_type, ");
    strcat (sql, "coord_dimension, srid, geometry_format) VALUES (");
    strcat (sql, "'");
    strcat (sql, (char *) tblname);
    strcat (sql, "', '");
    strcat (sql, (char *) column);
    strcat (sql, "', ");
    sprintf (dummy, "%d, %d, ", type, dimension);
    strcat (sql, dummy);
    if (srid <= 0)
	strcat (sql, "-1");
    else
      {
	  sprintf (dummy, "%d", srid);
	  strcat (sql, dummy);
      }
    strcat (sql, ", '");
    strcat (sql, xformat);
    strcat (sql, "')");
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    sqlite3_result_int (context, 1);
    return;
  error:
    fprintf (stderr, "RecoverFDOGeometryColumn() error: \"%s\"\n", errMsg);
    sqlite3_free (errMsg);
    sqlite3_result_int (context, 0);
    return;
}

static void
fnct_DiscardFDOGeometryColumn (sqlite3_context * context, int argc,
			       sqlite3_value ** argv)
{
/* SQL function:
/ DiscardFDOGeometryColumn(table, column)
/
/ removes TABLE.COLUMN from the Spatial MetaData
/ returns 1 on success
/ 0 on failure
*/
    const unsigned char *table;
    const unsigned char *column;
    char sql[1024];
    char *errMsg = NULL;
    int ret;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "DiscardFDOGeometryColumn() error: argument 1 [table_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    table = sqlite3_value_text (argv[0]);
    if (sqlite3_value_type (argv[1]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "DiscardFDOGeometryColumn() error: argument 2 [column_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    column = sqlite3_value_text (argv[1]);
    sprintf (sql,
	     "DELETE FROM geometry_columns WHERE f_table_name LIKE '%s' AND f_geometry_column LIKE '%s'",
	     (char *) table, (char *) column);
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    sqlite3_result_int (context, 1);
    return;
  error:
    fprintf (stderr, "DiscardFDOGeometryColumn() error: \"%s\"\n", errMsg);
    sqlite3_free (errMsg);
    sqlite3_result_int (context, 0);
    return;
}

static void
fnct_CreateSpatialIndex (sqlite3_context * context, int argc,
			 sqlite3_value ** argv)
{
/* SQL function:
/ CreateSpatialIndex(table, column )
/
/ creates a SpatialIndex based on Column and Table
/ returns 1 on success
/ 0 on failure
*/
    const unsigned char *table;
    const unsigned char *column;
    char sql[1024];
    char *errMsg = NULL;
    int ret;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "CreateSpatialIndex() error: argument 1 [table_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    table = sqlite3_value_text (argv[0]);
    if (sqlite3_value_type (argv[1]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "CreateSpatialIndex() error: argument 2 [column_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    column = sqlite3_value_text (argv[1]);
    strcpy (sql,
	    "UPDATE geometry_columns SET spatial_index_enabled = 1 WHERE f_table_name LIKE '");
    strcat (sql, (char *) table);
    strcat (sql, "' AND f_geometry_column LIKE '");
    strcat (sql, (char *) column);
    strcat (sql, "' AND spatial_index_enabled = 0");
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    if (sqlite3_changes (sqlite) == 0)
      {
	  fprintf (stderr,
		   "CreateSpatialIndex() error: either \"%s\".\"%s\" isn't a Geometry column or a SpatialIndex is already defined\n",
		   table, column);
	  sqlite3_result_int (context, 0);
	  return;
      }
    updateGeometryTriggers (sqlite, table, column);
    sqlite3_result_int (context, 1);
    return;
  error:
    fprintf (stderr, "CreateSpatialIndex() error: \"%s\"\n", errMsg);
    sqlite3_free (errMsg);
    sqlite3_result_int (context, 0);
    return;
}

static void
fnct_CreateMbrCache (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ CreateMbrCache(table, column )
/
/ creates an MBR Cache based on Column and Table
/ returns 1 on success
/ 0 on failure
*/
    const unsigned char *table;
    const unsigned char *column;
    char sql[1024];
    char *errMsg = NULL;
    int ret;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "CreateMbrCache() error: argument 1 [table_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    table = sqlite3_value_text (argv[0]);
    if (sqlite3_value_type (argv[1]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "CreateMbrCache() error: argument 2 [column_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    column = sqlite3_value_text (argv[1]);
    strcpy (sql,
	    "UPDATE geometry_columns SET spatial_index_enabled = 2 WHERE f_table_name LIKE '");
    strcat (sql, (char *) table);
    strcat (sql, "' AND f_geometry_column LIKE '");
    strcat (sql, (char *) column);
    strcat (sql, "' AND spatial_index_enabled = 0");
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    if (sqlite3_changes (sqlite) == 0)
      {
	  fprintf (stderr,
		   "CreateMbrCache() error: either \"%s\".\"%s\" isn't a Geometry column or a SpatialIndex is already defined\n",
		   table, column);
	  sqlite3_result_int (context, 0);
	  return;
      }
    updateGeometryTriggers (sqlite, table, column);
    sqlite3_result_int (context, 1);
    return;
  error:
    fprintf (stderr, "CreateMbrCache() error: \"%s\"\n", errMsg);
    sqlite3_free (errMsg);
    sqlite3_result_int (context, 0);
    return;
}

static void
fnct_DisableSpatialIndex (sqlite3_context * context, int argc,
			  sqlite3_value ** argv)
{
/* SQL function:
/ DisableSpatialIndex(table, column )
/
/ disables a SpatialIndex based on Column and Table
/ returns 1 on success
/ 0 on failure
*/
    const unsigned char *table;
    const unsigned char *column;
    char sql[1024];
    char *errMsg = NULL;
    int ret;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "DisableSpatialIndex() error: argument 1 [table_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    table = sqlite3_value_text (argv[0]);
    if (sqlite3_value_type (argv[1]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "DisableSpatialIndex() error: argument 2 [column_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    column = sqlite3_value_text (argv[1]);
    strcpy (sql,
	    "UPDATE geometry_columns SET spatial_index_enabled = 0 WHERE f_table_name LIKE '");
    strcat (sql, (char *) table);
    strcat (sql, "' AND f_geometry_column LIKE '");
    strcat (sql, (char *) column);
    strcat (sql, "' AND spatial_index_enabled <> 0");
    ret = sqlite3_exec (sqlite, sql, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK)
	goto error;
    if (sqlite3_changes (sqlite) == 0)
      {
	  fprintf (stderr,
		   "DisableSpatialIndex() error: either \"%s\".\"%s\" isn't a Geometry column or no SpatialIndex is defined\n",
		   table, column);
	  sqlite3_result_int (context, 0);
	  return;
      }
    updateGeometryTriggers (sqlite, table, column);
    sqlite3_result_int (context, 1);
    return;
  error:
    fprintf (stderr, "DisableSpatialIndex() error: \"%s\"\n", errMsg);
    sqlite3_free (errMsg);
    sqlite3_result_int (context, 0);
    return;
}

static void
fnct_RebuildGeometryTriggers (sqlite3_context * context, int argc,
			      sqlite3_value ** argv)
{
/* SQL function:
/ RebuildGeometryTriggers(table, column )
/
/ rebuilds Geometry Triggers (constraints)  based on Column and Table
/ returns 1 on success
/ 0 on failure
*/
    const unsigned char *table;
    const unsigned char *column;
    char sql[1024];
    char *errMsg = NULL;
    int ret;
    char **results;
    int rows;
    int columns;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "RebuildGeometryTriggers() error: argument 1 [table_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    table = sqlite3_value_text (argv[0]);
    if (sqlite3_value_type (argv[1]) != SQLITE_TEXT)
      {
	  fprintf (stderr,
		   "RebuildGeometryTriggers() error: argument 2 [column_name] is not of the String type\n");
	  sqlite3_result_int (context, 0);
	  return;
      }
    column = sqlite3_value_text (argv[1]);
    strcpy (sql,
	    "SELECT f_table_name FROM geometry_columns WHERE f_table_name LIKE '");
    strcat (sql, (char *) table);
    strcat (sql, "' AND f_geometry_column LIKE '");
    strcat (sql, (char *) column);
    strcat (sql, "'");
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, NULL);
    if (ret != SQLITE_OK)
	goto error;
    sqlite3_free_table (results);
    if (rows <= 0)
      {
	  fprintf (stderr,
		   "RebuildGeometryTriggers() error: \"%s\".\"%s\" isn't a Geometry column\n",
		   table, column);
	  sqlite3_result_int (context, 0);
	  return;
      }
    updateGeometryTriggers (sqlite, table, column);
    sqlite3_result_int (context, 1);
    return;
  error:
    fprintf (stderr, "RebuildGeometryTriggers() error: \"%s\"\n", errMsg);
    sqlite3_free (errMsg);
    sqlite3_result_int (context, 0);
    return;
}

static gaiaPointPtr
simplePoint (gaiaGeomCollPtr geo)
{
/* helper function
/ if this GEOMETRY contains only one POINT, and no other elementary geometry
/ the POINT address will be returned
/ otherwise NULL will be returned
*/
    int cnt = 0;
    gaiaPointPtr point;
    gaiaPointPtr this_point = NULL;
    if (!geo)
	return NULL;
    if (geo->FirstLinestring || geo->FirstPolygon)
	return NULL;
    point = geo->FirstPoint;
    while (point)
      {
	  /* counting how many POINTs are there */
	  cnt++;
	  this_point = point;
	  point = point->Next;
      }
    if (cnt == 1 && this_point)
	return this_point;
    return NULL;
}

static gaiaLinestringPtr
simpleLinestring (gaiaGeomCollPtr geo)
{
/* helper function
/ if this GEOMETRY contains only one LINESTRING, and no other elementary geometry
/ the LINESTRING address will be returned
/ otherwise NULL will be returned
*/
    int cnt = 0;
    gaiaLinestringPtr line;
    gaiaLinestringPtr this_line = NULL;
    if (!geo)
	return NULL;
    if (geo->FirstPoint || geo->FirstPolygon)
	return NULL;
    line = geo->FirstLinestring;
    while (line)
      {
	  /* counting how many LINESTRINGs are there */
	  cnt++;
	  this_line = line;
	  line = line->Next;
      }
    if (cnt == 1 && this_line)
	return this_line;
    return NULL;
}

static gaiaPolygonPtr
simplePolygon (gaiaGeomCollPtr geo)
{
/* helper function
/ if this GEOMETRY contains only one POLYGON, and no other elementary geometry
/ the POLYGON address will be returned
/ otherwise NULL will be returned
*/
    int cnt = 0;
    gaiaPolygonPtr polyg;
    gaiaPolygonPtr this_polyg = NULL;
    if (!geo)
	return NULL;
    if (geo->FirstPoint || geo->FirstLinestring)
	return NULL;
    polyg = geo->FirstPolygon;
    while (polyg)
      {
	  /* counting how many POLYGONs are there */
	  cnt++;
	  this_polyg = polyg;
	  polyg = polyg->Next;
      }
    if (cnt == 1 && this_polyg)
	return this_polyg;
    return NULL;
}

static void
fnct_AsText (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ AsText(BLOB encoded geometry)
/
/ returns the corresponding WKT encoded value
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int len;
    char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  gaiaOutWkt (geo, &p_result);
	  if (!p_result)
	      sqlite3_result_null (context);
	  else
	    {
		len = strlen (p_result);
		sqlite3_result_text (context, p_result, len, free);
	    }
      }
    gaiaFreeGeomColl (geo);
}

/*
/
/ AsSvg(geometry,[relative], [precision]) implementation
/
////////////////////////////////////////////////////////////
/
/ Author: Klaus Foerster klaus.foerster@svg.cc
/ version 0.9. 2008 September 21
 /
 */

static void
fnct_AsSvg (sqlite3_context * context, int argc, sqlite3_value ** argv,
	    int relative, int precision)
{
/* SQL function:
   AsSvg(BLOB encoded geometry, [int relative], [int precision])
   returns the corresponding SVG encoded value or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int len;
    char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  /* make sure relative is 0 or 1 */
	  if (relative > 0)
	      relative = 1;
	  else
	      relative = 0;
	  /* make sure precision is between 0 and 15 - default to 6 if absent */
	  if (precision > GAIA_SVG_DEFAULT_MAX_PRECISION)
	      precision = GAIA_SVG_DEFAULT_MAX_PRECISION;
	  if (precision < 0)
	      precision = 0;
	  /* produce SVG-notation - actual work is done in gaiageo/gg_wkt.c */
	  gaiaOutSvg (geo, &p_result, relative, precision);
	  if (!p_result)
	      sqlite3_result_null (context);
	  else
	    {
		len = strlen (p_result);
		sqlite3_result_text (context, p_result, len, free);
	    }
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_AsSvg1 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* called without additional arguments */
    fnct_AsSvg (context, argc, argv, GAIA_SVG_DEFAULT_RELATIVE,
		GAIA_SVG_DEFAULT_PRECISION);
}

static void
fnct_AsSvg2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* called with relative-switch */
    if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
	fnct_AsSvg (context, argc, argv, sqlite3_value_int (argv[1]),
		    GAIA_SVG_DEFAULT_PRECISION);
    else
	sqlite3_result_null (context);
}

static void
fnct_AsSvg3 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* called with relative-switch and precision-argument */
    if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER
	&& sqlite3_value_type (argv[2]) == SQLITE_INTEGER)
	fnct_AsSvg (context, argc, argv, sqlite3_value_int (argv[1]),
		    sqlite3_value_int (argv[2]));
    else
	sqlite3_result_null (context);
}

/* END of Klaus Foerster AsSvg() implementation */

static void
fnct_AsBinary (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ AsBinary(BLOB encoded geometry)
/
/ returns the corresponding WKB encoded value
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int len;
    unsigned char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  gaiaToWkb (geo, &p_result, &len);
	  if (!p_result)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_blob (context, p_result, len, free);
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_AsFGF (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ AsFGF(BLOB encoded geometry)
/
/ returns the corresponding FGF encoded value
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int len;
    unsigned char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    int coord_dims;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    if (sqlite3_value_type (argv[1]) != SQLITE_INTEGER)
      {
	  fprintf (stderr,
		   "AsFGF() error: argument 2 [geom_coords] is not of the Integer type\n");
	  sqlite3_result_null (context);
	  return;
      }
    coord_dims = sqlite3_value_int (argv[1]);
    if (coord_dims
	== 0 || coord_dims == 1 || coord_dims == 2 || coord_dims == 3)
	;
    else
      {
	  fprintf (stderr,
		   "AsFGF() error: argument 2 [geom_coords] out of range [0,1,2,3]\n");
	  sqlite3_result_null (context);
	  return;
      }
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  gaiaToFgf (geo, &p_result, &len, coord_dims);
	  if (!p_result)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_blob (context, p_result, len, free);
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_MakePoint1 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ MakePoint(double X, double Y)
/
/ builds a POINT 
/ or NULL if any error is encountered
*/
    int len;
    int int_value;
    unsigned char *p_result = NULL;
    double x;
    double y;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
	x = sqlite3_value_double (argv[0]);
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) == SQLITE_FLOAT)
	y = sqlite3_value_double (argv[1]);
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[1]);
	  y = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    gaiaMakePoint (x, y, -1, &p_result, &len);
    if (!p_result)
	sqlite3_result_null (context);
    else
	sqlite3_result_blob (context, p_result, len, free);
}

static void
fnct_MakePoint2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ MakePoint(double X, double Y, int SRID)
/
/ builds a POINT 
/ or NULL if any error is encountered
*/
    int len;
    int int_value;
    unsigned char *p_result = NULL;
    double x;
    double y;
    int srid;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
	x = sqlite3_value_double (argv[0]);
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) == SQLITE_FLOAT)
	y = sqlite3_value_double (argv[1]);
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[1]);
	  y = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[2]) == SQLITE_INTEGER)
	srid = sqlite3_value_int (argv[2]);
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    gaiaMakePoint (x, y, srid, &p_result, &len);
    if (!p_result)
	sqlite3_result_null (context);
    else
	sqlite3_result_blob (context, p_result, len, free);
}

static void
geom_from_text1 (sqlite3_context * context, int argc, sqlite3_value ** argv,
		 short type)
{
/* SQL function:
/ GeomFromText(WKT encoded geometry)
/
/ returns the current geometry by parsing WKT encoded string 
/ or NULL if any error is encountered
/
/ if *type* is a negative value can accept any GEOMETRY CLASS
/ otherwise only requests conforming with required CLASS are valid
*/
    int len;
    unsigned char *p_result = NULL;
    const unsigned char *text;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_TEXT)
      {
	  sqlite3_result_null (context);
	  return;
      }
    text = sqlite3_value_text (argv[0]);
    geo = gaiaParseWkt (text, type);
    if (geo == NULL)
      {
	  sqlite3_result_null (context);
	  return;
      }
    gaiaToSpatiaLiteBlobWkb (geo, &p_result, &len);
    gaiaFreeGeomColl (geo);
    sqlite3_result_blob (context, p_result, len, free);
}

static void
geom_from_text2 (sqlite3_context * context, int argc, sqlite3_value ** argv,
		 short type)
{
/* SQL function:
/ GeomFromText(WKT encoded geometry, SRID)
/
/ returns the current geometry by parsing WKT encoded string 
/ or NULL if any error is encountered
/
/ if *type* is a negative value can accept any GEOMETRY CLASS
/ otherwise only requests conforming with required CLASS are valid
*/
    int len;
    unsigned char *p_result = NULL;
    const unsigned char *text;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_TEXT)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_INTEGER)
      {
	  sqlite3_result_null (context);
	  return;
      }
    text = sqlite3_value_text (argv[0]);
    geo = gaiaParseWkt (text, type);
    if (geo == NULL)
      {
	  sqlite3_result_null (context);
	  return;
      }
    geo->Srid = sqlite3_value_int (argv[1]);
    gaiaToSpatiaLiteBlobWkb (geo, &p_result, &len);
    gaiaFreeGeomColl (geo);
    sqlite3_result_blob (context, p_result, len, free);
}

static int
check_wkb (const unsigned char *wkb, int size, short type)
{
/* checking type coherency for WKB encoded GEOMETRY */
    int little_endian;
    int wkb_type;
    int endian_arch = gaiaEndianArch ();
    if (size < 5)
	return 0;		/* too short to be a WKB */
    if (*(wkb + 0) == 0x01)
	little_endian = GAIA_LITTLE_ENDIAN;
    else if (*(wkb + 0) == 0x00)
	little_endian = GAIA_BIG_ENDIAN;
    else
	return 0;		/* illegal byte ordering; neither BIG-ENDIAN nor LITTLE-ENDIAN */
    wkb_type = gaiaImport32 (wkb + 1, little_endian, endian_arch);
    if (wkb_type ==
	GAIA_POINT
	|| wkb_type
	==
	GAIA_LINESTRING
	|| wkb_type
	==
	GAIA_POLYGON
	|| wkb_type
	==
	GAIA_MULTIPOINT
	|| wkb_type
	==
	GAIA_MULTILINESTRING
	|| wkb_type == GAIA_MULTIPOLYGON || wkb_type == GAIA_GEOMETRYCOLLECTION)
	;
    else
	return 0;		/* illegal GEOMETRY CLASS */
    if (type < 0)
	;			/* no restrinction about GEOMETRY CLASS TYPE */
    else
      {
	  if (wkb_type != type)
	      return 0;		/* invalid CLASS TYPE for request */
      }
    return 1;
}

static void
geom_from_wkb1 (sqlite3_context * context, int argc, sqlite3_value ** argv,
		short type)
{
/* SQL function:
/ GeomFromWKB(WKB encoded geometry)
/
/ returns the current geometry by parsing a WKB encoded blob 
/ or NULL if any error is encountered
/
/ if *type* is a negative value can accept any GEOMETRY CLASS
/ otherwise only requests conforming with required CLASS are valid
*/
    int len;
    int n_bytes;
    unsigned char *p_result = NULL;
    const unsigned char *wkb;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    wkb = sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    if (!check_wkb (wkb, n_bytes, type))
	return;
    geo = gaiaFromWkb (wkb, n_bytes);
    if (geo == NULL)
      {
	  sqlite3_result_null (context);
	  return;
      }
    gaiaToSpatiaLiteBlobWkb (geo, &p_result, &len);
    gaiaFreeGeomColl (geo);
    sqlite3_result_blob (context, p_result, len, free);
}

static void
geom_from_wkb2 (sqlite3_context * context, int argc, sqlite3_value ** argv,
		short type)
{
/* SQL function:
/ GeomFromWKB(WKB encoded geometry, SRID)
/
/ returns the current geometry by parsing a WKB encoded blob
/ or NULL if any error is encountered
/
/ if *type* is a negative value can accept any GEOMETRY CLASS
/ otherwise only requests conforming with required CLASS are valid
*/
    int len;
    int n_bytes;
    unsigned char *p_result = NULL;
    const unsigned char *wkb;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_INTEGER)
      {
	  sqlite3_result_null (context);
	  return;
      }
    wkb = sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    if (!check_wkb (wkb, n_bytes, type))
	return;
    geo = gaiaFromWkb (wkb, n_bytes);
    if (geo == NULL)
      {
	  sqlite3_result_null (context);
	  return;
      }
    geo->Srid = sqlite3_value_int (argv[1]);
    gaiaToSpatiaLiteBlobWkb (geo, &p_result, &len);
    gaiaFreeGeomColl (geo);
    sqlite3_result_blob (context, p_result, len, free);
}

static void
fnct_GeometryFromFGF1 (sqlite3_context * context, int argc,
		       sqlite3_value ** argv)
{
/* SQL function:
/ GeomFromFGF(FGF encoded geometry)
/
/ returns the current geometry by parsing an FGF encoded blob 
/ or NULL if any error is encountered
/
/ if *type* is a negative value can accept any GEOMETRY CLASS
/ otherwise only requests conforming with required CLASS are valid
*/
    int len;
    int n_bytes;
    unsigned char *p_result = NULL;
    const unsigned char *fgf;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    fgf = sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromFgf (fgf, n_bytes);
    if (geo == NULL)
      {
	  sqlite3_result_null (context);
	  return;
      }
    gaiaToSpatiaLiteBlobWkb (geo, &p_result, &len);
    gaiaFreeGeomColl (geo);
    sqlite3_result_blob (context, p_result, len, free);
}

static void
fnct_GeometryFromFGF2 (sqlite3_context * context, int argc,
		       sqlite3_value ** argv)
{
/* SQL function:
/ GeomFromFGF(FGF encoded geometry, SRID)
/
/ returns the current geometry by parsing an FGF encoded string 
/ or NULL if any error is encountered
/
/ if *type* is a negative value can accept any GEOMETRY CLASS
/ otherwise only requests conforming with required CLASS are valid
*/
    int len;
    int n_bytes;
    unsigned char *p_result = NULL;
    const unsigned char *fgf;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_INTEGER)
      {
	  sqlite3_result_null (context);
	  return;
      }
    fgf = sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromFgf (fgf, n_bytes);
    if (geo == NULL)
      {
	  sqlite3_result_null (context);
	  return;
      }
    geo->Srid = sqlite3_value_int (argv[1]);
    gaiaToSpatiaLiteBlobWkb (geo, &p_result, &len);
    gaiaFreeGeomColl (geo);
    sqlite3_result_blob (context, p_result, len, free);
}

/*
/ the following functions simply readdress the request to geom_from_text?()
/ setting the appropriate GEOMETRY CLASS TYPE
*/

static void
fnct_GeomFromText1 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_text1 (context, argc, argv, (short) -1);
}

static void
fnct_GeomFromText2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_text2 (context, argc, argv, (short) -1);
}

static void
fnct_GeomCollFromText1 (sqlite3_context * context, int argc,
			sqlite3_value ** argv)
{
    geom_from_text1 (context, argc, argv, (short) GAIA_GEOMETRYCOLLECTION);
}

static void
fnct_GeomCollFromText2 (sqlite3_context * context, int argc,
			sqlite3_value ** argv)
{
    geom_from_text2 (context, argc, argv, (short) GAIA_GEOMETRYCOLLECTION);
}

static void
fnct_LineFromText1 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_text1 (context, argc, argv, (short) GAIA_LINESTRING);
}

static void
fnct_LineFromText2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_text2 (context, argc, argv, (short) GAIA_LINESTRING);
}

static void
fnct_PointFromText1 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_text1 (context, argc, argv, (short) GAIA_POINT);
}

static void
fnct_PointFromText2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_text2 (context, argc, argv, (short) GAIA_POINT);
}

static void
fnct_PolyFromText1 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_text1 (context, argc, argv, (short) GAIA_POLYGON);
}

static void
fnct_PolyFromText2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_text2 (context, argc, argv, (short) GAIA_POLYGON);
}

static void
fnct_MLineFromText1 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_text1 (context, argc, argv, (short) GAIA_MULTILINESTRING);
}

static void
fnct_MLineFromText2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_text2 (context, argc, argv, (short) GAIA_MULTILINESTRING);
}

static void
fnct_MPointFromText1 (sqlite3_context * context, int argc,
		      sqlite3_value ** argv)
{
    geom_from_text1 (context, argc, argv, (short) GAIA_MULTIPOINT);
}

static void
fnct_MPointFromText2 (sqlite3_context * context, int argc,
		      sqlite3_value ** argv)
{
    geom_from_text2 (context, argc, argv, (short) GAIA_MULTIPOINT);
}

static void
fnct_MPolyFromText1 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_text1 (context, argc, argv, (short) GAIA_MULTIPOLYGON);
}

static void
fnct_MPolyFromText2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_text2 (context, argc, argv, (short) GAIA_MULTIPOLYGON);
}

/*
/ the following functions simply readdress the request to geom_from_wkb?()
/ setting the appropriate GEOMETRY CLASS TYPE
*/

static void
fnct_GeomFromWkb1 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_wkb1 (context, argc, argv, (short) -1);
}

static void
fnct_GeomFromWkb2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_wkb2 (context, argc, argv, (short) -1);
}

static void
fnct_GeomCollFromWkb1 (sqlite3_context * context, int argc,
		       sqlite3_value ** argv)
{
    geom_from_wkb1 (context, argc, argv, (short) GAIA_GEOMETRYCOLLECTION);
}

static void
fnct_GeomCollFromWkb2 (sqlite3_context * context, int argc,
		       sqlite3_value ** argv)
{
    geom_from_wkb2 (context, argc, argv, (short) GAIA_GEOMETRYCOLLECTION);
}

static void
fnct_LineFromWkb1 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_wkb1 (context, argc, argv, (short) GAIA_LINESTRING);
}

static void
fnct_LineFromWkb2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_wkb2 (context, argc, argv, (short) GAIA_LINESTRING);
}

static void
fnct_PointFromWkb1 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_wkb1 (context, argc, argv, (short) GAIA_POINT);
}

static void
fnct_PointFromWkb2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_wkb2 (context, argc, argv, (short) GAIA_POINT);
}

static void
fnct_PolyFromWkb1 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_wkb1 (context, argc, argv, (short) GAIA_POLYGON);
}

static void
fnct_PolyFromWkb2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_wkb2 (context, argc, argv, (short) GAIA_POLYGON);
}

static void
fnct_MLineFromWkb1 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_wkb1 (context, argc, argv, (short) GAIA_MULTILINESTRING);
}

static void
fnct_MLineFromWkb2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_wkb2 (context, argc, argv, (short) GAIA_MULTILINESTRING);
}

static void
fnct_MPointFromWkb1 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_wkb1 (context, argc, argv, (short) GAIA_MULTIPOINT);
}

static void
fnct_MPointFromWkb2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_wkb2 (context, argc, argv, (short) GAIA_MULTIPOINT);
}

static void
fnct_MPolyFromWkb1 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_wkb1 (context, argc, argv, (short) GAIA_MULTIPOLYGON);
}

static void
fnct_MPolyFromWkb2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    geom_from_wkb2 (context, argc, argv, (short) GAIA_MULTIPOLYGON);
}

static void
fnct_Dimension (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Dimension(BLOB encoded geometry)
/
/ returns:
/ 0 if geometry is a POINT or MULTIPOINT
/ 1 if geometry is a LINESTRING or MULTILINESTRING
/ 2 if geometry is a POLYGON or MULTIPOLYGON
/ 0, 1, 2, for GEOMETRYCOLLECTIONS according to geometries contained inside
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int dim;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  dim = gaiaDimension (geo);
	  sqlite3_result_int (context, dim);
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_GeometryType (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ GeometryType(BLOB encoded geometry)
/
/ returns the class for current geometry:
/ 'POINT' or 'MULTIPOINT'
/ 'LINESTRING' or 'MULTILINESTRING'
/ 'POLYGON' or 'MULTIPOLYGON'
/ 'GEOMETRYCOLLECTION' 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int len;
    int type;
    char *p_type = NULL;
    char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  type = gaiaGeometryType (geo);
	  switch (type)
	    {
	    case GAIA_POINT:
		p_type = "POINT";
		break;
	    case GAIA_MULTIPOINT:
		p_type = "MULTIPOINT";
		break;
	    case GAIA_LINESTRING:
		p_type = "LINESTRING";
		break;
	    case GAIA_MULTILINESTRING:
		p_type = "MULTILINESTRING";
		break;
	    case GAIA_POLYGON:
		p_type = "POLYGON";
		break;
	    case GAIA_MULTIPOLYGON:
		p_type = "MULTIPOLYGON";
		break;
	    case GAIA_GEOMETRYCOLLECTION:
		p_type = "GEOMETRYCOLLECTION";
		break;
	    };
	  if (p_type)
	    {
		len = strlen (p_type);
		p_result = malloc (len + 1);
		strcpy (p_result, p_type);
	    }
	  if (!p_result)
	      sqlite3_result_null (context);
	  else
	    {
		len = strlen (p_result);
		sqlite3_result_text (context, p_result, len, free);
	    }
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_GeometryAliasType (sqlite3_context * context, int argc,
			sqlite3_value ** argv)
{
/* SQL function:
/ GeometryAliasType(BLOB encoded geometry)
/
/ returns the alias-class for current geometry:
/ 'MULTIPOINT'
/ 'MULTILINESTRING'
/ 'MULTIPOLYGON'
/ 'GEOMETRYCOLLECTION' 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int len;
    int type;
    char *p_type = NULL;
    char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  type = gaiaGeometryAliasType (geo);
	  switch (type)
	    {
	    case GAIA_POINT:
		p_type = "POINT";
		break;
	    case GAIA_MULTIPOINT:
		p_type = "MULTIPOINT";
		break;
	    case GAIA_LINESTRING:
		p_type = "LINESTRING";
		break;
	    case GAIA_MULTILINESTRING:
		p_type = "MULTILINESTRING";
		break;
	    case GAIA_POLYGON:
		p_type = "POLYGON";
		break;
	    case GAIA_MULTIPOLYGON:
		p_type = "MULTIPOLYGON";
		break;
	    case GAIA_GEOMETRYCOLLECTION:
		p_type = "GEOMETRYCOLLECTION";
		break;
	    };
	  if (p_type)
	    {
		len = strlen (p_type);
		p_result = malloc (len + 1);
		strcpy (p_result, p_type);
	    }
	  if (!p_result)
	      sqlite3_result_null (context);
	  else
	    {
		len = strlen (p_result);
		sqlite3_result_text (context, p_result, len, free);
	    }
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_SRID (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Srid(BLOB encoded geometry)
/
/ returns the SRID
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
	sqlite3_result_int (context, geo->Srid);
    gaiaFreeGeomColl (geo);
}

static void
fnct_SetSRID (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ SetSrid(BLOBencoded geometry, srid)
/
/ returns a new geometry that is the original one received, but with the new SRID [no coordinates translation is applied]
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo = NULL;
    int srid;
    unsigned char *p_result = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
	srid = sqlite3_value_int (argv[1]);
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  geo->Srid = srid;
	  gaiaToSpatiaLiteBlobWkb (geo, &p_result, &n_bytes);
	  sqlite3_result_blob (context, p_result, n_bytes, free);
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_IsEmpty (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ IsEmpty(BLOB encoded geometry)
/
/ returns:
/ 1 if this geometry contains no elementary geometries
/ 0 otherwise
/ or -1 if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_int (context, 1);
    else
	sqlite3_result_int (context, gaiaIsEmpty (geo));
    gaiaFreeGeomColl (geo);
}

static void
fnct_Envelope (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Envelope(BLOB encoded geometry)
/
/ returns the MBR for current geometry 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int len;
    unsigned char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    gaiaGeomCollPtr bbox;
    gaiaPolygonPtr polyg;
    gaiaRingPtr rect;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  if (gaiaIsEmpty (geo))
	      sqlite3_result_null (context);
	  else
	    {
		gaiaMbrGeometry (geo);
		bbox = gaiaAllocGeomColl ();
		polyg = gaiaAddPolygonToGeomColl (bbox, 5, 0);
		rect = polyg->Exterior;
		gaiaSetPoint (rect->Coords, 0, geo->MinX, geo->MinY);	/* vertex # 1 */
		gaiaSetPoint (rect->Coords, 1, geo->MaxX, geo->MinY);	/* vertex # 2 */
		gaiaSetPoint (rect->Coords, 2, geo->MaxX, geo->MaxY);	/* vertex # 3 */
		gaiaSetPoint (rect->Coords, 3, geo->MinX, geo->MaxY);	/* vertex # 4 */
		gaiaSetPoint (rect->Coords, 4, geo->MinX, geo->MinY);	/* vertex # 5 [same as vertex # 1 to close the polygon] */
		gaiaToSpatiaLiteBlobWkb (bbox, &p_result, &len);
		gaiaFreeGeomColl (bbox);
		sqlite3_result_blob (context, p_result, len, free);
	    }
      }
    gaiaFreeGeomColl (geo);
}

static void
build_filter_mbr (sqlite3_context * context, int argc, sqlite3_value ** argv,
		  int mode)
{
/* SQL functions:
/ BuildMbrFilter(double X1, double Y1, double X2, double Y2)
/ FilterMBRWithin(double X1, double Y1, double X2, double Y2)
/ FilterMBRContain(double X1, double Y1, double X2, double Y2)
/ FilterMBRIntersects(double X1, double Y1, double X2, double Y2)
/
/ builds a generic filter for MBR from two points (identifying a rectangle's diagonal) 
/ or NULL if any error is encountered
*/
    int len;
    unsigned char *p_result = NULL;
    double x1;
    double y1;
    double x2;
    double y2;
    int int_value;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
	x1 = sqlite3_value_double (argv[0]);
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x1 = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) == SQLITE_FLOAT)
	y1 = sqlite3_value_double (argv[1]);
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[1]);
	  y1 = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[2]) == SQLITE_FLOAT)
	x2 = sqlite3_value_double (argv[2]);
    else if (sqlite3_value_type (argv[2]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[2]);
	  x2 = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[3]) == SQLITE_FLOAT)
	y2 = sqlite3_value_double (argv[3]);
    else if (sqlite3_value_type (argv[3]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[3]);
	  y2 = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    gaiaBuildFilterMbr (x1, y1, x2, y2, mode, &p_result, &len);
    if (!p_result)
	sqlite3_result_null (context);
    else
	sqlite3_result_blob (context, p_result, len, free);
}

/*
/ the following functions simply readdress the request to build_filter_mbr()
/ setting the appropriate MODe
*/

static void
fnct_BuildMbrFilter (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    build_filter_mbr (context, argc, argv, GAIA_FILTER_MBR_DECLARE);
}

static void
fnct_FilterMbrWithin (sqlite3_context * context, int argc,
		      sqlite3_value ** argv)
{
    build_filter_mbr (context, argc, argv, GAIA_FILTER_MBR_WITHIN);
}

static void
fnct_FilterMbrContains (sqlite3_context * context, int argc,
			sqlite3_value ** argv)
{
    build_filter_mbr (context, argc, argv, GAIA_FILTER_MBR_CONTAINS);
}

static void
fnct_FilterMbrIntersects (sqlite3_context * context, int argc,
			  sqlite3_value ** argv)
{
    build_filter_mbr (context, argc, argv, GAIA_FILTER_MBR_INTERSECTS);
}

static void
fnct_BuildMbr1 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ BuildMBR(double X1, double Y1, double X2, double Y2)
/
/ builds an MBR from two points (identifying a rectangle's diagonal) 
/ or NULL if any error is encountered
*/
    int len;
    unsigned char *p_result = NULL;
    double x1;
    double y1;
    double x2;
    double y2;
    int int_value;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
	x1 = sqlite3_value_double (argv[0]);
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x1 = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) == SQLITE_FLOAT)
	y1 = sqlite3_value_double (argv[1]);
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[1]);
	  y1 = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[2]) == SQLITE_FLOAT)
	x2 = sqlite3_value_double (argv[2]);
    else if (sqlite3_value_type (argv[2]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[2]);
	  x2 = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[3]) == SQLITE_FLOAT)
	y2 = sqlite3_value_double (argv[3]);
    else if (sqlite3_value_type (argv[3]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[3]);
	  y2 = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    gaiaBuildMbr (x1, y1, x2, y2, -1, &p_result, &len);
    if (!p_result)
	sqlite3_result_null (context);
    else
	sqlite3_result_blob (context, p_result, len, free);
}

static void
fnct_BuildMbr2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ BuildMBR(double X1, double Y1, double X2, double Y2, int SRID)
/
/ builds an MBR from two points (identifying a rectangle's diagonal) 
/ or NULL if any error is encountered
*/
    int len;
    unsigned char *p_result = NULL;
    double x1;
    double y1;
    double x2;
    double y2;
    int int_value;
    int srid;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
	x1 = sqlite3_value_double (argv[0]);
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x1 = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) == SQLITE_FLOAT)
	y1 = sqlite3_value_double (argv[1]);
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[1]);
	  y1 = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[2]) == SQLITE_FLOAT)
	x2 = sqlite3_value_double (argv[2]);
    else if (sqlite3_value_type (argv[2]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[2]);
	  x2 = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[3]) == SQLITE_FLOAT)
	y2 = sqlite3_value_double (argv[3]);
    else if (sqlite3_value_type (argv[3]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[3]);
	  y2 = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[4]) == SQLITE_INTEGER)
	srid = sqlite3_value_int (argv[4]);
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    gaiaBuildMbr (x1, y1, x2, y2, srid, &p_result, &len);
    if (!p_result)
	sqlite3_result_null (context);
    else
	sqlite3_result_blob (context, p_result, len, free);
}

static void
fnct_BuildCircleMbr1 (sqlite3_context * context, int argc,
		      sqlite3_value ** argv)
{
/* SQL function:
/ BuildCircleMBR(double X, double Y, double radius)
/
/ builds an MBR from two points (identifying a rectangle's diagonal) 
/ or NULL if any error is encountered
*/
    int len;
    unsigned char *p_result = NULL;
    double x;
    double y;
    double radius;
    int int_value;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
	x = sqlite3_value_double (argv[0]);
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) == SQLITE_FLOAT)
	y = sqlite3_value_double (argv[1]);
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[1]);
	  y = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[2]) == SQLITE_FLOAT)
	radius = sqlite3_value_double (argv[2]);
    else if (sqlite3_value_type (argv[2]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[2]);
	  radius = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    gaiaBuildCircleMbr (x, y, radius, -1, &p_result, &len);
    if (!p_result)
	sqlite3_result_null (context);
    else
	sqlite3_result_blob (context, p_result, len, free);
}

static void
fnct_BuildCircleMbr2 (sqlite3_context * context, int argc,
		      sqlite3_value ** argv)
{
/* SQL function:
/ BuildCircleMBR(double X, double Y, double radius, int SRID)
/
/ builds an MBR from two points (identifying a rectangle's diagonal) 
/ or NULL if any error is encountered
*/
    int len;
    unsigned char *p_result = NULL;
    double x;
    double y;
    double radius;
    int int_value;
    int srid;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
	x = sqlite3_value_double (argv[0]);
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) == SQLITE_FLOAT)
	y = sqlite3_value_double (argv[1]);
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[1]);
	  y = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[2]) == SQLITE_FLOAT)
	radius = sqlite3_value_double (argv[2]);
    else if (sqlite3_value_type (argv[2]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[2]);
	  radius = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[3]) == SQLITE_INTEGER)
	srid = sqlite3_value_int (argv[3]);
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    gaiaBuildCircleMbr (x, y, radius, srid, &p_result, &len);
    if (!p_result)
	sqlite3_result_null (context);
    else
	sqlite3_result_blob (context, p_result, len, free);
}

static void
fnct_MbrMinX (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ MbrMinX(BLOB encoded GEMETRY)
/
/ returns the MinX coordinate for current geometry's MBR 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    double coord;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    if (!gaiaGetMbrMinX (p_blob, n_bytes, &coord))
	sqlite3_result_null (context);
    else
	sqlite3_result_double (context, coord);
}

static void
fnct_MbrMaxX (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ MbrMaxX(BLOB encoded GEMETRY)
/
/ returns the MaxX coordinate for current geometry's MBR 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    double coord;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    if (!gaiaGetMbrMaxX (p_blob, n_bytes, &coord))
	sqlite3_result_null (context);
    else
	sqlite3_result_double (context, coord);
}

static void
fnct_MbrMinY (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ MbrMinY(BLOB encoded GEMETRY)
/
/ returns the MinY coordinate for current geometry's MBR 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    double coord;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    if (!gaiaGetMbrMinY (p_blob, n_bytes, &coord))
	sqlite3_result_null (context);
    else
	sqlite3_result_double (context, coord);
}

static void
fnct_MbrMaxY (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ MbrMaxY(BLOB encoded GEMETRY)
/
/ returns the MaxY coordinate for current geometry's MBR 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    double coord;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    if (!gaiaGetMbrMaxY (p_blob, n_bytes, &coord))
	sqlite3_result_null (context);
    else
	sqlite3_result_double (context, coord);
}

static void
fnct_BuildRings (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ BuildRings(BLOBencoded geometry)
/
/ returns a new geometry [set of closed RINGs] obtained by 
/ reassembling a set of  LINESTRINGs
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int len;
    unsigned char *p_result = NULL;
    gaiaGeomCollPtr line_geom = NULL;
    gaiaGeomCollPtr result = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    line_geom = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (line_geom == NULL)
      {
	  sqlite3_result_null (context);
	  return;
      }
/* one or more LINESTINGs are expected */
    if (line_geom->FirstPoint || line_geom->FirstPolygon)
	goto invalid;
    if (!line_geom->FirstLinestring)
	goto invalid;
    result = gaiaBuildRings (line_geom);
    if (!result)
	goto invalid;
    gaiaFreeGeomColl (line_geom);
    gaiaToSpatiaLiteBlobWkb (result, &p_result, &len);
    gaiaFreeGeomColl (result);
    sqlite3_result_blob (context, p_result, len, free);
    return;
  invalid:
    gaiaFreeGeomColl (line_geom);
    sqlite3_result_null (context);
}

static void
fnct_X (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ X(BLOB encoded POINT)
/
/ returns the X coordinate for current POINT geometry 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo = NULL;
    gaiaPointPtr point;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  point = simplePoint (geo);
	  if (!point)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_double (context, point->X);
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_Y (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Y(BLOB encoded POINT)
/
/ returns the Y coordinate for current POINT geometry 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo = NULL;
    gaiaPointPtr point;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  point = simplePoint (geo);
	  if (!point)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_double (context, point->Y);
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_NumPoints (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ NumPoints(BLOB encoded LINESTRING)
/
/ returns the numer of vertices for current LINESTRING geometry 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo = NULL;
    gaiaLinestringPtr line;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  line = simpleLinestring (geo);
	  if (!line)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_int (context, line->Points);
      }
    gaiaFreeGeomColl (geo);
}

static void
point_n (sqlite3_context * context, int argc, sqlite3_value ** argv,
	 int request)
{
/* SQL functions:
/ StartPoint(BLOB encoded LINESTRING geometry)
/ EndPoint(BLOB encoded LINESTRING geometry)
/ PointN(BLOB encoded LINESTRING geometry, integer point_no)
/
/ returns the Nth POINT for current LINESTRING geometry 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int vertex;
    int len;
    double x;
    double y;
    unsigned char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    gaiaGeomCollPtr result;
    gaiaLinestringPtr line;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (request == GAIA_POINTN)
      {
	  /* PointN() requires point index to be defined as an SQL function argument */
	  if (sqlite3_value_type (argv[1]) != SQLITE_INTEGER)
	    {
		sqlite3_result_null (context);
		return;
	    }
	  vertex = sqlite3_value_int (argv[1]);
      }
    else if (request == GAIA_END_POINT)
	vertex = -1;		/* EndPoint() specifies a negative point index */
    else
	vertex = 1;		/* StartPoint() */
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  line = simpleLinestring (geo);
	  if (!line)
	      sqlite3_result_null (context);
	  else
	    {
		if (vertex < 0)
		    vertex = line->Points - 1;
		else
		    vertex -= 1;	/* decreasing the point index by 1, because PointN counts starting at index 1 */
		if (vertex >= 0 && vertex < line->Points)
		  {
		      gaiaGetPoint (line->Coords, vertex, &x, &y);
		      result = gaiaAllocGeomColl ();
		      result->Srid = geo->Srid;
		      gaiaAddPointToGeomColl (result, x, y);
		  }
		else
		    result = NULL;
		if (!result)
		    sqlite3_result_null (context);
		else
		  {
		      gaiaToSpatiaLiteBlobWkb (result, &p_result, &len);
		      gaiaFreeGeomColl (result);
		      sqlite3_result_blob (context, p_result, len, free);
		  }
	    }
      }
    gaiaFreeGeomColl (geo);
}

/*
/ the following functions simply readdress the request to point_n()
/ setting the appropriate request mode
*/

static void
fnct_StartPoint (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    point_n (context, argc, argv, GAIA_START_POINT);
}

static void
fnct_EndPoint (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    point_n (context, argc, argv, GAIA_END_POINT);
}

static void
fnct_PointN (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    point_n (context, argc, argv, GAIA_POINTN);
}

static void
fnct_ExteriorRing (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL functions:
/ ExteriorRing(BLOB encoded POLYGON geometry)
/
/ returns the EXTERIOR RING for current POLYGON geometry 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int iv;
    double x;
    double y;
    int len;
    unsigned char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    gaiaGeomCollPtr result;
    gaiaPolygonPtr polyg;
    gaiaRingPtr ring;
    gaiaLinestringPtr line;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  polyg = simplePolygon (geo);
	  if (!polyg)
	      sqlite3_result_null (context);
	  else
	    {
		ring = polyg->Exterior;
		result = gaiaAllocGeomColl ();
		result->Srid = geo->Srid;
		line = gaiaAddLinestringToGeomColl (result, ring->Points);
		for (iv = 0; iv < line->Points; iv++)
		  {
		      gaiaGetPoint (ring->Coords, iv, &x, &y);
		      gaiaSetPoint (line->Coords, iv, x, y);
		  }
		gaiaToSpatiaLiteBlobWkb (result, &p_result, &len);
		gaiaFreeGeomColl (result);
		sqlite3_result_blob (context, p_result, len, free);
	    }
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_NumInteriorRings (sqlite3_context * context, int argc,
		       sqlite3_value ** argv)
{
/* SQL function:
/ NumInteriorRings(BLOB encoded POLYGON)
/
/ returns the number of INTERIOR RINGS for current POLYGON geometry 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo = NULL;
    gaiaPolygonPtr polyg;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  polyg = simplePolygon (geo);
	  if (!polyg)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_int (context, polyg->NumInteriors);
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_InteriorRingN (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL functions:
/ InteriorRingN(BLOB encoded POLYGON geometry)
/
/ returns the Nth INTERIOR RING for current POLYGON geometry 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int border;
    int iv;
    double x;
    double y;
    int len;
    unsigned char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    gaiaGeomCollPtr result;
    gaiaPolygonPtr polyg;
    gaiaRingPtr ring;
    gaiaLinestringPtr line;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_INTEGER)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    border = sqlite3_value_int (argv[1]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  polyg = simplePolygon (geo);
	  if (!polyg)
	      sqlite3_result_null (context);
	  else
	    {
		if (border >= 1 && border <= polyg->NumInteriors)
		  {
		      ring = polyg->Interiors + (border - 1);
		      result = gaiaAllocGeomColl ();
		      result->Srid = geo->Srid;
		      line = gaiaAddLinestringToGeomColl (result, ring->Points);
		      for (iv = 0; iv < line->Points; iv++)
			{
			    gaiaGetPoint (ring->Coords, iv, &x, &y);
			    gaiaSetPoint (line->Coords, iv, x, y);
			}
		      gaiaToSpatiaLiteBlobWkb (result, &p_result, &len);
		      gaiaFreeGeomColl (result);
		      sqlite3_result_blob (context, p_result, len, free);
		  }
		else
		    sqlite3_result_null (context);
	    }
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_NumGeometries (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ NumGeometries(BLOB encoded GEOMETRYCOLLECTION)
/
/ returns the number of elementary geometries for current geometry 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int cnt = 0;
    gaiaPointPtr point;
    gaiaLinestringPtr line;
    gaiaPolygonPtr polyg;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  point = geo->FirstPoint;
	  while (point)
	    {
		/* counts how many points are there */
		cnt++;
		point = point->Next;
	    }
	  line = geo->FirstLinestring;
	  while (line)
	    {
		/* counts how many linestrings are there */
		cnt++;
		line = line->Next;
	    }
	  polyg = geo->FirstPolygon;
	  while (polyg)
	    {
		/* counts how many polygons are there */
		cnt++;
		polyg = polyg->Next;
	    }
	  sqlite3_result_int (context, cnt);
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_GeometryN (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ GeometryN(BLOB encoded GEOMETRYCOLLECTION geometry)
/
/ returns the Nth geometry for current GEOMETRYCOLLECTION or MULTIxxxx geometry 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int entity;
    int len;
    int cnt = 0;
    int iv;
    int ib;
    double x;
    double y;
    gaiaPointPtr point;
    gaiaLinestringPtr line;
    gaiaLinestringPtr line2;
    gaiaPolygonPtr polyg;
    gaiaPolygonPtr polyg2;
    gaiaRingPtr ring_in;
    gaiaRingPtr ring_out;
    unsigned char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    gaiaGeomCollPtr result = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_INTEGER)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    entity = sqlite3_value_int (argv[1]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  point = geo->FirstPoint;
	  while (point)
	    {
		/* counts how many points are there */
		cnt++;
		if (cnt == entity)
		  {
		      /* ok, required elementary geometry is this POINT */
		      result = gaiaAllocGeomColl ();
		      result->Srid = geo->Srid;
		      gaiaAddPointToGeomColl (result, point->X, point->Y);
		      goto skip;
		  }
		point = point->Next;
	    }
	  line = geo->FirstLinestring;
	  while (line)
	    {
		/* counts how many linestrings are there */
		cnt++;
		if (cnt == entity)
		  {
		      /* ok, required elementary geometry is this LINESTRING */
		      result = gaiaAllocGeomColl ();
		      result->Srid = geo->Srid;
		      line2 =
			  gaiaAddLinestringToGeomColl (result, line->Points);
		      for (iv = 0; iv < line2->Points; iv++)
			{
			    gaiaGetPoint (line->Coords, iv, &x, &y);
			    gaiaSetPoint (line2->Coords, iv, x, y);
			}
		      goto skip;
		  }
		line = line->Next;
	    }
	  polyg = geo->FirstPolygon;
	  while (polyg)
	    {
		/* counts how many polygons are there */
		cnt++;
		if (cnt == entity)
		  {
		      /* ok, required elementary geometry is this POLYGON */
		      result = gaiaAllocGeomColl ();
		      result->Srid = geo->Srid;
		      ring_in = polyg->Exterior;
		      polyg2 =
			  gaiaAddPolygonToGeomColl (result, ring_in->Points,
						    polyg->NumInteriors);
		      ring_out = polyg2->Exterior;
		      for (iv = 0; iv < ring_out->Points; iv++)
			{
			    /* copying the exterior ring POINTs */
			    gaiaGetPoint (ring_in->Coords, iv, &x, &y);
			    gaiaSetPoint (ring_out->Coords, iv, x, y);
			}
		      for (ib = 0; ib < polyg2->NumInteriors; ib++)
			{
			    /* processing the interior rings */
			    ring_in = polyg->Interiors + ib;
			    ring_out =
				gaiaAddInteriorRing (polyg2, ib,
						     ring_in->Points);
			    for (iv = 0; iv < ring_out->Points; iv++)
			      {
				  gaiaGetPoint (ring_in->Coords, iv, &x, &y);
				  gaiaSetPoint (ring_out->Coords, iv, x, y);
			      }
			}
		      goto skip;
		  }
		polyg = polyg->Next;
	    }
	skip:
	  if (result)
	    {
		gaiaToSpatiaLiteBlobWkb (result, &p_result, &len);
		gaiaFreeGeomColl (result);
		sqlite3_result_blob (context, p_result, len, free);
	    }
	  else
	      sqlite3_result_null (context);
      }
    gaiaFreeGeomColl (geo);
}

static void
mbrs_eval (sqlite3_context * context, int argc, sqlite3_value ** argv,
	   int request)
{
/* SQL function:
/ MBRsomething(BLOB encoded GEOMETRY-1, BLOB encoded GEOMETRY-2)
/
/ returns:
/ 1 if the required spatial relationship between the two MBRs is TRUE
/ 0 otherwise
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int ret;
    gaiaGeomCollPtr geo1 = NULL;
    gaiaGeomCollPtr geo2 = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo1 = gaiaFromSpatiaLiteBlobMbr (p_blob, n_bytes);
    p_blob = (unsigned char *) sqlite3_value_blob (argv[1]);
    n_bytes = sqlite3_value_bytes (argv[1]);
    geo2 = gaiaFromSpatiaLiteBlobMbr (p_blob, n_bytes);
    if (!geo1 || !geo2)
	sqlite3_result_null (context);
    else
      {
	  ret = 0;
	  gaiaMbrGeometry (geo1);
	  gaiaMbrGeometry (geo2);
	  switch (request)
	    {
	    case GAIA_MBR_CONTAINS:
		ret = gaiaMbrsContains (geo1, geo2);
		break;
	    case GAIA_MBR_DISJOINT:
		ret = gaiaMbrsDisjoint (geo1, geo2);
		break;
	    case GAIA_MBR_EQUAL:
		ret = gaiaMbrsEqual (geo1, geo2);
		break;
	    case GAIA_MBR_INTERSECTS:
		ret = gaiaMbrsIntersects (geo1, geo2);
		break;
	    case GAIA_MBR_OVERLAPS:
		ret = gaiaMbrsOverlaps (geo1, geo2);
		break;
	    case GAIA_MBR_TOUCHES:
		ret = gaiaMbrsTouches (geo1, geo2);
		break;
	    case GAIA_MBR_WITHIN:
		ret = gaiaMbrsWithin (geo1, geo2);
		break;
	    }
	  if (ret < 0)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_int (context, ret);
      }
    gaiaFreeGeomColl (geo1);
    gaiaFreeGeomColl (geo2);
}

/*
/ the following functions simply readdress the mbr_eval()
/ setting the appropriate request mode
*/

static void
fnct_MbrContains (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    mbrs_eval (context, argc, argv, GAIA_MBR_CONTAINS);
}

static void
fnct_MbrDisjoint (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    mbrs_eval (context, argc, argv, GAIA_MBR_DISJOINT);
}

static void
fnct_MbrEqual (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    mbrs_eval (context, argc, argv, GAIA_MBR_EQUAL);
}

static void
fnct_MbrIntersects (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    mbrs_eval (context, argc, argv, GAIA_MBR_INTERSECTS);
}

static void
fnct_MbrOverlaps (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    mbrs_eval (context, argc, argv, GAIA_MBR_OVERLAPS);
}

static void
fnct_MbrTouches (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    mbrs_eval (context, argc, argv, GAIA_MBR_TOUCHES);
}

static void
fnct_MbrWithin (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    mbrs_eval (context, argc, argv, GAIA_MBR_WITHIN);
}

static void
fnct_ShiftCoords (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ ShiftCoords(BLOBencoded geometry, shiftX, shiftY)
/
/ returns a new geometry that is the original one received, but with shifted coordinates
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int len;
    unsigned char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    double shift_x;
    double shift_y;
    int int_value;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) == SQLITE_FLOAT)
	shift_x = sqlite3_value_double (argv[1]);
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[1]);
	  shift_x = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[2]) == SQLITE_FLOAT)
	shift_y = sqlite3_value_double (argv[2]);
    else if (sqlite3_value_type (argv[2]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[2]);
	  shift_y = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  gaiaShiftCoords (geo, shift_x, shift_y);
	  gaiaToSpatiaLiteBlobWkb (geo, &p_result, &len);
	  if (!p_result)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_blob (context, p_result, len, free);
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_ScaleCoords (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ ScaleCoords(BLOBencoded geometry, scale_factor_x [, scale_factor_y])
/
/ returns a new geometry that is the original one received, but with scaled coordinates
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int len;
    unsigned char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    double scale_x;
    double scale_y;
    int int_value;
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) == SQLITE_FLOAT)
	scale_x = sqlite3_value_double (argv[1]);
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[1]);
	  scale_x = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (argc == 2)
	scale_y = scale_x;	/* this one is an isotropic scaling request */
    else
      {
	  /* an anisotropic scaling is requested */
	  if (sqlite3_value_type (argv[2]) == SQLITE_FLOAT)
	      scale_y = sqlite3_value_double (argv[2]);
	  else if (sqlite3_value_type (argv[2]) == SQLITE_INTEGER)
	    {
		int_value = sqlite3_value_int (argv[2]);
		scale_y = int_value;
	    }
	  else
	    {
		sqlite3_result_null (context);
		return;
	    }
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  gaiaScaleCoords (geo, scale_x, scale_y);
	  gaiaToSpatiaLiteBlobWkb (geo, &p_result, &len);
	  if (!p_result)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_blob (context, p_result, len, free);
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_RotateCoords (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ RotateCoords(BLOBencoded geometry, angle)
/
/ returns a new geometry that is the original one received, but with rotated coordinates
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int len;
    unsigned char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    double angle;
    int int_value;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) == SQLITE_FLOAT)
	angle = sqlite3_value_double (argv[1]);
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[1]);
	  angle = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  gaiaRotateCoords (geo, angle);
	  gaiaToSpatiaLiteBlobWkb (geo, &p_result, &len);
	  if (!p_result)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_blob (context, p_result, len, free);
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_ReflectCoords (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ ReflectCoords(BLOBencoded geometry, x_axis,  y_axis)
/
/ returns a new geometry that is the original one received, but with mirrored coordinates
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int len;
    unsigned char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    int x_axis;
    int y_axis;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
	x_axis = sqlite3_value_int (argv[1]);
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[2]) == SQLITE_INTEGER)
	y_axis = sqlite3_value_int (argv[2]);
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  gaiaReflectCoords (geo, x_axis, y_axis);
	  gaiaToSpatiaLiteBlobWkb (geo, &p_result, &len);
	  if (!p_result)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_blob (context, p_result, len, free);
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_SwapCoords (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ SwapCoords(BLOBencoded geometry)
/
/ returns a new geometry that is the original one received, but with swapped x- and y-coordinate
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int len;
    unsigned char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  gaiaSwapCoords (geo);
	  gaiaToSpatiaLiteBlobWkb (geo, &p_result, &len);
	  if (!p_result)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_blob (context, p_result, len, free);
      }
    gaiaFreeGeomColl (geo);
}

static void
proj_params (sqlite3 * sqlite, int srid, char *proj_params)
{
/* retrives the PROJ params from SPATIAL_SYS_REF table, if possible */
    char sql[256];
    char **results;
    int rows;
    int columns;
    int i;
    int ret;
    char *errMsg = NULL;
    *proj_params = '\0';
    sprintf (sql,
	     "SELECT proj4text FROM spatial_ref_sys WHERE srid = %d", srid);
    ret = sqlite3_get_table (sqlite, sql, &results, &rows, &columns, &errMsg);
    if (ret != SQLITE_OK)
      {
	  fprintf (stderr, "unknown SRID: %d\t<%s>\n", srid, errMsg);
	  sqlite3_free (errMsg);
	  return;
      }
    for (i = 1; i <= rows; i++)
	strcpy (proj_params, results[(i * columns)]);
    if (*proj_params == '\0')
	fprintf (stderr, "unknown SRID: %d\n", srid);
    sqlite3_free_table (results);
}

static int
get_ellipse_params (sqlite3 * sqlite, int srid, double *a, double *b,
		    double *rf)
{
/* 
/ retrives the PROJ +ellps=xx [+a=xx +b=xx] params 
/from SPATIAL_SYS_REF table, if possible 
*/
    char proj4text[2048];
    char *p_proj;
    char *p_ellps;
    char *p_a;
    char *p_b;
    char *p_end;
    proj_params (sqlite, srid, proj4text);
    if (*proj4text == '\0')
	return 0;
/* parsing the proj4text geodesic string */
    p_proj = strstr (proj4text, "+proj=");
    p_ellps = strstr (proj4text, "+ellps=");
    p_a = strstr (proj4text, "+a=");
    p_b = strstr (proj4text, "+b=");
/* checking if +proj=longlat is true */
    if (!p_proj)
	return 0;
    p_end = strchr (p_proj, ' ');
    if (p_end)
	*p_end = '\0';
    if (strcmp (p_proj + 6, "longlat") != 0)
	return 0;
    if (p_ellps)
      {
	  /* tryng to retrieve the ellipsoid params by name */
	  p_end = strchr (p_ellps, ' ');
	  if (p_end)
	      *p_end = '\0';
	  if (gaiaEllipseParams (p_ellps + 7, a, b, rf))
	      return 1;
      }
    if (p_a && p_b)
      {
	  /* trying to retrieve the +a=xx and +b=xx args */
	  p_end = strchr (p_a, ' ');
	  if (p_end)
	      *p_end = '\0';
	  p_end = strchr (p_b, ' ');
	  if (p_end)
	      *p_end = '\0';
	  *a = atof (p_a + 3);
	  *b = atof (p_b + 3);
	  *rf = 1.0 / ((*a - *b) / *a);
	  return 1;
      }
    return 0;
}

#ifndef OMIT_PROJ		/* including PROJ.4 */

static void
fnct_Transform (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Transform(BLOBencoded geometry, srid)
/
/ returns a new geometry that is the original one received, but with the new SRID [no coordinates translation is applied]
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo = NULL;
    gaiaGeomCollPtr result;
    int srid_from;
    int srid_to;
    char proj_from[2048];
    char proj_to[2048];
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
	srid_to = sqlite3_value_int (argv[1]);
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  *proj_from = '\0';
	  *proj_to = '\0';
	  srid_from = geo->Srid;
	  proj_params (sqlite, srid_from, proj_from);
	  proj_params (sqlite, srid_to, proj_to);
	  if (*proj_to == '\0' || *proj_from == '\0')
	    {
		gaiaFreeGeomColl (geo);
		sqlite3_result_null (context);
		return;
	    }
	  result = gaiaTransform (geo, proj_from, proj_to);
	  if (!result)
	      sqlite3_result_null (context);
	  else
	    {
		/* builds the BLOB geometry to be returned */
		int len;
		unsigned char *p_result = NULL;
		result->Srid = srid_to;
		gaiaToSpatiaLiteBlobWkb (result, &p_result, &len);
		sqlite3_result_blob (context, p_result, len, free);
		gaiaFreeGeomColl (result);
	    }
      }
    gaiaFreeGeomColl (geo);
}

#endif /* end including PROJ.4 */

#ifndef OMIT_GEOS		/* including GEOS */

static void
fnct_Boundary (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Boundary(BLOB encoded geometry)
/
/ returns the combinatioral boundary for current geometry 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int len;
    unsigned char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    gaiaGeomCollPtr boundary;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  if (gaiaIsEmpty (geo))
	      sqlite3_result_null (context);
	  else
	    {
		boundary = gaiaBoundary (geo);
		if (!boundary)
		    sqlite3_result_null (context);
		else
		  {
		      gaiaToSpatiaLiteBlobWkb (boundary, &p_result, &len);
		      gaiaFreeGeomColl (boundary);
		      sqlite3_result_blob (context, p_result, len, free);
		  }
	    }
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_IsClosed (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ IsClosed(BLOB encoded LINESTRING or MULTILINESTRING geometry)
/
/ returns:
/ 1 if this LINESTRING is closed [or if this is a MULTILINESTRING and every LINESTRINGs are closed] 
/ 0 otherwise
/ or -1 if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo = NULL;
    gaiaLinestringPtr line;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_int (context, -1);
    else
      {
	  line = simpleLinestring (geo);
	  if (!line < 0)
	      sqlite3_result_int (context, -1);
	  else
	      sqlite3_result_int (context, gaiaIsClosed (line));
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_IsSimple (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ IsSimple(BLOB encoded GEOMETRY)
/
/ returns:
/ 1 if this GEOMETRY is simple
/ 0 otherwise
/ or -1 if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int ret;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_int (context, -1);
    else
      {
	  ret = gaiaIsSimple (geo);
	  if (ret < 0)
	      sqlite3_result_int (context, -1);
	  else
	      sqlite3_result_int (context, ret);
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_IsRing (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ IsRing(BLOB encoded LINESTRING geometry)
/
/ returns:
/ 1 if this LINESTRING is a valid RING
/ 0 otherwise
/ or -1 if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int ret;
    gaiaGeomCollPtr geo = NULL;
    gaiaLinestringPtr line;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_int (context, -1);
    else
      {
	  line = simpleLinestring (geo);
	  if (!line < 0)
	      sqlite3_result_int (context, -1);
	  else
	    {
		ret = gaiaIsRing (line);
		sqlite3_result_int (context, ret);
	    }
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_IsValid (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ IsValid(BLOB encoded GEOMETRY)
/
/ returns:
/ 1 if this GEOMETRY is a valid one
/ 0 otherwise
/ or -1 if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int ret;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_int (context, -1);
    else
      {
	  ret = gaiaIsValid (geo);
	  if (ret < 0)
	      sqlite3_result_int (context, -1);
	  else
	      sqlite3_result_int (context, ret);
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_Length (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Length(BLOB encoded GEOMETRYCOLLECTION)
/
/ returns  the total length for current geometry 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    double length = 0.0;
    int ret;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  ret = gaiaGeomCollLength (geo, &length);
	  if (!ret)
	      sqlite3_result_null (context);
	  sqlite3_result_double (context, length);
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_Area (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Area(BLOB encoded GEOMETRYCOLLECTION)
/
/ returns the total area for current geometry 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    double area = 0.0;
    int ret;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  ret = gaiaGeomCollArea (geo, &area);
	  if (!ret)
	      sqlite3_result_null (context);
	  sqlite3_result_double (context, area);
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_Centroid (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Centroid(BLOBencoded POLYGON or MULTIPOLYGON geometry)
/
/ returns a POINT representing the centroid for current POLYGON / MULTIPOLYGON geometry 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int len;
    int ret;
    double x;
    double y;
    unsigned char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    gaiaGeomCollPtr result;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  if (gaiaIsEmpty (geo))
	      sqlite3_result_null (context);
	  else
	    {
		ret = gaiaGeomCollCentroid (geo, &x, &y);
		if (!ret)
		    sqlite3_result_null (context);
		else
		  {
		      result = gaiaAllocGeomColl ();
		      result->Srid = geo->Srid;
		      gaiaAddPointToGeomColl (result, x, y);
		      gaiaToSpatiaLiteBlobWkb (result, &p_result, &len);
		      gaiaFreeGeomColl (result);
		      sqlite3_result_blob (context, p_result, len, free);
		  }
	    }
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_PointOnSurface (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ PointOnSurface(BLOBencoded POLYGON or MULTIPOLYGON geometry)
/
/ returns a POINT guaranteed to lie on the Surface
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int len;
    double x;
    double y;
    unsigned char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    gaiaGeomCollPtr result;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  if (!gaiaGetPointOnSurface (geo, &x, &y))
	      sqlite3_result_null (context);
	  else
	    {
		result = gaiaAllocGeomColl ();
		gaiaAddPointToGeomColl (result, x, y);
		result->Srid = geo->Srid;
		gaiaToSpatiaLiteBlobWkb (result, &p_result, &len);
		gaiaFreeGeomColl (result);
		sqlite3_result_blob (context, p_result, len, free);
	    }
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_Simplify (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Simplify(BLOBencoded geometry, tolerance)
/
/ returns a new geometry that is a caricature of the original one received, but simplified using the Douglas-Peuker algorihtm
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo = NULL;
    gaiaGeomCollPtr result;
    int int_value;
    double tolerance;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) == SQLITE_FLOAT)
	tolerance = sqlite3_value_double (argv[1]);
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[1]);
	  tolerance = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  result = gaiaGeomCollSimplify (geo, tolerance);
	  if (!result)
	      sqlite3_result_null (context);
	  else
	    {
		/* builds the BLOB geometry to be returned */
		int len;
		unsigned char *p_result = NULL;
		gaiaToSpatiaLiteBlobWkb (result, &p_result, &len);
		sqlite3_result_blob (context, p_result, len, free);
		gaiaFreeGeomColl (result);
	    }
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_SimplifyPreserveTopology (sqlite3_context * context, int argc,
			       sqlite3_value ** argv)
{
/* SQL function:
/ SimplifyPreserveTopology(BLOBencoded geometry, tolerance)
/
/ returns a new geometry that is a caricature of the original one received, but simplified using the Douglas-Peuker algorihtm [preserving topology]
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo = NULL;
    gaiaGeomCollPtr result;
    int int_value;
    double tolerance;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) == SQLITE_FLOAT)
	tolerance = sqlite3_value_double (argv[1]);
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[1]);
	  tolerance = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  result = gaiaGeomCollSimplifyPreserveTopology (geo, tolerance);
	  if (!result)
	      sqlite3_result_null (context);
	  else
	    {
		/* builds the BLOB geometry to be returned */
		int len;
		unsigned char *p_result = NULL;
		gaiaToSpatiaLiteBlobWkb (result, &p_result, &len);
		sqlite3_result_blob (context, p_result, len, free);
		gaiaFreeGeomColl (result);
	    }
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_ConvexHull (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ ConvexHull(BLOBencoded geometry)
/
/ returns a new geometry representing the CONVEX HULL for current geometry
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int len;
    unsigned char *p_result = NULL;
    gaiaGeomCollPtr geo = NULL;
    gaiaGeomCollPtr result;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  result = gaiaConvexHull (geo);
	  if (!result)
	      sqlite3_result_null (context);
	  else
	    {
		gaiaToSpatiaLiteBlobWkb (result, &p_result, &len);
		sqlite3_result_blob (context, p_result, len, free);
		gaiaFreeGeomColl (result);
	    }
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_Buffer (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Buffer(BLOBencoded geometry, radius)
/
/ returns a new geometry representing the BUFFER for current geometry
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo = NULL;
    gaiaGeomCollPtr result;
    double radius;
    int int_value;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) == SQLITE_FLOAT)
	radius = sqlite3_value_double (argv[1]);
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[1]);
	  radius = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  result = gaiaGeomCollBuffer (geo, radius, 30);
	  if (!result)
	      sqlite3_result_null (context);
	  else
	    {
		/* builds the BLOB geometry to be returned */
		int len;
		unsigned char *p_result = NULL;
		result->Srid = geo->Srid;
		gaiaToSpatiaLiteBlobWkb (result, &p_result, &len);
		sqlite3_result_blob (context, p_result, len, free);
		gaiaFreeGeomColl (result);
	    }
      }
    gaiaFreeGeomColl (geo);
}

static void
fnct_Intersection (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Intersection(BLOBencoded geom1, BLOBencoded geom2)
/
/ returns a new geometry representing the INTERSECTION of both geometries
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo1 = NULL;
    gaiaGeomCollPtr geo2 = NULL;
    gaiaGeomCollPtr result;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo1 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    p_blob = (unsigned char *) sqlite3_value_blob (argv[1]);
    n_bytes = sqlite3_value_bytes (argv[1]);
    geo2 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo1 || !geo2)
	sqlite3_result_null (context);
    else
      {
	  result = gaiaGeometryIntersection (geo1, geo2);
	  if (!result)
	      sqlite3_result_null (context);
	  else if (gaiaIsEmpty (result))
	    {
		gaiaFreeGeomColl (result);
		sqlite3_result_null (context);
	    }
	  else
	    {
		/* builds the BLOB geometry to be returned */
		int len;
		unsigned char *p_result = NULL;
		gaiaToSpatiaLiteBlobWkb (result, &p_result, &len);
		sqlite3_result_blob (context, p_result, len, free);
		gaiaFreeGeomColl (result);
	    }
      }
    gaiaFreeGeomColl (geo1);
    gaiaFreeGeomColl (geo2);
}

static void
fnct_Union_step (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Union(BLOBencoded geom)
/
/ aggregate function - STEP
/
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geom;
    gaiaGeomCollPtr result;
    gaiaGeomCollPtr *p;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geom = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geom)
	return;
    p = sqlite3_aggregate_context (context, sizeof (gaiaGeomCollPtr));
    if (!(*p))
      {
	  /* this is the first row */
	  *p = geom;
      }
    else
      {
	  /* subsequent rows */
	  result = gaiaGeometryUnion (*p, geom);
	  gaiaFreeGeomColl (*p);
	  *p = result;
	  gaiaFreeGeomColl (geom);
      }
}

static void
fnct_Union_final (sqlite3_context * context)
{
/* SQL function:
/ Union(BLOBencoded geom)
/
/ aggregate function - FINAL
/
*/
    gaiaGeomCollPtr result;
    gaiaGeomCollPtr *p = sqlite3_aggregate_context (context, 0);
    if (!p)
      {
	  sqlite3_result_null (context);
	  return;
      }
    result = *p;
    if (!result)
	sqlite3_result_null (context);
    else if (gaiaIsEmpty (result))
      {
	  gaiaFreeGeomColl (result);
	  sqlite3_result_null (context);
      }
    else
      {
	  /* builds the BLOB geometry to be returned */
	  int len;
	  unsigned char *p_result = NULL;
	  gaiaToSpatiaLiteBlobWkb (result, &p_result, &len);
	  sqlite3_result_blob (context, p_result, len, free);
	  gaiaFreeGeomColl (result);
      }
}

static void
fnct_Union (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Union(BLOBencoded geom1, BLOBencoded geom2)
/
/ returns a new geometry representing the UNION of both geometries
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo1 = NULL;
    gaiaGeomCollPtr geo2 = NULL;
    gaiaGeomCollPtr result;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo1 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    p_blob = (unsigned char *) sqlite3_value_blob (argv[1]);
    n_bytes = sqlite3_value_bytes (argv[1]);
    geo2 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo1 || !geo2)
	sqlite3_result_null (context);
    else
      {
	  result = gaiaGeometryUnion (geo1, geo2);
	  if (!result)
	      sqlite3_result_null (context);
	  else if (gaiaIsEmpty (result))
	    {
		gaiaFreeGeomColl (result);
		sqlite3_result_null (context);
	    }
	  else
	    {
		/* builds the BLOB geometry to be returned */
		int len;
		unsigned char *p_result = NULL;
		gaiaToSpatiaLiteBlobWkb (result, &p_result, &len);
		sqlite3_result_blob (context, p_result, len, free);
		gaiaFreeGeomColl (result);
	    }
      }
    gaiaFreeGeomColl (geo1);
    gaiaFreeGeomColl (geo2);
}

static void
fnct_Difference (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Difference(BLOBencoded geom1, BLOBencoded geom2)
/
/ returns a new geometry representing the DIFFERENCE of both geometries
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo1 = NULL;
    gaiaGeomCollPtr geo2 = NULL;
    gaiaGeomCollPtr result;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo1 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    p_blob = (unsigned char *) sqlite3_value_blob (argv[1]);
    n_bytes = sqlite3_value_bytes (argv[1]);
    geo2 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo1 || !geo2)
	sqlite3_result_null (context);
    else
      {
	  result = gaiaGeometryDifference (geo1, geo2);
	  if (!result)
	      sqlite3_result_null (context);
	  else if (gaiaIsEmpty (result))
	    {
		gaiaFreeGeomColl (result);
		sqlite3_result_null (context);
	    }
	  else
	    {
		/* builds the BLOB geometry to be returned */
		int len;
		unsigned char *p_result = NULL;
		gaiaToSpatiaLiteBlobWkb (result, &p_result, &len);
		sqlite3_result_blob (context, p_result, len, free);
		gaiaFreeGeomColl (result);
	    }
      }
    gaiaFreeGeomColl (geo1);
    gaiaFreeGeomColl (geo2);
}

static void
fnct_SymDifference (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ SymDifference(BLOBencoded geom1, BLOBencoded geom2)
/
/ returns a new geometry representing the SYMMETRIC DIFFERENCE of both geometries
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo1 = NULL;
    gaiaGeomCollPtr geo2 = NULL;
    gaiaGeomCollPtr result;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo1 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    p_blob = (unsigned char *) sqlite3_value_blob (argv[1]);
    n_bytes = sqlite3_value_bytes (argv[1]);
    geo2 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo1 || !geo2)
	sqlite3_result_null (context);
    else
      {
	  result = gaiaGeometrySymDifference (geo1, geo2);
	  if (!result)
	      sqlite3_result_null (context);
	  else if (gaiaIsEmpty (result))
	    {
		gaiaFreeGeomColl (result);
		sqlite3_result_null (context);
	    }
	  else
	    {
		/* builds the BLOB geometry to be returned */
		int len;
		unsigned char *p_result = NULL;
		gaiaToSpatiaLiteBlobWkb (result, &p_result, &len);
		sqlite3_result_blob (context, p_result, len, free);
		gaiaFreeGeomColl (result);
	    }
      }
    gaiaFreeGeomColl (geo1);
    gaiaFreeGeomColl (geo2);
}

static void
fnct_Equals (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Equals(BLOBencoded geom1, BLOBencoded geom2)
/
/ returns:
/ 1 if the two geometries are "spatially equal"
/ 0 otherwise
/ or -1 if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo1 = NULL;
    gaiaGeomCollPtr geo2 = NULL;
    int ret;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo1 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    p_blob = (unsigned char *) sqlite3_value_blob (argv[1]);
    n_bytes = sqlite3_value_bytes (argv[1]);
    geo2 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo1 || !geo2)
	sqlite3_result_int (context, -1);
    else
      {
	  ret = gaiaGeomCollEquals (geo1, geo2);
	  sqlite3_result_int (context, ret);
      }
    gaiaFreeGeomColl (geo1);
    gaiaFreeGeomColl (geo2);
}

static void
fnct_Intersects (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Intersects(BLOBencoded geom1, BLOBencoded geom2)
/
/ returns:
/ 1 if the two geometries do "spatially intersects"
/ 0 otherwise
/ or -1 if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo1 = NULL;
    gaiaGeomCollPtr geo2 = NULL;
    int ret;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo1 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    p_blob = (unsigned char *) sqlite3_value_blob (argv[1]);
    n_bytes = sqlite3_value_bytes (argv[1]);
    geo2 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo1 || !geo2)
	sqlite3_result_int (context, -1);
    else
      {
	  ret = gaiaGeomCollIntersects (geo1, geo2);
	  sqlite3_result_int (context, ret);
      }
    gaiaFreeGeomColl (geo1);
    gaiaFreeGeomColl (geo2);
}

static void
fnct_Disjoint (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Disjoint(BLOBencoded geom1, BLOBencoded geom2)
/
/ returns:
/ 1 if the two geometries are "spatially disjoint"
/ 0 otherwise
/ or -1 if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo1 = NULL;
    gaiaGeomCollPtr geo2 = NULL;
    int ret;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo1 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    p_blob = (unsigned char *) sqlite3_value_blob (argv[1]);
    n_bytes = sqlite3_value_bytes (argv[1]);
    geo2 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo1 || !geo2)
	sqlite3_result_int (context, -1);
    else
      {
	  ret = gaiaGeomCollDisjoint (geo1, geo2);
	  sqlite3_result_int (context, ret);
      }
    gaiaFreeGeomColl (geo1);
    gaiaFreeGeomColl (geo2);
}

static void
fnct_Overlaps (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Overlaps(BLOBencoded geom1, BLOBencoded geom2)
/
/ returns:
/ 1 if the two geometries do "spatially overlaps"
/ 0 otherwise
/ or -1 if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo1 = NULL;
    gaiaGeomCollPtr geo2 = NULL;
    int ret;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo1 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    p_blob = (unsigned char *) sqlite3_value_blob (argv[1]);
    n_bytes = sqlite3_value_bytes (argv[1]);
    geo2 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo1 || !geo2)
	sqlite3_result_int (context, -1);
    else
      {
	  ret = gaiaGeomCollOverlaps (geo1, geo2);
	  sqlite3_result_int (context, ret);
      }
    gaiaFreeGeomColl (geo1);
    gaiaFreeGeomColl (geo2);
}

static void
fnct_Crosses (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Crosses(BLOBencoded geom1, BLOBencoded geom2)
/
/ returns:
/ 1 if the two geometries do "spatially crosses"
/ 0 otherwise
/ or -1 if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo1 = NULL;
    gaiaGeomCollPtr geo2 = NULL;
    int ret;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo1 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    p_blob = (unsigned char *) sqlite3_value_blob (argv[1]);
    n_bytes = sqlite3_value_bytes (argv[1]);
    geo2 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo1 || !geo2)
	sqlite3_result_int (context, -1);
    else
      {
	  ret = gaiaGeomCollCrosses (geo1, geo2);
	  sqlite3_result_int (context, ret);
      }
    gaiaFreeGeomColl (geo1);
    gaiaFreeGeomColl (geo2);
}

static void
fnct_Touches (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Touches(BLOBencoded geom1, BLOBencoded geom2)
/
/ returns:
/ 1 if the two geometries do "spatially touches"
/ 0 otherwise
/ or -1 if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo1 = NULL;
    gaiaGeomCollPtr geo2 = NULL;
    int ret;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo1 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    p_blob = (unsigned char *) sqlite3_value_blob (argv[1]);
    n_bytes = sqlite3_value_bytes (argv[1]);
    geo2 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo1 || !geo2)
	sqlite3_result_int (context, -1);
    else
      {
	  ret = gaiaGeomCollTouches (geo1, geo2);
	  sqlite3_result_int (context, ret);
      }
    gaiaFreeGeomColl (geo1);
    gaiaFreeGeomColl (geo2);
}

static void
fnct_Within (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Within(BLOBencoded geom1, BLOBencoded geom2)
/
/ returns:
/ 1 if GEOM-1 is completely contained within GEOM-2
/ 0 otherwise
/ or -1 if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo1 = NULL;
    gaiaGeomCollPtr geo2 = NULL;
    int ret;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo1 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    p_blob = (unsigned char *) sqlite3_value_blob (argv[1]);
    n_bytes = sqlite3_value_bytes (argv[1]);
    geo2 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo1 || !geo2)
	sqlite3_result_int (context, -1);
    else
      {
	  ret = gaiaGeomCollWithin (geo1, geo2);
	  sqlite3_result_int (context, ret);
      }
    gaiaFreeGeomColl (geo1);
    gaiaFreeGeomColl (geo2);
}

static void
fnct_Contains (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Contains(BLOBencoded geom1, BLOBencoded geom2)
/
/ returns:
/ 1 if GEOM-1 completely contains GEOM-2
/ 0 otherwise
/ or -1 if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo1 = NULL;
    gaiaGeomCollPtr geo2 = NULL;
    int ret;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo1 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    p_blob = (unsigned char *) sqlite3_value_blob (argv[1]);
    n_bytes = sqlite3_value_bytes (argv[1]);
    geo2 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo1 || !geo2)
	sqlite3_result_int (context, -1);
    else
      {
	  ret = gaiaGeomCollContains (geo1, geo2);
	  sqlite3_result_int (context, ret);
      }
    gaiaFreeGeomColl (geo1);
    gaiaFreeGeomColl (geo2);
}

static void
fnct_Relate (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Relate(BLOBencoded geom1, BLOBencoded geom2, string pattern)
/
/ returns:
/ 1 if GEOM-1 and GEOM-2 have a spatial relationship as specified by the patternMatrix 
/ 0 otherwise
/ or -1 if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo1 = NULL;
    gaiaGeomCollPtr geo2 = NULL;
    int ret;
    const unsigned char *pattern;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    if (sqlite3_value_type (argv[2]) != SQLITE_TEXT)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo1 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    p_blob = (unsigned char *) sqlite3_value_blob (argv[1]);
    n_bytes = sqlite3_value_bytes (argv[1]);
    geo2 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    pattern = sqlite3_value_text (argv[2]);
    if (!geo1 || !geo2)
	sqlite3_result_int (context, -1);
    else
      {
	  ret = gaiaGeomCollRelate (geo1, geo2, (char *) pattern);
	  sqlite3_result_int (context, ret);
      }
    gaiaFreeGeomColl (geo1);
    gaiaFreeGeomColl (geo2);
}

static void
fnct_Distance (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Distance(BLOBencoded geom1, BLOBencoded geom2)
/
/ returns the distance between GEOM-1 and GEOM-2
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo1 = NULL;
    gaiaGeomCollPtr geo2 = NULL;
    double dist;
    int ret;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo1 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    p_blob = (unsigned char *) sqlite3_value_blob (argv[1]);
    n_bytes = sqlite3_value_bytes (argv[1]);
    geo2 = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo1 || !geo2)
	sqlite3_result_null (context);
    else
      {
	  ret = gaiaGeomCollDistance (geo1, geo2, &dist);
	  if (!ret)
	      sqlite3_result_null (context);
	  sqlite3_result_double (context, dist);
      }
    gaiaFreeGeomColl (geo1);
    gaiaFreeGeomColl (geo2);
}

static void
geos_error (const char *fmt, ...)
{
/* reporting some GEOS warning/error */
    va_list ap;
    fprintf (stderr, "GEOS: ");
    va_start (ap, fmt);
    vfprintf (stdout, fmt, ap);
    va_end (ap);
    fprintf (stdout, "\n");
}

static void
fnct_polygonize (sqlite3_context * context, gaiaGeomCollPtr geom_org,
		 int force_multipolygon)
{
/* a  common function performing any kind of polygonization op */
    gaiaGeomCollPtr geom_new = NULL;
    int len;
    unsigned char *p_result = NULL;
    if (!geom_org)
	goto invalid;
    geom_new = gaiaPolygonize (geom_org, force_multipolygon);
    if (!geom_new)
	goto invalid;
    gaiaFreeGeomColl (geom_org);
    gaiaToSpatiaLiteBlobWkb (geom_new, &p_result, &len);
    gaiaFreeGeomColl (geom_new);
    sqlite3_result_blob (context, p_result, len, free);
    return;
  invalid:
    if (geom_org)
	gaiaFreeGeomColl (geom_org);
    sqlite3_result_null (context);
}

/*
/ the following functions performs initial argument checking, 
/ and then readdressing the request to fnct_polygonize()
/ for actual processing
*/

static void
fnct_BdPolyFromText1 (sqlite3_context * context, int argc,
		      sqlite3_value ** argv)
{
/* SQL function:
/ BdPolyFromText(WKT encoded  LINESTRING)
/
/ returns the current geometry [POLYGON] by parsing a WKT encoded LINESTRING 
/ or NULL if any error is encountered
/
*/
    const unsigned char *text;
    gaiaGeomCollPtr geo = NULL;
    gaiaLinestringPtr ln;
    double x0;
    double y0;
    double xn;
    double yn;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_TEXT)
      {
	  sqlite3_result_null (context);
	  return;
      }
    text = sqlite3_value_text (argv[0]);
    geo = gaiaParseWkt (text, -1);
    if (geo == NULL)
      {
	  sqlite3_result_null (context);
	  return;
      }
    geo->Srid = -1;
/* one or more closed LINESTINGs are expected */
    if (geo->FirstPoint || geo->FirstPolygon)
	goto invalid;
    if (!geo->FirstLinestring)
	goto invalid;
    ln = geo->FirstLinestring;
    while (ln)
      {
	  gaiaGetPoint (ln->Coords, 0, &x0, &y0);
	  gaiaGetPoint (ln->Coords, ln->Points - 1, &xn, &yn);
	  if (x0 != xn || y0 != yn)
	      goto invalid;
	  ln = ln->Next;
      }
    fnct_polygonize (context, geo, 0);
    return;
  invalid:
    gaiaFreeGeomColl (geo);
    sqlite3_result_null (context);
}

static void
fnct_BdPolyFromText2 (sqlite3_context * context, int argc,
		      sqlite3_value ** argv)
{
/* SQL function:
/ BdPolyFromText(WKT encoded LINESTRING, SRID)
/
/ returns the current geometry [POLYGON] by parsing a WKT encoded LINESTRING 
/ or NULL if any error is encountered
/
*/
    const unsigned char *text;
    gaiaGeomCollPtr geo = NULL;
    gaiaLinestringPtr ln;
    double x0;
    double y0;
    double xn;
    double yn;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_TEXT)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_INTEGER)
      {
	  sqlite3_result_null (context);
	  return;
      }
    text = sqlite3_value_text (argv[0]);
    geo = gaiaParseWkt (text, -1);
    if (geo == NULL)
      {
	  sqlite3_result_null (context);
	  return;
      }
    geo->Srid = sqlite3_value_int (argv[1]);
/* one or more closed LINESTINGs are expected */
    if (geo->FirstPoint || geo->FirstPolygon)
	goto invalid;
    if (!geo->FirstLinestring)
	goto invalid;
    ln = geo->FirstLinestring;
    while (ln)
      {
	  gaiaGetPoint (ln->Coords, 0, &x0, &y0);
	  gaiaGetPoint (ln->Coords, ln->Points - 1, &xn, &yn);
	  if (x0 != xn || y0 != yn)
	      goto invalid;
	  ln = ln->Next;
      }
    fnct_polygonize (context, geo, 0);
    return;
  invalid:
    gaiaFreeGeomColl (geo);
    sqlite3_result_null (context);
}

static void
fnct_BdMPolyFromText1 (sqlite3_context * context, int argc,
		       sqlite3_value ** argv)
{
/* SQL function:
/ BdMPolyFromText(WKT encoded  MULTILINESTRING)
/
/ returns the current geometry [MULTIPOLYGON] by parsing a WKT encoded MULTILINESTRING 
/ or NULL if any error is encountered
/
*/
    const unsigned char *text;
    gaiaGeomCollPtr geo = NULL;
    gaiaLinestringPtr ln;
    double x0;
    double y0;
    double xn;
    double yn;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_TEXT)
      {
	  sqlite3_result_null (context);
	  return;
      }
    text = sqlite3_value_text (argv[0]);
    geo = gaiaParseWkt (text, -1);
    if (geo == NULL)
      {
	  sqlite3_result_null (context);
	  return;
      }
    geo->Srid = -1;
/* one or more closed LINESTINGs are expected */
    if (geo->FirstPoint || geo->FirstPolygon)
	goto invalid;
    if (!geo->FirstLinestring)
	goto invalid;
    ln = geo->FirstLinestring;
    while (ln)
      {
	  gaiaGetPoint (ln->Coords, 0, &x0, &y0);
	  gaiaGetPoint (ln->Coords, ln->Points - 1, &xn, &yn);
	  if (x0 != xn || y0 != yn)
	      goto invalid;
	  ln = ln->Next;
      }
    fnct_polygonize (context, geo, 1);
    return;
  invalid:
    gaiaFreeGeomColl (geo);
    sqlite3_result_null (context);
}

static void
fnct_BdMPolyFromText2 (sqlite3_context * context, int argc,
		       sqlite3_value ** argv)
{
/* SQL function:
/ BdMPolyFromText(WKT encoded MULTILINESTRING, SRID)
/
/ returns the current geometry [MULTIPOLYGON] by parsing a WKT encoded MULTILINESTRING 
/ or NULL if any error is encountered
/
*/
    const unsigned char *text;
    gaiaGeomCollPtr geo = NULL;
    gaiaLinestringPtr ln;
    double x0;
    double y0;
    double xn;
    double yn;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_TEXT)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_INTEGER)
      {
	  sqlite3_result_null (context);
	  return;
      }
    text = sqlite3_value_text (argv[0]);
    geo = gaiaParseWkt (text, -1);
    if (geo == NULL)
      {
	  sqlite3_result_null (context);
	  return;
      }
    geo->Srid = sqlite3_value_int (argv[1]);
/* one or more closed LINESTINGs are expected */
    if (geo->FirstPoint || geo->FirstPolygon)
	goto invalid;
    if (!geo->FirstLinestring)
	goto invalid;
    ln = geo->FirstLinestring;
    while (ln)
      {
	  gaiaGetPoint (ln->Coords, 0, &x0, &y0);
	  gaiaGetPoint (ln->Coords, ln->Points - 1, &xn, &yn);
	  if (x0 != xn || y0 != yn)
	      goto invalid;
	  ln = ln->Next;
      }
    fnct_polygonize (context, geo, 1);
    return;
  invalid:
    gaiaFreeGeomColl (geo);
    sqlite3_result_null (context);
}

static void
fnct_BdPolyFromWKB1 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ BdPolyFromWKB(WKB encoded  LINESTRING)
/
/ returns the current geometry [POLYGON] by parsing a WKB encoded LINESTRING 
/ or NULL if any error is encountered
/
*/
    int n_bytes;
    const unsigned char *wkb;
    gaiaGeomCollPtr geo = NULL;
    gaiaLinestringPtr ln;
    double x0;
    double y0;
    double xn;
    double yn;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    wkb = sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    if (!check_wkb (wkb, n_bytes, -1))
	return;
    geo = gaiaFromWkb (wkb, n_bytes);
    if (geo == NULL)
      {
	  sqlite3_result_null (context);
	  return;
      }
    geo->Srid = -1;
/* one or more closed LINESTINGs are expected */
    if (geo->FirstPoint || geo->FirstPolygon)
	goto invalid;
    if (!geo->FirstLinestring)
	goto invalid;
    ln = geo->FirstLinestring;
    while (ln)
      {
	  gaiaGetPoint (ln->Coords, 0, &x0, &y0);
	  gaiaGetPoint (ln->Coords, ln->Points - 1, &xn, &yn);
	  if (x0 != xn || y0 != yn)
	      goto invalid;
	  ln = ln->Next;
      }
    fnct_polygonize (context, geo, 0);
    return;
  invalid:
    gaiaFreeGeomColl (geo);
    sqlite3_result_null (context);
}

static void
fnct_BdPolyFromWKB2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ BdPolyFromWKB(WKB encoded  LINESTRING)
/
/ returns the current geometry [POLYGON] by parsing a WKB encoded LINESTRING 
/ or NULL if any error is encountered
/
*/
    int n_bytes;
    const unsigned char *wkb;
    gaiaGeomCollPtr geo = NULL;
    gaiaLinestringPtr ln;
    double x0;
    double y0;
    double xn;
    double yn;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_INTEGER)
      {
	  sqlite3_result_null (context);
	  return;
      }
    wkb = sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    if (!check_wkb (wkb, n_bytes, -1))
	return;
    geo = gaiaFromWkb (wkb, n_bytes);
    if (geo == NULL)
      {
	  sqlite3_result_null (context);
	  return;
      }
    geo->Srid = sqlite3_value_int (argv[1]);
/* one or more closed LINESTINGs are expected */
    if (geo->FirstPoint || geo->FirstPolygon)
	goto invalid;
    if (!geo->FirstLinestring)
	goto invalid;
    ln = geo->FirstLinestring;
    while (ln)
      {
	  gaiaGetPoint (ln->Coords, 0, &x0, &y0);
	  gaiaGetPoint (ln->Coords, ln->Points - 1, &xn, &yn);
	  if (x0 != xn || y0 != yn)
	      goto invalid;
	  ln = ln->Next;
      }
    fnct_polygonize (context, geo, 0);
    return;
  invalid:
    gaiaFreeGeomColl (geo);
    sqlite3_result_null (context);
}

static void
fnct_BdMPolyFromWKB1 (sqlite3_context * context, int argc,
		      sqlite3_value ** argv)
{
/* SQL function:
/ BdMPolyFromWKB(WKB encoded  MULTILINESTRING)
/
/ returns the current geometry [MULTIPOLYGON] by parsing a WKB encoded MULTILINESTRING 
/ or NULL if any error is encountered
/
*/
    int n_bytes;
    const unsigned char *wkb;
    gaiaGeomCollPtr geo = NULL;
    gaiaLinestringPtr ln;
    double x0;
    double y0;
    double xn;
    double yn;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    wkb = sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    if (!check_wkb (wkb, n_bytes, -1))
	return;
    geo = gaiaFromWkb (wkb, n_bytes);
    if (geo == NULL)
      {
	  sqlite3_result_null (context);
	  return;
      }
    geo->Srid = -1;
/* one or more closed LINESTINGs are expected */
    if (geo->FirstPoint || geo->FirstPolygon)
	goto invalid;
    if (!geo->FirstLinestring)
	goto invalid;
    ln = geo->FirstLinestring;
    while (ln)
      {
	  gaiaGetPoint (ln->Coords, 0, &x0, &y0);
	  gaiaGetPoint (ln->Coords, ln->Points - 1, &xn, &yn);
	  if (x0 != xn || y0 != yn)
	      goto invalid;
	  ln = ln->Next;
      }
    fnct_polygonize (context, geo, 1);
    return;
  invalid:
    gaiaFreeGeomColl (geo);
    sqlite3_result_null (context);
}

static void
fnct_BdMPolyFromWKB2 (sqlite3_context * context, int argc,
		      sqlite3_value ** argv)
{
/* SQL function:
/ BdMPolyFromWKB(WKB encoded  MULTILINESTRING)
/
/ returns the current geometry [MULTIPOLYGON] by parsing a WKB encoded MULTILINESTRING 
/ or NULL if any error is encountered
/
*/
    int n_bytes;
    const unsigned char *wkb;
    gaiaGeomCollPtr geo = NULL;
    gaiaLinestringPtr ln;
    double x0;
    double y0;
    double xn;
    double yn;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_INTEGER)
      {
	  sqlite3_result_null (context);
	  return;
      }
    wkb = sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    if (!check_wkb (wkb, n_bytes, -1))
	return;
    geo = gaiaFromWkb (wkb, n_bytes);
    if (geo == NULL)
      {
	  sqlite3_result_null (context);
	  return;
      }
    geo->Srid = sqlite3_value_int (argv[1]);
/* one or more closed LINESTINGs are expected */
    if (geo->FirstPoint || geo->FirstPolygon)
	goto invalid;
    if (!geo->FirstLinestring)
	goto invalid;
    ln = geo->FirstLinestring;
    while (ln)
      {
	  gaiaGetPoint (ln->Coords, 0, &x0, &y0);
	  gaiaGetPoint (ln->Coords, ln->Points - 1, &xn, &yn);
	  if (x0 != xn || y0 != yn)
	      goto invalid;
	  ln = ln->Next;
      }
    fnct_polygonize (context, geo, 1);
    return;
  invalid:
    gaiaFreeGeomColl (geo);
    sqlite3_result_null (context);
}

static void
fnct_Polygonize1 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Polygonize(BLOBencoded geometry)
/
/ returns a new geometry [POLYGON or MULTIPOLYGON] representing 
/ the polygonization for current LINESTRING or MULTILINESTRING geometry
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo = NULL;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (geo == NULL)
      {
	  sqlite3_result_null (context);
	  return;
      }
/* one or more LINESTINGs are expected */
    if (geo->FirstPoint || geo->FirstPolygon)
	goto invalid;
    if (!geo->FirstLinestring)
	goto invalid;
    fnct_polygonize (context, geo, 0);
    return;
  invalid:
    gaiaFreeGeomColl (geo);
    sqlite3_result_null (context);
}

static void
fnct_Polygonize2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ Polygonize(BLOBencoded geometry, BOOL force_multipolygon)
/
/ returns a new geometry [POLYGON or MULTIPOLYGON] representing 
/ the polygonization for current LINESTRING or MULTILINESTRING geometry
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geo = NULL;
    int force_multipolygon;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) != SQLITE_INTEGER)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (geo == NULL)
      {
	  sqlite3_result_null (context);
	  return;
      }
    force_multipolygon = sqlite3_value_int (argv[1]);
/* one or more LINESTINGs are expected */
    if (geo->FirstPoint || geo->FirstPolygon)
	goto invalid;
    if (!geo->FirstLinestring)
	goto invalid;
    fnct_polygonize (context, geo, force_multipolygon);
    return;
  invalid:
    gaiaFreeGeomColl (geo);
    sqlite3_result_null (context);
}

#endif /* end including GEOS */

#ifndef OMIT_MATHSQL		/* supporting SQL math functions */

static void
fnct_math_abs (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ abs(double X)
/
/ Returns the absolute value of X
/ or NULL if any error is encountered
*/
    sqlite3_int64 int_value;
    double x;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
      {
	  x = fabs (sqlite3_value_double (argv[0]));
	  sqlite3_result_double (context, x);
      }
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = math_llabs (sqlite3_value_int64 (argv[0]));
	  sqlite3_result_int64 (context, int_value);
      }
    else
	sqlite3_result_null (context);
}

static void
fnct_math_acos (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ acos(double X)
/
/ Returns the arc cosine of X, that is, the value whose cosine is X
/ or NULL if any error is encountered
*/
    int int_value;
    double x;
    GAIA_UNUSED ();
    errno = 0;
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
      {
	  x = acos (sqlite3_value_double (argv[0]));
	  if (errno == EDOM)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_double (context, x);
      }
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
	  x = acos (x);
	  if (errno == EDOM)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_double (context, x);
      }
    else
	sqlite3_result_null (context);
}

static void
fnct_math_asin (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ asin(double X)
/
/ Returns the arc sine of X, that is, the value whose sine is X
/ or NULL if any error is encountered
*/
    int int_value;
    double x;
    GAIA_UNUSED ();
    errno = 0;
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
      {
	  x = asin (sqlite3_value_double (argv[0]));
	  if (errno == EDOM)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_double (context, x);
      }
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
	  x = asin (x);
	  if (errno == EDOM)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_double (context, x);
      }
    else
	sqlite3_result_null (context);
}

static void
fnct_math_atan (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ atan(double X)
/
/ Returns the arc tangent of X, that is, the value whose tangent is X
/ or NULL if any error is encountered
*/
    int int_value;
    double x;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
      {
	  x = atan (sqlite3_value_double (argv[0]));
	  sqlite3_result_double (context, x);
      }
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
	  x = atan (x);
	  sqlite3_result_double (context, x);
      }
    else
	sqlite3_result_null (context);
}

static void
fnct_math_ceil (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ ceil(double X)
/
/ Returns the smallest integer value not less than X
/ or NULL if any error is encountered
*/
    int int_value;
    double x;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
      {
	  x = ceil (sqlite3_value_double (argv[0]));
	  sqlite3_result_double (context, x);
      }
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
	  x = ceil (x);
	  sqlite3_result_double (context, x);
      }
    else
	sqlite3_result_null (context);
}

static void
fnct_math_cos (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ cos(double X)
/
/ Returns the cosine of X, where X is given in radians
/ or NULL if any error is encountered
*/
    int int_value;
    double x;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
      {
	  x = cos (sqlite3_value_double (argv[0]));
	  sqlite3_result_double (context, x);
      }
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
	  x = cos (x);
	  sqlite3_result_double (context, x);
      }
    else
	sqlite3_result_null (context);
}

static void
fnct_math_cot (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ cot(double X)
/
/ Returns the cotangent of X
/ or NULL if any error is encountered
*/
    int int_value;
    double x;
    double tang;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
	x = sqlite3_value_double (argv[0]);
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    tang = tan (x);
    if (tang == 0.0)
      {
	  sqlite3_result_null (context);
	  return;
      }
    x = 1.0 / tang;
    sqlite3_result_double (context, x);
}

static void
fnct_math_degrees (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ degrees(double X)
/
/ Returns the argument X, converted from radians to degrees
/ or NULL if any error is encountered
*/
    int int_value;
    double x;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
	x = sqlite3_value_double (argv[0]);
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    x = x * 57.29577951308232;
    sqlite3_result_double (context, x);
}

static void
fnct_math_exp (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ exp(double X)
/
/ Returns the value of e (the base of natural logarithms) raised to the power of X
/ or NULL if any error is encountered
*/
    int int_value;
    double x;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
      {
	  x = exp (sqlite3_value_double (argv[0]));
	  sqlite3_result_double (context, x);
      }
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
	  x = exp (x);
	  sqlite3_result_double (context, x);
      }
    else
	sqlite3_result_null (context);
}

static void
fnct_math_floor (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ floor(double X)
/
/ Returns the largest integer value not greater than X
/ or NULL if any error is encountered
*/
    int int_value;
    double x;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
      {
	  x = floor (sqlite3_value_double (argv[0]));
	  sqlite3_result_double (context, x);
      }
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
	  x = floor (x);
	  sqlite3_result_double (context, x);
      }
    else
	sqlite3_result_null (context);
}

static void
fnct_math_logn (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ log(double X)
/
/ Returns the natural logarithm of X; that is, the base-e logarithm of X
/ or NULL if any error is encountered
*/
    int int_value;
    double x;
    errno = 0;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
      {
	  x = log (sqlite3_value_double (argv[0]));
	  if (errno == EDOM || errno == ERANGE)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_double (context, x);
      }
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
	  x = log (x);
	  if (errno == EDOM || errno == ERANGE)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_double (context, x);
      }
    else
	sqlite3_result_null (context);
}

static void
fnct_math_logn2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ log(double B, double X)
/
/ Returns the logarithm of X to the base B
/ or NULL if any error is encountered
*/
    int int_value;
    double x = 0.0;
    double b = 1.0;
    double log1;
    double log2;
    errno = 0;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
	x = sqlite3_value_double (argv[0]);
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) == SQLITE_FLOAT)
	b = sqlite3_value_double (argv[1]);
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[1]);
	  b = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (x <= 0.0 || b <= 1.0)
      {
	  sqlite3_result_null (context);
	  return;
      }
    log1 = log (x);
    if (errno == EDOM || errno == ERANGE)
      {
	  sqlite3_result_null (context);
	  return;
      }
    log2 = log (b);
    if (errno == EDOM || errno == ERANGE)
      {
	  sqlite3_result_null (context);
	  return;
      }
    sqlite3_result_double (context, log1 / log2);
}

static void
fnct_math_log_2 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ log2(double X)
/
/ Returns the base-2 logarithm of X
/ or NULL if any error is encountered
*/
    int int_value;
    double x;
    double log1;
    double log2;
    errno = 0;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
	x = sqlite3_value_double (argv[0]);
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    log1 = log (x);
    if (errno == EDOM || errno == ERANGE)
      {
	  sqlite3_result_null (context);
	  return;
      }
    log2 = log (2.0);
    sqlite3_result_double (context, log1 / log2);
}

static void
fnct_math_log_10 (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ log10(double X)
/
/ Returns the base-10 logarithm of X
/ or NULL if any error is encountered
*/
    int int_value;
    double x;
    double log1;
    double log2;
    GAIA_UNUSED ();
    errno = 0;
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
	x = sqlite3_value_double (argv[0]);
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    log1 = log (x);
    if (errno == EDOM || errno == ERANGE)
      {
	  sqlite3_result_null (context);
	  return;
      }
    log2 = log (10.0);
    sqlite3_result_double (context, log1 / log2);
}

static void
fnct_math_pi (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ pi(void)
/
/ Returns the value of (pi)
*/
    GAIA_UNUSED ();
    sqlite3_result_double (context, 3.14159265358979323846);
}

static void
fnct_math_pow (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ pow(double X, double Y)
/
/ Returns the value of X raised to the power of Y.
/ or NULL if any error is encountered
*/
    int int_value;
    double x;
    double y;
    double p;
    GAIA_UNUSED ();
    errno = 0;
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
	x = sqlite3_value_double (argv[0]);
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (sqlite3_value_type (argv[1]) == SQLITE_FLOAT)
	y = sqlite3_value_double (argv[1]);
    else if (sqlite3_value_type (argv[1]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[1]);
	  y = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    p = pow (x, y);
    if (errno == EDOM)
	sqlite3_result_null (context);
    else
	sqlite3_result_double (context, p);
}

static void
fnct_math_stddev_step (sqlite3_context * context, int argc,
		       sqlite3_value ** argv)
{
/* SQL function:
/ stddev_pop(double X)
/ stddev_samp(double X)
/ var_pop(double X)
/ var_samp(double X)
/
/ aggregate function - STEP
/
*/
    struct stddev_str *p;
    int int_value;
    double x;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
	x = sqlite3_value_double (argv[0]);
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
      }
    else
	return;
    p = sqlite3_aggregate_context (context, sizeof (struct stddev_str));
    if (!(p->cleaned))
      {
	  p->cleaned = 1;
	  p->mean = x;
	  p->quot = 0.0;
	  p->count = 0.0;
      }
    p->count += 1.0;
    p->quot =
	p->quot +
	(((p->count - 1.0) * ((x - p->mean) * (x - p->mean))) / p->count);
    p->mean = p->mean + ((x - p->mean) / p->count);
}

static void
fnct_math_stddev_pop_final (sqlite3_context * context)
{
/* SQL function:
/ stddev_pop(double X)
/ aggregate function -  FINAL
/
*/
    double x;
    struct stddev_str *p = sqlite3_aggregate_context (context, 0);
    if (!p)
      {
	  sqlite3_result_null (context);
	  return;
      }
    x = sqrt (p->quot / (p->count - 1.0));
    sqlite3_result_double (context, x);
}

static void
fnct_math_stddev_samp_final (sqlite3_context * context)
{
/* SQL function:
/ stddev_samp(double X)
/ aggregate function -  FINAL
/
*/
    double x;
    struct stddev_str *p = sqlite3_aggregate_context (context, 0);
    if (!p)
      {
	  sqlite3_result_null (context);
	  return;
      }
    x = sqrt (p->quot / p->count);
    sqlite3_result_double (context, x);
}

static void
fnct_math_var_pop_final (sqlite3_context * context)
{
/* SQL function:
/ var_pop(double X)
/ aggregate function -  FINAL
/
*/
    double x;
    struct stddev_str *p = sqlite3_aggregate_context (context, 0);
    if (!p)
      {
	  sqlite3_result_null (context);
	  return;
      }
    x = p->quot / (p->count - 1.0);
    sqlite3_result_double (context, x);
}

static void
fnct_math_var_samp_final (sqlite3_context * context)
{
/* SQL function:
/ var_samp(double X)
/ aggregate function -  FINAL
/
*/
    double x;
    struct stddev_str *p = sqlite3_aggregate_context (context, 0);
    if (!p)
      {
	  sqlite3_result_null (context);
	  return;
      }
    x = p->quot / p->count;
    sqlite3_result_double (context, x);
}

static void
fnct_math_radians (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ radians(double X)
/
/ Returns the argument X, converted from degrees to radians
/ or NULL if any error is encountered
*/
    int int_value;
    double x;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
	x = sqlite3_value_double (argv[0]);
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    x = x * .0174532925199432958;
    sqlite3_result_double (context, x);
}


static void
fnct_math_round (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ round(double X)
/
/ Returns the the nearest integer, but round halfway cases away from zero
/ or NULL if any error is encountered
*/
    int int_value;
    double x;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
      {
	  x = math_round (sqlite3_value_double (argv[0]));
	  sqlite3_result_double (context, x);
      }
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
	  x = math_round (x);
	  sqlite3_result_double (context, x);
      }
    else
	sqlite3_result_null (context);
}

static void
fnct_math_sign (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ sign(double X)
/
/ Returns the sign of the argument as -1, 0, or 1, depending on whether X is negative, zero, or positive
/ or NULL if any error is encountered
*/
    int int_value;
    double x;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
	x = sqlite3_value_double (argv[0]);
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (x > 0.0)
	sqlite3_result_double (context, 1.0);
    else if (x < 0.0)
	sqlite3_result_double (context, -1.0);
    else
	sqlite3_result_double (context, 0.0);
}

static void
fnct_math_sin (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ sin(double X)
/
/ Returns the sine of X, where X is given in radians
/ or NULL if any error is encountered
*/
    int int_value;
    double x;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
      {
	  x = sin (sqlite3_value_double (argv[0]));
	  sqlite3_result_double (context, x);
      }
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
	  x = sin (x);
	  sqlite3_result_double (context, x);
      }
    else
	sqlite3_result_null (context);
}

static void
fnct_math_sqrt (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ sqrt(double X)
/
/ Returns the square root of a non-negative number X
/ or NULL if any error is encountered
*/
    int int_value;
    double x;
    GAIA_UNUSED ();
    errno = 0;
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
      {
	  x = sqrt (sqlite3_value_double (argv[0]));
	  if (errno)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_double (context, x);
      }
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
	  x = sqrt (x);
	  if (errno == EDOM)
	      sqlite3_result_null (context);
	  else
	      sqlite3_result_double (context, x);
      }
    else
	sqlite3_result_null (context);
}

static void
fnct_math_tan (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ tan(double X)
/
/ Returns the tangent of X, where X is given in radians
/ or NULL if any error is encountered
*/
    int int_value;
    double x;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
      {
	  x = tan (sqlite3_value_double (argv[0]));
	  sqlite3_result_double (context, x);
      }
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  x = int_value;
	  x = tan (x);
	  sqlite3_result_double (context, x);
      }
    else
	sqlite3_result_null (context);
}

#endif /* end supporting SQL math functions */

static void
fnct_GeomFromExifGpsBlob (sqlite3_context * context, int argc,
			  sqlite3_value ** argv)
{
/* SQL function:
/ GeomFromExifGpsBlob(BLOB encoded image)
/
/ returns:
/ a POINT geometry
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    gaiaGeomCollPtr geom;
    unsigned char *geoblob;
    int geosize;
    double longitude;
    double latitude;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    if (gaiaGetGpsCoords (p_blob, n_bytes, &longitude, &latitude))
      {
	  geom = gaiaAllocGeomColl ();
	  geom->Srid = 4326;
	  gaiaAddPointToGeomColl (geom, longitude, latitude);
	  gaiaToSpatiaLiteBlobWkb (geom, &geoblob, &geosize);
	  gaiaFreeGeomColl (geom);
	  sqlite3_result_blob (context, geoblob, geosize, free);
      }
    else
	sqlite3_result_null (context);
}

static void
blob_guess (sqlite3_context * context, int argc, sqlite3_value ** argv,
	    int request)
{
/* SQL function:
/ IsGifBlob(BLOB encoded image)
/ IsPngBlob, IsJpegBlob, IsExifBlob, IsExifGpsBlob, IsTiffBlob,
/ IsWaveletBlob, IsZipBlob, IsPdfBlob,IsGeometryBlob
/
/ returns:
/ 1 if the required BLOB_TYPE is TRUE
/ 0 otherwise
/ or -1 if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    int blob_type;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_int (context, -1);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    blob_type = gaiaGuessBlobType (p_blob, n_bytes);
    if (request == GAIA_GEOMETRY_BLOB)
      {
	  if (blob_type == GAIA_GEOMETRY_BLOB)
	      sqlite3_result_int (context, 1);
	  else
	      sqlite3_result_int (context, 0);
	  return;
      }
    if (request == GAIA_ZIP_BLOB)
      {
	  if (blob_type == GAIA_ZIP_BLOB)
	      sqlite3_result_int (context, 1);
	  else
	      sqlite3_result_int (context, 0);
	  return;
      }
    if (request == GAIA_PDF_BLOB)
      {
	  if (blob_type == GAIA_PDF_BLOB)
	      sqlite3_result_int (context, 1);
	  else
	      sqlite3_result_int (context, 0);
	  return;
      }
    if (request == GAIA_WAVELET_BLOB)
      {
	  if (blob_type == GAIA_WAVELET_BLOB)
	      sqlite3_result_int (context, 1);
	  else
	      sqlite3_result_int (context, 0);
	  return;
      }
    if (request == GAIA_TIFF_BLOB)
      {
	  if (blob_type == GAIA_TIFF_BLOB)
	      sqlite3_result_int (context, 1);
	  else
	      sqlite3_result_int (context, 0);
	  return;
      }
    if (request == GAIA_GIF_BLOB)
      {
	  if (blob_type == GAIA_GIF_BLOB)
	      sqlite3_result_int (context, 1);
	  else
	      sqlite3_result_int (context, 0);
	  return;
      }
    if (request == GAIA_PNG_BLOB)
      {
	  if (blob_type == GAIA_PNG_BLOB)
	      sqlite3_result_int (context, 1);
	  else
	      sqlite3_result_int (context, 0);
	  return;
      }
    if (request == GAIA_JPEG_BLOB)
      {
	  if (blob_type == GAIA_JPEG_BLOB || blob_type == GAIA_EXIF_BLOB
	      || blob_type == GAIA_EXIF_GPS_BLOB)
	      sqlite3_result_int (context, 1);
	  else
	      sqlite3_result_int (context, 0);
	  return;
      }
    if (request == GAIA_EXIF_BLOB)
      {
	  if (blob_type == GAIA_EXIF_BLOB || blob_type == GAIA_EXIF_GPS_BLOB)
	    {
		sqlite3_result_int (context, 1);
	    }
	  else
	      sqlite3_result_int (context, 0);
	  return;
      }
    if (request == GAIA_EXIF_GPS_BLOB)
      {
	  if (blob_type == GAIA_EXIF_GPS_BLOB)
	    {
		sqlite3_result_int (context, 1);
	    }
	  else
	      sqlite3_result_int (context, 0);
	  return;
      }
    sqlite3_result_int (context, -1);
}

/*
/ the following functions simply readdress the blob_guess()
/ setting the appropriate request mode
*/

static void
fnct_IsGeometryBlob (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    blob_guess (context, argc, argv, GAIA_GEOMETRY_BLOB);
}

static void
fnct_IsZipBlob (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    blob_guess (context, argc, argv, GAIA_ZIP_BLOB);
}

static void
fnct_IsPdfBlob (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    blob_guess (context, argc, argv, GAIA_PDF_BLOB);
}

static void
fnct_IsWaveletBlob (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    blob_guess (context, argc, argv, GAIA_WAVELET_BLOB);
}

static void
fnct_IsTiffBlob (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    blob_guess (context, argc, argv, GAIA_TIFF_BLOB);
}

static void
fnct_IsGifBlob (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    blob_guess (context, argc, argv, GAIA_GIF_BLOB);
}

static void
fnct_IsPngBlob (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    blob_guess (context, argc, argv, GAIA_PNG_BLOB);
}

static void
fnct_IsJpegBlob (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    blob_guess (context, argc, argv, GAIA_JPEG_BLOB);
}

static void
fnct_IsExifBlob (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    blob_guess (context, argc, argv, GAIA_EXIF_BLOB);
}

static void
fnct_IsExifGpsBlob (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    blob_guess (context, argc, argv, GAIA_EXIF_GPS_BLOB);
}

static void
fnct_GeodesicLength (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
/* SQL function:
/ GeodesicLength(BLOB encoded GEOMETRYCOLLECTION)
/
/ returns  the total Geodesic length for current geometry 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    double l;
    double length = 0.0;
    int ret;
    double a;
    double b;
    double rf;
    gaiaGeomCollPtr geo = NULL;
    gaiaLinestringPtr line;
    gaiaPolygonPtr polyg;
    gaiaRingPtr ring;
    int ib;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  if (get_ellipse_params (sqlite, geo->Srid, &a, &b, &rf))
	    {
		line = geo->FirstLinestring;
		while (line)
		  {
		      /* Linestrings */
		      l = gaiaGeodesicTotalLength (a, b, rf, line->Coords,
						   line->Points);
		      if (l < 0.0)
			{
			    length = -1.0;
			    break;
			}
		      length += l;
		      line = line->Next;
		  }
		if (length >= 0)
		  {
		      /* Polygons */
		      polyg = geo->FirstPolygon;
		      while (polyg)
			{
			    /* exterior Ring */
			    ring = polyg->Exterior;
			    l = gaiaGeodesicTotalLength (a, b, rf, ring->Coords,
							 ring->Points);
			    if (l < 0.0)
			      {
				  length = -1.0;
				  break;
			      }
			    length += l;
			    for (ib = 0; ib < polyg->NumInteriors; ib++)
			      {
				  /* interior Rings */
				  ring = polyg->Interiors + ib;
				  l = gaiaGeodesicTotalLength (a, b, rf,
							       ring->Coords,
							       ring->Points);
				  if (l < 0.0)
				    {
					length = -1.0;
					break;
				    }
				  length += l;
			      }
			    if (length < 0.0)
				break;
			    polyg = polyg->Next;
			}
		  }
		if (length < 0.0)
		    sqlite3_result_null (context);
		else
		    sqlite3_result_double (context, length);
	    }
	  else
	      sqlite3_result_null (context);
	  gaiaFreeGeomColl (geo);
      }
}

static void
fnct_GreatCircleLength (sqlite3_context * context, int argc,
			sqlite3_value ** argv)
{
/* SQL function:
/ GreatCircleLength(BLOB encoded GEOMETRYCOLLECTION)
/
/ returns  the total Great Circle length for current geometry 
/ or NULL if any error is encountered
*/
    unsigned char *p_blob;
    int n_bytes;
    double l;
    double length = 0.0;
    int ret;
    double a;
    double b;
    double rf;
    gaiaGeomCollPtr geo = NULL;
    gaiaLinestringPtr line;
    gaiaPolygonPtr polyg;
    gaiaRingPtr ring;
    int ib;
    sqlite3 *sqlite = sqlite3_context_db_handle (context);
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) != SQLITE_BLOB)
      {
	  sqlite3_result_null (context);
	  return;
      }
    p_blob = (unsigned char *) sqlite3_value_blob (argv[0]);
    n_bytes = sqlite3_value_bytes (argv[0]);
    geo = gaiaFromSpatiaLiteBlobWkb (p_blob, n_bytes);
    if (!geo)
	sqlite3_result_null (context);
    else
      {
	  if (get_ellipse_params (sqlite, geo->Srid, &a, &b, &rf))
	    {
		line = geo->FirstLinestring;
		while (line)
		  {
		      /* Linestrings */
		      length +=
			  gaiaGreatCircleTotalLength (a, b, rf, line->Coords,
						      line->Points);
		      line = line->Next;
		  }
		if (length >= 0)
		  {
		      /* Polygons */
		      polyg = geo->FirstPolygon;
		      while (polyg)
			{
			    /* exterior Ring */
			    ring = polyg->Exterior;
			    length +=
				gaiaGreatCircleTotalLength (a, b, rf,
							    ring->Coords,
							    ring->Points);
			    for (ib = 0; ib < polyg->NumInteriors; ib++)
			      {
				  /* interior Rings */
				  ring = polyg->Interiors + ib;
				  length +=
				      gaiaGreatCircleTotalLength (a, b, rf,
								  ring->Coords,
								  ring->Points);
			      }
			    polyg = polyg->Next;
			}
		  }
		sqlite3_result_double (context, length);
	    }
	  else
	      sqlite3_result_null (context);
	  gaiaFreeGeomColl (geo);
      }
}

static void
convertUnit (sqlite3_context * context, int argc, sqlite3_value ** argv,
	     int unit_from, int unit_to)
{
/* SQL functions:
/ CvtToKm(), CvtToDm(), CvtToCm(), CvtToMm(), CvtToKmi(), CvtToIn(), CvtToYd(),
/ CvtToMi(), CvtToFath(), CvtToCh(), CvtToUsIn(), CvtToUsFt(), CvtToUsYd(),
/ CvtToUsMi(), CvtToIndFt(), CvtToIndYd(), CvtToIndCh(), 
/ CvtFromKm(), CvtFromDm(), CvtFromCm(), CvtFromMm(), CvtFromKmi(), 
/ CvtFromIn(), CvtFromYd(), CvtFromMi(), CvtFromFath(), CvtFromCh(), CvtFromUsIn(),
/ CvtFromUsFt(), CvtFromUsYd(), CvtFromUsMi(), CvtFromIndFt(), CvtFromIndYd(), 
/ CvtFromIndCh()
/
/ converts a Length from one unit to a different one
/ or NULL if any error is encountered
*/
    double cvt;
    double value;
    int int_value;
    GAIA_UNUSED ();
    if (sqlite3_value_type (argv[0]) == SQLITE_FLOAT)
	value = sqlite3_value_double (argv[0]);
    else if (sqlite3_value_type (argv[0]) == SQLITE_INTEGER)
      {
	  int_value = sqlite3_value_int (argv[0]);
	  value = int_value;
      }
    else
      {
	  sqlite3_result_null (context);
	  return;
      }
    if (!gaiaConvertLength (value, unit_from, unit_to, &cvt))
	sqlite3_result_null (context);
    else
	sqlite3_result_double (context, cvt);
}

static void
fnct_cvtToKm (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_M, GAIA_KM);
}

static void
fnct_cvtToDm (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_M, GAIA_DM);
}

static void
fnct_cvtToCm (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_M, GAIA_CM);
}

static void
fnct_cvtToMm (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_M, GAIA_MM);
}

static void
fnct_cvtToKmi (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_M, GAIA_KMI);
}

static void
fnct_cvtToIn (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_M, GAIA_IN);
}

static void
fnct_cvtToYd (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_M, GAIA_YD);
}

static void
fnct_cvtToMi (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_M, GAIA_MI);
}

static void
fnct_cvtToFath (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_M, GAIA_FATH);
}

static void
fnct_cvtToCh (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_M, GAIA_CH);
}

static void
fnct_cvtToUsIn (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_M, GAIA_US_IN);
}

static void
fnct_cvtToUsFt (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_M, GAIA_US_FT);
}

static void
fnct_cvtToUsYd (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_M, GAIA_US_YD);
}

static void
fnct_cvtToUsMi (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_M, GAIA_US_MI);
}

static void
fnct_cvtToIndFt (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_M, GAIA_IND_FT);
}

static void
fnct_cvtToIndYd (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_M, GAIA_IND_YD);
}

static void
fnct_cvtToIndCh (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_M, GAIA_IND_CH);
}

static void
fnct_cvtFromKm (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_KM, GAIA_M);
}

static void
fnct_cvtFromDm (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_DM, GAIA_M);
}

static void
fnct_cvtFromCm (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_CM, GAIA_M);
}

static void
fnct_cvtFromMm (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_MM, GAIA_M);
}

static void
fnct_cvtFromKmi (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_KMI, GAIA_M);
}

static void
fnct_cvtFromIn (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_IN, GAIA_M);
}

static void
fnct_cvtFromYd (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_YD, GAIA_M);
}

static void
fnct_cvtFromMi (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_MI, GAIA_M);
}

static void
fnct_cvtFromFath (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_FATH, GAIA_M);
}

static void
fnct_cvtFromCh (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_CH, GAIA_M);
}

static void
fnct_cvtFromUsIn (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_US_IN, GAIA_M);
}

static void
fnct_cvtFromUsFt (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_US_FT, GAIA_M);
}

static void
fnct_cvtFromUsYd (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_US_YD, GAIA_M);
}

static void
fnct_cvtFromUsMi (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_US_MI, GAIA_M);
}

static void
fnct_cvtFromIndFt (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_IND_FT, GAIA_M);
}

static void
fnct_cvtFromIndYd (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_IND_YD, GAIA_M);
}

static void
fnct_cvtFromIndCh (sqlite3_context * context, int argc, sqlite3_value ** argv)
{
    convertUnit (context, argc, argv, GAIA_IND_CH, GAIA_M);
}

static void
init_static_spatialite (sqlite3 * db, char **pzErrMsg,
			const sqlite3_api_routines * pApi)
{
    SQLITE_EXTENSION_INIT2 (pApi);
/* setting the POSIX locale for numeric */
    setlocale (LC_NUMERIC, "POSIX");
    *pzErrMsg = NULL;
    sqlite3_create_function (db, "spatialite_version", 0, SQLITE_ANY, 0,
			     fnct_spatialite_version, 0, 0);
    sqlite3_create_function (db, "proj4_version", 0, SQLITE_ANY, 0,
			     fnct_proj4_version, 0, 0);
    sqlite3_create_function (db, "geos_version", 0, SQLITE_ANY, 0,
			     fnct_geos_version, 0, 0);
    sqlite3_create_function (db, "GeometryConstraints", 3, SQLITE_ANY, 0,
			     fnct_GeometryConstraints, 0, 0);
    sqlite3_create_function (db, "CheckSpatialMetaData", 0, SQLITE_ANY, 0,
			     fnct_CheckSpatialMetaData, 0, 0);
    sqlite3_create_function (db, "AutoFDOStart", 0, SQLITE_ANY, 0,
			     fnct_AutoFDOStart, 0, 0);
    sqlite3_create_function (db, "AutoFDOStop", 0, SQLITE_ANY, 0,
			     fnct_AutoFDOStop, 0, 0);
    sqlite3_create_function (db, "InitFDOSpatialMetaData", 0, SQLITE_ANY, 0,
			     fnct_InitFDOSpatialMetaData, 0, 0);
    sqlite3_create_function (db, "AddFDOGeometryColumn", 6, SQLITE_ANY, 0,
			     fnct_AddFDOGeometryColumn, 0, 0);
    sqlite3_create_function (db, "RecoverFDOGeometryColumn", 6, SQLITE_ANY, 0,
			     fnct_RecoverFDOGeometryColumn, 0, 0);
    sqlite3_create_function (db, "DiscardFDOGeometryColumn", 2, SQLITE_ANY, 0,
			     fnct_DiscardFDOGeometryColumn, 0, 0);
    sqlite3_create_function (db, "InitSpatialMetaData", 0, SQLITE_ANY, 0,
			     fnct_InitSpatialMetaData, 0, 0);
    sqlite3_create_function (db, "AddGeometryColumn", 5, SQLITE_ANY, 0,
			     fnct_AddGeometryColumn, 0, 0);
    sqlite3_create_function (db, "AddGeometryColumn", 6, SQLITE_ANY, 0,
			     fnct_AddGeometryColumn, 0, 0);
    sqlite3_create_function (db, "RecoverGeometryColumn", 5, SQLITE_ANY, 0,
			     fnct_RecoverGeometryColumn, 0, 0);
    sqlite3_create_function (db, "DiscardGeometryColumn", 2, SQLITE_ANY, 0,
			     fnct_DiscardGeometryColumn, 0, 0);
    sqlite3_create_function (db, "CreateSpatialIndex", 2, SQLITE_ANY, 0,
			     fnct_CreateSpatialIndex, 0, 0);
    sqlite3_create_function (db, "CreateMbrCache", 2, SQLITE_ANY, 0,
			     fnct_CreateMbrCache, 0, 0);
    sqlite3_create_function (db, "DisableSpatialIndex", 2, SQLITE_ANY, 0,
			     fnct_DisableSpatialIndex, 0, 0);
    sqlite3_create_function (db, "RebuildGeometryTriggers", 2, SQLITE_ANY, 0,
			     fnct_RebuildGeometryTriggers, 0, 0);
    sqlite3_create_function (db, "AsText", 1, SQLITE_ANY, 0, fnct_AsText, 0, 0);
    sqlite3_create_function (db, "AsSvg", 1, SQLITE_ANY, 0, fnct_AsSvg1, 0, 0);
    sqlite3_create_function (db, "AsSvg", 2, SQLITE_ANY, 0, fnct_AsSvg2, 0, 0);
    sqlite3_create_function (db, "AsSvg", 3, SQLITE_ANY, 0, fnct_AsSvg3, 0, 0);
    sqlite3_create_function (db, "AsFGF", 2, SQLITE_ANY, 0, fnct_AsFGF, 0, 0);
    sqlite3_create_function (db, "AsBinary", 1, SQLITE_ANY, 0, fnct_AsBinary, 0,
			     0);
    sqlite3_create_function (db, "GeomFromText", 1, SQLITE_ANY, 0,
			     fnct_GeomFromText1, 0, 0);
    sqlite3_create_function (db, "GeomFromText", 2, SQLITE_ANY, 0,
			     fnct_GeomFromText2, 0, 0);
    sqlite3_create_function (db, "GeometryFromText", 1, SQLITE_ANY, 0,
			     fnct_GeomFromText1, 0, 0);
    sqlite3_create_function (db, "GeometryFromText", 2, SQLITE_ANY, 0,
			     fnct_GeomFromText2, 0, 0);
    sqlite3_create_function (db, "GeomCollFromText", 1, SQLITE_ANY, 0,
			     fnct_GeomCollFromText1, 0, 0);
    sqlite3_create_function (db, "GeomCollFromText", 2, SQLITE_ANY, 0,
			     fnct_GeomCollFromText2, 0, 0);
    sqlite3_create_function (db, "GeometryCollectionFromText", 1, SQLITE_ANY, 0,
			     fnct_GeomCollFromText1, 0, 0);
    sqlite3_create_function (db, "GeometryCollectionFromText", 2, SQLITE_ANY, 0,
			     fnct_GeomCollFromText2, 0, 0);
    sqlite3_create_function (db, "PointFromText", 1, SQLITE_ANY, 0,
			     fnct_PointFromText1, 0, 0);
    sqlite3_create_function (db, "PointFromText", 2, SQLITE_ANY, 0,
			     fnct_PointFromText2, 0, 0);
    sqlite3_create_function (db, "LineFromText", 1, SQLITE_ANY, 0,
			     fnct_LineFromText1, 0, 0);
    sqlite3_create_function (db, "LineFromText", 2, SQLITE_ANY, 0,
			     fnct_LineFromText2, 0, 0);
    sqlite3_create_function (db, "LineStringFromText", 1, SQLITE_ANY, 0,
			     fnct_LineFromText1, 0, 0);
    sqlite3_create_function (db, "LineStringFromText", 2, SQLITE_ANY, 0,
			     fnct_LineFromText2, 0, 0);
    sqlite3_create_function (db, "PolyFromText", 1, SQLITE_ANY, 0,
			     fnct_PolyFromText1, 0, 0);
    sqlite3_create_function (db, "PolyFromText", 2, SQLITE_ANY, 0,
			     fnct_PolyFromText2, 0, 0);
    sqlite3_create_function (db, "PolygonFromText", 1, SQLITE_ANY, 0,
			     fnct_PolyFromText1, 0, 0);
    sqlite3_create_function (db, "PolygonFromText", 2, SQLITE_ANY, 0,
			     fnct_PolyFromText2, 0, 0);
    sqlite3_create_function (db, "MPointFromText", 1, SQLITE_ANY, 0,
			     fnct_MPointFromText1, 0, 0);
    sqlite3_create_function (db, "MPointFromText", 2, SQLITE_ANY, 0,
			     fnct_MPointFromText2, 0, 0);
    sqlite3_create_function (db, "MultiPointFromText", 1, SQLITE_ANY, 0,
			     fnct_MPointFromText1, 0, 0);
    sqlite3_create_function (db, "MultiPointFromText", 2, SQLITE_ANY, 0,
			     fnct_MPointFromText2, 0, 0);
    sqlite3_create_function (db, "MLineFromText", 1, SQLITE_ANY, 0,
			     fnct_MLineFromText1, 0, 0);
    sqlite3_create_function (db, "MLineFromText", 2, SQLITE_ANY, 0,
			     fnct_MLineFromText2, 0, 0);
    sqlite3_create_function (db, "MultiLineStringFromText", 1, SQLITE_ANY, 0,
			     fnct_MLineFromText1, 0, 0);
    sqlite3_create_function (db, "MultiLineStringFromText", 2, SQLITE_ANY, 0,
			     fnct_MLineFromText2, 0, 0);
    sqlite3_create_function (db, "MPolyFromText", 1, SQLITE_ANY, 0,
			     fnct_MPolyFromText1, 0, 0);
    sqlite3_create_function (db, "MPolyFromText", 2, SQLITE_ANY, 0,
			     fnct_MPolyFromText2, 0, 0);
    sqlite3_create_function (db, "MultiPolygonFromText", 1, SQLITE_ANY, 0,
			     fnct_MPolyFromText1, 0, 0);
    sqlite3_create_function (db, "MultiPolygonFromText", 2, SQLITE_ANY, 0,
			     fnct_MPolyFromText2, 0, 0);
    sqlite3_create_function (db, "GeomFromWKB", 1, SQLITE_ANY, 0,
			     fnct_GeomFromWkb1, 0, 0);
    sqlite3_create_function (db, "GeomFromWKB", 2, SQLITE_ANY, 0,
			     fnct_GeomFromWkb2, 0, 0);
    sqlite3_create_function (db, "GeometryFromWKB", 1, SQLITE_ANY, 0,
			     fnct_GeomFromWkb1, 0, 0);
    sqlite3_create_function (db, "GeometryFromWKB", 2, SQLITE_ANY, 0,
			     fnct_GeomFromWkb2, 0, 0);
    sqlite3_create_function (db, "GeomCollFromWKB", 1, SQLITE_ANY, 0,
			     fnct_GeomCollFromWkb1, 0, 0);
    sqlite3_create_function (db, "GeomCollFromWKB", 2, SQLITE_ANY, 0,
			     fnct_GeomCollFromWkb2, 0, 0);
    sqlite3_create_function (db, "GeometryCollectionFromWKB", 1, SQLITE_ANY, 0,
			     fnct_GeomCollFromWkb1, 0, 0);
    sqlite3_create_function (db, "GeometryCollectionFromWKB", 2, SQLITE_ANY, 0,
			     fnct_GeomCollFromWkb2, 0, 0);
    sqlite3_create_function (db, "PointFromWKB", 1, SQLITE_ANY, 0,
			     fnct_PointFromWkb1, 0, 0);
    sqlite3_create_function (db, "PointFromWKB", 2, SQLITE_ANY, 0,
			     fnct_PointFromWkb2, 0, 0);
    sqlite3_create_function (db, "LineFromWKB", 1, SQLITE_ANY, 0,
			     fnct_LineFromWkb1, 0, 0);
    sqlite3_create_function (db, "LineFromWKB", 2, SQLITE_ANY, 0,
			     fnct_LineFromWkb2, 0, 0);
    sqlite3_create_function (db, "LineStringFromWKB", 1, SQLITE_ANY, 0,
			     fnct_LineFromWkb1, 0, 0);
    sqlite3_create_function (db, "LineStringFromWKB", 2, SQLITE_ANY, 0,
			     fnct_LineFromWkb2, 0, 0);
    sqlite3_create_function (db, "PolyFromWKB", 1, SQLITE_ANY, 0,
			     fnct_PolyFromWkb1, 0, 0);
    sqlite3_create_function (db, "PolyFromWKB", 2, SQLITE_ANY, 0,
			     fnct_PolyFromWkb2, 0, 0);
    sqlite3_create_function (db, "PolygonFromWKB", 1, SQLITE_ANY, 0,
			     fnct_PolyFromWkb1, 0, 0);
    sqlite3_create_function (db, "PolygonFromWKB", 2, SQLITE_ANY, 0,
			     fnct_PolyFromWkb2, 0, 0);
    sqlite3_create_function (db, "MPointFromWKB", 1, SQLITE_ANY, 0,
			     fnct_MPointFromWkb1, 0, 0);
    sqlite3_create_function (db, "MPointFromWKB", 2, SQLITE_ANY, 0,
			     fnct_MPointFromWkb2, 0, 0);
    sqlite3_create_function (db, "MultiPointFromWKB", 1, SQLITE_ANY, 0,
			     fnct_MPointFromWkb1, 0, 0);
    sqlite3_create_function (db, "MultiPointFromWKB", 2, SQLITE_ANY, 0,
			     fnct_MPointFromWkb2, 0, 0);
    sqlite3_create_function (db, "MLineFromWKB", 1, SQLITE_ANY, 0,
			     fnct_MLineFromWkb1, 0, 0);
    sqlite3_create_function (db, "MLineFromWKB", 2, SQLITE_ANY, 0,
			     fnct_MLineFromWkb2, 0, 0);
    sqlite3_create_function (db, "MultiLineStringFromWKB", 1, SQLITE_ANY, 0,
			     fnct_MLineFromWkb1, 0, 0);
    sqlite3_create_function (db, "MultiLineStringFromWKB", 2, SQLITE_ANY, 0,
			     fnct_MLineFromWkb2, 0, 0);
    sqlite3_create_function (db, "MPolyFromWKB", 1, SQLITE_ANY, 0,
			     fnct_MPolyFromWkb1, 0, 0);
    sqlite3_create_function (db, "MPolyFromWKB", 2, SQLITE_ANY, 0,
			     fnct_MPolyFromWkb2, 0, 0);
    sqlite3_create_function (db, "MultiPolygonFromWKB", 1, SQLITE_ANY, 0,
			     fnct_MPolyFromWkb1, 0, 0);
    sqlite3_create_function (db, "MultiPolygonFromWKB", 2, SQLITE_ANY, 0,
			     fnct_MPolyFromWkb2, 0, 0);
    sqlite3_create_function (db, "GeomFromFGF", 1, SQLITE_ANY, 0,
			     fnct_GeometryFromFGF1, 0, 0);
    sqlite3_create_function (db, "GeomFromFGF", 2, SQLITE_ANY, 0,
			     fnct_GeometryFromFGF2, 0, 0);
    sqlite3_create_function (db, "Dimension", 1, SQLITE_ANY, 0, fnct_Dimension,
			     0, 0);
    sqlite3_create_function (db, "GeometryType", 1, SQLITE_ANY, 0,
			     fnct_GeometryType, 0, 0);
    sqlite3_create_function (db, "GeometryAliasType", 1, SQLITE_ANY, 0,
			     fnct_GeometryAliasType, 0, 0);
    sqlite3_create_function (db, "SRID", 1, SQLITE_ANY, 0, fnct_SRID, 0, 0);
    sqlite3_create_function (db, "SetSRID", 2, SQLITE_ANY, 0, fnct_SetSRID, 0,
			     0);
    sqlite3_create_function (db, "IsEmpty", 1, SQLITE_ANY, 0, fnct_IsEmpty, 0,
			     0);
    sqlite3_create_function (db, "Envelope", 1, SQLITE_ANY, 0, fnct_Envelope, 0,
			     0);
    sqlite3_create_function (db, "X", 1, SQLITE_ANY, 0, fnct_X, 0, 0);
    sqlite3_create_function (db, "Y", 1, SQLITE_ANY, 0, fnct_Y, 0, 0);
    sqlite3_create_function (db, "NumPoints", 1, SQLITE_ANY, 0, fnct_NumPoints,
			     0, 0);
    sqlite3_create_function (db, "StartPoint", 1, SQLITE_ANY, 0,
			     fnct_StartPoint, 0, 0);
    sqlite3_create_function (db, "EndPoint", 1, SQLITE_ANY, 0, fnct_EndPoint, 0,
			     0);
    sqlite3_create_function (db, "PointN", 2, SQLITE_ANY, 0, fnct_PointN, 0, 0);
    sqlite3_create_function (db, "ExteriorRing", 1, SQLITE_ANY, 0,
			     fnct_ExteriorRing, 0, 0);
    sqlite3_create_function (db, "NumInteriorRing", 1, SQLITE_ANY, 0,
			     fnct_NumInteriorRings, 0, 0);
    sqlite3_create_function (db, "NumInteriorRings", 1, SQLITE_ANY, 0,
			     fnct_NumInteriorRings, 0, 0);
    sqlite3_create_function (db, "InteriorRingN", 2, SQLITE_ANY, 0,
			     fnct_InteriorRingN, 0, 0);
    sqlite3_create_function (db, "NumGeometries", 1, SQLITE_ANY, 0,
			     fnct_NumGeometries, 0, 0);
    sqlite3_create_function (db, "GeometryN", 2, SQLITE_ANY, 0, fnct_GeometryN,
			     0, 0);
    sqlite3_create_function (db, "MBRContains", 2, SQLITE_ANY, 0,
			     fnct_MbrContains, 0, 0);
    sqlite3_create_function (db, "MbrDisjoint", 2, SQLITE_ANY, 0,
			     fnct_MbrDisjoint, 0, 0);
    sqlite3_create_function (db, "MBREqual", 2, SQLITE_ANY, 0, fnct_MbrEqual, 0,
			     0);
    sqlite3_create_function (db, "MbrIntersects", 2, SQLITE_ANY, 0,
			     fnct_MbrIntersects, 0, 0);
    sqlite3_create_function (db, "MBROverlaps", 2, SQLITE_ANY, 0,
			     fnct_MbrOverlaps, 0, 0);
    sqlite3_create_function (db, "MbrTouches", 2, SQLITE_ANY, 0,
			     fnct_MbrTouches, 0, 0);
    sqlite3_create_function (db, "MbrWithin", 2, SQLITE_ANY, 0, fnct_MbrWithin,
			     0, 0);
    sqlite3_create_function (db, "ShiftCoords", 3, SQLITE_ANY, 0,
			     fnct_ShiftCoords, 0, 0);
    sqlite3_create_function (db, "ShiftCoordinates", 3, SQLITE_ANY, 0,
			     fnct_ShiftCoords, 0, 0);
    sqlite3_create_function (db, "ScaleCoords", 2, SQLITE_ANY, 0,
			     fnct_ScaleCoords, 0, 0);
    sqlite3_create_function (db, "ScaleCoordinates", 2, SQLITE_ANY, 0,
			     fnct_ScaleCoords, 0, 0);
    sqlite3_create_function (db, "ScaleCoords", 3, SQLITE_ANY, 0,
			     fnct_ScaleCoords, 0, 0);
    sqlite3_create_function (db, "ScaleCoordinates", 3, SQLITE_ANY, 0,
			     fnct_ScaleCoords, 0, 0);
    sqlite3_create_function (db, "RotateCoords", 2, SQLITE_ANY, 0,
			     fnct_RotateCoords, 0, 0);
    sqlite3_create_function (db, "RotateCoordinates", 2, SQLITE_ANY, 0,
			     fnct_RotateCoords, 0, 0);
    sqlite3_create_function (db, "ReflectCoords", 3, SQLITE_ANY, 0,
			     fnct_ReflectCoords, 0, 0);
    sqlite3_create_function (db, "ReflectCoordinates", 3, SQLITE_ANY, 0,
			     fnct_ReflectCoords, 0, 0);
    sqlite3_create_function (db, "SwapCoords", 1, SQLITE_ANY, 0,
			     fnct_ReflectCoords, 0, 0);
    sqlite3_create_function (db, "SwapCoordinates", 1, SQLITE_ANY, 0,
			     fnct_ReflectCoords, 0, 0);
    sqlite3_create_function (db, "BuildMbr", 4, SQLITE_ANY, 0, fnct_BuildMbr1,
			     0, 0);
    sqlite3_create_function (db, "BuildMbr", 5, SQLITE_ANY, 0, fnct_BuildMbr2,
			     0, 0);
    sqlite3_create_function (db, "BuildCircleMbr", 3, SQLITE_ANY, 0,
			     fnct_BuildCircleMbr1, 0, 0);
    sqlite3_create_function (db, "BuildCircleMbr", 4, SQLITE_ANY, 0,
			     fnct_BuildCircleMbr2, 0, 0);
    sqlite3_create_function (db, "MbrMinX", 1, SQLITE_ANY, 0, fnct_MbrMinX, 0,
			     0);
    sqlite3_create_function (db, "MbrMaxX", 1, SQLITE_ANY, 0, fnct_MbrMaxX, 0,
			     0);
    sqlite3_create_function (db, "MbrMinY", 1, SQLITE_ANY, 0, fnct_MbrMinY, 0,
			     0);
    sqlite3_create_function (db, "MbrMaxY", 1, SQLITE_ANY, 0, fnct_MbrMaxY, 0,
			     0);
    sqlite3_create_function (db, "MakePoint", 2, SQLITE_ANY, 0, fnct_MakePoint1,
			     0, 0);
    sqlite3_create_function (db, "MakePoint", 3, SQLITE_ANY, 0, fnct_MakePoint2,
			     0, 0);
    sqlite3_create_function (db, "BuildMbrFilter", 4, SQLITE_ANY, 0,
			     fnct_BuildMbrFilter, 0, 0);
    sqlite3_create_function (db, "FilterMbrWithin", 4, SQLITE_ANY, 0,
			     fnct_FilterMbrWithin, 0, 0);
    sqlite3_create_function (db, "FilterMbrContains", 4, SQLITE_ANY, 0,
			     fnct_FilterMbrContains, 0, 0);
    sqlite3_create_function (db, "FilterMbrIntersects", 4, SQLITE_ANY, 0,
			     fnct_FilterMbrIntersects, 0, 0);
    sqlite3_create_function (db, "BuildRings", 1, SQLITE_ANY, 0,
			     fnct_BuildRings, 0, 0);

/* some BLOB/JPEG/EXIF functions */
    sqlite3_create_function (db, "IsGeometryBlob", 1, SQLITE_ANY, 0,
			     fnct_IsGeometryBlob, 0, 0);
    sqlite3_create_function (db, "IsZipBlob", 1, SQLITE_ANY, 0, fnct_IsZipBlob,
			     0, 0);
    sqlite3_create_function (db, "IsPdfBlob", 1, SQLITE_ANY, 0, fnct_IsPdfBlob,
			     0, 0);
    sqlite3_create_function (db, "IsWaveletBlob", 1, SQLITE_ANY, 0,
			     fnct_IsWaveletBlob, 0, 0);
    sqlite3_create_function (db, "IsTiffBlob", 1, SQLITE_ANY, 0,
			     fnct_IsTiffBlob, 0, 0);
    sqlite3_create_function (db, "IsGifBlob", 1, SQLITE_ANY, 0, fnct_IsGifBlob,
			     0, 0);
    sqlite3_create_function (db, "IsPngBlob", 1, SQLITE_ANY, 0, fnct_IsPngBlob,
			     0, 0);
    sqlite3_create_function (db, "IsJpegBlob", 1, SQLITE_ANY, 0,
			     fnct_IsJpegBlob, 0, 0);
    sqlite3_create_function (db, "IsExifBlob", 1, SQLITE_ANY, 0,
			     fnct_IsExifBlob, 0, 0);
    sqlite3_create_function (db, "IsExifGpsBlob", 1, SQLITE_ANY, 0,
			     fnct_IsExifGpsBlob, 0, 0);
    sqlite3_create_function (db, "GeomFromExifGpsBlob", 1, SQLITE_ANY, 0,
			     fnct_GeomFromExifGpsBlob, 0, 0);

/* some Geodesic functions */
    sqlite3_create_function (db, "GreatCircleLength", 1, SQLITE_ANY, 0,
			     fnct_GreatCircleLength, 0, 0);
    sqlite3_create_function (db, "GeodesicLength", 1, SQLITE_ANY, 0,
			     fnct_GeodesicLength, 0, 0);

/* some Length Unit conversion functions */
    sqlite3_create_function (db, "CvtToKm", 1, SQLITE_ANY, 0, fnct_cvtToKm, 0,
			     0);
    sqlite3_create_function (db, "CvtToDm", 1, SQLITE_ANY, 0, fnct_cvtToDm, 0,
			     0);
    sqlite3_create_function (db, "CvtToCm", 1, SQLITE_ANY, 0, fnct_cvtToCm, 0,
			     0);
    sqlite3_create_function (db, "CvtToMm", 1, SQLITE_ANY, 0, fnct_cvtToMm, 0,
			     0);
    sqlite3_create_function (db, "CvtToKmi", 1, SQLITE_ANY, 0, fnct_cvtToKmi, 0,
			     0);
    sqlite3_create_function (db, "CvtToIn", 1, SQLITE_ANY, 0, fnct_cvtToIn, 0,
			     0);
    sqlite3_create_function (db, "CvtToYd", 1, SQLITE_ANY, 0, fnct_cvtToYd, 0,
			     0);
    sqlite3_create_function (db, "CvtToMi", 1, SQLITE_ANY, 0, fnct_cvtToMi, 0,
			     0);
    sqlite3_create_function (db, "CvtToFath", 1, SQLITE_ANY, 0, fnct_cvtToFath,
			     0, 0);
    sqlite3_create_function (db, "CvtToCh", 1, SQLITE_ANY, 0, fnct_cvtToCh, 0,
			     0);
    sqlite3_create_function (db, "CvtToUsIn", 1, SQLITE_ANY, 0, fnct_cvtToUsIn,
			     0, 0);
    sqlite3_create_function (db, "CvtToUsFt", 1, SQLITE_ANY, 0, fnct_cvtToUsFt,
			     0, 0);
    sqlite3_create_function (db, "CvtToUsYd", 1, SQLITE_ANY, 0, fnct_cvtToUsYd,
			     0, 0);
    sqlite3_create_function (db, "CvtToUsMi", 1, SQLITE_ANY, 0, fnct_cvtToUsMi,
			     0, 0);
    sqlite3_create_function (db, "CvtToIndFt", 1, SQLITE_ANY, 0,
			     fnct_cvtToIndFt, 0, 0);
    sqlite3_create_function (db, "CvtToIndYd", 1, SQLITE_ANY, 0,
			     fnct_cvtToIndYd, 0, 0);
    sqlite3_create_function (db, "CvtToIndCh", 1, SQLITE_ANY, 0,
			     fnct_cvtToIndCh, 0, 0);
    sqlite3_create_function (db, "CvtFromKm", 1, SQLITE_ANY, 0, fnct_cvtFromKm,
			     0, 0);
    sqlite3_create_function (db, "CvtFromDm", 1, SQLITE_ANY, 0, fnct_cvtFromDm,
			     0, 0);
    sqlite3_create_function (db, "CvtFromCm", 1, SQLITE_ANY, 0, fnct_cvtFromCm,
			     0, 0);
    sqlite3_create_function (db, "CvtFromMm", 1, SQLITE_ANY, 0, fnct_cvtFromMm,
			     0, 0);
    sqlite3_create_function (db, "CvtFromKmi", 1, SQLITE_ANY, 0,
			     fnct_cvtFromKmi, 0, 0);
    sqlite3_create_function (db, "CvtFromIn", 1, SQLITE_ANY, 0, fnct_cvtFromIn,
			     0, 0);
    sqlite3_create_function (db, "CvtFromYd", 1, SQLITE_ANY, 0, fnct_cvtFromYd,
			     0, 0);
    sqlite3_create_function (db, "CvtFromMi", 1, SQLITE_ANY, 0, fnct_cvtFromMi,
			     0, 0);
    sqlite3_create_function (db, "CvtFromFath", 1, SQLITE_ANY, 0,
			     fnct_cvtFromFath, 0, 0);
    sqlite3_create_function (db, "CvtFromCh", 1, SQLITE_ANY, 0, fnct_cvtFromCh,
			     0, 0);
    sqlite3_create_function (db, "CvtFromUsIn", 1, SQLITE_ANY, 0,
			     fnct_cvtFromUsIn, 0, 0);
    sqlite3_create_function (db, "CvtFromUsFt", 1, SQLITE_ANY, 0,
			     fnct_cvtFromUsFt, 0, 0);
    sqlite3_create_function (db, "CvtFromUsYd", 1, SQLITE_ANY, 0,
			     fnct_cvtFromUsYd, 0, 0);
    sqlite3_create_function (db, "CvtFromUsMi", 1, SQLITE_ANY, 0,
			     fnct_cvtFromUsMi, 0, 0);
    sqlite3_create_function (db, "CvtFromIndFt", 1, SQLITE_ANY, 0,
			     fnct_cvtFromIndFt, 0, 0);
    sqlite3_create_function (db, "CvtFromIndYd", 1, SQLITE_ANY, 0,
			     fnct_cvtFromIndYd, 0, 0);
    sqlite3_create_function (db, "CvtFromIndCh", 1, SQLITE_ANY, 0,
			     fnct_cvtFromIndCh, 0, 0);

#ifndef OMIT_MATHSQL		/* supporting SQL math functions */

/* some extra math functions */
    sqlite3_create_function (db, "abs", 1, SQLITE_ANY, 0, fnct_math_abs, 0, 0);
    sqlite3_create_function (db, "acos", 1, SQLITE_ANY, 0, fnct_math_acos, 0,
			     0);
    sqlite3_create_function (db, "asin", 1, SQLITE_ANY, 0, fnct_math_asin, 0,
			     0);
    sqlite3_create_function (db, "atan", 1, SQLITE_ANY, 0, fnct_math_atan, 0,
			     0);
    sqlite3_create_function (db, "ceil", 1, SQLITE_ANY, 0, fnct_math_ceil, 0,
			     0);
    sqlite3_create_function (db, "ceiling", 1, SQLITE_ANY, 0, fnct_math_ceil, 0,
			     0);
    sqlite3_create_function (db, "cos", 1, SQLITE_ANY, 0, fnct_math_cos, 0, 0);
    sqlite3_create_function (db, "cot", 1, SQLITE_ANY, 0, fnct_math_cot, 0, 0);
    sqlite3_create_function (db, "degrees", 1, SQLITE_ANY, 0, fnct_math_degrees,
			     0, 0);
    sqlite3_create_function (db, "exp", 1, SQLITE_ANY, 0, fnct_math_exp, 0, 0);
    sqlite3_create_function (db, "floor", 1, SQLITE_ANY, 0, fnct_math_floor, 0,
			     0);
    sqlite3_create_function (db, "ln", 1, SQLITE_ANY, 0, fnct_math_logn, 0, 0);
    sqlite3_create_function (db, "log", 1, SQLITE_ANY, 0, fnct_math_logn, 0, 0);
    sqlite3_create_function (db, "log", 2, SQLITE_ANY, 0, fnct_math_logn2, 0,
			     0);
    sqlite3_create_function (db, "log2", 1, SQLITE_ANY, 0, fnct_math_log_2, 0,
			     0);
    sqlite3_create_function (db, "log10", 1, SQLITE_ANY, 0, fnct_math_log_10, 0,
			     0);
    sqlite3_create_function (db, "pi", 0, SQLITE_ANY, 0, fnct_math_pi, 0, 0);
    sqlite3_create_function (db, "pow", 2, SQLITE_ANY, 0, fnct_math_pow, 0, 0);
    sqlite3_create_function (db, "power", 2, SQLITE_ANY, 0, fnct_math_pow, 0,
			     0);
    sqlite3_create_function (db, "radians", 1, SQLITE_ANY, 0, fnct_math_radians,
			     0, 0);
    sqlite3_create_function (db, "round", 1, SQLITE_ANY, 0, fnct_math_round, 0,
			     0);
    sqlite3_create_function (db, "sign", 1, SQLITE_ANY, 0, fnct_math_sign, 0,
			     0);
    sqlite3_create_function (db, "sin", 1, SQLITE_ANY, 0, fnct_math_sin, 0, 0);
    sqlite3_create_function (db, "stddev_pop", 1, SQLITE_ANY, 0, 0,
			     fnct_math_stddev_step, fnct_math_stddev_pop_final);
    sqlite3_create_function (db, "stddev_samp", 1, SQLITE_ANY, 0, 0,
			     fnct_math_stddev_step,
			     fnct_math_stddev_samp_final);
    sqlite3_create_function (db, "sqrt", 1, SQLITE_ANY, 0, fnct_math_sqrt, 0,
			     0);
    sqlite3_create_function (db, "tan", 1, SQLITE_ANY, 0, fnct_math_tan, 0, 0);
    sqlite3_create_function (db, "var_pop", 1, SQLITE_ANY, 0, 0,
			     fnct_math_stddev_step, fnct_math_var_pop_final);
    sqlite3_create_function (db, "var_samp", 1, SQLITE_ANY, 0, 0,
			     fnct_math_stddev_step, fnct_math_var_samp_final);

#endif /* end supporting SQL math functions */

#ifndef OMIT_PROJ		/* including PROJ.4 */

    sqlite3_create_function (db, "Transform", 2, SQLITE_ANY, 0, fnct_Transform,
			     0, 0);

#endif /* end including PROJ.4 */

#ifndef OMIT_GEOS		/* including GEOS */

    initGEOS (geos_error, geos_error);
    sqlite3_create_function (db, "Boundary", 1, SQLITE_ANY, 0, fnct_Boundary, 0,
			     0);
    sqlite3_create_function (db, "IsClosed", 1, SQLITE_ANY, 0, fnct_IsClosed, 0,
			     0);
    sqlite3_create_function (db, "IsSimple", 1, SQLITE_ANY, 0, fnct_IsSimple, 0,
			     0);
    sqlite3_create_function (db, "IsRing", 1, SQLITE_ANY, 0, fnct_IsRing, 0, 0);
    sqlite3_create_function (db, "IsValid", 1, SQLITE_ANY, 0, fnct_IsValid, 0,
			     0);
    sqlite3_create_function (db, "GLength", 1, SQLITE_ANY, 0, fnct_Length, 0,
			     0);
    sqlite3_create_function (db, "Area", 1, SQLITE_ANY, 0, fnct_Area, 0, 0);
    sqlite3_create_function (db, "Centroid", 1, SQLITE_ANY, 0, fnct_Centroid, 0,
			     0);
    sqlite3_create_function (db, "PointOnSurface", 1, SQLITE_ANY, 0,
			     fnct_PointOnSurface, 0, 0);
    sqlite3_create_function (db, "Simplify", 2, SQLITE_ANY, 0, fnct_Simplify, 0,
			     0);
    sqlite3_create_function (db, "SimplifyPreserveTopology", 2, SQLITE_ANY, 0,
			     fnct_SimplifyPreserveTopology, 0, 0);
    sqlite3_create_function (db, "ConvexHull", 1, SQLITE_ANY, 0,
			     fnct_ConvexHull, 0, 0);
    sqlite3_create_function (db, "Buffer", 2, SQLITE_ANY, 0, fnct_Buffer, 0, 0);
    sqlite3_create_function (db, "Intersection", 2, SQLITE_ANY, 0,
			     fnct_Intersection, 0, 0);
    sqlite3_create_function (db, "GUnion", 1, SQLITE_ANY, 0, 0, fnct_Union_step,
			     fnct_Union_final);
    sqlite3_create_function (db, "GUnion", 2, SQLITE_ANY, 0, fnct_Union, 0, 0);
    sqlite3_create_function (db, "Difference", 2, SQLITE_ANY, 0,
			     fnct_Difference, 0, 0);
    sqlite3_create_function (db, "SymDifference", 2, SQLITE_ANY, 0,
			     fnct_SymDifference, 0, 0);
    sqlite3_create_function (db, "Equals", 2, SQLITE_ANY, 0, fnct_Equals, 0, 0);
    sqlite3_create_function (db, "Intersects", 2, SQLITE_ANY, 0,
			     fnct_Intersects, 0, 0);
    sqlite3_create_function (db, "Disjoint", 2, SQLITE_ANY, 0, fnct_Disjoint, 0,
			     0);
    sqlite3_create_function (db, "Overlaps", 2, SQLITE_ANY, 0, fnct_Overlaps, 0,
			     0);
    sqlite3_create_function (db, "Crosses", 2, SQLITE_ANY, 0, fnct_Crosses, 0,
			     0);
    sqlite3_create_function (db, "Touches", 2, SQLITE_ANY, 0, fnct_Touches, 0,
			     0);
    sqlite3_create_function (db, "Within", 2, SQLITE_ANY, 0, fnct_Within, 0, 0);
    sqlite3_create_function (db, "Contains", 2, SQLITE_ANY, 0, fnct_Contains, 0,
			     0);
    sqlite3_create_function (db, "Relate", 3, SQLITE_ANY, 0, fnct_Relate, 0, 0);
    sqlite3_create_function (db, "Distance", 2, SQLITE_ANY, 0, fnct_Distance, 0,
			     0);
    sqlite3_create_function (db, "BdPolyFromText", 1, SQLITE_ANY, 0,
			     fnct_BdPolyFromText1, 0, 0);
    sqlite3_create_function (db, "BdPolyFromText", 2, SQLITE_ANY, 0,
			     fnct_BdPolyFromText2, 0, 0);
    sqlite3_create_function (db, "BdMPolyFromText", 1, SQLITE_ANY, 0,
			     fnct_BdMPolyFromText1, 0, 0);
    sqlite3_create_function (db, "BdMPolyFromText", 2, SQLITE_ANY, 0,
			     fnct_BdMPolyFromText2, 0, 0);
    sqlite3_create_function (db, "BdPolyFromWKB", 1, SQLITE_ANY, 0,
			     fnct_BdPolyFromWKB1, 0, 0);
    sqlite3_create_function (db, "BdPolyFromWKB", 2, SQLITE_ANY, 0,
			     fnct_BdPolyFromWKB2, 0, 0);
    sqlite3_create_function (db, "BdMPolyFromWKB", 1, SQLITE_ANY, 0,
			     fnct_BdMPolyFromWKB1, 0, 0);
    sqlite3_create_function (db, "BdMPolyFromWKB", 2, SQLITE_ANY, 0,
			     fnct_BdMPolyFromWKB2, 0, 0);
    sqlite3_create_function (db, "Polygonize", 1, SQLITE_ANY, 0,
			     fnct_Polygonize1, 0, 0);
    sqlite3_create_function (db, "Polygonize", 2, SQLITE_ANY, 0,
			     fnct_Polygonize2, 0, 0);

#endif /* end including GEOS */

/* initializing the VirtualShape  extension */
    virtualshape_extension_init (db);
/* initializing the VirtualText extension */
    virtualtext_extension_init (db);
/* initializing the VirtualNetwork  extension */
    virtualnetwork_extension_init (db);
/* initializing the MbrCache  extension */
    mbrcache_extension_init (db);
/* initializing the VirtualFDO  extension */
    virtualfdo_extension_init (db);
/* setting a timeout handler */
    sqlite3_busy_timeout (db, 5000);
}

void
spatialite_init (int verbose)
{
/* used when SQLite initializes SpatiaLite via statically linked lib */
    sqlite3_auto_extension ((void (*)(void)) init_static_spatialite);
    if (verbose)
      {
	  printf ("SpatiaLite version ..: %s", spatialite_version ());
	  printf ("\tSupported Extensions:\n");
	  printf ("\t- 'VirtualShape'\t[direct Shapefile access]\n");
	  printf ("\t- 'VirtualText\t\t[direct CSV/TXT access]\n");
	  printf ("\t- 'VirtualNetwork\t[Dijkstra shortest path]\n");
	  printf ("\t- 'RTree'\t\t[Spatial Index - R*Tree]\n");
	  printf ("\t- 'MbrCache'\t\t[Spatial Index - MBR cache]\n");
	  printf ("\t- 'VirtualFDO'\t\t[FDO-OGR interoperability]\n");
	  printf ("\t- 'SpatiaLite'\t\t[Spatial SQL - OGC]\n");
      }
#ifndef OMIT_PROJ		/* PROJ.4 version */
    if (verbose)
	printf ("PROJ.4 version ......: %s\n", pj_get_release ());
#endif /* end including PROJ.4 */
#ifndef OMIT_GEOS		/* GEOS version */
    if (verbose)
	printf ("GEOS version ........: %s\n", GEOSversion ());
#endif /* end GEOS version */
}

SPATIALITE_DECLARE int
sqlite3_extension_init (sqlite3 * db, char **pzErrMsg,
			const sqlite3_api_routines * pApi)
{
/* SQLite invokes this routine once when it dynamically loads the extension. */
    SQLITE_EXTENSION_INIT2 (pApi);
    setlocale (LC_NUMERIC, "POSIX");
    *pzErrMsg = NULL;
    sqlite3_create_function (db, "spatialite_version", 0, SQLITE_ANY, 0,
			     fnct_spatialite_version, 0, 0);
    sqlite3_create_function (db, "proj4_version", 0, SQLITE_ANY, 0,
			     fnct_proj4_version, 0, 0);
    sqlite3_create_function (db, "geos_version", 0, SQLITE_ANY, 0,
			     fnct_geos_version, 0, 0);
    sqlite3_create_function (db, "GeometryConstraints", 3, SQLITE_ANY, 0,
			     fnct_GeometryConstraints, 0, 0);
    sqlite3_create_function (db, "CheckSpatialMetaData", 0, SQLITE_ANY, 0,
			     fnct_CheckSpatialMetaData, 0, 0);
    sqlite3_create_function (db, "AutoFDOStart", 0, SQLITE_ANY, 0,
			     fnct_AutoFDOStart, 0, 0);
    sqlite3_create_function (db, "AutoFDOStop", 0, SQLITE_ANY, 0,
			     fnct_AutoFDOStop, 0, 0);
    sqlite3_create_function (db, "InitFDOSpatialMetaData", 0, SQLITE_ANY, 0,
			     fnct_InitFDOSpatialMetaData, 0, 0);
    sqlite3_create_function (db, "AddFDOGeometryColumn", 6, SQLITE_ANY, 0,
			     fnct_AddFDOGeometryColumn, 0, 0);
    sqlite3_create_function (db, "RecoverFDOGeometryColumn", 6, SQLITE_ANY, 0,
			     fnct_RecoverFDOGeometryColumn, 0, 0);
    sqlite3_create_function (db, "DiscardFDOGeometryColumn", 2, SQLITE_ANY, 0,
			     fnct_DiscardFDOGeometryColumn, 0, 0);
    sqlite3_create_function (db, "InitSpatialMetaData", 0, SQLITE_ANY, 0,
			     fnct_InitSpatialMetaData, 0, 0);
    sqlite3_create_function (db, "AddGeometryColumn", 5, SQLITE_ANY, 0,
			     fnct_AddGeometryColumn, 0, 0);
    sqlite3_create_function (db, "AddGeometryColumn", 6, SQLITE_ANY, 0,
			     fnct_AddGeometryColumn, 0, 0);
    sqlite3_create_function (db, "RecoverGeometryColumn", 5, SQLITE_ANY, 0,
			     fnct_RecoverGeometryColumn, 0, 0);
    sqlite3_create_function (db, "DiscardGeometryColumn", 2, SQLITE_ANY, 0,
			     fnct_DiscardGeometryColumn, 0, 0);
    sqlite3_create_function (db, "CreateSpatialIndex", 2, SQLITE_ANY, 0,
			     fnct_CreateSpatialIndex, 0, 0);
    sqlite3_create_function (db, "CreateMbrCache", 2, SQLITE_ANY, 0,
			     fnct_CreateMbrCache, 0, 0);
    sqlite3_create_function (db, "DisableSpatialIndex", 2, SQLITE_ANY, 0,
			     fnct_DisableSpatialIndex, 0, 0);
    sqlite3_create_function (db, "RebuildGeometryTriggers", 2, SQLITE_ANY, 0,
			     fnct_RebuildGeometryTriggers, 0, 0);
    sqlite3_create_function (db, "AsText", 1, SQLITE_ANY, 0, fnct_AsText, 0, 0);
    sqlite3_create_function (db, "AsSvg", 1, SQLITE_ANY, 0, fnct_AsSvg1, 0, 0);
    sqlite3_create_function (db, "AsSvg", 2, SQLITE_ANY, 0, fnct_AsSvg2, 0, 0);
    sqlite3_create_function (db, "AsSvg", 3, SQLITE_ANY, 0, fnct_AsSvg3, 0, 0);
    sqlite3_create_function (db, "AsFGF", 2, SQLITE_ANY, 0, fnct_AsFGF, 0, 0);
    sqlite3_create_function (db, "AsBinary", 1, SQLITE_ANY, 0, fnct_AsBinary, 0,
			     0);
    sqlite3_create_function (db, "GeomFromText", 1, SQLITE_ANY, 0,
			     fnct_GeomFromText1, 0, 0);
    sqlite3_create_function (db, "GeomFromText", 2, SQLITE_ANY, 0,
			     fnct_GeomFromText2, 0, 0);
    sqlite3_create_function (db, "GeometryFromText", 1, SQLITE_ANY, 0,
			     fnct_GeomFromText1, 0, 0);
    sqlite3_create_function (db, "GeometryFromText", 2, SQLITE_ANY, 0,
			     fnct_GeomFromText2, 0, 0);
    sqlite3_create_function (db, "GeomCollFromText", 1, SQLITE_ANY, 0,
			     fnct_GeomCollFromText1, 0, 0);
    sqlite3_create_function (db, "GeomCollFromText", 2, SQLITE_ANY, 0,
			     fnct_GeomCollFromText2, 0, 0);
    sqlite3_create_function (db, "GeometryCollectionFromText", 1, SQLITE_ANY, 0,
			     fnct_GeomCollFromText1, 0, 0);
    sqlite3_create_function (db, "GeometryCollectionFromText", 2, SQLITE_ANY, 0,
			     fnct_GeomCollFromText2, 0, 0);
    sqlite3_create_function (db, "LineFromText", 1, SQLITE_ANY, 0,
			     fnct_LineFromText1, 0, 0);
    sqlite3_create_function (db, "LineFromText", 2, SQLITE_ANY, 0,
			     fnct_LineFromText2, 0, 0);
    sqlite3_create_function (db, "LineStringFromText", 1, SQLITE_ANY, 0,
			     fnct_LineFromText1, 0, 0);
    sqlite3_create_function (db, "LineStringFromText", 2, SQLITE_ANY, 0,
			     fnct_LineFromText2, 0, 0);
    sqlite3_create_function (db, "MLineFromText", 1, SQLITE_ANY, 0,
			     fnct_MLineFromText1, 0, 0);
    sqlite3_create_function (db, "MLineFromText", 2, SQLITE_ANY, 0,
			     fnct_MLineFromText2, 0, 0);
    sqlite3_create_function (db, "MultiLineStringFromText", 1, SQLITE_ANY, 0,
			     fnct_MLineFromText1, 0, 0);
    sqlite3_create_function (db, "MultiLineStringFromText", 2, SQLITE_ANY, 0,
			     fnct_MLineFromText2, 0, 0);
    sqlite3_create_function (db, "MPointFromText", 1, SQLITE_ANY, 0,
			     fnct_MPointFromText1, 0, 0);
    sqlite3_create_function (db, "MPointFromText", 2, SQLITE_ANY, 0,
			     fnct_MPointFromText2, 0, 0);
    sqlite3_create_function (db, "MultiPointFromText", 1, SQLITE_ANY, 0,
			     fnct_MPointFromText1, 0, 0);
    sqlite3_create_function (db, "MultiPointFromText", 2, SQLITE_ANY, 0,
			     fnct_MPointFromText2, 0, 0);
    sqlite3_create_function (db, "MPolyFromText", 1, SQLITE_ANY, 0,
			     fnct_MPolyFromText1, 0, 0);
    sqlite3_create_function (db, "MPolyFromText", 2, SQLITE_ANY, 0,
			     fnct_MPolyFromText2, 0, 0);
    sqlite3_create_function (db, "MultiPolygonFromText", 1, SQLITE_ANY, 0,
			     fnct_MPolyFromText1, 0, 0);
    sqlite3_create_function (db, "MultiPolygonFromText", 2, SQLITE_ANY, 0,
			     fnct_MPolyFromText2, 0, 0);
    sqlite3_create_function (db, "PointFromText", 1, SQLITE_ANY, 0,
			     fnct_PointFromText1, 0, 0);
    sqlite3_create_function (db, "PointFromText", 2, SQLITE_ANY, 0,
			     fnct_PointFromText2, 0, 0);
    sqlite3_create_function (db, "PolyFromText", 1, SQLITE_ANY, 0,
			     fnct_PolyFromText1, 0, 0);
    sqlite3_create_function (db, "PolyFromText", 2, SQLITE_ANY, 0,
			     fnct_PolyFromText2, 0, 0);
    sqlite3_create_function (db, "PolygonFromText", 1, SQLITE_ANY, 0,
			     fnct_PolyFromText1, 0, 0);
    sqlite3_create_function (db, "PolygomFromText", 2, SQLITE_ANY, 0,
			     fnct_PolyFromText2, 0, 0);
    sqlite3_create_function (db, "GeomFromWKB", 1, SQLITE_ANY, 0,
			     fnct_GeomFromWkb1, 0, 0);
    sqlite3_create_function (db, "GeomFromWKB", 2, SQLITE_ANY, 0,
			     fnct_GeomFromWkb2, 0, 0);
    sqlite3_create_function (db, "GeometryFromWKB", 1, SQLITE_ANY, 0,
			     fnct_GeomFromWkb1, 0, 0);
    sqlite3_create_function (db, "GeometryFromWKB", 2, SQLITE_ANY, 0,
			     fnct_GeomFromWkb2, 0, 0);
    sqlite3_create_function (db, "GeomCollFromWKB", 1, SQLITE_ANY, 0,
			     fnct_GeomCollFromWkb1, 0, 0);
    sqlite3_create_function (db, "GeomCollFromWKB", 2, SQLITE_ANY, 0,
			     fnct_GeomCollFromWkb2, 0, 0);
    sqlite3_create_function (db, "GeometryCollectionFromWKB", 1, SQLITE_ANY, 0,
			     fnct_GeomCollFromWkb1, 0, 0);
    sqlite3_create_function (db, "GeometryCollectionFromWKB", 2, SQLITE_ANY, 0,
			     fnct_GeomCollFromWkb2, 0, 0);
    sqlite3_create_function (db, "LineFromWKB", 1, SQLITE_ANY, 0,
			     fnct_LineFromWkb1, 0, 0);
    sqlite3_create_function (db, "LineFromWKB", 2, SQLITE_ANY, 0,
			     fnct_LineFromWkb2, 0, 0);
    sqlite3_create_function (db, "LineStringFromWKB", 1, SQLITE_ANY, 0,
			     fnct_LineFromWkb1, 0, 0);
    sqlite3_create_function (db, "LineStringFromWKB", 2, SQLITE_ANY, 0,
			     fnct_LineFromWkb2, 0, 0);
    sqlite3_create_function (db, "MLineFromWKB", 1, SQLITE_ANY, 0,
			     fnct_MLineFromWkb1, 0, 0);
    sqlite3_create_function (db, "MLineFromWKB", 2, SQLITE_ANY, 0,
			     fnct_MLineFromWkb2, 0, 0);
    sqlite3_create_function (db, "MultiLineStringFromWKB", 1, SQLITE_ANY, 0,
			     fnct_MLineFromWkb1, 0, 0);
    sqlite3_create_function (db, "MultiLineStringFromWKB", 2, SQLITE_ANY, 0,
			     fnct_MLineFromWkb2, 0, 0);
    sqlite3_create_function (db, "MPointFromWKB", 1, SQLITE_ANY, 0,
			     fnct_MPointFromWkb1, 0, 0);
    sqlite3_create_function (db, "MPointFromWKB", 2, SQLITE_ANY, 0,
			     fnct_MPointFromWkb2, 0, 0);
    sqlite3_create_function (db, "MultiPointFromWKB", 1, SQLITE_ANY, 0,
			     fnct_MPointFromWkb1, 0, 0);
    sqlite3_create_function (db, "MultiPointFromWKB", 2, SQLITE_ANY, 0,
			     fnct_MPointFromWkb2, 0, 0);
    sqlite3_create_function (db, "MPolyFromWKB", 1, SQLITE_ANY, 0,
			     fnct_MPolyFromWkb1, 0, 0);
    sqlite3_create_function (db, "MPolyFromWKB", 2, SQLITE_ANY, 0,
			     fnct_MPolyFromWkb2, 0, 0);
    sqlite3_create_function (db, "MultiPolygonFromWKB", 1, SQLITE_ANY, 0,
			     fnct_MPolyFromWkb1, 0, 0);
    sqlite3_create_function (db, "MultiPolygomFromWKB", 2, SQLITE_ANY, 0,
			     fnct_MPolyFromWkb2, 0, 0);
    sqlite3_create_function (db, "PointFromWKB", 1, SQLITE_ANY, 0,
			     fnct_PointFromWkb1, 0, 0);
    sqlite3_create_function (db, "PointFromWKB", 2, SQLITE_ANY, 0,
			     fnct_PointFromWkb2, 0, 0);
    sqlite3_create_function (db, "PolyFromWKB", 1, SQLITE_ANY, 0,
			     fnct_PolyFromWkb1, 0, 0);
    sqlite3_create_function (db, "PolyFromWKB", 2, SQLITE_ANY, 0,
			     fnct_PolyFromWkb2, 0, 0);
    sqlite3_create_function (db, "PolygonFromWKB", 1, SQLITE_ANY, 0,
			     fnct_PolyFromWkb1, 0, 0);
    sqlite3_create_function (db, "PolygonFromWKB", 2, SQLITE_ANY, 0,
			     fnct_PolyFromWkb2, 0, 0);
    sqlite3_create_function (db, "GeomFromFGF", 1, SQLITE_ANY, 0,
			     fnct_GeometryFromFGF1, 0, 0);
    sqlite3_create_function (db, "GeomFromFGF", 2, SQLITE_ANY, 0,
			     fnct_GeometryFromFGF2, 0, 0);
    sqlite3_create_function (db, "Dimension", 1, SQLITE_ANY, 0, fnct_Dimension,
			     0, 0);
    sqlite3_create_function (db, "GeometryType", 1, SQLITE_ANY, 0,
			     fnct_GeometryType, 0, 0);
    sqlite3_create_function (db, "GeometryAliasType", 1, SQLITE_ANY, 0,
			     fnct_GeometryAliasType, 0, 0);
    sqlite3_create_function (db, "SRID", 1, SQLITE_ANY, 0, fnct_SRID, 0, 0);
    sqlite3_create_function (db, "SetSrid", 2, SQLITE_ANY, 0, fnct_SetSRID, 0,
			     0);
    sqlite3_create_function (db, "IsEmpty", 1, SQLITE_ANY, 0, fnct_IsEmpty, 0,
			     0);
    sqlite3_create_function (db, "Envelope", 1, SQLITE_ANY, 0, fnct_Envelope, 0,
			     0);
    sqlite3_create_function (db, "X", 1, SQLITE_ANY, 0, fnct_X, 0, 0);
    sqlite3_create_function (db, "Y", 1, SQLITE_ANY, 0, fnct_Y, 0, 0);
    sqlite3_create_function (db, "NumPoints", 1, SQLITE_ANY, 0, fnct_NumPoints,
			     0, 0);
    sqlite3_create_function (db, "StartPoint", 1, SQLITE_ANY, 0,
			     fnct_StartPoint, 0, 0);
    sqlite3_create_function (db, "EndPoint", 1, SQLITE_ANY, 0, fnct_EndPoint, 0,
			     0);
    sqlite3_create_function (db, "PointN", 2, SQLITE_ANY, 0, fnct_PointN, 0, 0);
    sqlite3_create_function (db, "ExteriorRing", 1, SQLITE_ANY, 0,
			     fnct_ExteriorRing, 0, 0);
    sqlite3_create_function (db, "NumInteriorRings", 1, SQLITE_ANY, 0,
			     fnct_NumInteriorRings, 0, 0);
    sqlite3_create_function (db, "NumInteriorRing", 1, SQLITE_ANY, 0,
			     fnct_NumInteriorRings, 0, 0);
    sqlite3_create_function (db, "InteriorRingN", 2, SQLITE_ANY, 0,
			     fnct_InteriorRingN, 0, 0);
    sqlite3_create_function (db, "NumGeometries", 1, SQLITE_ANY, 0,
			     fnct_NumGeometries, 0, 0);
    sqlite3_create_function (db, "GeometryN", 2, SQLITE_ANY, 0, fnct_GeometryN,
			     0, 0);
    sqlite3_create_function (db, "MBRContains", 2, SQLITE_ANY, 0,
			     fnct_MbrContains, 0, 0);
    sqlite3_create_function (db, "MBRDisjoint", 2, SQLITE_ANY, 0,
			     fnct_MbrDisjoint, 0, 0);
    sqlite3_create_function (db, "MBREqual", 2, SQLITE_ANY, 0, fnct_MbrEqual, 0,
			     0);
    sqlite3_create_function (db, "MBRIntersects", 2, SQLITE_ANY, 0,
			     fnct_MbrIntersects, 0, 0);
    sqlite3_create_function (db, "MBROverlaps", 2, SQLITE_ANY, 0,
			     fnct_MbrOverlaps, 0, 0);
    sqlite3_create_function (db, "MBRTouches", 2, SQLITE_ANY, 0,
			     fnct_MbrTouches, 0, 0);
    sqlite3_create_function (db, "MBRWithin", 2, SQLITE_ANY, 0, fnct_MbrWithin,
			     0, 0);
    sqlite3_create_function (db, "ShiftCoords", 3, SQLITE_ANY, 0,
			     fnct_ShiftCoords, 0, 0);
    sqlite3_create_function (db, "ShiftCoordinates", 3, SQLITE_ANY, 0,
			     fnct_ShiftCoords, 0, 0);
    sqlite3_create_function (db, "ScaleCoords", 2, SQLITE_ANY, 0,
			     fnct_ScaleCoords, 0, 0);
    sqlite3_create_function (db, "ScaleCoords", 3, SQLITE_ANY, 0,
			     fnct_ScaleCoords, 0, 0);
    sqlite3_create_function (db, "ScaleCoordinates", 2, SQLITE_ANY, 0,
			     fnct_ScaleCoords, 0, 0);
    sqlite3_create_function (db, "ScaleCoordinates", 3, SQLITE_ANY, 0,
			     fnct_ScaleCoords, 0, 0);
    sqlite3_create_function (db, "RotateCoords", 2, SQLITE_ANY, 0,
			     fnct_RotateCoords, 0, 0);
    sqlite3_create_function (db, "ReflectCoords", 3, SQLITE_ANY, 0,
			     fnct_ReflectCoords, 0, 0);
    sqlite3_create_function (db, "RotateCoordinates", 2, SQLITE_ANY, 0,
			     fnct_RotateCoords, 0, 0);
    sqlite3_create_function (db, "ReflectCoordinates", 3, SQLITE_ANY, 0,
			     fnct_ReflectCoords, 0, 0);
    sqlite3_create_function (db, "SwapCoordinates", 1, SQLITE_ANY, 0,
			     fnct_SwapCoords, 0, 0);
    sqlite3_create_function (db, "BuildMbr", 4, SQLITE_ANY, 0, fnct_BuildMbr1,
			     0, 0);
    sqlite3_create_function (db, "BuildMbr", 5, SQLITE_ANY, 0, fnct_BuildMbr2,
			     0, 0);
    sqlite3_create_function (db, "BuildCircleMbr", 3, SQLITE_ANY, 0,
			     fnct_BuildCircleMbr1, 0, 0);
    sqlite3_create_function (db, "BuildCircleMbr", 4, SQLITE_ANY, 0,
			     fnct_BuildCircleMbr2, 0, 0);
    sqlite3_create_function (db, "MbrMinX", 1, SQLITE_ANY, 0, fnct_MbrMinX, 0,
			     0);
    sqlite3_create_function (db, "MbrMaxX", 1, SQLITE_ANY, 0, fnct_MbrMaxX, 0,
			     0);
    sqlite3_create_function (db, "MbrMinY", 1, SQLITE_ANY, 0, fnct_MbrMinY, 0,
			     0);
    sqlite3_create_function (db, "MbrMaxY", 1, SQLITE_ANY, 0, fnct_MbrMaxY, 0,
			     0);
    sqlite3_create_function (db, "MakePoint", 2, SQLITE_ANY, 0, fnct_MakePoint1,
			     0, 0);
    sqlite3_create_function (db, "MakePoint", 3, SQLITE_ANY, 0, fnct_MakePoint2,
			     0, 0);
    sqlite3_create_function (db, "BuildMbrFilter", 4, SQLITE_ANY, 0,
			     fnct_BuildMbrFilter, 0, 0);
    sqlite3_create_function (db, "FilterMbrWithin", 4, SQLITE_ANY, 0,
			     fnct_FilterMbrWithin, 0, 0);
    sqlite3_create_function (db, "FilterMbrContains", 4, SQLITE_ANY, 0,
			     fnct_FilterMbrContains, 0, 0);
    sqlite3_create_function (db, "FilterMbrIntersects", 4, SQLITE_ANY, 0,
			     fnct_FilterMbrIntersects, 0, 0);
    sqlite3_create_function (db, "BuildRings", 1, SQLITE_ANY, 0,
			     fnct_BuildRings, 0, 0);

/* some BLOB/JPEG/EXIF functions */
    sqlite3_create_function (db, "IsGeometryBlob", 1, SQLITE_ANY, 0,
			     fnct_IsGeometryBlob, 0, 0);
    sqlite3_create_function (db, "IsZipBlob", 1, SQLITE_ANY, 0, fnct_IsZipBlob,
			     0, 0);
    sqlite3_create_function (db, "IsPdfBlob", 1, SQLITE_ANY, 0, fnct_IsPdfBlob,
			     0, 0);
    sqlite3_create_function (db, "IsWaveletBlob", 1, SQLITE_ANY, 0,
			     fnct_IsWaveletBlob, 0, 0);
    sqlite3_create_function (db, "IsTiffBlob", 1, SQLITE_ANY, 0,
			     fnct_IsTiffBlob, 0, 0);
    sqlite3_create_function (db, "IsGifBlob", 1, SQLITE_ANY, 0, fnct_IsGifBlob,
			     0, 0);
    sqlite3_create_function (db, "IsPngBlob", 1, SQLITE_ANY, 0, fnct_IsPngBlob,
			     0, 0);
    sqlite3_create_function (db, "IsJpegBlob", 1, SQLITE_ANY, 0,
			     fnct_IsJpegBlob, 0, 0);
    sqlite3_create_function (db, "IsExifBlob", 1, SQLITE_ANY, 0,
			     fnct_IsExifBlob, 0, 0);
    sqlite3_create_function (db, "IsExifGpsBlob", 1, SQLITE_ANY, 0,
			     fnct_IsExifGpsBlob, 0, 0);
    sqlite3_create_function (db, "GeomFromExifGpsBlob", 1, SQLITE_ANY, 0,
			     fnct_GeomFromExifGpsBlob, 0, 0);

/* some Geodesic functions */
    sqlite3_create_function (db, "GreatCircleLength", 1, SQLITE_ANY, 0,
			     fnct_GreatCircleLength, 0, 0);
    sqlite3_create_function (db, "GeodesicLength", 1, SQLITE_ANY, 0,
			     fnct_GeodesicLength, 0, 0);

/* some Length Unit conversion functions */
    sqlite3_create_function (db, "CvtToKm", 1, SQLITE_ANY, 0, fnct_cvtToKm, 0,
			     0);
    sqlite3_create_function (db, "CvtToDm", 1, SQLITE_ANY, 0, fnct_cvtToDm, 0,
			     0);
    sqlite3_create_function (db, "CvtToCm", 1, SQLITE_ANY, 0, fnct_cvtToCm, 0,
			     0);
    sqlite3_create_function (db, "CvtToMm", 1, SQLITE_ANY, 0, fnct_cvtToMm, 0,
			     0);
    sqlite3_create_function (db, "CvtToKmi", 1, SQLITE_ANY, 0, fnct_cvtToKmi, 0,
			     0);
    sqlite3_create_function (db, "CvtToIn", 1, SQLITE_ANY, 0, fnct_cvtToIn, 0,
			     0);
    sqlite3_create_function (db, "CvtToYd", 1, SQLITE_ANY, 0, fnct_cvtToYd, 0,
			     0);
    sqlite3_create_function (db, "CvtToMi", 1, SQLITE_ANY, 0, fnct_cvtToMi, 0,
			     0);
    sqlite3_create_function (db, "CvtToFath", 1, SQLITE_ANY, 0, fnct_cvtToFath,
			     0, 0);
    sqlite3_create_function (db, "CvtToCh", 1, SQLITE_ANY, 0, fnct_cvtToCh, 0,
			     0);
    sqlite3_create_function (db, "CvtToUsIn", 1, SQLITE_ANY, 0, fnct_cvtToUsIn,
			     0, 0);
    sqlite3_create_function (db, "CvtToUsFt", 1, SQLITE_ANY, 0, fnct_cvtToUsFt,
			     0, 0);
    sqlite3_create_function (db, "CvtToUsYd", 1, SQLITE_ANY, 0, fnct_cvtToUsYd,
			     0, 0);
    sqlite3_create_function (db, "CvtToUsMi", 1, SQLITE_ANY, 0, fnct_cvtToUsMi,
			     0, 0);
    sqlite3_create_function (db, "CvtToIndFt", 1, SQLITE_ANY, 0,
			     fnct_cvtToIndFt, 0, 0);
    sqlite3_create_function (db, "CvtToIndYd", 1, SQLITE_ANY, 0,
			     fnct_cvtToIndYd, 0, 0);
    sqlite3_create_function (db, "CvtToIndCh", 1, SQLITE_ANY, 0,
			     fnct_cvtToIndCh, 0, 0);
    sqlite3_create_function (db, "CvtFromKm", 1, SQLITE_ANY, 0, fnct_cvtFromKm,
			     0, 0);
    sqlite3_create_function (db, "CvtFromDm", 1, SQLITE_ANY, 0, fnct_cvtFromDm,
			     0, 0);
    sqlite3_create_function (db, "CvtFromCm", 1, SQLITE_ANY, 0, fnct_cvtFromCm,
			     0, 0);
    sqlite3_create_function (db, "CvtFromMm", 1, SQLITE_ANY, 0, fnct_cvtFromMm,
			     0, 0);
    sqlite3_create_function (db, "CvtFromKmi", 1, SQLITE_ANY, 0,
			     fnct_cvtFromKmi, 0, 0);
    sqlite3_create_function (db, "CvtFromIn", 1, SQLITE_ANY, 0, fnct_cvtFromIn,
			     0, 0);
    sqlite3_create_function (db, "CvtFromYd", 1, SQLITE_ANY, 0, fnct_cvtFromYd,
			     0, 0);
    sqlite3_create_function (db, "CvtFromMi", 1, SQLITE_ANY, 0, fnct_cvtFromMi,
			     0, 0);
    sqlite3_create_function (db, "CvtFromFath", 1, SQLITE_ANY, 0,
			     fnct_cvtFromFath, 0, 0);
    sqlite3_create_function (db, "CvtFromCh", 1, SQLITE_ANY, 0, fnct_cvtFromCh,
			     0, 0);
    sqlite3_create_function (db, "CvtFromUsIn", 1, SQLITE_ANY, 0,
			     fnct_cvtFromUsIn, 0, 0);
    sqlite3_create_function (db, "CvtFromUsFt", 1, SQLITE_ANY, 0,
			     fnct_cvtFromUsFt, 0, 0);
    sqlite3_create_function (db, "CvtFromUsYd", 1, SQLITE_ANY, 0,
			     fnct_cvtFromUsYd, 0, 0);
    sqlite3_create_function (db, "CvtFromUsMi", 1, SQLITE_ANY, 0,
			     fnct_cvtFromUsMi, 0, 0);
    sqlite3_create_function (db, "CvtFromIndFt", 1, SQLITE_ANY, 0,
			     fnct_cvtFromIndFt, 0, 0);
    sqlite3_create_function (db, "CvtFromIndYd", 1, SQLITE_ANY, 0,
			     fnct_cvtFromIndYd, 0, 0);
    sqlite3_create_function (db, "CvtFromIndCh", 1, SQLITE_ANY, 0,
			     fnct_cvtFromIndCh, 0, 0);

#ifndef OMIT_MATHSQL		/* supporting SQL math functions */

/* some extra math functions */
    sqlite3_create_function (db, "abs", 1, SQLITE_ANY, 0, fnct_math_abs, 0, 0);
    sqlite3_create_function (db, "acos", 1, SQLITE_ANY, 0, fnct_math_acos, 0,
			     0);
    sqlite3_create_function (db, "asin", 1, SQLITE_ANY, 0, fnct_math_asin, 0,
			     0);
    sqlite3_create_function (db, "atan", 1, SQLITE_ANY, 0, fnct_math_atan, 0,
			     0);
    sqlite3_create_function (db, "ceil", 1, SQLITE_ANY, 0, fnct_math_ceil, 0,
			     0);
    sqlite3_create_function (db, "ceiling", 1, SQLITE_ANY, 0, fnct_math_ceil, 0,
			     0);
    sqlite3_create_function (db, "cos", 1, SQLITE_ANY, 0, fnct_math_cos, 0, 0);
    sqlite3_create_function (db, "cot", 1, SQLITE_ANY, 0, fnct_math_cot, 0, 0);
    sqlite3_create_function (db, "degrees", 1, SQLITE_ANY, 0, fnct_math_degrees,
			     0, 0);
    sqlite3_create_function (db, "exp", 1, SQLITE_ANY, 0, fnct_math_exp, 0, 0);
    sqlite3_create_function (db, "floor", 1, SQLITE_ANY, 0, fnct_math_floor, 0,
			     0);
    sqlite3_create_function (db, "ln", 1, SQLITE_ANY, 0, fnct_math_logn, 0, 0);
    sqlite3_create_function (db, "log", 1, SQLITE_ANY, 0, fnct_math_logn, 0, 0);
    sqlite3_create_function (db, "log", 2, SQLITE_ANY, 0, fnct_math_logn2, 0,
			     0);
    sqlite3_create_function (db, "log2", 1, SQLITE_ANY, 0, fnct_math_log_2, 0,
			     0);
    sqlite3_create_function (db, "log10", 1, SQLITE_ANY, 0, fnct_math_log_10, 0,
			     0);
    sqlite3_create_function (db, "pi", 0, SQLITE_ANY, 0, fnct_math_pi, 0, 0);
    sqlite3_create_function (db, "pow", 2, SQLITE_ANY, 0, fnct_math_pow, 0, 0);
    sqlite3_create_function (db, "power", 2, SQLITE_ANY, 0, fnct_math_pow, 0,
			     0);
    sqlite3_create_function (db, "radians", 1, SQLITE_ANY, 0, fnct_math_radians,
			     0, 0);
    sqlite3_create_function (db, "round", 1, SQLITE_ANY, 0, fnct_math_round, 0,
			     0);
    sqlite3_create_function (db, "sign", 1, SQLITE_ANY, 0, fnct_math_sign, 0,
			     0);
    sqlite3_create_function (db, "sin", 1, SQLITE_ANY, 0, fnct_math_sin, 0, 0);
    sqlite3_create_function (db, "stddev_pop", 1, SQLITE_ANY, 0, 0,
			     fnct_math_stddev_step, fnct_math_stddev_pop_final);
    sqlite3_create_function (db, "stddev_samp", 1, SQLITE_ANY, 0, 0,
			     fnct_math_stddev_step,
			     fnct_math_stddev_samp_final);
    sqlite3_create_function (db, "sqrt", 1, SQLITE_ANY, 0, fnct_math_sqrt, 0,
			     0);
    sqlite3_create_function (db, "tan", 1, SQLITE_ANY, 0, fnct_math_tan, 0, 0);
    sqlite3_create_function (db, "var_pop", 1, SQLITE_ANY, 0, 0,
			     fnct_math_stddev_step, fnct_math_var_pop_final);
    sqlite3_create_function (db, "var_samp", 1, SQLITE_ANY, 0, 0,
			     fnct_math_stddev_step, fnct_math_var_samp_final);

#endif /* end supporting SQL math functions */

#ifndef OMIT_PROJ		/* including PROJ.4 */

    sqlite3_create_function (db, "Transform", 2, SQLITE_ANY, 0, fnct_Transform,
			     0, 0);

#endif /* end including PROJ.4 */

#ifndef OMIT_GEOS		/* including GEOS */

    initGEOS (geos_error, geos_error);
    sqlite3_create_function (db, "Equals", 2, SQLITE_ANY, 0, fnct_Equals, 0, 0);
    sqlite3_create_function (db, "Intersects", 2, SQLITE_ANY, 0,
			     fnct_Intersects, 0, 0);
    sqlite3_create_function (db, "Disjoint", 2, SQLITE_ANY, 0, fnct_Disjoint, 0,
			     0);
    sqlite3_create_function (db, "Overlaps", 2, SQLITE_ANY, 0, fnct_Overlaps, 0,
			     0);
    sqlite3_create_function (db, "Crosses", 2, SQLITE_ANY, 0, fnct_Crosses, 0,
			     0);
    sqlite3_create_function (db, "Touches", 2, SQLITE_ANY, 0, fnct_Touches, 0,
			     0);
    sqlite3_create_function (db, "Within", 2, SQLITE_ANY, 0, fnct_Within, 0, 0);
    sqlite3_create_function (db, "Contains", 2, SQLITE_ANY, 0, fnct_Contains, 0,
			     0);
    sqlite3_create_function (db, "Relate", 3, SQLITE_ANY, 0, fnct_Relate, 0, 0);
    sqlite3_create_function (db, "Distance", 2, SQLITE_ANY, 0, fnct_Distance, 0,
			     0);
    sqlite3_create_function (db, "Intersection", 2, SQLITE_ANY, 0,
			     fnct_Intersection, 0, 0);
    sqlite3_create_function (db, "Difference", 2, SQLITE_ANY, 0,
			     fnct_Difference, 0, 0);
    sqlite3_create_function (db, "GUnion", 1, SQLITE_ANY, 0, 0, fnct_Union_step,
			     fnct_Union_final);
    sqlite3_create_function (db, "GUnion", 2, SQLITE_ANY, 0, fnct_Union, 0, 0);
    sqlite3_create_function (db, "SymDifference", 2, SQLITE_ANY, 0,
			     fnct_SymDifference, 0, 0);
    sqlite3_create_function (db, "Boundary", 1, SQLITE_ANY, 0, fnct_Boundary, 0,
			     0);
    sqlite3_create_function (db, "GLength", 1, SQLITE_ANY, 0, fnct_Length, 0,
			     0);
    sqlite3_create_function (db, "Area", 1, SQLITE_ANY, 0, fnct_Area, 0, 0);
    sqlite3_create_function (db, "Centroid", 1, SQLITE_ANY, 0, fnct_Centroid, 0,
			     0);
    sqlite3_create_function (db, "PointOnSurface", 1, SQLITE_ANY, 0,
			     fnct_PointOnSurface, 0, 0);
    sqlite3_create_function (db, "Simplify", 2, SQLITE_ANY, 0, fnct_Simplify, 0,
			     0);
    sqlite3_create_function (db, "SimplifyPreserveTopology", 2, SQLITE_ANY, 0,
			     fnct_SimplifyPreserveTopology, 0, 0);
    sqlite3_create_function (db, "ConvexHull", 1, SQLITE_ANY, 0,
			     fnct_ConvexHull, 0, 0);
    sqlite3_create_function (db, "Buffer", 2, SQLITE_ANY, 0, fnct_Buffer, 0, 0);
    sqlite3_create_function (db, "IsClosed", 1, SQLITE_ANY, 0, fnct_IsClosed, 0,
			     0);
    sqlite3_create_function (db, "IsSimple", 1, SQLITE_ANY, 0, fnct_IsSimple, 0,
			     0);
    sqlite3_create_function (db, "IsRing", 1, SQLITE_ANY, 0, fnct_IsRing, 0, 0);
    sqlite3_create_function (db, "IsValid", 1, SQLITE_ANY, 0, fnct_IsValid, 0,
			     0);
    sqlite3_create_function (db, "BdPolyFromText", 1, SQLITE_ANY, 0,
			     fnct_BdPolyFromText1, 0, 0);
    sqlite3_create_function (db, "BdPolyFromText", 2, SQLITE_ANY, 0,
			     fnct_BdPolyFromText2, 0, 0);
    sqlite3_create_function (db, "BdMPolyFromText", 1, SQLITE_ANY, 0,
			     fnct_BdMPolyFromText1, 0, 0);
    sqlite3_create_function (db, "BdMPolyFromText", 2, SQLITE_ANY, 0,
			     fnct_BdMPolyFromText2, 0, 0);
    sqlite3_create_function (db, "BdPolyFromWKB", 1, SQLITE_ANY, 0,
			     fnct_BdPolyFromWKB1, 0, 0);
    sqlite3_create_function (db, "BdPolyFromWKB", 2, SQLITE_ANY, 0,
			     fnct_BdPolyFromWKB2, 0, 0);
    sqlite3_create_function (db, "BdMPolyFromWKB", 1, SQLITE_ANY, 0,
			     fnct_BdMPolyFromWKB1, 0, 0);
    sqlite3_create_function (db, "BdMPolyFromWKB", 2, SQLITE_ANY, 0,
			     fnct_BdMPolyFromWKB2, 0, 0);
    sqlite3_create_function (db, "Polygonize", 1, SQLITE_ANY, 0,
			     fnct_Polygonize1, 0, 0);
    sqlite3_create_function (db, "Polygonize", 2, SQLITE_ANY, 0,
			     fnct_Polygonize2, 0, 0);

#endif /* end including GEOS */

/* initializing the VirtualShape  extension */
    virtualshape_extension_init (db);
/* initializing the VirtualText  extension */
    virtualtext_extension_init (db);
/* initializing the VirtualNetwork  extension */
    virtualnetwork_extension_init (db);
/* initializing the MbrCache  extension */
    mbrcache_extension_init (db);
/* initializing the VirtualFDO  extension */
    virtualfdo_extension_init (db);
/* setting a timeout handler */
    sqlite3_busy_timeout (db, 5000);

    printf ("SpatiaLite version ..: %s", spatialite_version ());
    printf ("\tSupported Extensions:\n");
    printf ("\t- 'VirtualShape'\t[direct Shapefile access]\n");
    printf ("\t- 'VirtualText'\t\t[direct CSV/TXT access]\n");
    printf ("\t- 'VirtualNetwork\t[Dijkstra shortest path]\n");
    printf ("\t- 'RTree'\t\t[Spatial Index - R*Tree]\n");
    printf ("\t- 'MbrCache'\t\t[Spatial Index - MBR cache]\n");
    printf ("\t- 'VirtualFDO'\t\t[FDO-OGR interoperability]\n");
    printf ("\t- 'SpatiaLite'\t\t[Spatial SQL - OGC]\n");
#ifndef OMIT_PROJ		/* PROJ.4 version */
    printf ("PROJ.4 %s\n", pj_get_release ());
    fflush (stdout);
#endif /* end including PROJ.4 */
#ifndef OMIT_GEOS		/* GEOS version */
    printf ("GEOS version %s\n", GEOSversion ());
    fflush (stdout);
#endif /* end GEOS version */
    return 0;
}

SPATIALITE_DECLARE sqlite3_int64
math_llabs (sqlite3_int64 value)
{
/* replacing the C99 llabs() function */
    return value < 0 ? -value : value;
}

SPATIALITE_DECLARE double
math_round (double value)
{
/* replacing the C99 round() function */
    double min = floor (value);
    if (fabs (value - min) < 0.5)
	return min;
    return min + 1.0;
}
