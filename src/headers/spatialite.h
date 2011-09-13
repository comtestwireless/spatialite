/* 
 spatialite.h -- Gaia spatial support for SQLite 
  
 version 3.0, 2011 July 20

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

#ifdef DLL_EXPORT
#define SPATIALITE_DECLARE __declspec(dllexport)
#else
#define SPATIALITE_DECLARE extern
#endif

#ifndef _SPATIALITE_H
#define _SPATIALITE_H

#ifdef __cplusplus
extern "C"
{
#endif

    SPATIALITE_DECLARE const char *spatialite_version (void);
    SPATIALITE_DECLARE void spatialite_init (int verbose);
    
    /**
     Cleanup spatialite 
     
     This function performs general cleanup, essentially undoing the effect
     of spatialite_init().
     
     \sa spatialite_init
    */
    SPATIALITE_DECLARE void spatialite_cleanup();
    
    
    SPATIALITE_DECLARE int dump_shapefile (sqlite3 * sqlite, char *table,
					   char *column, char *shp_path,
					   char *charset, char *geom_type,
					   int verbose, int *rows,
					   char *err_msg);
    SPATIALITE_DECLARE int load_shapefile (sqlite3 * sqlite, char *shp_path,
					   char *table, char *charset, int srid,
					   char *column, int coerce2d,
					   int compressed, int verbose,
					   int spatial_index, int *rows,
					   char *err_msg);
    SPATIALITE_DECLARE int load_dbf (sqlite3 * sqlite, char *shp_path,
				     char *table, char *charset, int verbose,
				     int *rows, char *err_msg);
    SPATIALITE_DECLARE int dump_dbf (sqlite3 * sqlite, char *table,
				     char *dbf_path, char *charset,
				     char *err_msg);
    SPATIALITE_DECLARE int load_XL (sqlite3 * sqlite, const char *path,
				    const char *table,
				    unsigned int worksheetIndex,
				    int first_titles, unsigned int *rows,
				    char *err_msg);
    SPATIALITE_DECLARE double math_round (double value);
    SPATIALITE_DECLARE sqlite3_int64 math_llabs (sqlite3_int64 value);
    SPATIALITE_DECLARE int spatial_ref_sys_init (sqlite3 * sqlite, int verbose);
    SPATIALITE_DECLARE int
	is_kml_constant (sqlite3 * sqlite, char *table, char *column);
    SPATIALITE_DECLARE int dump_kml (sqlite3 * sqlite, char *table,
				     char *geom_col, char *kml_path,
				     char *name_col, char *desc_col,
				     int precision);
    SPATIALITE_DECLARE void check_duplicated_rows (sqlite3 * sqlite,
						  char *table, int *dupl_count);
    SPATIALITE_DECLARE void remove_duplicated_rows (sqlite3 * sqlite,
						    char *table);
    SPATIALITE_DECLARE void elementary_geometries (sqlite3 * sqlite,
						   char *inTable,
						   char *geometry,
						   char *outTable, char *pKey,
						   char *multiId);

#ifdef __cplusplus
}
#endif

#endif				/* _SPATIALITE_H */
