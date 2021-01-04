/*
 version.c -- Gaia spatial support for SQLite

 version 5.0, 2020 August 1
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
 
Portions created by the Initial Developer are Copyright (C) 2008-2020
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

#if defined(_WIN32) && !defined(__MINGW32__)
#include "config-msvc.h"
#else
#include "config.h"
#endif

#include <spatialite/sqlite.h>

#include <spatialite.h>

const char spatialiteversion[] = SPATIALITE_VERSION;

#if defined(_WIN32) && !defined(__MINGW32__)
#ifdef _WIN64
const char spatialitetargetcpu[] = "Windows_64bit";
#else
const char spatialitetargetcpu[] = "Windows_32bit";
#endif
#else
const char spatialitetargetcpu[] = SPATIALITE_TARGET_CPU;
#endif

SPATIALITE_DECLARE const char *
spatialite_version (void)
{
    return spatialiteversion;
}

SPATIALITE_DECLARE const char *
spatialite_target_cpu (void)
{
    return spatialitetargetcpu;
}
