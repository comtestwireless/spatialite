project(mod_spatialite)
cmake_minimum_required(VERSION 3.14)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_BINARY_DIR})
list(APPEND CMAKE_PREFIX_PATH ${CMAKE_BINARY_DIR})

# Download automatically, you can also just copy the conan.cmake file
if(NOT EXISTS "${CMAKE_BINARY_DIR}/conan.cmake")
  message(STATUS "Downloading conan.cmake from https://github.com/conan-io/cmake-conan")
  file(DOWNLOAD "https://raw.githubusercontent.com/conan-io/cmake-conan/v0.16.1/conan.cmake"
                "${CMAKE_BINARY_DIR}/conan.cmake"
                EXPECTED_HASH SHA256=396e16d0f5eabdc6a14afddbcfff62a54a7ee75c6da23f32f7a31bc85db23484
                TLS_VERIFY ON)
endif()

include(${CMAKE_BINARY_DIR}/conan.cmake)

conan_cmake_configure(REQUIRES
                          sqlite3/3.34.0
                          geos/3.9.0
                          boost/1.77.0
                          librttopo/1.1.0
                          zlib/1.2.11
                      GENERATORS cmake_find_package)

conan_cmake_autodetect(settings)

conan_cmake_install(PATH_OR_REFERENCE .
                    BUILD missing
                    SETTINGS ${settings})



SET(BUILD_LIBPROJ_SHARED OFF)

set(SRC_LIBPROJ_PROJECTIONS
  PROJ/src/projections/aeqd.cpp
  PROJ/src/projections/gnom.cpp
  PROJ/src/projections/laea.cpp
  PROJ/src/projections/mod_ster.cpp
  PROJ/src/projections/nsper.cpp
  PROJ/src/projections/nzmg.cpp
  PROJ/src/projections/ortho.cpp
  PROJ/src/projections/stere.cpp
  PROJ/src/projections/sterea.cpp
  PROJ/src/projections/aea.cpp
  PROJ/src/projections/bipc.cpp
  PROJ/src/projections/bonne.cpp
  PROJ/src/projections/eqdc.cpp
  PROJ/src/projections/isea.cpp
  PROJ/src/projections/ccon.cpp
  PROJ/src/projections/imw_p.cpp
  PROJ/src/projections/krovak.cpp
  PROJ/src/projections/lcc.cpp
  PROJ/src/projections/poly.cpp
  PROJ/src/projections/rpoly.cpp
  PROJ/src/projections/sconics.cpp
  PROJ/src/projections/rouss.cpp
  PROJ/src/projections/cass.cpp
  PROJ/src/projections/cc.cpp
  PROJ/src/projections/cea.cpp
  PROJ/src/projections/eqc.cpp
  PROJ/src/projections/gall.cpp
  PROJ/src/projections/labrd.cpp
  PROJ/src/projections/lsat.cpp
  PROJ/src/projections/misrsom.cpp
  PROJ/src/projections/merc.cpp
  PROJ/src/projections/mill.cpp
  PROJ/src/projections/ocea.cpp
  PROJ/src/projections/omerc.cpp
  PROJ/src/projections/somerc.cpp
  PROJ/src/projections/tcc.cpp
  PROJ/src/projections/tcea.cpp
  PROJ/src/projections/times.cpp
  PROJ/src/projections/tmerc.cpp
  PROJ/src/projections/tobmerc.cpp
  PROJ/src/projections/airy.cpp
  PROJ/src/projections/aitoff.cpp
  PROJ/src/projections/august.cpp
  PROJ/src/projections/bacon.cpp
  PROJ/src/projections/bertin1953.cpp
  PROJ/src/projections/chamb.cpp
  PROJ/src/projections/hammer.cpp
  PROJ/src/projections/lagrng.cpp
  PROJ/src/projections/larr.cpp
  PROJ/src/projections/lask.cpp
  PROJ/src/projections/latlong.cpp
  PROJ/src/projections/nicol.cpp
  PROJ/src/projections/ob_tran.cpp
  PROJ/src/projections/oea.cpp
  PROJ/src/projections/tpeqd.cpp
  PROJ/src/projections/vandg.cpp
  PROJ/src/projections/vandg2.cpp
  PROJ/src/projections/vandg4.cpp
  PROJ/src/projections/wag7.cpp
  PROJ/src/projections/lcca.cpp
  PROJ/src/projections/geos.cpp
  PROJ/src/projections/boggs.cpp
  PROJ/src/projections/collg.cpp
  PROJ/src/projections/comill.cpp
  PROJ/src/projections/crast.cpp
  PROJ/src/projections/denoy.cpp
  PROJ/src/projections/eck1.cpp
  PROJ/src/projections/eck2.cpp
  PROJ/src/projections/eck3.cpp
  PROJ/src/projections/eck4.cpp
  PROJ/src/projections/eck5.cpp
  PROJ/src/projections/fahey.cpp
  PROJ/src/projections/fouc_s.cpp
  PROJ/src/projections/gins8.cpp
  PROJ/src/projections/gstmerc.cpp
  PROJ/src/projections/gn_sinu.cpp
  PROJ/src/projections/goode.cpp
  PROJ/src/projections/igh.cpp
  PROJ/src/projections/hatano.cpp
  PROJ/src/projections/loxim.cpp
  PROJ/src/projections/mbt_fps.cpp
  PROJ/src/projections/mbtfpp.cpp
  PROJ/src/projections/mbtfpq.cpp
  PROJ/src/projections/moll.cpp
  PROJ/src/projections/nell.cpp
  PROJ/src/projections/nell_h.cpp
  PROJ/src/projections/patterson.cpp
  PROJ/src/projections/putp2.cpp
  PROJ/src/projections/putp3.cpp
  PROJ/src/projections/putp4p.cpp
  PROJ/src/projections/putp5.cpp
  PROJ/src/projections/putp6.cpp
  PROJ/src/projections/qsc.cpp
  PROJ/src/projections/robin.cpp
  PROJ/src/projections/sch.cpp
  PROJ/src/projections/sts.cpp
  PROJ/src/projections/urm5.cpp
  PROJ/src/projections/urmfps.cpp
  PROJ/src/projections/wag2.cpp
  PROJ/src/projections/wag3.cpp
  PROJ/src/projections/wink1.cpp
  PROJ/src/projections/wink2.cpp
  PROJ/src/projections/healpix.cpp
  PROJ/src/projections/natearth.cpp
  PROJ/src/projections/natearth2.cpp
  PROJ/src/projections/calcofi.cpp
  PROJ/src/projections/eqearth.cpp
)

set(SRC_LIBPROJ_CONVERSIONS
  PROJ/src/conversions/axisswap.cpp
  PROJ/src/conversions/cart.cpp
  PROJ/src/conversions/geoc.cpp
  PROJ/src/conversions/geocent.cpp
  PROJ/src/conversions/noop.cpp
  PROJ/src/conversions/unitconvert.cpp
)

set(SRC_LIBPROJ_TRANSFORMATIONS
  PROJ/src/transformations/affine.cpp
  PROJ/src/transformations/deformation.cpp
  PROJ/src/transformations/helmert.cpp
  PROJ/src/transformations/hgridshift.cpp
  PROJ/src/transformations/horner.cpp
  PROJ/src/transformations/molodensky.cpp
  PROJ/src/transformations/vgridshift.cpp
)

set(SRC_LIBPROJ_ISO19111
  PROJ/src/iso19111/static.cpp
  PROJ/src/iso19111/util.cpp
  PROJ/src/iso19111/metadata.cpp
  PROJ/src/iso19111/common.cpp
  PROJ/src/iso19111/crs.cpp
  PROJ/src/iso19111/datum.cpp
  PROJ/src/iso19111/coordinatesystem.cpp
  PROJ/src/iso19111/coordinateoperation.cpp
  PROJ/src/iso19111/io.cpp
  PROJ/src/iso19111/internal.cpp
  PROJ/src/iso19111/factory.cpp
  PROJ/src/iso19111/c_api.cpp
)

set(SRC_LIBPROJ_CORE
  PROJ/src/4D_api.cpp
  PROJ/src/aasincos.cpp
  PROJ/src/adjlon.cpp
  PROJ/src/apply_gridshift.cpp
  PROJ/src/apply_vgridshift.cpp
  PROJ/src/auth.cpp
  PROJ/src/ctx.cpp
  PROJ/src/datum_set.cpp
  PROJ/src/datums.cpp
  PROJ/src/deriv.cpp
  PROJ/src/dmstor.cpp
  PROJ/src/ell_set.cpp
  PROJ/src/ellps.cpp
  PROJ/src/errno.cpp
  PROJ/src/factors.cpp
  PROJ/src/fileapi.cpp
  PROJ/src/fwd.cpp
  PROJ/src/gauss.cpp
  PROJ/src/gc_reader.cpp
  PROJ/src/geocent.cpp
  PROJ/src/geocent.h
  PROJ/src/geodesic.c
  PROJ/src/gridcatalog.cpp
  PROJ/src/gridinfo.cpp
  PROJ/src/gridlist.cpp
  PROJ/src/init.cpp
  PROJ/src/initcache.cpp
  PROJ/src/internal.cpp
  PROJ/src/inv.cpp
  PROJ/src/jniproj.cpp
  PROJ/src/list.cpp
  PROJ/src/log.cpp
  PROJ/src/malloc.cpp
  PROJ/src/mlfn.cpp
  PROJ/src/msfn.cpp
  PROJ/src/mutex.cpp
  PROJ/src/nad_cvt.cpp
  PROJ/src/nad_init.cpp
  PROJ/src/nad_intr.cpp
  PROJ/src/open_lib.cpp
  PROJ/src/param.cpp
  PROJ/src/phi2.cpp
  PROJ/src/pipeline.cpp
  PROJ/src/pj_list.h
  PROJ/src/pr_list.cpp
  PROJ/src/proj_internal.h
  PROJ/src/proj_mdist.cpp
  PROJ/src/qsfn.cpp
  PROJ/src/release.cpp
  PROJ/src/rtodms.cpp
  PROJ/src/strerrno.cpp
  PROJ/src/strtod.cpp
  PROJ/src/transform.cpp
  PROJ/src/tsfn.cpp
  PROJ/src/units.cpp
  PROJ/src/utils.cpp
  PROJ/src/wkt1_generated_parser.c
  PROJ/src/wkt1_generated_parser.h
  PROJ/src/wkt1_parser.cpp
  PROJ/src/wkt1_parser.h
  PROJ/src/wkt2_generated_parser.c
  PROJ/src/wkt2_generated_parser.h
  PROJ/src/wkt2_parser.cpp
  PROJ/src/wkt2_parser.h
  PROJ/src/wkt_parser.cpp
  PROJ/src/wkt_parser.hpp
  PROJ/src/zpoly1.cpp
  PROJ/src/proj_json_streaming_writer.hpp
  PROJ/src/proj_json_streaming_writer.cpp
  PROJ/src/tracing.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/proj_config.h
)

set(HEADERS_LIBPROJ
  PROJ/src/proj_api.h
  PROJ/src/proj.h
  PROJ/src/proj_experimental.h
  PROJ/src/proj_constants.h
  PROJ/src/geodesic.h
)

find_package(SQLite3)

include(CheckIncludeFiles)
include(CheckLibraryExists)
include(CheckFunctionExists)

# check needed include file
check_include_files(dlfcn.h HAVE_DLFCN_H)
check_include_files(inttypes.h HAVE_INTTYPES_H)
check_include_files(jni.h HAVE_JNI_H)
check_include_files(memory.h HAVE_MEMORY_H)
check_include_files(stdint.h HAVE_STDINT_H)
check_include_files(stdlib.h HAVE_STDLIB_H)
check_include_files(string.h HAVE_STRING_H)
check_include_files(sys/stat.h HAVE_SYS_STAT_H)
check_include_files(sys/types.h HAVE_SYS_TYPES_H)
check_include_files(unistd.h HAVE_UNISTD_H)
check_include_files("stdlib.h;stdarg.h;string.h;float.h" STDC_HEADERS)

check_function_exists(localeconv HAVE_LOCALECONV)
check_function_exists(strerror HAVE_STRERROR)

check_library_exists(m ceil "" HAVE_LIBM)

configure_file(PROJ/cmake/proj_config.cmake.in ${CMAKE_CURRENT_BINARY_DIR}/proj_config.h)


add_library(proj STATIC)

set_target_properties(proj PROPERTIES
    CXX_VISIBILITY_PRESET hidden
    C_VISIBILITY_PRESET hidden
    POSITION_INDEPENDENT_CODE ON)

target_sources(proj
    PRIVATE
    ${SRC_LIBPROJ_PROJECTIONS}
    ${SRC_LIBPROJ_CONVERSIONS}
    ${SRC_LIBPROJ_TRANSFORMATIONS}
    ${SRC_LIBPROJ_ISO19111}
    ${SRC_LIBPROJ_CORE}
    ${HEADERS_LIBPROJ}
)

target_include_directories(proj
    PUBLIC
    PROJ/src
)


target_include_directories(proj
    PRIVATE
    PROJ/include
    ${CMAKE_CURRENT_BINARY_DIR}
    ${SQLite_SQLite3_INCLUDE_DIRS}
)


find_package(geos)
find_package(Boost)
find_package(librttopo)
find_package(ZLIB)

set(SOURCE_FILES
    src/versioninfo/location.cpp
    src/connection_cache/alloc_cache.c
    src/connection_cache/gg_sequence.c
    src/control_points/gaia_control_points.c
    src/control_points/grass_crs3d.c
    src/control_points/grass_georef_tps.c
    src/control_points/grass_georef.c
    src/cutter/gaia_cutter.c
    src/dxf/dxf_load_distinct.c
    src/dxf/dxf_load_mixed.c
    src/dxf/dxf_loader.c
    src/dxf/dxf_parser.c
    src/dxf/dxf_writer.c
    src/gaiaaux/gg_sqlaux.c
    src/gaiaaux/gg_utf8.c
    src/gaiaexif/gaia_exif.c
    src/gaiageo/gg_advanced.c
    src/gaiageo/gg_endian.c
    src/gaiageo/gg_ewkt.c
    src/gaiageo/gg_extras.c
    src/gaiageo/gg_geodesic.c
    src/gaiageo/gg_geoJSON.c
    src/gaiageo/gg_geometries.c
    src/gaiageo/gg_geoscvt.c
    src/gaiageo/gg_gml.c
    src/gaiageo/gg_kml.c
    src/gaiageo/gg_matrix.c
    src/gaiageo/gg_relations_ext.c
    src/gaiageo/gg_relations.c
    src/gaiageo/gg_rttopo.c
    src/gaiageo/gg_shape.c
    src/gaiageo/gg_transform.c
    src/gaiageo/gg_vanuatu.c
    src/gaiageo/gg_voronoj.c
    src/gaiageo/gg_wkb.c
    src/gaiageo/gg_wkt.c
    src/gaiageo/gg_xml.c
    src/geopackage/gaia_cvt_gpkg.c
    src/geopackage/gpkg_add_geometry_triggers.c
    src/geopackage/gpkg_add_spatial_index.c
    src/geopackage/gpkg_add_tile_triggers.c
    src/geopackage/gpkg_get_normal_row.c
    src/geopackage/gpkg_get_normal_zoom.c
    src/geopackage/gpkgAddGeometryColumn.c
    src/geopackage/gpkgBinary.c
    src/geopackage/gpkgCreateBaseTables.c
    src/geopackage/gpkgCreateTilesTable.c
    src/geopackage/gpkgCreateTilesZoomLevel.c
    src/geopackage/gpkgGetImageType.c
    src/geopackage/gpkgInsertEpsgSRID.c
    src/geopackage/gpkgMakePoint.c
    src/md5/gaia_md5.c
    src/md5/md5.c
    src/shapefiles/shapefiles.c
    src/shapefiles/validator.c
    src/spatialite/create_routing.c
    src/spatialite/dbobj_scopes.c
    src/spatialite/extra_tables.c
    src/spatialite/mbrcache.c
    src/spatialite/metatables.c
    src/spatialite/pause.c
    src/spatialite/se_helpers.c
    src/spatialite/spatialite_init.c
    src/spatialite/spatialite.c
    src/spatialite/srid_aux.c
    src/spatialite/statistics.c
    src/spatialite/table_cloner.c
    src/spatialite/virtualbbox.c
    src/spatialite/virtualdbf.c
    src/spatialite/virtualelementary.c
    src/spatialite/virtualfdo.c
    src/spatialite/virtualgeojson.c
    src/spatialite/virtualgpkg.c
    src/spatialite/virtualknn.c
    src/spatialite/virtualnetwork.c
    src/spatialite/virtualrouting.c
    src/spatialite/virtualshape.c
    src/spatialite/virtualspatialindex.c
    src/spatialite/virtualXL.c
    src/spatialite/virtualxpath.c
    src/srsinit/epsg_inlined_00.c
    src/srsinit/epsg_inlined_01.c
    src/srsinit/epsg_inlined_02.c
    src/srsinit/epsg_inlined_03.c
    src/srsinit/epsg_inlined_04.c
    src/srsinit/epsg_inlined_05.c
    src/srsinit/epsg_inlined_06.c
    src/srsinit/epsg_inlined_07.c
    src/srsinit/epsg_inlined_08.c
    src/srsinit/epsg_inlined_09.c
    src/srsinit/epsg_inlined_10.c
    src/srsinit/epsg_inlined_11.c
    src/srsinit/epsg_inlined_12.c
    src/srsinit/epsg_inlined_13.c
    src/srsinit/epsg_inlined_14.c
    src/srsinit/epsg_inlined_15.c
    src/srsinit/epsg_inlined_16.c
    src/srsinit/epsg_inlined_17.c
    src/srsinit/epsg_inlined_18.c
    src/srsinit/epsg_inlined_19.c
    src/srsinit/epsg_inlined_20.c
    src/srsinit/epsg_inlined_21.c
    src/srsinit/epsg_inlined_22.c
    src/srsinit/epsg_inlined_23.c
    src/srsinit/epsg_inlined_24.c
    src/srsinit/epsg_inlined_25.c
    src/srsinit/epsg_inlined_26.c
    src/srsinit/epsg_inlined_27.c
    src/srsinit/epsg_inlined_28.c
    src/srsinit/epsg_inlined_29.c
    src/srsinit/epsg_inlined_30.c
    src/srsinit/epsg_inlined_31.c
    src/srsinit/epsg_inlined_32.c
    src/srsinit/epsg_inlined_33.c
    src/srsinit/epsg_inlined_34.c
    src/srsinit/epsg_inlined_35.c
    src/srsinit/epsg_inlined_36.c
    src/srsinit/epsg_inlined_37.c
    src/srsinit/epsg_inlined_38.c
    src/srsinit/epsg_inlined_39.c
    src/srsinit/epsg_inlined_40.c
    src/srsinit/epsg_inlined_41.c
    src/srsinit/epsg_inlined_42.c
    src/srsinit/epsg_inlined_43.c
    src/srsinit/epsg_inlined_44.c
    src/srsinit/epsg_inlined_45.c
    src/srsinit/epsg_inlined_46.c
    src/srsinit/epsg_inlined_47.c
    src/srsinit/epsg_inlined_48.c
    src/srsinit/epsg_inlined_49.c
    src/srsinit/epsg_inlined_50.c
    src/srsinit/epsg_inlined_51.c
    src/srsinit/epsg_inlined_52.c
    src/srsinit/epsg_inlined_53.c
    src/srsinit/epsg_inlined_54.c
    src/srsinit/epsg_inlined_55.c
    src/srsinit/epsg_inlined_56.c
    src/srsinit/epsg_inlined_57.c
    src/srsinit/epsg_inlined_58.c
    src/srsinit/epsg_inlined_59.c
    src/srsinit/epsg_inlined_extra.c
    src/srsinit/epsg_inlined_prussian.c
    src/srsinit/epsg_inlined_wgs84_00.c
    src/srsinit/epsg_inlined_wgs84_01.c
    src/srsinit/srs_init.c
    src/stored_procedures/stored_procedures.c
    src/topology/gaia_auxnet.c
    src/topology/gaia_auxtopo_table.c
    src/topology/gaia_auxtopo.c
    src/topology/gaia_netstmts.c
    src/topology/gaia_network.c
    src/topology/gaia_topology.c
    src/topology/gaia_topostmts.c
    src/topology/lwn_network.c
    src/topology/net_callbacks.c
    src/topology/topo_callbacks.c
    src/versioninfo/version.c
    src/virtualtext/virtualtext.c
    src/wfs/wfs_in.c
) 

add_library(mod_spatialite SHARED ${SOURCE_FILES})

set_target_properties(mod_spatialite PROPERTIES
    CXX_VISIBILITY_PRESET hidden
    C_VISIBILITY_PRESET hidden
    POSITION_INDEPENDENT_CODE ON
)


target_compile_definitions(proj PUBLIC
    PROJ_DLL=
    )

target_compile_definitions(mod_spatialite PUBLIC
    LOADABLE_EXTENSION
    NO_FULL_RTREE
    OMIT_FREEXL=1
    OMIT_ICONV=1
    OMIT_KNN=1
    PROJ_DLL=
    YY_NO_UNISTD_H)


target_include_directories(mod_spatialite PRIVATE ./ ./src/headers ${SQLite_SQLite3_INCLUDE_DIRS})
target_link_libraries(mod_spatialite PRIVATE Boost::boost)
target_link_libraries(mod_spatialite PRIVATE GEOS::geos_c)
target_link_libraries(mod_spatialite PRIVATE librttopo::librttopo)
target_link_libraries(mod_spatialite PRIVATE Boost::filesystem)
target_link_libraries(mod_spatialite PRIVATE proj)
target_link_libraries(mod_spatialite PRIVATE ZLIB::ZLIB)
