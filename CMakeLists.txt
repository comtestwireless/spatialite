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
                          proj/7.2.1
                          boost/1.77.0
                      GENERATORS cmake_find_package)

conan_cmake_autodetect(settings)

conan_cmake_install(PATH_OR_REFERENCE .
                    BUILD missing
                    SETTINGS ${settings})


find_package(sqlite3)
find_package(geos)
find_package(proj)
find_package(boost)

include_directories(./ ./src/headers)

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

target_compile_definitions(mod_spatialite PUBLIC
    LOADABLE_EXTENSION
    OMIT_FREEXL=1
    OMIT_ICONV=1
    YY_NO_UNISTD_H)

target_link_libraries(mod_spatialite INTERFACE SQLite3::sqlite3)
target_link_libraries(mod_spatialite INTERFACE Boost::boost)
target_link_libraries(mod_spatialite PRIVATE GEOS::geos_c)
target_link_libraries(mod_spatialite PRIVATE PROJ::proj)
target_link_libraries(mod_spatialite PRIVATE Boost::filesystem)
 