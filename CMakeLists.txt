cmake_minimum_required (VERSION 2.6)
project (datapump)
set (datapump_version_major 1)
set (datapump_version_minor 0)

file(REMOVE_RECURSE ${CMAKE_SOURCE_DIR}/CMakeFiles ${CMAKE_SOURCE_DIR}/CMakeCache.txt)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${PROJECT_SOURCE_DIR}/cmake/")
find_package(MySQL)
find_package(GLIB2)

set(CMAKE_C_FLAGS "-Wall -Wno-deprecated-declarations -Wunused -Wwrite-strings -Wno-strict-aliasing -Wextra -Wshadow -Werror -O3 -g  ${MYSQL_CFLAGS}")
include_directories(${MYSQL_INCLUDE_DIR} ${GLIB2_INCLUDE_DIR})

configure_file(
     "${PROJECT_SOURCE_DIR}/datapumpconfig.h.in"
     "${PROJECT_BINARY_DIR}/datapumpconfig.h"
)

find_path(GLIB_INCLUDE_DIR NAMES glib.h PATH_SUFFIXES glib-2.0)

include_directories("${PROJECT_BINARY_DIR}")
add_executable(datapump datapump.c mysqlconn.c util.c dir.c)
target_link_libraries(datapump ${MYSQL_LIBRARIES} ${GLIB2_LIBRARIES} ${GTHREAD2_LIBRARIES})

