cmake_minimum_required(VERSION 2.8.11)

#-------------------------------------------------------------------------------
# Initialize
#-------------------------------------------------------------------------------

project(qclient)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
include(GNUInstallDirs)

#-------------------------------------------------------------------------------
# Search for dependencies
#-------------------------------------------------------------------------------
find_package(hiredis REQUIRED)
find_package(GTest REQUIRED)

#-------------------------------------------------------------------------------
# Compiler options
#-------------------------------------------------------------------------------
add_definitions(-Wall -Wextra -Werror -Wno-unused-parameter -std=c++11 -g3 -fPIC)

#-------------------------------------------------------------------------------
# Build fmt library for string conversions
#-------------------------------------------------------------------------------
add_subdirectory(src/fmt)
add_subdirectory(test)

#-------------------------------------------------------------------------------
# Build source
#-------------------------------------------------------------------------------
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/src/
  ${HIREDIS_INCLUDE_DIRS})

add_library(qclient STATIC
  src/QClient.cc
  src/QHash.cc
)

target_link_libraries(
  qclient
  fmt
  ${HIREDIS_LIBRARIES})

set_target_properties(
  qclient PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# Install
#-------------------------------------------------------------------------------
install(
  TARGETS qclient
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
