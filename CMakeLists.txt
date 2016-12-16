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

#-------------------------------------------------------------------------------
# Compiler options
#-------------------------------------------------------------------------------

add_definitions(-Wall -Wextra -Werror -Wno-unused-parameter -std=c++11 -g3 -fPIC)

#-------------------------------------------------------------------------------
# Build source
#-------------------------------------------------------------------------------

include_directories(
  ${HIREDIS_INCLUDE_DIRS}
  include/
)

add_library(qclient STATIC
  src/QClient.cc
)

target_link_libraries(
  qclient
  ${HIREDIS_LIBRARIES}
)

#-------------------------------------------------------------------------------
# Install
#-------------------------------------------------------------------------------

install(
  TARGETS qclient
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
)
