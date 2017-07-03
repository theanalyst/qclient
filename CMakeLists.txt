cmake_minimum_required(VERSION 2.8.11)

#-------------------------------------------------------------------------------
# Initialize
#-------------------------------------------------------------------------------

project(qclient)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
include(GNUInstallDirs)
option(PACKAGEONLY "Build without dependencies" OFF)

#-------------------------------------------------------------------------------
# Search for dependencies
#-------------------------------------------------------------------------------
if (NOT PACKAGEONLY)
  find_package(hiredis REQUIRED)
  find_package(GTest)
  find_package(OpenSSL REQUIRED)
else ()
  message(STATUS "Runing CMake in package only mode.")
endif()

#-------------------------------------------------------------------------------
# Compiler options
#-------------------------------------------------------------------------------
add_definitions(-Wall -Wextra -Werror -Wno-unused-parameter -std=c++11 -g3 -fPIC)

#-------------------------------------------------------------------------------
# Build fmt library for string conversions
#-------------------------------------------------------------------------------
if (NOT TARGET fmt)
  include_directories(src/fmt)
  add_subdirectory(src/fmt)
endif()

if (GTEST_FOUND)
  add_subdirectory(test)
endif()

#-------------------------------------------------------------------------------
# Build source
#-------------------------------------------------------------------------------
include_directories(
  ./include
  ./src/
  ${HIREDIS_INCLUDE_DIRS}
  ${OPENSSL_INCLUDE_DIR})

add_library(qclient STATIC
  src/AsyncHandler.cc
  src/ConnectionInitiator.cc
  src/NetworkStream.cc
  src/QClient.cc
  src/QHash.cc
  src/QSet.cc
  src/TlsFilter.cc
)

target_link_libraries(
  qclient
  fmt
  ${HIREDIS_LIBRARIES}
  ${OPENSSL_LIBRARIES})

set_target_properties(
  qclient PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# Install
#-------------------------------------------------------------------------------
install(
  DIRECTORY
  include/qclient
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/qclient)

install(
  TARGETS qclient
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
