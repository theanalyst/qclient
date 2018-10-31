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
# Prevent fmt from adding std=c++11
#-------------------------------------------------------------------------------
set(FMT_USE_CPP11 OFF CACHE BOOL "Prevent fmt from adding std=c++11")

#-------------------------------------------------------------------------------
# Compiler options
#-------------------------------------------------------------------------------
add_definitions(-Wall -Wextra -Werror -Wno-unused-parameter -fPIC -Wno-implicit-fallthrough)

#-------------------------------------------------------------------------------
# Is there folly support?
# If so, we expect to find relevant include directories and libraries in
# FOLLY_INCLUDE_DIRS and FOLLY_LIBRARIES.
#-------------------------------------------------------------------------------
if(FOLLY_FOUND)
  message("Building QClient with Folly support.")
  add_definitions(-DHAVE_FOLLY=1)
else()
  message("Building QClient without Folly support.")
endif()

#-------------------------------------------------------------------------------
# Build fmt library for string conversions
#-------------------------------------------------------------------------------
if (NOT TARGET fmt)
  include_directories(src/fmt)
  add_subdirectory(src/fmt)
endif()

if(TARGET gtest_main)
  add_subdirectory(test)
endif()

#-------------------------------------------------------------------------------
# Build source
#-------------------------------------------------------------------------------
include_directories(
  ./include
  ./src/
  ${FOLLY_INCLUDE_DIRS}
  ${HIREDIS_INCLUDE_DIRS}
  ${OPENSSL_INCLUDE_DIR}
)

add_library(qclient STATIC
  src/AsyncHandler.cc
  src/BackgroundFlusher.cc
  src/BaseSubscriber.cc
  src/CallbackExecutorThread.cc
  src/ConnectionHandler.cc
  src/ConnectionInitiator.cc
  src/EncodedRequest.cc
  src/FaultInjector.cc
  src/Formatting.cc
  src/FutureHandler.cc
  src/GlobalInterceptor.cc
  src/Handshake.cc
  src/NetworkStream.cc
  src/Options.cc
  src/QClient.cc
  src/QHash.cc
  src/QSet.cc
  src/ResponseBuilder.cc
  src/TlsFilter.cc
  src/WriterThread.cc
)

target_link_libraries(
  qclient
  fmt
  ${HIREDIS_LIBRARIES}
  ${OPENSSL_LIBRARIES}
  ${FOLLY_LIBRARIES}
)

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
