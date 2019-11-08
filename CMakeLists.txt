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
  include_directories(${FOLLY_INCLUDE_DIRS})
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
  ${OPENSSL_INCLUDE_DIR}
)

add_library(qclient STATIC
  src/network/AsyncConnector.cc
  src/network/FileDescriptor.cc
  src/network/HostResolver.cc
  src/network/NetworkStream.cc

  src/pubsub/BaseSubscriber.cc
  src/pubsub/MessageParser.cc
  src/pubsub/Subscriber.cc

  src/reader/reader.cc
  src/reader/sds.cc

  src/shared/SharedDeque.cc
  src/shared/SharedHash.cc
  src/shared/SharedManager.cc
  src/shared/SharedSerialization.cc
  src/shared/TransientSharedHash.cc

  src/structures/QDeque.cc
  src/structures/QHash.cc
  src/structures/QLocalityHash.cc
  src/structures/QSet.cc

  src/AsyncHandler.cc
  src/BackgroundFlusher.cc
  src/CallbackExecutorThread.cc
  src/ConnectionCore.cc
  src/EncodedRequest.cc
  src/EndpointDecider.cc
  src/FaultInjector.cc
  src/Formatting.cc
  src/FutureHandler.cc
  src/GlobalInterceptor.cc
  src/Handshake.cc
  src/Options.cc
  src/QClient.cc
  src/QuarkDBVersion.cc
  src/ResponseBuilder.cc
  src/ResponseParsing.cc
  src/TlsFilter.cc
  src/WriterThread.cc
)

target_link_libraries(
  qclient
  fmt
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
