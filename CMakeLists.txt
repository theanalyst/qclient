cmake_minimum_required(VERSION 3.14)

#-------------------------------------------------------------------------------
# Initialize
#-------------------------------------------------------------------------------

project(qclient)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

if (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set(CMAKE_INSTALL_PREFIX "/usr" CACHE PATH "Default install prefix: /usr" FORCE)
endif ()

include(GNUInstallDirs)
option(PACKAGEONLY "Build without dependencies" OFF)

#------------------------------------------------------------------------------
# Determine if qclient is built as a subproject (using add_subdirectory)
# or if it is the master project.
#-------------------------------------------------------------------------------
set(MASTER_PROJECT OFF)
if (CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
  set(MASTER_PROJECT ON)
endif()

#-------------------------------------------------------------------------------
# Search for dependencies
#-------------------------------------------------------------------------------
if (NOT PACKAGEONLY)
  if (MASTER_PROJECT)
    find_package(GTest)
  endif()

  find_package(OpenSSL REQUIRED)
  find_package(uuid REQUIRED)
  find_package(fmt REQUIRED)
else ()
  message(STATUS "Runing CMake in package only mode.")
endif()

#-------------------------------------------------------------------------------
# Compiler options
#-------------------------------------------------------------------------------
add_definitions(-Wall -Wextra -Wno-unused-parameter -fPIC -Wno-implicit-fallthrough)


if(TARGET gtest_main)
  add_subdirectory(test)
endif()

#-------------------------------------------------------------------------------
# Build source
#-------------------------------------------------------------------------------
include_directories(
  ./include
  ./src/
  ${OPENSSL_INCLUDE_DIR}
)

set(QCLIENT_SRCS
  src/network/AsyncConnector.cc
  src/network/FileDescriptor.cc
  src/network/HostResolver.cc
  src/network/NetworkStream.cc

  src/pubsub/BaseSubscriber.cc
  src/pubsub/MessageParser.cc
  src/pubsub/Subscriber.cc

  src/reader/reader.cc
  src/reader/sds.cc

  src/shared/BinarySerializer.cc
  src/shared/Communicator.cc
  src/shared/CommunicatorListener.cc
  src/shared/PendingRequestVault.cc
  src/shared/PersistentSharedHash.cc
  src/shared/SharedDeque.cc
  src/shared/SharedHash.cc
  src/shared/SharedHashSubscription.cc
  src/shared/SharedManager.cc
  src/shared/SharedSerialization.cc
  src/shared/TransientSharedHash.cc
  src/shared/UpdateBatch.cc

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
  src/FlusherQueueHandler.cc
  src/FutureHandler.cc
  src/GlobalInterceptor.cc
  src/Handshake.cc
  src/Options.cc
  src/QClient.cc
  src/QuarkDBVersion.cc
  src/ResponseBuilder.cc
  src/ResponseParsing.cc
  src/TlsFilter.cc
  src/WriterThread.cc)

add_library(Qclient-Objects OBJECT ${QCLIENT_SRCS})
add_library(Qclient-no-folly-Objects OBJECT ${QCLIENT_SRCS})

target_link_libraries(Qclient-Objects PUBLIC fmt)
target_link_libraries(Qclient-no-folly-Objects PUBLIC fmt)

#-------------------------------------------------------------------------------
# Is there folly support?
# If so, we expect to find relevant include directories and libraries in
# FOLLY_INCLUDE_DIRS and FOLLY_LIBRARIES.
#-------------------------------------------------------------------------------
if (FOLLY_FOUND)
  message(STATUS "Building QClient with and without Folly support.")
  target_compile_definitions(Qclient-Objects PUBLIC HAVE_FOLLY=1)
  target_include_directories(Qclient-Objects SYSTEM PUBLIC ${FOLLY_INCLUDE_DIRS})
  target_link_libraries(Qclient-Objects PUBLIC ${FOLLY_LIBRARIES})
else()
  message(STATUS "Building QClient without Folly support.")
endif()

if (ATOMIC_FOUND)
  target_link_libraries(Qclient-Objects PUBLIC ATOMIC::ATOMIC)
  target_link_libraries(Qclient-no-folly-Objects PUBLIC ATOMIC::ATOMIC)
endif()

add_library(qclient STATIC $<TARGET_OBJECTS:Qclient-Objects>)
add_library(qclient-no-folly STATIC $<TARGET_OBJECTS:Qclient-no-folly-Objects>)

target_link_libraries(qclient PUBLIC
  Qclient-Objects
  ${UUID_LIBRARIES}
  ${OPENSSL_LIBRARIES})

target_link_libraries(qclient-no-folly PUBLIC
  Qclient-no-folly-Objects
  ${UUID_LIBRARIES}
  ${OPENSSL_LIBRARIES})

set_property(TARGET qclient APPEND PROPERTY
  INTERFACE_INCLUDE_DIRECTORIES
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include/qclient>)

set_property(TARGET qclient-no-folly APPEND PROPERTY
  INTERFACE_INCLUDE_DIRECTORIES
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include/qclient>)

set_target_properties(qclient PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
set_target_properties(qclient-no-folly PROPERTIES POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# Install
#-------------------------------------------------------------------------------
install(DIRECTORY include/qclient
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/qclient)

install(TARGETS qclient qclient-no-folly
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
