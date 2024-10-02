add_subdirectory(functional)

add_definitions(-DQCLIENT_IS_UNDER_TEST=1)

#-------------------------------------------------------------------------------
# Build unit tests
#-------------------------------------------------------------------------------
include_directories(
  ${GTEST_INCLUDE_DIRS}
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include)

add_executable(qclient-tests
  binary-serializer.cc
  communicator.cc
  formatting.cc
  general.cc
  network-stream.cc
  parsing.cc
  pubsub.cc
  queueing.cc
  response-builder.cc
  shared.cc)

set_target_properties(qclient-tests
  PROPERTIES POSITION_INDEPENDENT_CODE TRUE)

target_link_libraries(qclient-tests PRIVATE
  qclient
  gtest_main
  ${FOLLY_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT})

install(TARGETS qclient-tests
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})
