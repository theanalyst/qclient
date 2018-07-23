#-------------------------------------------------------------------------------
# Build functional tests
#-------------------------------------------------------------------------------
include_directories(
  ${GTEST_INCLUDE_DIRS}
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/../../include)

add_executable(
  qclient-functional-tests
  hash.cc
  set.cc
  ping.cc
  test-config.cc
)

target_link_libraries(
  qclient-functional-tests
  qclient
  pthread
  gtest_main
  ${FOLLY_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT})
