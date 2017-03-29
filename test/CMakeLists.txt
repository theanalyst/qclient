#-------------------------------------------------------------------------------
# Build unit tests
#-------------------------------------------------------------------------------
include_directories(
  ${GTEST_INCLUDE_DIRS}
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include)

add_executable(
  tests_qclient
  hash.cc
  set.cc
  ping.cc)

target_link_libraries(
  tests_qclient
  qclient
  ${GTEST_BOTH_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT})
