add_subdirectory(functional)

#-------------------------------------------------------------------------------
# Build unit tests
#-------------------------------------------------------------------------------
include_directories(
  ${GTEST_INCLUDE_DIRS}
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/../src
  ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

add_executable(
  qclient-tests
  formatting.cc
  general.cc
  network-stream.cc
  pubsub.cc
  thread-safe-queue.cc
)

target_link_libraries(
  qclient-tests
  qclient
  gtest_main
  ${FOLLY_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT}
)
